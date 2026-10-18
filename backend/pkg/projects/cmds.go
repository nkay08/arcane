package projects

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"time"

	composetypes "github.com/compose-spec/compose-go/v2/types"
	"github.com/containerd/errdefs"
	"github.com/docker/compose/v5/pkg/api"
	"github.com/docker/compose/v5/pkg/compose"
	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/api/types/registry"
	"github.com/moby/moby/client"
	"go.getarcane.app/docker"
	"go.getarcane.app/docker/compat"
	kit "go.getarcane.app/kit/pkg"
	"golang.org/x/sync/errgroup"

	"github.com/getarcaneapp/arcane/backend/v2/pkg/libarcane/timeouts"
	"github.com/getarcaneapp/arcane/backend/v2/pkg/utils"
)

// defaultComposeTimeout is applied to compose operations that have been
// detached from the HTTP request context. It must be generous enough to
// cover large image pulls + health-check waits.
const defaultComposeTimeout = 30 * time.Minute

// composeStopTimeoutSeconds is the container stop timeout used by the
// podman-compose compat fallbacks.
const composeStopTimeoutSeconds = 10

// podman-compose stamps these labels on the containers it creates. Arcane
// mirrors the ones it can authoritatively know so Podman tooling and
// podman-compose see containers Arcane deploys.
const (
	podmanComposeProjectLabelInternal = "io.podman.compose.project"
	podmanComposeServiceLabelInternal = "io.podman.compose.service"
	podmanSystemdUnitLabelInternal    = "PODMAN_SYSTEMD_UNIT"
)

// detachFromHTTPContextInternal creates a new context derived from
// context.WithoutCancel(parent) that carries any values from the parent
// (such as types.ProgressWriterKey) but is **not** cancelled or
// deadline-bounded by the parent. This allows compose operations to survive
// HTTP request timeouts and proxy deadline cancellations. A standalone timeout
// is applied so the operation cannot run forever. See #1209.
func detachFromHTTPContextInternal(parent context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	if utils.IsAppLifecycleContext(parent) {
		return context.WithTimeout(parent, timeout)
	}
	ctx := context.WithoutCancel(parent)
	return context.WithTimeout(ctx, timeout)
}

// podmanComposeInvisibleInternal reports whether compose v5's default container
// filters can see the container. Those filters require the config-hash label,
// which podman-compose does not set — its containers are invisible to the
// compose service, so label-based lifecycle operations silently no-op on them.
// Compose v5's own lifecycle-hook runners also omit the config-hash label, so
// they are excluded by their hook label.
func podmanComposeInvisibleInternal(c container.Summary) bool {
	return podmanComposeInvisibleLabelsInternal(c.Labels)
}

// podmanComposeInvisibleLabelsInternal is the label-level form of
// podmanComposeInvisibleInternal, shared by networks and volumes whose summaries
// expose labels without a container type. A container without the config-hash
// label is invisible to compose v5; hook runners and one-off containers omit it
// too but are not podman-compose creations, so they are excluded (podman-compose
// never sets either the hook or the one-off label).
func podmanComposeInvisibleLabelsInternal(labels map[string]string) bool {
	if _, ok := labels[api.ConfigHashLabel]; ok {
		return false
	}
	if _, ok := labels[api.HookLabel]; ok {
		return false
	}
	_, hasOneOffLabel := labels[api.OneoffLabel]
	return !hasOneOffLabel
}

// serviceSelectedInternal matches a container's service label against the
// requested service names, case-insensitively.
func serviceSelectedInternal(services []string, label string) bool {
	for _, s := range services {
		if strings.EqualFold(s, label) {
			return true
		}
	}
	return false
}

// projectContainersInternal lists every container carrying the compose project
// label whose value matches projectName case-insensitively. Unlike compose's
// own filters, only the project label is required, so podman-compose containers
// (which lack the config-hash and one-off labels) remain visible.
func (c *Client) projectContainersInternal(ctx context.Context, projectName string) ([]container.Summary, error) {
	filter := make(client.Filters)
	filter = filter.Add("label", api.ProjectLabel)
	list, err := c.dockerCli.Client().ContainerList(ctx, client.ContainerListOptions{All: true, Filters: filter})
	if err != nil {
		return nil, err
	}
	var out []container.Summary
	for _, item := range list.Items {
		if projectLabelMatchesInternal(item.Labels, projectName) {
			out = append(out, item)
		}
	}
	return out, nil
}

func canonicalContainerNameInternal(c container.Summary) string {
	for _, name := range c.Names {
		if strings.HasPrefix(name, "/") {
			return strings.TrimPrefix(name, "/")
		}
	}
	return ""
}

func (c *Client) stopAndRemoveContainerInternal(ctx context.Context, ctr container.Summary) error {
	timeout := composeStopTimeoutSeconds
	if _, err := c.dockerCli.Client().ContainerStop(ctx, ctr.ID, client.ContainerStopOptions{Timeout: &timeout}); err != nil && !errdefs.IsNotFound(err) {
		return err
	}
	if _, err := c.dockerCli.Client().ContainerRemove(ctx, ctr.ID, client.ContainerRemoveOptions{Force: true}); err != nil && !errdefs.IsNotFound(err) {
		return err
	}
	return nil
}

// removeContainersWithDependentsInternal force-removes a set of containers that
// may depend on one another. The engine refuses to remove a container while a
// dependent still exists (a shared network namespace, a link, or a Podman pod
// member), so removal runs in passes: each pass clears whatever became
// removable, and the pass that makes no progress reports the remainder.
func (c *Client) removeContainersWithDependentsInternal(ctx context.Context, containers []container.Summary) error {
	return removeInDependencyOrderInternal(containers, func(ctr container.Summary) error {
		return c.stopAndRemoveContainerInternal(ctx, ctr)
	})
}

func removeInDependencyOrderInternal(containers []container.Summary, remove func(container.Summary) error) error {
	pending := containers
	var lastErr error
	for len(pending) > 0 {
		var blocked []container.Summary
		var errs []error
		removedAny := false
		for _, ctr := range pending {
			if err := remove(ctr); err != nil {
				blocked = append(blocked, ctr)
				errs = append(errs, fmt.Errorf("remove container %s: %w", ctr.ID[:12], err))
				continue
			}
			removedAny = true
		}
		lastErr = errors.Join(errs...)
		if !removedAny {
			break
		}
		pending = blocked
	}
	return lastErr
}

// removeInvisibleProjectContainersInternal force-removes project containers
// compose v5 cannot see (podman-compose creations). ComposeDown relies on it:
// v5's own down returns success while those containers keep running.
func (c *Client) removeInvisibleProjectContainersInternal(ctx context.Context, projectName string) error {
	containers, err := c.projectContainersInternal(ctx, projectName)
	if err != nil {
		return err
	}
	var invisible []container.Summary
	for _, ctr := range containers {
		if podmanComposeInvisibleInternal(ctr) {
			invisible = append(invisible, ctr)
		}
	}
	return c.removeContainersWithDependentsInternal(ctx, invisible)
}

// removeInvisibleProjectNetworksInternal removes project networks compose v5
// cannot see (podman-compose creations), which its Down leaves behind.
func (c *Client) removeInvisibleProjectNetworksInternal(ctx context.Context, projectName string) error {
	filter := make(client.Filters)
	filter = filter.Add("label", api.ProjectLabel)
	list, err := c.dockerCli.Client().NetworkList(ctx, client.NetworkListOptions{Filters: filter})
	if err != nil {
		return err
	}
	var errs []error
	for _, net := range list.Items {
		if !podmanComposeInvisibleLabelsInternal(net.Labels) || !projectLabelMatchesInternal(net.Labels, projectName) {
			continue
		}
		if _, err := c.dockerCli.Client().NetworkRemove(ctx, net.ID, client.NetworkRemoveOptions{}); err != nil && !errdefs.IsNotFound(err) {
			errs = append(errs, fmt.Errorf("remove invisible project network %s: %w", net.Name, err))
		}
	}
	return errors.Join(errs...)
}

// removeInvisibleProjectVolumesInternal removes project volumes compose v5
// cannot see (podman-compose creations) when the caller asked for volumes to go,
// mirroring Down's RemoveVolumes semantics.
func (c *Client) removeInvisibleProjectVolumesInternal(ctx context.Context, projectName string) error {
	filter := make(client.Filters)
	filter = filter.Add("label", api.ProjectLabel)
	list, err := c.dockerCli.Client().VolumeList(ctx, client.VolumeListOptions{Filters: filter})
	if err != nil {
		return err
	}
	var errs []error
	for _, vol := range list.Items {
		if !podmanComposeInvisibleLabelsInternal(vol.Labels) || !projectLabelMatchesInternal(vol.Labels, projectName) {
			continue
		}
		if _, err := c.dockerCli.Client().VolumeRemove(ctx, vol.Name, client.VolumeRemoveOptions{}); err != nil && !errdefs.IsNotFound(err) {
			errs = append(errs, fmt.Errorf("remove invisible project volume %s: %w", vol.Name, err))
		}
	}
	return errors.Join(errs...)
}

// projectLabelMatchesInternal compares a resource's compose project label to the
// requested project name case-insensitively, matching projectContainersInternal.
func projectLabelMatchesInternal(labels map[string]string, projectName string) bool {
	return strings.EqualFold(strings.TrimSpace(labels[api.ProjectLabel]), projectName)
}

// stopInvisibleProjectServicesInternal stops invisible containers belonging to
// the requested services; ComposeStop relies on it the same way ComposeDown
// relies on removeInvisibleProjectContainersInternal.
func (c *Client) stopInvisibleProjectServicesInternal(ctx context.Context, projectName string, services []string) error {
	containers, err := c.projectContainersInternal(ctx, projectName)
	if err != nil {
		return err
	}
	timeout := composeStopTimeoutSeconds
	var errs []error
	for _, ctr := range containers {
		if !podmanComposeInvisibleInternal(ctr) {
			continue
		}
		if !serviceSelectedInternal(services, ctr.Labels[api.ServiceLabel]) {
			continue
		}
		if _, err := c.dockerCli.Client().ContainerStop(ctx, ctr.ID, client.ContainerStopOptions{Timeout: &timeout}); err != nil && !errdefs.IsNotFound(err) {
			errs = append(errs, fmt.Errorf("stop invisible service container %s: %w", ctr.ID[:12], err))
		}
	}
	return errors.Join(errs...)
}

func (c *Client) restartInvisibleProjectServicesInternal(ctx context.Context, projectName string, services []string) error {
	containers, err := c.projectContainersInternal(ctx, projectName)
	if err != nil {
		return err
	}
	var errs []error
	for _, ctr := range containers {
		if !podmanComposeInvisibleInternal(ctr) {
			continue
		}
		if !serviceSelectedInternal(services, ctr.Labels[api.ServiceLabel]) {
			continue
		}
		if _, err := c.dockerCli.Client().ContainerRestart(ctx, ctr.ID, client.ContainerRestartOptions{}); err != nil && !errdefs.IsNotFound(err) {
			errs = append(errs, fmt.Errorf("restart invisible service container %s: %w", ctr.ID[:12], err))
		}
	}
	return errors.Join(errs...)
}

// removeInvisibleContainerConflictsInternal removes the containers compose v5
// cannot see (podman-compose creations) that the upcoming deploy would collide
// with: every invisible container belonging to a service being created, plus
// any whose name matches an explicit container_name. Without the service match
// `up` creates a second set of containers under Docker's naming (project-service-1)
// next to the podman-compose ones (project_service_1); without the name match it
// fails with "name is already in use".
func (c *Client) removeInvisibleContainerConflictsInternal(ctx context.Context, projectName string, proj *composetypes.Project) error {
	containers, err := c.projectContainersInternal(ctx, projectName)
	if err != nil {
		return err
	}
	selectedServices := make(map[string]struct{}, len(proj.Services))
	containerNames := make(map[string]struct{}, len(proj.Services))
	for _, svc := range proj.Services {
		selectedServices[strings.ToLower(strings.TrimSpace(svc.Name))] = struct{}{}
		if svc.ContainerName != "" {
			containerNames[svc.ContainerName] = struct{}{}
		}
	}

	var conflicts []container.Summary
	for _, ctr := range containers {
		if !podmanComposeInvisibleInternal(ctr) {
			continue
		}
		_, serviceMatch := selectedServices[strings.ToLower(strings.TrimSpace(ctr.Labels[api.ServiceLabel]))]
		_, nameMatch := containerNames[canonicalContainerNameInternal(ctr)]
		if serviceMatch || nameMatch {
			conflicts = append(conflicts, ctr)
		}
	}
	return c.removeContainersWithDependentsInternal(ctx, conflicts)
}

// invisibleProjectContainersInternal lists project containers compose v5 cannot
// see (podman-compose creations), optionally narrowed to the requested services.
func (c *Client) invisibleProjectContainersInternal(ctx context.Context, projectName string, services []string) ([]container.Summary, error) {
	containers, err := c.projectContainersInternal(ctx, projectName)
	if err != nil {
		return nil, err
	}
	var out []container.Summary
	for _, ctr := range containers {
		if !podmanComposeInvisibleInternal(ctr) {
			continue
		}
		if len(services) > 0 && !serviceSelectedInternal(services, ctr.Labels[api.ServiceLabel]) {
			continue
		}
		out = append(out, ctr)
	}
	return out, nil
}

// composeContainerSummaryInternal renders a Docker container summary in the
// shape compose's Ps returns so the invisible containers can be merged into it.
func composeContainerSummaryInternal(c container.Summary) api.ContainerSummary {
	publishers := make(api.PortPublishers, 0, len(c.Ports))
	for _, p := range c.Ports {
		publishers = append(publishers, api.PortPublisher{
			TargetPort:    int(p.PrivatePort),
			PublishedPort: int(p.PublicPort),
			Protocol:      p.Type,
		})
	}
	var health container.HealthStatus
	if c.Health != nil {
		health = c.Health.Status
	}
	return api.ContainerSummary{
		ID:         c.ID,
		Name:       canonicalContainerNameInternal(c),
		Names:      c.Names,
		Image:      c.Image,
		Project:    docker.ComposeProjectLabel(c.Labels),
		Service:    docker.ComposeServiceLabel(c.Labels),
		State:      c.State,
		Status:     c.Status,
		Health:     health,
		Publishers: publishers,
		Labels:     c.Labels,
	}
}

// detectEngineInternal resolves the connected engine, reporting the detection
// error so callers can decide whether to degrade gracefully.
func detectEngineInternal(ctx context.Context, apiClient client.APIClient) (compat.EngineCompatibilityInfo, error) {
	serverVersion, err := apiClient.ServerVersion(ctx, client.ServerVersionOptions{})
	if err != nil {
		return compat.EngineCompatibilityInfo{}, err
	}
	infoResult, err := apiClient.Info(ctx, client.InfoOptions{})
	if err != nil {
		return compat.EngineCompatibilityInfo{}, err
	}
	return compat.DetectEngineCompatibility(serverVersion, infoResult.Info), nil
}

// mirrorPodmanComposeLabelsInternal stamps the podman-compose identity labels
// Arcane can authoritatively know on the services about to be created, so Podman
// tooling and `podman-compose ps/down` recognise them. It deliberately does not
// fabricate io.podman.compose.config-hash, which podman-compose derives from its
// own normalized model.
func mirrorPodmanComposeLabelsInternal(projectName string, proj *composetypes.Project) {
	systemdUnit := "podman-compose@" + projectName + ".service"
	for name, svc := range proj.Services {
		svc.CustomLabels = svc.CustomLabels.
			Add(podmanComposeProjectLabelInternal, projectName).
			Add(podmanComposeServiceLabelInternal, name).
			Add(podmanSystemdUnitLabelInternal, systemdUnit)
		proj.Services[name] = svc
	}
}

// mirrorPodmanComposeLabelsForClientInternal mirrors labels only on the Podman
// engine, leaving Docker deployments untouched.
func mirrorPodmanComposeLabelsForClientInternal(ctx context.Context, apiClient client.APIClient, projectName string, proj *composetypes.Project) {
	engine, err := detectEngineInternal(ctx, apiClient)
	if err != nil {
		slog.WarnContext(ctx, "failed to detect container engine; skipping podman-compose label mirroring", "error", err)
		return
	}
	if strings.EqualFold(strings.TrimSpace(engine.Name), "podman") {
		mirrorPodmanComposeLabelsInternal(projectName, proj)
	}
}

func ComposeRestart(ctx context.Context, proj *composetypes.Project, services []string) error {
	restartCtx, cancel := detachFromHTTPContextInternal(ctx, defaultComposeTimeout)
	defer cancel()

	c, err := NewClient(restartCtx, "", nil, nil)
	if err != nil {
		return err
	}
	defer func() { _ = c.Close() }()

	if err := c.svc.Restart(restartCtx, proj.Name, api.RestartOptions{Project: proj, Services: services}); err != nil {
		return err
	}
	return c.restartInvisibleProjectServicesInternal(restartCtx, proj.Name, services)
}

func ComposeStop(ctx context.Context, proj *composetypes.Project, services []string) error {
	if len(services) == 0 {
		return nil
	}
	stopCtx, cancel := detachFromHTTPContextInternal(ctx, defaultComposeTimeout)
	defer cancel()

	c, err := NewClient(stopCtx, "", nil, nil)
	if err != nil {
		return err
	}
	defer func() { _ = c.Close() }()

	if err := c.svc.Stop(stopCtx, proj.Name, api.StopOptions{Services: services}); err != nil {
		return err
	}
	return c.stopInvisibleProjectServicesInternal(stopCtx, proj.Name, services)
}

func ComposeUp(ctx context.Context,
	proj *composetypes.Project,
	services []string,
	removeOrphans,
	forceRecreate,
	recreateVolumes bool,
	authConfigs map[string]registry.AuthConfig,
	waitTimeout time.Duration,
) error {
	selected, err := SelectServices(proj, services)
	if err != nil {
		return err
	}
	proj = selected
	if waitTimeout <= 0 {
		waitTimeout = timeouts.DefaultDeployWait
	}

	// Detach from the HTTP request context so that proxy timeouts and client
	// disconnects do not cancel a long-running compose up. See #1209. The
	// operation deadline must exceed the configured wait, otherwise a Deploy
	// Wait Timeout above defaultComposeTimeout would be cut short; the extra
	// defaultComposeTimeout leaves room for image pulls and container creation
	// that happen before the wait starts.
	composeCtx, cancel := detachFromHTTPContextInternal(ctx, waitTimeout+defaultComposeTimeout)
	defer cancel()

	// Compose prompts before recreating a volume whose config diverged (data
	// loss). Only answer yes when the caller explicitly opted in; otherwise the
	// default prompt declines and logs the question.
	var prompt compose.Prompt
	if recreateVolumes {
		prompt = compose.AlwaysOkPrompt()
	}

	// COMPOSE_* deployment variables ride on the loaded project's environment.
	// Selection errors were already surfaced at load, so parse best-effort here.
	envOpts, _ := ParseComposeEnvOptions(proj.WorkingDir, EnvMap(proj.Environment))

	var clientOptions []composeClientOptionInternal
	if envOpts.ParallelLimit > 0 {
		clientOptions = append(clientOptions, withComposeServiceOptionsInternal(compose.WithMaxConcurrency(envOpts.ParallelLimit)))
	}

	c, err := NewClient(composeCtx, "", authConfigs, prompt, clientOptions...)
	if err != nil {
		return err
	}
	defer func() { _ = c.Close() }()

	upOptions, startOptions := composeUpOptionsInternal(proj, services, removeOrphans, forceRecreate, waitTimeout, envOpts)

	// podman-compose containers are invisible to compose v5's label filters
	// (they lack the config-hash and one-off labels). Remove the ones this
	// deploy would collide with before creating anything, so `up` adopts them
	// instead of running a duplicate set under Docker's naming.
	if err := c.removeInvisibleContainerConflictsInternal(composeCtx, proj.Name, proj); err != nil {
		return err
	}
	mirrorPodmanComposeLabelsForClientInternal(composeCtx, c.dockerCli.Client(), proj.Name, proj)

	return c.svc.Up(composeCtx, proj, api.UpOptions{Create: upOptions, Start: startOptions})
}

// ComposeCreate force-recreates services without starting them, so a stopped
// container picks up config changes (a recreated namespace provider, for one)
// and stays stopped.
func ComposeCreate(ctx context.Context, proj *composetypes.Project, services []string, authConfigs map[string]registry.AuthConfig) error {
	selected, err := SelectServices(proj, services)
	if err != nil {
		return err
	}
	createCtx, cancel := detachFromHTTPContextInternal(ctx, defaultComposeTimeout)
	defer cancel()

	c, err := NewClient(createCtx, "", authConfigs, nil)
	if err != nil {
		return err
	}
	defer func() { _ = c.Close() }()

	if err := c.removeInvisibleContainerConflictsInternal(createCtx, selected.Name, selected); err != nil {
		return err
	}
	mirrorPodmanComposeLabelsForClientInternal(createCtx, c.dockerCli.Client(), selected.Name, selected)

	return c.svc.Create(createCtx, selected, api.CreateOptions{Services: services, Recreate: api.RecreateForce, RecreateDependencies: api.RecreateDiverged})
}

func composeUpOptionsInternal(proj *composetypes.Project,
	services []string,
	removeOrphans,
	forceRecreate bool,
	waitTimeout time.Duration,
	envOpts ComposeEnvOptions) (api.CreateOptions,
	api.StartOptions,
) {
	recreatePolicy := kit.Ternary(forceRecreate, api.RecreateForce, api.RecreateDiverged)

	upOptions := api.CreateOptions{
		Services:             services,
		Recreate:             recreatePolicy,
		RecreateDependencies: api.RecreateDiverged,
		// A caller opt-in (or GitOps-forced true) still wins over COMPOSE_REMOVE_ORPHANS.
		RemoveOrphans: removeOrphans || envOpts.RemoveOrphans,
		IgnoreOrphans: envOpts.IgnoreOrphans,
	}

	startOptions := api.StartOptions{
		Project:     proj,
		Services:    services,
		Wait:        true,
		WaitTimeout: waitTimeout,
		// CascadeFail ensures that if a dependency fails its health check,
		// the error propagates correctly instead of being ignored
		OnExit: api.CascadeFail,
	}

	return upOptions, startOptions
}

// ComposePs lists a project's compose containers. dockerHost is the
// config-resolved docker host used to reuse the shared read-only compose
// client; empty uses the environment-resolved host.
func ComposePs(ctx context.Context, dockerHost string, proj *composetypes.Project, services []string, all bool) ([]api.ContainerSummary, error) {
	c, err := plainComposeClientInternal(dockerHost)
	if err != nil {
		return nil, err
	}

	summaries, err := c.svc.Ps(ctx, proj.Name, api.PsOptions{All: all, Services: services})
	if err != nil {
		return nil, err
	}

	// Compose v5's Ps filters out containers lacking the config-hash/one-off
	// labels, so podman-compose containers would be reported as absent. Merge
	// them back in so project status, dependencies, and details stay accurate.
	invisible, invisibleErr := c.invisibleProjectContainersInternal(ctx, proj.Name, services)
	if invisibleErr != nil {
		slog.WarnContext(ctx, "failed to list podman-compose containers for compose ps", "projectName", proj.Name, "error", invisibleErr)
		return summaries, nil
	}
	if len(invisible) == 0 {
		return summaries, nil
	}

	seen := make(map[string]struct{}, len(summaries))
	for _, summary := range summaries {
		seen[summary.ID] = struct{}{}
	}
	for _, ctr := range invisible {
		if _, ok := seen[ctr.ID]; ok {
			continue
		}
		if !all && ctr.State != container.StateRunning {
			continue
		}
		summaries = append(summaries, composeContainerSummaryInternal(ctr))
	}
	return summaries, nil
}

func ComposeGenerate(ctx context.Context, dockerHost, projectName string, containerIDs []string) (string, error) {
	c, err := plainComposeClientInternal(dockerHost)
	if err != nil {
		return "", err
	}

	model, err := c.svc.Generate(ctx, api.GenerateOptions{ProjectName: projectName, Containers: containerIDs})
	if err != nil {
		return "", err
	}
	content, err := model.MarshalYAML()
	return string(content), err
}

func ComposeDown(ctx context.Context, proj *composetypes.Project, removeVolumes bool) error {
	downCtx, cancel := detachFromHTTPContextInternal(ctx, defaultComposeTimeout)
	defer cancel()

	c, err := NewClient(downCtx, "", nil, nil)
	if err != nil {
		return err
	}
	defer func() { _ = c.Close() }()

	if err := c.svc.Down(downCtx, proj.Name, api.DownOptions{RemoveOrphans: true, Volumes: removeVolumes}); err != nil {
		return err
	}
	// podman-compose containers are invisible to compose v5's label filters:
	// v5's down silently skips them, so remove the leftovers directly. The same
	// applies to the networks it created, and to its volumes on `down -v`.
	cleanupErrs := []error{
		c.removeInvisibleProjectContainersInternal(downCtx, proj.Name),
		c.removeInvisibleProjectNetworksInternal(downCtx, proj.Name),
	}
	if removeVolumes {
		cleanupErrs = append(cleanupErrs, c.removeInvisibleProjectVolumesInternal(downCtx, proj.Name))
	}
	return errors.Join(cleanupErrs...)
}

// ComposeLogs streams a project's logs through the compose service. The
// one-shot client for this call is built with the log-parity wrapper so
// non-TTY containers keep their stderr metadata; deploy, attach, and other
// compose operations use plain clients.
//
// Compose v5's log filter hides podman-compose containers (they lack the
// config-hash label), so their logs are streamed directly and merged into the
// same project stream.
func ComposeLogs(ctx context.Context, projectName string, out io.Writer, follow bool, tail, since string, timestamps bool) error {
	c, err := NewClient(ctx, "", nil, nil, streamDemuxedLogsInternal)
	if err != nil {
		return err
	}
	defer func() { _ = c.Close() }()

	consumer := &writerConsumer{out: out}
	logOptions := api.LogOptions{Follow: follow, Tail: tail, Since: since, Timestamps: timestamps}

	invisible, invisibleErr := c.invisibleProjectContainersInternal(ctx, projectName, nil)
	if invisibleErr != nil {
		slog.WarnContext(ctx, "failed to list podman-compose containers for compose logs", "projectName", projectName, "error", invisibleErr)
	}
	if len(invisible) == 0 {
		return c.svc.Logs(ctx, projectName, consumer, logOptions)
	}

	composeErrCh := make(chan error, 1)
	go func() {
		composeErrCh <- c.svc.Logs(ctx, projectName, consumer, logOptions)
	}()
	directErr := c.streamContainerLogsInternal(ctx, invisible, consumer, follow, tail, since, timestamps)
	return errors.Join(<-composeErrCh, directErr)
}

// streamContainerLogsInternal streams logs for the containers compose v5 cannot
// see, writing through the shared consumer so podman-compose and compose
// containers interleave as one project stream.
func (c *Client) streamContainerLogsInternal(ctx context.Context, containers []container.Summary, consumer *writerConsumer, follow bool, tail, since string, timestamps bool) error {
	apiClient := c.dockerCli.Client()
	group, groupCtx := errgroup.WithContext(ctx)
	for _, ctr := range containers {
		group.Go(func() error {
			return streamSingleContainerLogsInternal(groupCtx, apiClient, ctr, consumer, follow, tail, since, timestamps)
		})
	}
	return group.Wait()
}

func streamSingleContainerLogsInternal(ctx context.Context, apiClient client.APIClient, ctr container.Summary, consumer *writerConsumer, follow bool, tail, since string, timestamps bool) error {
	inspect, err := apiClient.ContainerInspect(ctx, ctr.ID, client.ContainerInspectOptions{})
	if err != nil {
		return err
	}
	isTTY := inspect.Container.Config != nil && inspect.Container.Config.Tty

	logs, err := apiClient.ContainerLogs(ctx, ctr.ID, client.ContainerLogsOptions{
		ShowStdout: true,
		ShowStderr: true,
		Follow:     follow,
		Tail:       tail,
		Since:      since,
		Timestamps: timestamps,
	})
	if err != nil {
		return err
	}
	defer func() { _ = logs.Close() }()

	name := canonicalContainerNameInternal(ctr)
	logsChan := make(chan string, 16)
	streamErrCh := make(chan error, 1)
	go func() {
		streamErrCh <- docker.StreamContainerLogs(ctx, logs, logsChan, follow, isTTY)
		close(logsChan)
	}()
	for line := range logsChan {
		consumer.Log(name, line)
	}
	return <-streamErrCh
}

// ListGlobalComposeContainers lists every container carrying a compose
// project label. This is a plain ContainerList — no compose service is
// needed — so dockerClient should be the process-wide Docker client
// singleton. When nil (callers wired without one, e.g. tests), a compose
// client for the config-resolved dockerHost is used instead.
func ListGlobalComposeContainers(ctx context.Context, dockerClient client.APIClient, dockerHost string) ([]container.Summary, error) {
	cli := dockerClient
	if cli == nil {
		c, err := plainComposeClientInternal(dockerHost)
		if err != nil {
			return nil, err
		}
		cli = c.dockerCli.Client()
	}

	filter := make(client.Filters)
	filter = filter.Add("label", "com.docker.compose.project")

	listResult, err := cli.ContainerList(ctx, client.ContainerListOptions{
		All:     true,
		Filters: filter,
	})
	if err != nil {
		return nil, err
	}

	return listResult.Items, nil
}

// FindComposeServiceContainerID prefers a running service container over a stopped predecessor.
func FindComposeServiceContainerID(ctx context.Context, dockerHost, projectName, serviceName string) (string, error) {
	containers, err := ComposePs(ctx, dockerHost, &composetypes.Project{Name: projectName}, []string{serviceName}, true)
	if err != nil {
		return "", err
	}
	var firstMatch string
	for _, c := range containers {
		if c.Service != serviceName {
			continue
		}
		firstMatch = cmp.Or(firstMatch, c.ID)
		if c.State == "running" {
			return c.ID, nil
		}
	}
	return firstMatch, nil
}

// FindComposeReplica matches a replacement by project, service, and its recorded replica number.
func FindComposeReplica(containers []container.Summary, labels map[string]string) (container.Summary, bool) {
	projectName := docker.ComposeProjectLabel(labels)
	serviceName := docker.ComposeServiceLabel(labels)
	if projectName == "" || serviceName == "" {
		return container.Summary{}, false
	}
	number := strings.TrimSpace(labels[api.ContainerNumberLabel])
	for _, candidate := range containers {
		if docker.ComposeProjectLabel(candidate.Labels) != projectName || docker.ComposeServiceLabel(candidate.Labels) != serviceName {
			continue
		}
		if number != "" && strings.TrimSpace(candidate.Labels[api.ContainerNumberLabel]) != number {
			continue
		}
		return candidate, true
	}
	return container.Summary{}, false
}

// FormatPorts renders compose port publishers as "published:target/proto"
// (or "target/proto" when the port is not published).
func FormatPorts(publishers []api.PortPublisher) []string {
	var ports []string
	for _, pub := range publishers {
		if pub.PublishedPort > 0 {
			ports = append(ports, fmt.Sprintf("%d:%d/%s", pub.PublishedPort, pub.TargetPort, pub.Protocol))
		} else {
			ports = append(ports, fmt.Sprintf("%d/%s", pub.TargetPort, pub.Protocol))
		}
	}
	return ports
}

// FormatDockerPorts renders container port summaries in the same shape as
// FormatPorts, for containers Arcane sees outside a compose project.
func FormatDockerPorts(ports []container.PortSummary) []string {
	var res []string
	for _, p := range ports {
		if p.PublicPort == 0 {
			res = append(res, fmt.Sprintf("%d/%s", p.PrivatePort, p.Type))
		} else {
			res = append(res, fmt.Sprintf("%d:%d/%s", p.PublicPort, p.PrivatePort, p.Type))
		}
	}
	return res
}
