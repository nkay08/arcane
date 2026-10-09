package projects

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"io"
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
	kit "go.getarcane.app/kit/pkg"

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
func podmanComposeInvisibleInternal(c container.Summary) bool {
	_, ok := c.Labels[api.ConfigHashLabel]
	return !ok
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
		if strings.EqualFold(item.Labels[api.ProjectLabel], projectName) {
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

// removeInvisibleProjectContainersInternal force-removes project containers
// compose v5 cannot see (podman-compose creations). ComposeDown relies on it:
// v5's own down returns success while those containers keep running.
func (c *Client) removeInvisibleProjectContainersInternal(ctx context.Context, projectName string) error {
	containers, err := c.projectContainersInternal(ctx, projectName)
	if err != nil {
		return err
	}
	var errs []error
	for _, ctr := range containers {
		if !podmanComposeInvisibleInternal(ctr) {
			continue
		}
		if err := c.stopAndRemoveContainerInternal(ctx, ctr); err != nil {
			errs = append(errs, fmt.Errorf("remove invisible project container %s: %w", ctr.ID[:12], err))
		}
	}
	return errors.Join(errs...)
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

// removeInvisibleNameConflictsInternal removes invisible containers whose name
// collides with a container_name the upcoming deploy is about to create.
// Compose v5 cannot see those containers, so without this step create fails
// with "name is already in use" instead of recreating them.
func (c *Client) removeInvisibleNameConflictsInternal(ctx context.Context, projectName string, proj *composetypes.Project) error {
	containers, err := c.projectContainersInternal(ctx, projectName)
	if err != nil {
		return err
	}
	byName := make(map[string]container.Summary, len(containers))
	for _, ctr := range containers {
		if !podmanComposeInvisibleInternal(ctr) {
			continue
		}
		if name := canonicalContainerNameInternal(ctr); name != "" {
			byName[name] = ctr
		}
	}
	var errs []error
	for _, svc := range proj.Services {
		if svc.ContainerName == "" {
			continue
		}
		if ctr, ok := byName[svc.ContainerName]; ok {
			if err := c.stopAndRemoveContainerInternal(ctx, ctr); err != nil {
				errs = append(errs, fmt.Errorf("remove conflicting container %s: %w", svc.ContainerName, err))
			}
		}
	}
	return errors.Join(errs...)
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
	// (they lack the config-hash label): remove any whose name collides with a
	// container_name about to be created, otherwise create fails with
	// "name is already in use".
	if err := c.removeInvisibleNameConflictsInternal(composeCtx, proj.Name, proj); err != nil {
		return err
	}

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

	return c.svc.Ps(ctx, proj.Name, api.PsOptions{All: all, Services: services})
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
	// v5's down silently skips them, so remove the leftovers directly.
	return c.removeInvisibleProjectContainersInternal(downCtx, proj.Name)
}

// ComposeLogs streams a project's logs through the compose service. The
// one-shot client for this call is built with the log-parity wrapper so
// non-TTY containers keep their stderr metadata; deploy, attach, and other
// compose operations use plain clients.
func ComposeLogs(ctx context.Context, projectName string, out io.Writer, follow bool, tail, since string, timestamps bool) error {
	c, err := NewClient(ctx, "", nil, nil, streamDemuxedLogsInternal)
	if err != nil {
		return err
	}
	defer func() { _ = c.Close() }()

	return c.svc.Logs(ctx, projectName, &writerConsumer{out: out}, api.LogOptions{Follow: follow, Tail: tail, Since: since, Timestamps: timestamps})
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
