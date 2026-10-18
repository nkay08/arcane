package projects

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	composetypes "github.com/compose-spec/compose-go/v2/types"
	"github.com/docker/compose/v5/pkg/api"
	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/client"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/getarcaneapp/arcane/backend/v2/pkg/utils"
)

func TestDetachFromHTTPContextInternal(t *testing.T) {
	t.Run("survives parent cancellation", func(t *testing.T) {
		parent, parentCancel := context.WithCancel(t.Context())
		detached, detachedCancel := detachFromHTTPContextInternal(parent, defaultComposeTimeout)
		defer detachedCancel()

		// Cancel the parent (simulates HTTP request ending).
		parentCancel()

		// The detached context must still be alive.
		require.NoError(t, detached.Err())

		deadline, ok := detached.Deadline()
		require.True(t, ok)
		require.False(t, deadline.IsZero())
	})

	t.Run("preserves context values", func(t *testing.T) {
		type testKey struct{}
		parent := context.WithValue(t.Context(), testKey{}, "hello")
		detached, cancel := detachFromHTTPContextInternal(parent, defaultComposeTimeout)
		defer cancel()

		require.Equal(t, "hello", detached.Value(testKey{}))
	})

	t.Run("has its own deadline", func(t *testing.T) {
		detached, cancel := detachFromHTTPContextInternal(t.Context(), defaultComposeTimeout)
		defer cancel()

		deadline, ok := detached.Deadline()
		require.True(t, ok)
		require.False(t, deadline.IsZero())
	})

	t.Run("survives parent deadline expiry", func(t *testing.T) {
		parent, parentCancel := context.WithTimeout(t.Context(), 1*time.Millisecond)
		defer parentCancel()

		time.Sleep(5 * time.Millisecond) // ensure parent deadline has passed

		detached, detachedCancel := detachFromHTTPContextInternal(parent, defaultComposeTimeout)
		defer detachedCancel()

		require.NoError(t, detached.Err())

		deadline, ok := detached.Deadline()
		require.True(t, ok)
		require.InDelta(t, float64(defaultComposeTimeout), float64(time.Until(deadline)), float64(5*time.Second))
	})

	t.Run("deadline scales with requested timeout", func(t *testing.T) {
		detached, cancel := detachFromHTTPContextInternal(t.Context(), 2*time.Hour)
		defer cancel()

		deadline, ok := detached.Deadline()
		require.True(t, ok)
		require.InDelta(t, float64(2*time.Hour), float64(time.Until(deadline)), float64(5*time.Second))
	})

	t.Run("app lifecycle context cancels detached work on shutdown", func(t *testing.T) {
		appCtx, cancelApp := context.WithCancel(utils.WithAppLifecycleContext(t.Context()))
		detached, detachedCancel := detachFromHTTPContextInternal(appCtx, defaultComposeTimeout)
		defer detachedCancel()

		cancelApp()

		require.ErrorIs(t, detached.Err(), context.Canceled)
	})
}

func TestComposeStopSkipsWhenNoServicesSpecified(t *testing.T) {
	t.Setenv("DOCKER_HOST", "tcp://127.0.0.1:9")

	err := ComposeStop(t.Context(), &composetypes.Project{Name: "test"}, nil)
	require.NoError(t, err)

	err = ComposeStop(t.Context(), &composetypes.Project{Name: "test"}, []string{})
	require.NoError(t, err)
}

func TestComposeUpOptions_RemoveOrphans(t *testing.T) {
	proj := &composetypes.Project{Name: "test"}

	t.Run("removeOrphans true propagates to CreateOptions", func(t *testing.T) {
		upOptions, _ := composeUpOptionsInternal(proj, nil, true, false, 0, ComposeEnvOptions{})
		require.True(t, upOptions.RemoveOrphans)
	})

	t.Run("removeOrphans false leaves CreateOptions disabled", func(t *testing.T) {
		upOptions, _ := composeUpOptionsInternal(proj, nil, false, false, 0, ComposeEnvOptions{})
		require.False(t, upOptions.RemoveOrphans)
	})

	t.Run("removeOrphans is independent of forceRecreate", func(t *testing.T) {
		// forceRecreate drives the Recreate policy, not RemoveOrphans.
		upOptions, _ := composeUpOptionsInternal(proj, nil, true, true, 0, ComposeEnvOptions{})
		require.True(t, upOptions.RemoveOrphans)
		require.Equal(t, api.RecreateForce, upOptions.Recreate)

		upOptions, _ = composeUpOptionsInternal(proj, nil, false, true, 0, ComposeEnvOptions{})
		require.False(t, upOptions.RemoveOrphans)
		require.Equal(t, api.RecreateForce, upOptions.Recreate)
	})

	t.Run("COMPOSE_REMOVE_ORPHANS / COMPOSE_IGNORE_ORPHANS propagate", func(t *testing.T) {
		upOptions, _ := composeUpOptionsInternal(proj, nil, false, false, 0, ComposeEnvOptions{RemoveOrphans: true, IgnoreOrphans: true})
		require.True(t, upOptions.RemoveOrphans)
		require.True(t, upOptions.IgnoreOrphans)
	})
}

func TestListGlobalComposeContainersUsesProvidedClient(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasSuffix(r.URL.Path, "/containers/json") {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `[{"Id":"abc123","Labels":{"com.docker.compose.project":"demo"}}]`)
	}))
	t.Cleanup(server.Close)

	apiClient, err := client.New(
		client.WithHost(server.URL),
		client.WithAPIVersion("1.41"),
		client.WithHTTPClient(server.Client()),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = apiClient.Close() })

	// The bogus dockerHost proves the provided client wins over the
	// fallback host.
	containers, err := ListGlobalComposeContainers(t.Context(), apiClient, "tcp://unused.example.com:2375")
	require.NoError(t, err)
	require.Len(t, containers, 1)
	require.Equal(t, "abc123", containers[0].ID)
}

func TestMirrorPodmanComposeLabelsInternal(t *testing.T) {
	proj := &composetypes.Project{
		Name: "demo",
		Services: composetypes.Services{
			"web": {Name: "web"},
			"db":  {Name: "db", CustomLabels: composetypes.Labels{"existing": "kept"}},
		},
	}

	mirrorPodmanComposeLabelsInternal("demo", proj)

	for _, name := range []string{"web", "db"} {
		labels := proj.Services[name].CustomLabels
		require.Equal(t, "demo", labels[podmanComposeProjectLabelInternal])
		require.Equal(t, name, labels[podmanComposeServiceLabelInternal])
		require.Equal(t, "podman-compose@demo.service", labels[podmanSystemdUnitLabelInternal])
	}
	require.Equal(t, "kept", proj.Services["db"].CustomLabels["existing"])
}

func TestPodmanComposeInvisibleInternal(t *testing.T) {
	tests := []struct {
		name   string
		labels map[string]string
		want   bool
	}{
		{name: "podman-compose container lacks config hash", labels: map[string]string{api.ProjectLabel: "demo"}, want: true},
		{name: "compose v5 container carries config hash", labels: map[string]string{api.ConfigHashLabel: "abc"}, want: false},
		{name: "compose hook runner is excluded by hook label", labels: map[string]string{api.HookLabel: "pre_start"}, want: false},
		{name: "one-off container is excluded by one-off label", labels: map[string]string{api.OneoffLabel: "True"}, want: false},
		{name: "non-one-off label without config hash is excluded", labels: map[string]string{api.OneoffLabel: "False"}, want: false},
		{name: "no labels", labels: nil, want: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, podmanComposeInvisibleInternal(container.Summary{Labels: tt.labels}))
		})
	}
}

func TestRemoveInDependencyOrderInternalRemovesBlockedParentsAfterDependents(t *testing.T) {
	dependent := container.Summary{ID: "dependent-container-id"}
	parent := container.Summary{ID: "parent--container-id"}
	removed := map[string]bool{}
	remove := func(ctr container.Summary) error {
		if ctr.ID == parent.ID && !removed[dependent.ID] {
			return errors.New("container has dependent containers which must be removed before it")
		}
		removed[ctr.ID] = true
		return nil
	}

	// Parent is offered first: the first pass blocks it, the next pass succeeds
	// once the dependent is gone.
	require.NoError(t, removeInDependencyOrderInternal([]container.Summary{parent, dependent}, remove))
	require.True(t, removed[parent.ID])
	require.True(t, removed[dependent.ID])
}

func TestRemoveInDependencyOrderInternalReportsRemainingAfterNoProgress(t *testing.T) {
	ctr := container.Summary{ID: "blocked-container-id"}
	remove := func(container.Summary) error {
		return errors.New("has dependent containers which must be removed before it")
	}

	err := removeInDependencyOrderInternal([]container.Summary{ctr}, remove)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "has dependent containers")
}

func TestProjectLabelMatchesInternal(t *testing.T) {
	tests := []struct {
		name        string
		labels      map[string]string
		projectName string
		want        bool
	}{
		{name: "exact", labels: map[string]string{api.ProjectLabel: "demo"}, projectName: "demo", want: true},
		{name: "case insensitive and trimmed", labels: map[string]string{api.ProjectLabel: "  Demo "}, projectName: "demo", want: true},
		{name: "different project", labels: map[string]string{api.ProjectLabel: "other"}, projectName: "demo", want: false},
		{name: "missing label", labels: nil, projectName: "demo", want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, projectLabelMatchesInternal(tt.labels, tt.projectName))
		})
	}
}

func TestComposeContainerSummaryInternal(t *testing.T) {
	summary := composeContainerSummaryInternal(container.Summary{
		ID:     "abc123",
		Names:  []string{"/demo_web_1"},
		Image:  "nginx:alpine",
		State:  container.StateRunning,
		Status: "Up 2 minutes",
		Labels: map[string]string{
			api.ProjectLabel: "demo",
			api.ServiceLabel: "web",
		},
		Ports:  []container.PortSummary{{PrivatePort: 80, PublicPort: 8080, Type: "tcp"}},
		Health: &container.HealthSummary{Status: container.Healthy},
	})

	assert.Equal(t, "abc123", summary.ID)
	assert.Equal(t, "demo_web_1", summary.Name)
	assert.Equal(t, "nginx:alpine", summary.Image)
	assert.Equal(t, "demo", summary.Project)
	assert.Equal(t, "web", summary.Service)
	assert.Equal(t, container.StateRunning, summary.State)
	assert.Equal(t, container.Healthy, summary.Health)
	assert.Equal(t, api.PortPublishers{{TargetPort: 80, PublishedPort: 8080, Protocol: "tcp"}}, summary.Publishers)
}

func TestComposeContainerSummaryInternalWithoutHealth(t *testing.T) {
	summary := composeContainerSummaryInternal(container.Summary{ID: "abc123", State: container.StateExited})

	assert.Equal(t, container.HealthStatus(""), summary.Health)
	assert.Empty(t, summary.Publishers)
}

func TestFormatDockerPorts(t *testing.T) {
	tests := []struct {
		name     string
		input    []container.PortSummary
		expected []string
	}{
		{
			name: "public port",
			input: []container.PortSummary{
				{PublicPort: 8080, PrivatePort: 80, Type: "tcp"},
			},
			expected: []string{"8080:80/tcp"},
		},
		{
			name: "private only",
			input: []container.PortSummary{
				{PrivatePort: 80, Type: "tcp"},
			},
			expected: []string{"80/tcp"},
		},
		{
			name:     "empty",
			input:    []container.PortSummary{},
			expected: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := FormatDockerPorts(tt.input)
			assert.Equal(t, tt.expected, got)
		})
	}
}
