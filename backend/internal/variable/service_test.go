package variable

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"maps"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/getarcaneapp/arcane/types/v2/env"
	"github.com/libtnb/sqlite"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.getarcane.app/sys/crypto"
	"gorm.io/gorm"

	"github.com/getarcaneapp/arcane/backend/v2/internal/common"
	"github.com/getarcaneapp/arcane/backend/v2/internal/config"
	"github.com/getarcaneapp/arcane/backend/v2/internal/database"
	"github.com/getarcaneapp/arcane/backend/v2/internal/environment"
	"github.com/getarcaneapp/arcane/backend/v2/internal/kv"
	"github.com/getarcaneapp/arcane/backend/v2/internal/settings"
	"github.com/getarcaneapp/arcane/backend/v2/pkg/projects"
	"github.com/getarcaneapp/arcane/backend/v2/pkg/remenv"
	"github.com/getarcaneapp/arcane/backend/v2/pkg/utils"
)

func newSettingsServiceForVariableTestInternal(t testing.TB, ctx context.Context, db *database.DB) (*settings.SettingsService, error) {
	t.Helper()
	svc, err := settings.NewSettingsService(ctx, db, &config.Config{})
	if err == nil {
		t.Cleanup(func() { require.NoError(t, svc.Stop(context.WithoutCancel(t.Context()))) })
	}
	return svc, err
}

func setupVariableServiceTest(t *testing.T) (*VariableService, *database.DB, string) {
	t.Helper()

	dsn := fmt.Sprintf("file:%s?mode=memory&cache=shared", strings.NewReplacer("/", "_", " ", "_").Replace(t.Name()))
	db, err := gorm.Open(sqlite.Open(dsn), &gorm.Config{})
	require.NoError(t, err)
	require.NoError(t, db.AutoMigrate(
		&GlobalVariable{},
		&environment.Environment{},
		&kv.KVEntry{},
		&settings.SettingVariable{},
	))

	sqlDB, err := db.DB()
	require.NoError(t, err)
	sqlDB.SetMaxOpenConns(1)
	sqlDB.SetMaxIdleConns(1)

	crypto.InitEncryption(&crypto.Config{
		EncryptionKey: "test-encryption-key-for-testing-32bytes-min",
		Environment:   "test",
	})

	projectsDir := t.TempDir()
	dbWrap := &database.DB{DB: db}
	settingsSvc, err := newSettingsServiceForVariableTestInternal(t, t.Context(), dbWrap)
	require.NoError(t, err)
	require.NoError(t, settingsSvc.UpdateSetting(t.Context(), "projectsDirectory", projectsDir))

	service := NewVariableService(dbWrap, nil, settingsSvc, kv.NewKVService(dbWrap))
	return service, dbWrap, projectsDir
}

func createVariableTestEnvironment(t *testing.T, db *database.DB, id string) {
	t.Helper()
	require.NoError(t, db.Create(&environment.Environment{
		ID:      id,
		Name:    "env-" + id,
		ApiUrl:  "http://env-" + id,
		Status:  "online",
		Enabled: true,
	}).Error)
}

func TestCreateVariable_SecretEncryptedAtRestAndRedactedOnList(t *testing.T) {
	service, db, _ := setupVariableServiceTest(t)
	ctx := t.Context()

	created, err := service.CreateVariable(ctx, env.CreateGlobalVariableRequest{
		Key:             "API_TOKEN",
		Value:           "super-secret",
		IsSecret:        true,
		AllEnvironments: true,
	})
	require.NoError(t, err)
	require.Empty(t, created.Value, "mutation response must not echo the secret value")

	var stored GlobalVariable
	require.NoError(t, db.First(&stored, "id = ?", created.ID).Error)
	require.NotEqual(t, "super-secret", stored.Value, "secret must not be stored as plaintext")
	decrypted, err := crypto.Decrypt(stored.Value)
	require.NoError(t, err)
	require.Equal(t, "super-secret", decrypted)

	listed, err := service.ListVariables(ctx)
	require.NoError(t, err)
	require.Len(t, listed, 1)
	require.True(t, listed[0].IsSecret)
	require.Empty(t, listed[0].Value)
}

func TestUpdateVariable_OmittedSecretValuePreservesCiphertextAndRedactsResponse(t *testing.T) {
	service, db, _ := setupVariableServiceTest(t)
	ctx := t.Context()
	created := createMaterializedSecretVariableInternal(t, service)

	var before GlobalVariable
	require.NoError(t, db.WithContext(ctx).First(&before, "id = ?", created.ID).Error)
	renamedKey := "RENAMED_API_TOKEN"
	updated, err := service.UpdateVariable(ctx, created.ID, env.UpdateGlobalVariableRequest{Key: &renamedKey})
	require.NoError(t, err)
	require.True(t, updated.IsSecret)
	require.Empty(t, updated.Value)

	var after GlobalVariable
	require.NoError(t, db.WithContext(ctx).First(&after, "id = ?", created.ID).Error)
	require.Equal(t, before.Value, after.Value, "omitting value must preserve the stored ciphertext")
	decrypted, err := crypto.Decrypt(after.Value)
	require.NoError(t, err)
	require.Equal(t, "super-secret", decrypted)

	listed, err := service.ListVariables(ctx)
	require.NoError(t, err)
	require.Len(t, listed, 1)
	require.Empty(t, listed[0].Value)
}

func TestUpdateVariable_SecretToPlainRequiresReplacementValue(t *testing.T) {
	service, db, _ := setupVariableServiceTest(t)
	ctx := t.Context()
	created := createMaterializedSecretVariableInternal(t, service)

	plain := false
	_, err := service.UpdateVariable(ctx, created.ID, env.UpdateGlobalVariableRequest{IsSecret: &plain})
	require.Error(t, err)
	require.ErrorIs(t, err, common.ErrGlobalVariableSecretValueRequired)

	replacement := "public-value"
	updated, err := service.UpdateVariable(ctx, created.ID, env.UpdateGlobalVariableRequest{
		Value:    &replacement,
		IsSecret: &plain,
	})
	require.NoError(t, err)
	require.False(t, updated.IsSecret)
	require.Equal(t, replacement, updated.Value)

	var stored GlobalVariable
	require.NoError(t, db.WithContext(ctx).First(&stored, "id = ?", created.ID).Error)
	require.False(t, stored.IsSecret)
	require.Equal(t, replacement, stored.Value)
}

func TestResolveEffectiveVariables_EnvScopedOverridesAllEnv(t *testing.T) {
	service, db, _ := setupVariableServiceTest(t)
	ctx := t.Context()
	createVariableTestEnvironment(t, db, "env-a")
	createVariableTestEnvironment(t, db, "env-b")

	_, err := service.CreateVariable(ctx, env.CreateGlobalVariableRequest{
		Key: "REGION", Value: "default", AllEnvironments: true,
	})
	require.NoError(t, err)
	_, err = service.CreateVariable(ctx, env.CreateGlobalVariableRequest{
		Key: "REGION", Value: "eu-west", EnvironmentIDs: []string{"env-a"},
	})
	require.NoError(t, err)

	// A second variable with the same key and an overlapping scope is rejected.
	_, err = service.CreateVariable(ctx, env.CreateGlobalVariableRequest{
		Key: "REGION", Value: "dup", EnvironmentIDs: []string{"env-a", "env-b"},
	})
	require.Error(t, err)

	// A specific scope without environments must not widen to all environments.
	_, err = service.CreateVariable(ctx, env.CreateGlobalVariableRequest{
		Key: "NO_SCOPE", Value: "x", AllEnvironments: false, EnvironmentIDs: []string{},
	})
	require.ErrorIs(t, err, common.ErrGlobalVariableScopeRequired, "expected GlobalVariableScopeRequiredError, got %v", err)

	forA, err := service.resolveEffectiveVariablesInternal(ctx, "env-a")
	require.NoError(t, err)
	require.Equal(t, []env.Variable{{Key: "REGION", Value: "eu-west"}}, forA)

	forB, err := service.resolveEffectiveVariablesInternal(ctx, "env-b")
	require.NoError(t, err)
	require.Equal(t, []env.Variable{{Key: "REGION", Value: "default"}}, forB)
}

func TestWriteLocalEnvFile_RejectsNewlineInjectionKey(t *testing.T) {
	service, _, projectsDir := setupVariableServiceTest(t)

	err := service.WriteLocalEnvFile(t.Context(), []env.Variable{
		{Key: "BENIGN\nINJECTED", Value: "x"},
	})
	require.ErrorIs(t, err, common.ErrInvalidEnvKey, "expected InvalidEnvKeyError, got %v", err)

	_, statErr := os.Stat(filepath.Join(projectsDir, ".env.global"))
	require.True(t, os.IsNotExist(statErr), ".env.global must not be written on validation failure")
}

func TestWriteLocalEnvFile_PreservesExistingFileMode(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("file mode bits are not represented on Windows FS")
	}
	service, _, projectsDir := setupVariableServiceTest(t)
	ctx := t.Context()
	envPath := filepath.Join(projectsDir, ".env.global")

	require.NoError(t, service.WriteLocalEnvFile(ctx, []env.Variable{{Key: "A", Value: "1"}}))
	info, err := os.Stat(envPath)
	require.NoError(t, err)
	require.Equal(t, utils.FilePerm, info.Mode().Perm(), "new file must use the configured default mode")

	require.NoError(t, os.Chmod(envPath, 0o600))
	require.NoError(t, service.WriteLocalEnvFile(ctx, []env.Variable{{Key: "A", Value: "2"}}))
	info, err = os.Stat(envPath)
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0o600), info.Mode().Perm(), "rewrite must keep the existing mode")
	content, err := os.ReadFile(envPath)
	require.NoError(t, err)
	require.Contains(t, string(content), "A=2")
}

func TestWriteLocalEnvFile_FileModeLeavesFileUntouched(t *testing.T) {
	service, _, projectsDir := setupVariableServiceTest(t)
	ctx := t.Context()

	require.NoError(t, service.settingsService.UpdateSetting(ctx, "variablesFileMode", "true"))
	require.True(t, service.FileModeEnabled(ctx))

	envPath := filepath.Join(projectsDir, ".env.global")
	content := "DC_PATH_BASE=/home/data/server_base\n"
	require.NoError(t, os.WriteFile(envPath, []byte(content), 0o600))

	writeErr := service.WriteLocalEnvFile(ctx, []env.Variable{{Key: "OTHER", Value: "x"}})
	require.ErrorIs(t, writeErr, common.ErrVariablesFileMode)

	got, err := os.ReadFile(envPath)
	require.NoError(t, err)
	assert.Equal(t, content, string(got), "file mode must leave .env.global untouched")

	_, err = service.CreateVariable(ctx, env.CreateGlobalVariableRequest{Key: "A", Value: "1"})
	require.ErrorIs(t, err, common.ErrVariablesFileMode)

	listed, err := service.ListVariables(ctx)
	require.NoError(t, err)
	require.Len(t, listed, 1, "UI must see the file contents in file mode")
	assert.Equal(t, "DC_PATH_BASE", listed[0].Key)
	assert.Equal(t, "/home/data/server_base", listed[0].Value)
}

func TestReadLocalEnvFile_ResolvesValuesLikeDeployment(t *testing.T) {
	service, _, projectsDir := setupVariableServiceTest(t)
	ctx := t.Context()

	content := strings.Join([]string{
		"# global variables",
		"BASE_DIR=/srv/app",
		"export DATA_DIR=\"$BASE_DIR/data\"",
		"OPTIONAL=${MISSING:-fallback}",
		"INLINE=value # trailing comment",
		"SINGLE='$BASE_DIR/literal'",
		"VOLUME_DIR=${BASE_DIR}/volumes",
		"",
	}, "\n")
	require.NoError(t, os.WriteFile(filepath.Join(projectsDir, ".env.global"), []byte(content), 0o600))

	vars, err := service.ReadLocalEnvFile(ctx)
	require.NoError(t, err)

	got := make(map[string]string, len(vars))
	for _, variable := range vars {
		got[variable.Key] = variable.Value
	}
	assert.Equal(t, "/srv/app/data", got["DATA_DIR"])
	assert.Equal(t, "fallback", got["OPTIONAL"])
	assert.Equal(t, "value", got["INLINE"])
	assert.Equal(t, "$BASE_DIR/literal", got["SINGLE"], "single quotes must suppress expansion")
	assert.Equal(t, "/srv/app/volumes", got["VOLUME_DIR"])

	keys := make([]string, 0, len(vars))
	for _, variable := range vars {
		keys = append(keys, variable.Key)
	}
	assert.Equal(t, slices.Sorted(maps.Keys(got)), keys, "variables are returned in key order")

	// The variables API must present exactly what deployment interpolation sees.
	loader := projects.NewEnvLoader(projectsDir, projectsDir, false)
	envMap, _, err := loader.LoadEnvironment(ctx)
	require.NoError(t, err)
	for key, value := range got {
		assert.Equal(t, envMap[key], value, "value for %s must match deployment interpolation", key)
	}
}

func TestSyncEnvironment_LocalWritesEnvGlobalFile(t *testing.T) {
	service, _, projectsDir := setupVariableServiceTest(t)
	ctx := t.Context()

	_, err := service.CreateVariable(ctx, env.CreateGlobalVariableRequest{
		Key: "DB_PASSWORD", Value: "hunter2", IsSecret: true, AllEnvironments: true,
	})
	require.NoError(t, err)

	require.NoError(t, service.SyncEnvironment(ctx, "0"))

	content, err := os.ReadFile(filepath.Join(projectsDir, ".env.global"))
	require.NoError(t, err)
	require.Contains(t, string(content), "DB_PASSWORD=hunter2", "materialized file must contain the decrypted secret")

	statuses := service.SyncStatuses()
	require.Len(t, statuses, 1)
	require.Equal(t, "synced", statuses[0].Status)
}

type capturedVariableSyncRequestInternal struct {
	Method  string
	Path    string
	Headers map[string]string
	Body    []byte
}

func TestSyncEnvironment_DirectMaterializesSecretsThroughAgentRoute(t *testing.T) {
	service, db, _ := setupVariableServiceTest(t)
	token := "direct-agent-token"

	var requestMu sync.Mutex
	requests := make([]capturedVariableSyncRequestInternal, 0, 2)
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		body, err := io.ReadAll(request.Body)
		if !assert.NoError(t, err) {
			return
		}
		requestMu.Lock()
		requests = append(requests, capturedVariableSyncRequestInternal{
			Method: request.Method,
			Path:   request.URL.Path,
			Headers: map[string]string{
				remenv.HeaderAPIKey:     request.Header.Get(remenv.HeaderAPIKey),
				remenv.HeaderAgentToken: request.Header.Get(remenv.HeaderAgentToken),
			},
			Body: body,
		})
		requestMu.Unlock()

		writer.Header().Set("Content-Type", "application/json")
		if request.Method == http.MethodGet {
			_, _ = writer.Write([]byte(`{"success":true,"data":[]}`))
			return
		}
		_, _ = writer.Write([]byte(`{"success":true,"data":{"message":"updated"}}`))
	}))
	defer server.Close()

	require.NoError(t, db.Create(&environment.Environment{
		ID:          "env-direct",
		Name:        "Direct",
		ApiUrl:      server.URL,
		Status:      string(environment.EnvironmentStatusOnline),
		Enabled:     true,
		AccessToken: &token,
	}).Error)
	environmentService := environment.NewEnvironmentService(db, nil, nil, nil, service.settingsService, nil)
	service.proxyEnvironmentJSON = environmentService.ProxyJSONRequest
	createMaterializedSecretVariableInternal(t, service)

	require.NoError(t, service.SyncEnvironment(t.Context(), "env-direct"))
	requestMu.Lock()
	captured := append([]capturedVariableSyncRequestInternal(nil), requests...)
	requestMu.Unlock()
	requireMaterializationRequestsInternal(t, captured, token)
}

func TestSyncEnvironment_EdgeMaterializesSecretsThroughAgentRoute(t *testing.T) {
	service, db, _ := setupVariableServiceTest(t)
	token := "edge-agent-token"
	require.NoError(t, db.Create(&environment.Environment{
		ID:          "env-edge",
		Name:        "Edge",
		ApiUrl:      "http://edge.invalid",
		Status:      string(environment.EnvironmentStatusOnline),
		Enabled:     true,
		IsEdge:      true,
		AccessToken: &token,
	}).Error)

	var requestMu sync.Mutex
	requests := make([]capturedVariableSyncRequestInternal, 0, 2)
	service.proxyEnvironmentJSON = func(_ context.Context, environmentID, method, path string, body []byte, response any) error {
		require.Equal(t, "env-edge", environmentID)
		requestMu.Lock()
		requests = append(requests, capturedVariableSyncRequestInternal{
			Method: method,
			Path:   path,
			Headers: map[string]string{
				remenv.HeaderAPIKey:     token,
				remenv.HeaderAgentToken: token,
			},
			Body: append([]byte(nil), body...),
		})
		requestMu.Unlock()
		if method == http.MethodGet {
			return json.Unmarshal([]byte(`{"success":true,"data":[]}`), response)
		}
		return json.Unmarshal([]byte(`{"success":true,"data":{"message":"updated"}}`), response)
	}
	createMaterializedSecretVariableInternal(t, service)

	require.NoError(t, service.SyncEnvironment(t.Context(), "env-edge"))
	requestMu.Lock()
	captured := append([]capturedVariableSyncRequestInternal(nil), requests...)
	requestMu.Unlock()
	requireMaterializationRequestsInternal(t, captured, token)
}

func createMaterializedSecretVariableInternal(t *testing.T, service *VariableService) *env.GlobalVariable {
	t.Helper()
	created, err := service.CreateVariable(t.Context(), env.CreateGlobalVariableRequest{
		Key:             "API_TOKEN",
		Value:           "super-secret",
		IsSecret:        true,
		AllEnvironments: true,
	})
	require.NoError(t, err)
	return created
}

func requireMaterializationRequestsInternal(t *testing.T, requests []capturedVariableSyncRequestInternal, token string) {
	t.Helper()
	require.Len(t, requests, 2)

	require.Equal(t, http.MethodGet, requests[0].Method)
	require.Equal(t, agentVariablesPath, requests[0].Path)
	require.Empty(t, requests[0].Body)
	require.Equal(t, token, requests[0].Headers[remenv.HeaderAPIKey])
	require.Equal(t, token, requests[0].Headers[remenv.HeaderAgentToken])

	require.Equal(t, http.MethodPut, requests[1].Method)
	require.Equal(t, agentVariablesPath, requests[1].Path)
	require.Equal(t, token, requests[1].Headers[remenv.HeaderAPIKey])
	require.Equal(t, token, requests[1].Headers[remenv.HeaderAgentToken])
	var materialized env.Summary
	require.NoError(t, json.Unmarshal(requests[1].Body, &materialized))
	require.Equal(t, []env.Variable{{Key: "API_TOKEN", Value: "super-secret"}}, materialized.Variables)
}
