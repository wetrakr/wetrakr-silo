package provider

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	pluginv1 "github.com/Silo-Server/silo-plugin-sdk/pkg/pluginproto/silo/plugin/v1"
)

const (
	applied  = pluginv1.WatchSyncApplyStatus_WATCH_SYNC_APPLY_STATUS_APPLIED
	noChange = pluginv1.WatchSyncApplyStatus_WATCH_SYNC_APPLY_STATUS_NO_CHANGE
	retry    = pluginv1.WatchSyncApplyStatus_WATCH_SYNC_APPLY_STATUS_RETRY
	rejected = pluginv1.WatchSyncApplyStatus_WATCH_SYNC_APPLY_STATUS_REJECTED
)

func TestRequestsCarryAppKeyVersionAndToken(t *testing.T) {
	fake, server := newFake(t)
	fake.handle("GET", "/account/settings", jsonReply(200, map[string]any{
		"id":     42,
		"info":   map[string]any{"username": "ana", "display_name": "Ana"},
		"images": map[string]any{"avatar": "ana.jpg"},
	}))
	resp, err := server.GetAccount(context.Background(), &pluginv1.WatchSyncGetAccountRequest{Context: authContext()})
	if err != nil || resp.GetFault() != nil {
		t.Fatalf("GetAccount: %v %v", err, resp.GetFault())
	}
	account := resp.GetAccount()
	if account.GetExternalSubject() != "42" || account.GetUsername() != "ana" || account.GetDisplayName() != "Ana" {
		t.Fatalf("account = %v", account)
	}
	if account.GetProfileUrl() != "https://wetrakr.com/user/ana" || account.GetAvatarUrl() != "https://api.wetrakr.com/uploads/avatars/ana.jpg" {
		t.Fatalf("urls = %q %q", account.GetProfileUrl(), account.GetAvatarUrl())
	}
	header := fake.calls("GET", "/account/settings")[0].header
	if header.Get("wetrakr-api-key") != "client-123" || header.Get("wetrakr-api-version") != "1" ||
		header.Get("Authorization") != "Bearer "+testToken || header.Get("User-Agent") != "WeTrakr-Silo/1.2.3" {
		t.Fatalf("headers = %v", header)
	}
}

func TestMissingClientIDIsAnAdminFault(t *testing.T) {
	_, server := newFake(t)
	auth := authContext()
	auth.ProviderConfig = &pluginv1.WatchSyncProviderConfig{}
	resp, _ := server.GetAccount(context.Background(), &pluginv1.WatchSyncGetAccountRequest{Context: auth})
	if resp.GetFault().GetCode() != pluginv1.WatchSyncFaultCode_WATCH_SYNC_FAULT_CODE_PERMISSION_DENIED {
		t.Fatalf("fault = %v", resp.GetFault())
	}
}

func TestBuiltInClientIDIsTheDefault(t *testing.T) {
	previous := DefaultClientID
	DefaultClientID = "built-in"
	t.Cleanup(func() { DefaultClientID = previous })
	app, fault := appConfigFrom(&pluginv1.WatchSyncProviderConfig{})
	if fault != nil || app.clientID != "built-in" {
		t.Fatalf("app = %v %v", app, fault)
	}
	app, _ = appConfigFrom(&pluginv1.WatchSyncProviderConfig{Values: map[string]string{configClientID: "own"}})
	if app.clientID != "own" {
		t.Fatalf("an administrator's client id must win, got %q", app.clientID)
	}
}

func TestRefreshRotatesBothTokens(t *testing.T) {
	fake, server := newFake(t)
	fake.handle("POST", "/oauth/token/refresh", jsonReply(200, map[string]any{
		"access_token": "new-access", "refresh_token": "new-refresh", "expires_in": 604800,
	}))
	resp, _ := server.RefreshCredentials(context.Background(), &pluginv1.WatchSyncRefreshCredentialsRequest{Context: authContext()})
	credentials := resp.GetCredentials()
	if credentials.GetAccessToken() != "new-access" || credentials.GetRefreshToken() != "new-refresh" || credentials.GetExpiresAt() == nil {
		t.Fatalf("credentials = %v (fault %v)", credentials, resp.GetFault())
	}
	if body := fake.calls("POST", "/oauth/token/refresh")[0].body; body["refresh_token"] != "refresh-token" {
		t.Fatalf("refresh body = %v", body)
	}
}

func TestRejectedRefreshAsksToReconnect(t *testing.T) {
	fake, server := newFake(t)
	fake.handle("POST", "/oauth/token/refresh", jsonReply(401, map[string]any{"error": "INVALID_REFRESH_TOKEN", "message": "Invalid refresh token"}))
	resp, _ := server.RefreshCredentials(context.Background(), &pluginv1.WatchSyncRefreshCredentialsRequest{Context: authContext()})
	if resp.GetFault().GetCode() != pluginv1.WatchSyncFaultCode_WATCH_SYNC_FAULT_CODE_INVALID_CREDENTIAL {
		t.Fatalf("fault = %v", resp.GetFault())
	}
}

func TestDeviceFlow(t *testing.T) {
	fake, server := newFake(t)
	fake.handle("POST", "/oauth/device/code", jsonReply(200, map[string]any{
		"device_code": "dev-1", "user_code": "K7PX4M", "verification_url": "https://wetrakr.com/activate", "expires_in": 600, "interval": 5,
	}))
	auth := authContext()
	start, _ := server.DeviceAuthorization().Start(context.Background(), &pluginv1.WatchSyncDeviceAuthorizationServiceStartRequest{
		CapabilityId: capabilityID, ProviderConfig: auth.GetProviderConfig(),
	})
	if start.GetFault() != nil || start.GetUserCode() != "K7PX4M" || start.GetVerificationUrlComplete() != "https://wetrakr.com/activate?code=K7PX4M&platform=silo" {
		t.Fatalf("start = %v", start)
	}
	if body := fake.calls("POST", "/oauth/device/code")[0].body; body["client_id"] != "client-123" {
		t.Fatalf("device code body = %v", body)
	}

	cases := []struct {
		status int
		body   map[string]any
		want   pluginv1.WatchSyncDeviceAuthorizationStatus
	}{
		{400, map[string]any{"error": "authorization_pending"}, pluginv1.WatchSyncDeviceAuthorizationStatus_WATCH_SYNC_DEVICE_AUTHORIZATION_STATUS_PENDING},
		{429, map[string]any{"error": "slow_down"}, pluginv1.WatchSyncDeviceAuthorizationStatus_WATCH_SYNC_DEVICE_AUTHORIZATION_STATUS_PENDING},
		{410, map[string]any{"error": "expired_token"}, pluginv1.WatchSyncDeviceAuthorizationStatus_WATCH_SYNC_DEVICE_AUTHORIZATION_STATUS_EXPIRED},
		{409, map[string]any{"error": "already_used"}, pluginv1.WatchSyncDeviceAuthorizationStatus_WATCH_SYNC_DEVICE_AUTHORIZATION_STATUS_EXPIRED},
		{418, map[string]any{"error": "access_denied"}, pluginv1.WatchSyncDeviceAuthorizationStatus_WATCH_SYNC_DEVICE_AUTHORIZATION_STATUS_DENIED},
		{200, map[string]any{"access_token": "a", "refresh_token": "r", "expires_in": 604800}, pluginv1.WatchSyncDeviceAuthorizationStatus_WATCH_SYNC_DEVICE_AUTHORIZATION_STATUS_AUTHORIZED},
	}
	for _, tc := range cases {
		fake.handle("POST", "/oauth/device/token", jsonReply(tc.status, tc.body))
		poll, _ := server.DeviceAuthorization().Poll(context.Background(), &pluginv1.WatchSyncDeviceAuthorizationServicePollRequest{
			CapabilityId: capabilityID, ProviderConfig: auth.GetProviderConfig(), ProviderState: start.GetProviderState(),
		})
		if poll.GetStatus() != tc.want || poll.GetFault() != nil {
			t.Errorf("HTTP %d: status %v fault %v, want %v", tc.status, poll.GetStatus(), poll.GetFault(), tc.want)
		}
		if tc.status == 200 && poll.GetCredentials().GetRefreshToken() != "r" {
			t.Errorf("credentials = %v", poll.GetCredentials())
		}
	}
	body := fake.calls("POST", "/oauth/device/token")[0].body
	if body["code"] != "dev-1" || body["client_id"] != "client-123" {
		t.Fatalf("poll body = %v", body)
	}
	if _, sent := body["client_secret"]; sent {
		t.Fatal("an installed plugin must not send a client secret")
	}
}

func TestRevokedTokenFailsTheWholeBatch(t *testing.T) {
	fake, server := newFake(t)
	fake.handle("POST", "/sync/tracking", jsonReply(401, map[string]any{"error": "TOKEN_REVOKED"}))
	resp, _ := server.ApplyEvents(context.Background(), &pluginv1.WatchSyncApplyEventsRequest{
		Context: authContext(),
		Events:  []*pluginv1.WatchSyncEvent{movieEvent("e1", pluginv1.WatchSyncOperation_WATCH_SYNC_OPERATION_MARK_WATCHED, "155")},
	})
	if resp.GetFault().GetCode() != pluginv1.WatchSyncFaultCode_WATCH_SYNC_FAULT_CODE_INVALID_CREDENTIAL {
		t.Fatalf("fault = %v", resp.GetFault())
	}
}

func TestRateLimitIsRetriedInPlaceThenDeferred(t *testing.T) {
	fake, server := newFake(t)
	calls := 0
	fake.handle("POST", "/sync/favorites", func(w http.ResponseWriter, r *http.Request) {
		calls++
		if calls == 1 {
			w.Header().Set("Retry-After", "1")
			jsonReply(429, map[string]any{"error": "CONCURRENCY_LIMIT"})(w, r)
			return
		}
		jsonReply(200, map[string]any{})(w, r)
	})
	resp, _ := server.ApplyEvents(context.Background(), &pluginv1.WatchSyncApplyEventsRequest{
		Context: authContext(),
		Events:  []*pluginv1.WatchSyncEvent{movieEvent("e1", pluginv1.WatchSyncOperation_WATCH_SYNC_OPERATION_ADD_FAVORITE, "155")},
	})
	if calls != 2 || resp.GetFault() != nil || resp.GetResults()[0].GetStatus() != applied {
		t.Fatalf("calls %d, response %v", calls, resp)
	}

	fake.handle("POST", "/sync/favorites", func(w http.ResponseWriter, r *http.Request) {
		jsonReply(429, map[string]any{"error": "APP_USER_QUOTA_EXCEEDED"})(w, r)
	})
	resp, _ = server.ApplyEvents(context.Background(), &pluginv1.WatchSyncApplyEventsRequest{
		Context: authContext(),
		Events:  []*pluginv1.WatchSyncEvent{movieEvent("e2", pluginv1.WatchSyncOperation_WATCH_SYNC_OPERATION_ADD_FAVORITE, "155")},
	})
	fault := resp.GetFault()
	if fault.GetCode() != pluginv1.WatchSyncFaultCode_WATCH_SYNC_FAULT_CODE_RATE_LIMITED || fault.GetRetryAfter().AsDuration() <= 0 {
		t.Fatalf("a daily quota must defer the connection, got %v", fault)
	}
}

func TestErrorCodeReadsBothShapes(t *testing.T) {
	if code := errorCodeFrom([]byte(`{"error":"QUOTA_EXCEEDED","message":"x"}`)); code != "QUOTA_EXCEEDED" {
		t.Fatalf("string code = %q", code)
	}
	if code := errorCodeFrom([]byte(`{"success":false,"error":{"code":"INVALID_MEDIA_ID"}}`)); code != "INVALID_MEDIA_ID" {
		t.Fatalf("object code = %q", code)
	}
	if code := errorCodeFrom([]byte(`{"error":"has spaces and secrets"}`)); code != "" {
		t.Fatalf("free text must not be read as a code, got %q", code)
	}
}

func mustJSON(t *testing.T, value any) string {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}

func TestShortDeadlineStillSendsTheEvent(t *testing.T) {
	fake, server := newFake(t)
	fake.handle("POST", "/scrobble/stop", jsonReply(201, map[string]any{"action": "scrobble"}))
	// Silo gives a playback stop sent through its Jellyfin API five seconds.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stop := movieEvent("s", pluginv1.WatchSyncOperation_WATCH_SYNC_OPERATION_SCROBBLE_STOP, "155")
	stop.PositionSeconds, stop.DurationSeconds, stop.Completed = 95, 100, true
	resp, _ := server.ApplyEvents(ctx, &pluginv1.WatchSyncApplyEventsRequest{Context: authContext(), Events: []*pluginv1.WatchSyncEvent{stop}})
	if resp.GetFault() != nil || resp.GetResults()[0].GetStatus() != applied || len(fake.calls("POST", "/scrobble/stop")) != 1 {
		t.Fatalf("a stop with a five-second deadline must be sent, got %v", resp)
	}
}
