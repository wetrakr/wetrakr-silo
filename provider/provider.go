package provider

import (
	"context"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	pluginv1 "github.com/Silo-Server/silo-plugin-sdk/pkg/pluginproto/silo/plugin/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	capabilityID = "wetrakr"

	// Optional install-wide settings, from the "app" global config. Without
	// them the plugin uses WeTrakr's own Silo app and the public API.
	configClientID = "app.client_id"
	configBaseURL  = "app.base_url"

	// An RPC stops starting upstream work after syncBudget, or earlier so that
	// its last request, bounded by defaultRequestTimeout, still ends
	// syncDeadlineMargin before the host's two-minute deadline.
	syncBudget         = 90 * time.Second
	syncDeadlineMargin = 10 * time.Second

	notConfiguredMessage = "WeTrakr is not set up on this server: this build has no WeTrakr app key, so an administrator must enter one as the client ID in the plugin settings"
	timeLimitMessage     = "WeTrakr sync time limit reached; the event will be retried"

	profileURLBase = "https://wetrakr.com/user/"
	avatarURLBase  = "https://api.wetrakr.com/uploads/avatars/"
)

// DefaultClientID is the app key of WeTrakr's Silo app, which every server
// uses unless an administrator enters another one. It is public: it travels in
// every request header. Release builds set it with
// -ldflags "-X github.com/wetrakr/wetrakr-silo/provider.DefaultClientID=...".
var DefaultClientID = ""

// Server implements the watch-sync provider for WeTrakr. It keeps no state
// between RPCs except request pacing and a short cache of last activities,
// both keyed by token digest.
type Server struct {
	pluginv1.UnimplementedWatchSyncProviderServer
	http      *http.Client
	baseURL   string
	userAgent string
	writes    *credentialLimiter
	reads     *credentialLimiter
	// activities caches GET /sync/last_activities for one sync run, which asks
	// for several state families one after another.
	activities *activityCache
	// responseBytes bounds the encoded items of one ListRemoteState response.
	responseBytes int
	// sleep and now are replaced by tests.
	sleep func(context.Context, time.Duration) error
	now   func() time.Time
}

// NewServer returns a server that calls WeTrakr with httpClient, or a default
// client when nil. version is the plugin version sent in the User-Agent.
func NewServer(httpClient *http.Client, version string) *Server {
	if httpClient == nil {
		httpClient = &http.Client{Timeout: defaultRequestTimeout}
	}
	s := &Server{
		http:          httpClient,
		baseURL:       defaultBaseURL,
		userAgent:     userAgent(version),
		writes:        newCredentialLimiter(writeInterval, writeBurst),
		reads:         newCredentialLimiter(readInterval, readBurst),
		sleep:         sleepContext,
		responseBytes: 2 << 20,
		now:           time.Now,
	}
	s.activities = &activityCache{entries: make(map[[32]byte]activityEntry), now: s.clock}
	return s
}

// DeviceAuthorization returns the device-code service that shares this
// server's HTTP client.
func (s *Server) DeviceAuthorization() pluginv1.WatchSyncDeviceAuthorizationServiceServer {
	return &deviceAuthorization{server: s}
}

// userAgent identifies the plugin to WeTrakr. Only HTTP token characters of
// the version are kept, so an unusual build version cannot break requests.
func userAgent(version string) string {
	version = strings.TrimPrefix(strings.TrimSpace(version), "v")
	kept := strings.Map(func(r rune) rune {
		if r < 0x80 && (r >= '0' && r <= '9' || r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || strings.ContainsRune("!#$%&'*+-.^_`|~", r)) {
			return r
		}
		return -1
	}, version)
	if kept == "" {
		kept = "dev"
	}
	return "WeTrakr-Silo/" + kept
}

func (s *Server) clock() time.Time {
	if s.now != nil {
		return s.now()
	}
	return time.Now()
}

type appConfig struct {
	clientID string
	baseURL  string
}

// appConfigFrom reads the app key and API host, falling back to the built-in
// ones. The host sends plain values in values and secrets in secret_values;
// either map is accepted.
func appConfigFrom(config *pluginv1.WatchSyncProviderConfig) (appConfig, *pluginv1.WatchSyncFault) {
	lookup := func(key string) string {
		if value := strings.TrimSpace(config.GetValues()[key]); value != "" {
			return value
		}
		return strings.TrimSpace(config.GetSecretValues()[key])
	}
	app := appConfig{clientID: lookup(configClientID), baseURL: lookup(configBaseURL)}
	if app.clientID == "" {
		app.clientID = strings.TrimSpace(DefaultClientID)
	}
	if app.clientID == "" {
		return appConfig{}, newFault(pluginv1.WatchSyncFaultCode_WATCH_SYNC_FAULT_CODE_PERMISSION_DENIED, notConfiguredMessage)
	}
	if app.baseURL != "" {
		parsed, err := url.Parse(app.baseURL)
		if err != nil || parsed.Scheme != "https" && parsed.Scheme != "http" || parsed.Host == "" {
			return appConfig{}, newFault(pluginv1.WatchSyncFaultCode_WATCH_SYNC_FAULT_CODE_PERMISSION_DENIED,
				"The WeTrakr API URL in the plugin settings is not a valid http(s) URL")
		}
		app.baseURL = strings.TrimRight(app.baseURL, "/")
	}
	return app, nil
}

// appClient returns a client for the unauthenticated OAuth endpoints.
func (s *Server) appClient(capability string, config *pluginv1.WatchSyncProviderConfig) (*apiClient, *pluginv1.WatchSyncFault) {
	if capability != capabilityID {
		return nil, invalidRequestFault("Unknown WeTrakr capability")
	}
	app, fault := appConfigFrom(config)
	if fault != nil {
		return nil, fault
	}
	return &apiClient{server: s, app: app, baseURL: app.baseURL}, nil
}

// authenticatedClient returns a client that signs requests with the
// connection's access token.
func (s *Server) authenticatedClient(auth *pluginv1.WatchSyncAuthenticatedContext) (*apiClient, *pluginv1.WatchSyncFault) {
	client, fault := s.appClient(auth.GetCapabilityId(), auth.GetProviderConfig())
	if fault != nil {
		return nil, fault
	}
	client.token = strings.TrimSpace(auth.GetCredentials().GetAccessToken())
	if client.token == "" {
		return nil, invalidRequestFault("WeTrakr access token is required")
	}
	return client, nil
}

// window bounds one RPC: requests use the returned context, which ends
// syncDeadlineMargin before the host's deadline, and no upstream work starts
// after stopAt.
func (s *Server) window(ctx context.Context) (context.Context, context.CancelFunc, time.Time) {
	now := s.clock()
	stopAt := now.Add(syncBudget)
	if deadline, ok := ctx.Deadline(); ok {
		end := deadline.Add(-syncDeadlineMargin)
		if latest := end.Add(-defaultRequestTimeout); latest.Before(stopAt) {
			stopAt = latest
		}
		ctx, cancel := context.WithDeadline(ctx, end)
		return ctx, cancel, stopAt
	}
	ctx, cancel := context.WithTimeout(ctx, syncBudget+defaultRequestTimeout)
	return ctx, cancel, stopAt
}

func (s *Server) GetAccount(ctx context.Context, req *pluginv1.WatchSyncGetAccountRequest) (*pluginv1.WatchSyncGetAccountResponse, error) {
	client, fault := s.authenticatedClient(req.GetContext())
	if fault != nil {
		return &pluginv1.WatchSyncGetAccountResponse{Fault: fault}, nil
	}
	ctx, cancel, _ := s.window(ctx)
	defer cancel()
	var settings wtSettings
	if _, fault := client.do(ctx, request{method: http.MethodGet, path: "/account/settings"}, &settings); fault != nil {
		return &pluginv1.WatchSyncGetAccountResponse{Fault: fault}, nil
	}
	if settings.ID <= 0 {
		return &pluginv1.WatchSyncGetAccountResponse{Fault: temporaryFault("WeTrakr did not return the account id")}, nil
	}
	// The numeric id never changes, unlike the username, so it binds the
	// connection to the account.
	account := &pluginv1.WatchSyncAccount{
		ExternalSubject: intString(settings.ID),
		Username:        settings.Info.Username,
		DisplayName:     settings.Info.DisplayName,
	}
	if account.DisplayName == "" {
		account.DisplayName = settings.Info.Username
	}
	if settings.Info.Username != "" {
		account.ProfileUrl = profileURLBase + url.PathEscape(settings.Info.Username)
	}
	if avatar := strings.TrimSpace(settings.Images.Avatar); avatar != "" {
		if strings.HasPrefix(avatar, "https://") {
			account.AvatarUrl = avatar
		} else {
			account.AvatarUrl = avatarURLBase + url.PathEscape(strings.TrimPrefix(avatar, "/"))
		}
	}
	return &pluginv1.WatchSyncGetAccountResponse{Account: account}, nil
}

// RefreshCredentials exchanges the refresh token for new tokens. WeTrakr
// rotates refresh tokens on every refresh, so the response replaces both.
func (s *Server) RefreshCredentials(ctx context.Context, req *pluginv1.WatchSyncRefreshCredentialsRequest) (*pluginv1.WatchSyncCredentialResponse, error) {
	auth := req.GetContext()
	client, fault := s.appClient(auth.GetCapabilityId(), auth.GetProviderConfig())
	if fault != nil {
		return &pluginv1.WatchSyncCredentialResponse{Fault: fault}, nil
	}
	refreshToken := strings.TrimSpace(auth.GetCredentials().GetRefreshToken())
	if refreshToken == "" {
		return &pluginv1.WatchSyncCredentialResponse{Fault: newFault(pluginv1.WatchSyncFaultCode_WATCH_SYNC_FAULT_CODE_INVALID_CREDENTIAL,
			"The WeTrakr connection has no refresh token; reconnect WeTrakr")}, nil
	}
	ctx, cancel, _ := s.window(ctx)
	defer cancel()
	var token wtToken
	if _, fault := client.do(ctx, request{
		method: http.MethodPost,
		path:   "/oauth/token/refresh",
		body:   map[string]string{"refresh_token": refreshToken},
	}, &token); fault != nil {
		return &pluginv1.WatchSyncCredentialResponse{Fault: fault}, nil
	}
	credentials := s.credentials(token, refreshToken)
	if credentials == nil {
		return &pluginv1.WatchSyncCredentialResponse{Fault: temporaryFault("WeTrakr returned no access token")}, nil
	}
	return &pluginv1.WatchSyncCredentialResponse{Credentials: credentials}, nil
}

// credentials converts a token response into complete credentials, or nil
// when it holds no access token. A response without a refresh token keeps the
// previous one.
func (s *Server) credentials(token wtToken, previousRefresh string) *pluginv1.WatchSyncCredentials {
	if strings.TrimSpace(token.AccessToken) == "" {
		return nil
	}
	credentials := &pluginv1.WatchSyncCredentials{
		AccessToken:  token.AccessToken,
		RefreshToken: token.RefreshToken,
		TokenType:    "bearer",
	}
	if credentials.RefreshToken == "" {
		credentials.RefreshToken = token.NewRefreshToken
	}
	if credentials.RefreshToken == "" {
		credentials.RefreshToken = previousRefresh
	}
	if token.ExpiresIn > 0 {
		credentials.ExpiresAt = timestamppb.New(s.clock().UTC().Add(time.Duration(token.ExpiresIn) * time.Second))
	}
	return credentials
}

func (s *Server) ApplyEvents(ctx context.Context, req *pluginv1.WatchSyncApplyEventsRequest) (*pluginv1.WatchSyncApplyEventsResponse, error) {
	client, fault := s.authenticatedClient(req.GetContext())
	if fault != nil {
		return &pluginv1.WatchSyncApplyEventsResponse{Fault: fault}, nil
	}
	settings := settingsFrom(req.GetContext())
	ctx, cancel, stopAt := s.window(ctx)
	defer cancel()
	events := req.GetEvents()
	results := make([]*pluginv1.WatchSyncApplyResult, len(events))
	for start := 0; start < len(events); {
		end := groupEnd(events, start)
		if !s.clock().Before(stopAt) {
			for index := start; index < len(events); index++ {
				results[index] = resultFromFault(events[index].GetEventId(), temporaryFault(timeLimitMessage))
			}
			break
		}
		if fault := s.applyGroup(ctx, client, settings, events[start:end], results[start:end]); fault != nil {
			return &pluginv1.WatchSyncApplyEventsResponse{Fault: fault}, nil
		}
		start = end
	}
	for index, result := range results {
		if result == nil {
			results[index] = resultFromFault(events[index].GetEventId(), temporaryFault("WeTrakr sync did not reach this event; it will be retried"))
		}
	}
	// A write changes what the next read should fetch.
	s.activities.forget(client.token)
	return &pluginv1.WatchSyncApplyEventsResponse{Results: results}, nil
}

// groupEnd returns the end of the run of events starting at start that share
// one operation. Batched operations go to WeTrakr in one request; scrobbles
// go one at a time.
func groupEnd(events []*pluginv1.WatchSyncEvent, start int) int {
	operation := events[start].GetOperation()
	if !batched(operation) {
		return start + 1
	}
	end := start + 1
	for end < len(events) && events[end].GetOperation() == operation {
		end++
	}
	return end
}

func batched(operation pluginv1.WatchSyncOperation) bool {
	switch operation {
	case pluginv1.WatchSyncOperation_WATCH_SYNC_OPERATION_MARK_WATCHED,
		pluginv1.WatchSyncOperation_WATCH_SYNC_OPERATION_MARK_UNWATCHED,
		pluginv1.WatchSyncOperation_WATCH_SYNC_OPERATION_ADD_FAVORITE,
		pluginv1.WatchSyncOperation_WATCH_SYNC_OPERATION_REMOVE_FAVORITE,
		pluginv1.WatchSyncOperation_WATCH_SYNC_OPERATION_ADD_TO_WATCHLIST,
		pluginv1.WatchSyncOperation_WATCH_SYNC_OPERATION_REMOVE_FROM_WATCHLIST,
		pluginv1.WatchSyncOperation_WATCH_SYNC_OPERATION_SET_RATING,
		pluginv1.WatchSyncOperation_WATCH_SYNC_OPERATION_REMOVE_RATING,
		pluginv1.WatchSyncOperation_WATCH_SYNC_OPERATION_MARK_DROPPED,
		pluginv1.WatchSyncOperation_WATCH_SYNC_OPERATION_UNMARK_DROPPED:
		return true
	default:
		return false
	}
}

// applyGroup applies events that share one operation and fills out, which is
// parallel to events. A fault that concerns the whole connection is returned
// instead.
func (s *Server) applyGroup(ctx context.Context, client *apiClient, settings connectionSettings, events []*pluginv1.WatchSyncEvent, out []*pluginv1.WatchSyncApplyResult) *pluginv1.WatchSyncFault {
	group := eventGroup{events: events, out: out}
	// A repeated event id is the same event, delivered twice: it shares the
	// first one's result instead of being written twice.
	first := make(map[string]int, len(events))
	for index, event := range events {
		eventID := strings.TrimSpace(event.GetEventId())
		switch {
		case eventID == "":
			out[index] = rejectedResult("", "Watch event ID is required")
			continue
		case event.GetMedia() == nil:
			out[index] = rejectedResult(event.GetEventId(), "Watch event media is required")
			continue
		}
		if earlier, ok := first[eventID]; ok {
			group.repeats = append(group.repeats, [2]int{index, earlier})
			continue
		}
		first[eventID] = index
		group.pending = append(group.pending, index)
	}
	if len(group.pending) == 0 {
		return nil
	}
	var fault *pluginv1.WatchSyncFault
	switch operation := events[group.pending[0]].GetOperation(); operation {
	case pluginv1.WatchSyncOperation_WATCH_SYNC_OPERATION_MARK_WATCHED:
		fault = s.applyWatched(ctx, client, settings, group)
	case pluginv1.WatchSyncOperation_WATCH_SYNC_OPERATION_MARK_UNWATCHED:
		fault = s.applyUnwatched(ctx, client, group)
	case pluginv1.WatchSyncOperation_WATCH_SYNC_OPERATION_ADD_FAVORITE,
		pluginv1.WatchSyncOperation_WATCH_SYNC_OPERATION_REMOVE_FAVORITE:
		fault = s.applyFavorites(ctx, client, operation, group)
	case pluginv1.WatchSyncOperation_WATCH_SYNC_OPERATION_ADD_TO_WATCHLIST,
		pluginv1.WatchSyncOperation_WATCH_SYNC_OPERATION_REMOVE_FROM_WATCHLIST,
		pluginv1.WatchSyncOperation_WATCH_SYNC_OPERATION_MARK_DROPPED,
		pluginv1.WatchSyncOperation_WATCH_SYNC_OPERATION_UNMARK_DROPPED:
		fault = s.applyListStatus(ctx, client, operation, group)
	case pluginv1.WatchSyncOperation_WATCH_SYNC_OPERATION_SET_RATING,
		pluginv1.WatchSyncOperation_WATCH_SYNC_OPERATION_REMOVE_RATING:
		fault = s.applyRatings(ctx, client, operation, group)
	case pluginv1.WatchSyncOperation_WATCH_SYNC_OPERATION_SCROBBLE_START,
		pluginv1.WatchSyncOperation_WATCH_SYNC_OPERATION_SCROBBLE_PAUSE,
		pluginv1.WatchSyncOperation_WATCH_SYNC_OPERATION_SCROBBLE_STOP:
		fault = s.applyScrobble(ctx, client, operation, group)
	default:
		group.rejectAll("WeTrakr does not support this watch operation")
	}
	if fault != nil {
		return fault
	}
	for _, pair := range group.repeats {
		result := out[pair[1]]
		out[pair[0]] = &pluginv1.WatchSyncApplyResult{EventId: events[pair[0]].GetEventId(), Status: result.GetStatus(), Fault: result.GetFault()}
	}
	return nil
}

// eventGroup is a run of events with one operation. pending lists the
// indexes, into events and out, of the events still without a result;
// repeats pairs a repeated event with the first event of its id.
type eventGroup struct {
	events  []*pluginv1.WatchSyncEvent
	out     []*pluginv1.WatchSyncApplyResult
	pending []int
	repeats [][2]int
}

func (g eventGroup) set(index int, status pluginv1.WatchSyncApplyStatus) {
	g.out[index] = &pluginv1.WatchSyncApplyResult{EventId: g.events[index].GetEventId(), Status: status}
}

func (g eventGroup) reject(index int, message string) {
	g.out[index] = rejectedResult(g.events[index].GetEventId(), message)
}

func (g eventGroup) rejectAll(message string) {
	for _, index := range g.pending {
		g.reject(index, message)
	}
}

// fail records a request failure for the given events, or returns the fault
// when it concerns the whole connection.
func (g eventGroup) fail(indexes []int, f *pluginv1.WatchSyncFault) *pluginv1.WatchSyncFault {
	if connectionWide(f) {
		return f
	}
	for _, index := range indexes {
		g.out[index] = resultFromFault(g.events[index].GetEventId(), f)
	}
	return nil
}

func resultFromFault(eventID string, f *pluginv1.WatchSyncFault) *pluginv1.WatchSyncApplyResult {
	status := pluginv1.WatchSyncApplyStatus_WATCH_SYNC_APPLY_STATUS_REJECTED
	if f.GetCode() == pluginv1.WatchSyncFaultCode_WATCH_SYNC_FAULT_CODE_TEMPORARY ||
		f.GetCode() == pluginv1.WatchSyncFaultCode_WATCH_SYNC_FAULT_CODE_RATE_LIMITED {
		status = pluginv1.WatchSyncApplyStatus_WATCH_SYNC_APPLY_STATUS_RETRY
	}
	return &pluginv1.WatchSyncApplyResult{EventId: eventID, Status: status, Fault: f}
}

func rejectedResult(eventID, message string) *pluginv1.WatchSyncApplyResult {
	return resultFromFault(eventID, invalidRequestFault(message))
}

// connectionSettings are the per-profile switches the descriptor declares.
type connectionSettings struct {
	logRewatches bool
}

const settingLogRewatches = "log_rewatches"

func settingsFrom(auth *pluginv1.WatchSyncAuthenticatedContext) connectionSettings {
	settings := connectionSettings{logRewatches: true}
	if value, ok := auth.GetConnectionSettings()[settingLogRewatches]; ok {
		settings.logRewatches = strings.EqualFold(strings.TrimSpace(value), "true")
	}
	return settings
}

// activityCache keeps one account's last activities for a few seconds, so
// the state families of one sync run share a single read.
type activityCache struct {
	mu      sync.Mutex
	entries map[[32]byte]activityEntry
	now     func() time.Time
}

type activityEntry struct {
	at         time.Time
	activities wtActivities
}
