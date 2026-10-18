package provider

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	pluginv1 "github.com/Silo-Server/silo-plugin-sdk/pkg/pluginproto/silo/plugin/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const testToken = "access-token"

// fakeWeTrakr records every request and answers with the handler for its
// method and path.
type fakeWeTrakr struct {
	t        *testing.T
	mu       sync.Mutex
	requests []recorded
	handlers map[string]http.HandlerFunc
}

type recorded struct {
	method string
	path   string
	query  map[string][]string
	header http.Header
	body   map[string]any
}

func newFake(t *testing.T) (*fakeWeTrakr, *Server) {
	t.Helper()
	fake := &fakeWeTrakr{t: t, handlers: map[string]http.HandlerFunc{}}
	srv := httptest.NewServer(fake)
	t.Cleanup(srv.Close)
	server := NewServer(srv.Client(), "1.2.3")
	server.baseURL = srv.URL
	server.sleep = func(context.Context, time.Duration) error { return nil }
	// Pacing is tested on its own; here it would only slow the tests.
	server.writes = newCredentialLimiter(time.Nanosecond, 1000)
	server.reads = newCredentialLimiter(time.Nanosecond, 1000)
	return fake, server
}

func (f *fakeWeTrakr) handle(method, path string, handler http.HandlerFunc) {
	f.handlers[method+" "+path] = handler
}

func (f *fakeWeTrakr) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	data, _ := io.ReadAll(r.Body)
	var body map[string]any
	if len(data) > 0 {
		if err := json.Unmarshal(data, &body); err != nil {
			f.t.Errorf("request body is not JSON: %s", data)
		}
	}
	f.mu.Lock()
	f.requests = append(f.requests, recorded{method: r.Method, path: r.URL.Path, query: r.URL.Query(), header: r.Header.Clone(), body: body})
	handler, ok := f.handlers[r.Method+" "+r.URL.Path]
	f.mu.Unlock()
	if !ok {
		f.t.Errorf("unexpected request %s %s", r.Method, r.URL.String())
		http.Error(w, "not found", http.StatusNotFound)
		return
	}
	handler(w, r)
}

func (f *fakeWeTrakr) calls(method, path string) []recorded {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []recorded
	for _, request := range f.requests {
		if request.method == method && request.path == path {
			out = append(out, request)
		}
	}
	return out
}

func jsonReply(status int, body any) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_ = json.NewEncoder(w).Encode(body)
	}
}

func authContext() *pluginv1.WatchSyncAuthenticatedContext {
	return &pluginv1.WatchSyncAuthenticatedContext{
		CapabilityId:   capabilityID,
		ProviderConfig: &pluginv1.WatchSyncProviderConfig{Values: map[string]string{configClientID: "client-123"}},
		Credentials:    &pluginv1.WatchSyncCredentials{AccessToken: testToken, RefreshToken: "refresh-token"},
	}
}

func movieEvent(id string, operation pluginv1.WatchSyncOperation, tmdb string) *pluginv1.WatchSyncEvent {
	return &pluginv1.WatchSyncEvent{
		EventId:    id,
		Operation:  operation,
		OccurredAt: timestamppb.New(time.Date(2026, 10, 1, 21, 0, 0, 0, time.UTC)),
		Media: &pluginv1.WatchSyncMedia{
			MediaType:   pluginv1.WatchSyncMediaType_WATCH_SYNC_MEDIA_TYPE_MOVIE,
			ExternalIds: map[string]string{"tmdb": tmdb},
		},
	}
}

func episodeEvent(id string, operation pluginv1.WatchSyncOperation, own map[string]string, showTVDB string, season, number int32) *pluginv1.WatchSyncEvent {
	return &pluginv1.WatchSyncEvent{
		EventId:    id,
		Operation:  operation,
		OccurredAt: timestamppb.New(time.Date(2026, 10, 1, 22, 0, 0, 0, time.UTC)),
		Media: &pluginv1.WatchSyncMedia{
			MediaType:         pluginv1.WatchSyncMediaType_WATCH_SYNC_MEDIA_TYPE_EPISODE,
			ExternalIds:       own,
			SeriesExternalIds: map[string]string{"tvdb": showTVDB},
			SeasonNumber:      season,
			EpisodeNumber:     number,
		},
	}
}

func statuses(results []*pluginv1.WatchSyncApplyResult) []pluginv1.WatchSyncApplyStatus {
	out := make([]pluginv1.WatchSyncApplyStatus, len(results))
	for i, result := range results {
		out[i] = result.GetStatus()
	}
	return out
}
