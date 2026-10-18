package provider

import (
	"context"
	"net/http"
	"testing"

	pluginv1 "github.com/Silo-Server/silo-plugin-sdk/pkg/pluginproto/silo/plugin/v1"
)

func list(t *testing.T, server *Server, kind pluginv1.WatchSyncRemoteStateKind, cursor, pageToken string) *pluginv1.WatchSyncListRemoteStateResponse {
	t.Helper()
	resp, err := server.ListRemoteState(context.Background(), &pluginv1.WatchSyncListRemoteStateRequest{
		Context: authContext(), Cursor: cursor, PageToken: pageToken, PageSize: 1000,
		StateKinds: []pluginv1.WatchSyncRemoteStateKind{kind},
	})
	if err != nil || resp.GetFault() != nil {
		t.Fatalf("ListRemoteState: %v %v", err, resp.GetFault())
	}
	return resp
}

// traverse walks every page of a traversal and returns the items and the
// final page.
func traverse(t *testing.T, server *Server, kind pluginv1.WatchSyncRemoteStateKind, cursor string) ([]*pluginv1.WatchSyncRemoteState, *pluginv1.WatchSyncListRemoteStateResponse) {
	t.Helper()
	var items []*pluginv1.WatchSyncRemoteState
	token := ""
	for page := 0; page < 20; page++ {
		resp := list(t, server, kind, cursor, token)
		items = append(items, resp.GetItems()...)
		if resp.GetNextPageToken() == "" {
			return items, resp
		}
		if resp.GetNextCursor() != "" {
			t.Fatal("a durable cursor before the final page")
		}
		token = resp.GetNextPageToken()
	}
	t.Fatal("traversal did not end")
	return nil, nil
}

func activities(stamps map[string]map[string]string) http.HandlerFunc {
	return jsonReply(200, stamps)
}

func TestWatchedTraversalWalksCursorsAndThenReadsOnlyChanges(t *testing.T) {
	fake, server := newFake(t)
	fake.handle("GET", "/sync/last_activities", activities(map[string]map[string]string{
		"movies":   {"last_tracking_watched_at": "2026-10-10T10:00:00.000Z"},
		"episodes": {"last_tracking_watched_at": "2026-10-11T10:00:00.000Z"},
	}))
	fake.handle("GET", "/sync/tracking/watched/movies", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("after") == "" {
			w.Header().Set("X-Pagination-Next", "cursor-2")
			jsonReply(200, []map[string]any{{"type": "movie", "id": 126, "ids": map[string]any{"tmdb": 155, "imdb": "tt0468569"}, "last_watched_at": "2026-09-12T20:41:00.000Z", "plays": 2}})(w, r)
			return
		}
		jsonReply(200, []map[string]any{{"type": "movie", "id": 7, "ids": map[string]any{}, "last_watched_at": "2026-09-12T20:41:00.000Z"}})(w, r)
	})
	fake.handle("GET", "/sync/tracking/watched/episodes", jsonReply(200, []map[string]any{{
		"type": "episode", "id": 62085, "ids": map[string]any{"tvdb": 349232}, "show_ids": map[string]any{"tvdb": 81189},
		"season_number": 1, "number": 1, "last_watched_at": "2026-09-13T20:00:00.000Z", "plays": 1,
	}}))

	kind := pluginv1.WatchSyncRemoteStateKind_WATCH_SYNC_REMOTE_STATE_KIND_WATCHED
	items, last := traverse(t, server, kind, "")
	if len(items) != 2 {
		t.Fatalf("items = %v", items)
	}
	movie := items[0]
	if movie.GetProviderItemKey() != "imdb:tt0468569" || movie.GetWatched().GetPlayCount() != 2 || movie.GetMedia().GetExternalIds()["tmdb"] != "155" {
		t.Fatalf("movie = %v", movie)
	}
	episode := items[1]
	if episode.GetProviderItemKey() != "tvdb:349232" || episode.GetMedia().GetSeriesExternalIds()["tvdb"] != "81189" || episode.GetMedia().GetEpisodeNumber() != 1 {
		t.Fatalf("episode = %v", episode)
	}
	if len(last.GetWarnings()) != 1 {
		t.Fatalf("warnings = %v, want one for the movie without ids", last.GetWarnings())
	}
	if last.GetCompleteSnapshot() {
		t.Fatal("watched reads are incremental")
	}
	moviesCalls := fake.calls("GET", "/sync/tracking/watched/movies")
	if q := moviesCalls[0].query; q["compact"][0] != "true" || q["limit"][0] != "1000" || q["from_date"] != nil {
		t.Fatalf("first read query = %v", q)
	}
	if moviesCalls[1].query["after"][0] != "cursor-2" {
		t.Fatalf("second page must send the cursor, got %v", moviesCalls[1].query)
	}
	cursor := last.GetNextCursor()

	// Nothing moved: no list is read.
	server.activities.forget(testToken)
	_, again := traverse(t, server, kind, cursor)
	if len(fake.calls("GET", "/sync/tracking/watched/movies")) != 2 || again.GetNextCursor() != cursor {
		t.Fatal("an unchanged account must not be read again")
	}

	// Only episodes moved: only episodes are read, from the previous mark.
	fake.handle("GET", "/sync/last_activities", activities(map[string]map[string]string{
		"movies":   {"last_tracking_watched_at": "2026-10-10T10:00:00.000Z"},
		"episodes": {"last_tracking_watched_at": "2026-10-12T08:00:00.000Z"},
	}))
	server.activities.forget(testToken)
	traverse(t, server, kind, cursor)
	if len(fake.calls("GET", "/sync/tracking/watched/movies")) != 2 {
		t.Fatal("movies did not change and must not be read")
	}
	episodeCalls := fake.calls("GET", "/sync/tracking/watched/episodes")
	if got := episodeCalls[len(episodeCalls)-1].query["from_date"][0]; got != "2026-10-11T09:58:00.000Z" {
		t.Fatalf("from_date = %q, want the previous mark minus the overlap", got)
	}
}

func TestWatchlistIsACompleteSnapshotWhenItMoves(t *testing.T) {
	fake, server := newFake(t)
	fake.handle("GET", "/sync/last_activities", activities(map[string]map[string]string{
		"movies": {"last_tracking_planning_at": "2026-10-10T10:00:00.000Z"},
	}))
	fake.handle("GET", "/sync/tracking/planning/movies", jsonReply(200, []map[string]any{
		{"type": "movie", "id": 126, "ids": map[string]any{"tmdb": 155}, "status": "planning", "tracked_at": "2026-09-20T18:02:00.000Z"},
	}))
	fake.handle("GET", "/sync/tracking/planning/shows", jsonReply(200, []map[string]any{
		{"type": "show", "id": 1396, "ids": map[string]any{"tmdb": 1396, "tvdb": 81189}, "status": "planning"},
	}))
	items, last := traverse(t, server, pluginv1.WatchSyncRemoteStateKind_WATCH_SYNC_REMOTE_STATE_KIND_WATCHLIST, "")
	if len(items) != 2 || !last.GetCompleteSnapshot() {
		t.Fatalf("items %v complete %v", items, last.GetCompleteSnapshot())
	}
	if items[1].GetProviderItemKey() != "tvdb:81189" || items[1].GetMedia().GetMediaType() != pluginv1.WatchSyncMediaType_WATCH_SYNC_MEDIA_TYPE_SERIES || items[1].GetWatchlist() == nil {
		t.Fatalf("show = %v", items[1])
	}
	if items[0].GetWatchlist().GetListedAt() == nil {
		t.Fatal("listed_at should carry tracked_at")
	}
}

func TestRatingsTurnIncrementalWithTombstones(t *testing.T) {
	fake, server := newFake(t)
	stamp := "2026-10-10T10:00:00.000Z"
	fake.handle("GET", "/sync/last_activities", func(w http.ResponseWriter, r *http.Request) {
		activities(map[string]map[string]string{"ratings": {"all": stamp}})(w, r)
	})
	fake.handle("GET", "/sync/ratings/movies", jsonReply(200, []map[string]any{
		{"type": "movie", "id": 126, "ids": map[string]any{"tmdb": 155}, "rating": 8.5, "rated_at": "2026-09-12T20:41:00.000Z"},
		{"type": "movie", "id": 127, "ids": map[string]any{"tmdb": 156}, "status": "removed", "removed_at": "2026-09-13T20:41:00.000Z"},
	}))
	fake.handle("GET", "/sync/ratings/shows", jsonReply(200, []map[string]any{}))
	kind := pluginv1.WatchSyncRemoteStateKind_WATCH_SYNC_REMOTE_STATE_KIND_RATING
	items, first := traverse(t, server, kind, "")
	if !first.GetCompleteSnapshot() {
		t.Fatal("the first ratings read is a complete snapshot")
	}
	if items[0].GetRating().GetRating() != 9 || !items[1].GetRating().GetRemoved() {
		t.Fatalf("ratings = %v", items)
	}
	stamp = "2026-10-12T10:00:00.000Z"
	server.activities.forget(testToken)
	_, next := traverse(t, server, kind, first.GetNextCursor())
	if next.GetCompleteSnapshot() {
		t.Fatal("later ratings reads are incremental")
	}
	calls := fake.calls("GET", "/sync/ratings/movies")
	if calls[1].query["from_date"][0] != "2026-10-10T09:58:00.000Z" {
		t.Fatalf("from_date = %v", calls[1].query)
	}
}

func TestProgressAndDropped(t *testing.T) {
	fake, server := newFake(t)
	fake.handle("GET", "/sync/last_activities", activities(map[string]map[string]string{
		"movies": {"last_playback_at": "2026-10-10T10:00:00.000Z"},
		"shows":  {"last_tracking_dropped_at": "2026-10-10T10:00:00.000Z"},
	}))
	fake.handle("GET", "/sync/tracking/playing/movies", jsonReply(200, []map[string]any{
		{"type": "movie", "id": 126, "ids": map[string]any{"tmdb": 155}, "status": "paused", "progress_percent": 42.5, "tracked_at": "2026-10-10T09:00:00.000Z"},
	}))
	fake.handle("GET", "/sync/tracking/playing/episodes", jsonReply(200, []map[string]any{}))
	fake.handle("GET", "/sync/tracking/dropped/shows", jsonReply(200, []map[string]any{
		{"type": "show", "id": 1, "ids": map[string]any{"tvdb": 5}, "status": "dropped"},
	}))
	progress, _ := traverse(t, server, pluginv1.WatchSyncRemoteStateKind_WATCH_SYNC_REMOTE_STATE_KIND_PROGRESS, "")
	if len(progress) != 1 || progress[0].GetProgress().GetProgressPercent() != 42.5 || progress[0].GetProgress().GetPausedAt() == nil {
		t.Fatalf("progress = %v", progress)
	}
	dropped, last := traverse(t, server, pluginv1.WatchSyncRemoteStateKind_WATCH_SYNC_REMOTE_STATE_KIND_DROPPED, "")
	if len(dropped) != 1 || dropped[0].GetDropped() == nil || !last.GetCompleteSnapshot() {
		t.Fatalf("dropped = %v", dropped)
	}
	// One read of last activities served both families.
	if n := len(fake.calls("GET", "/sync/last_activities")); n != 1 {
		t.Fatalf("last activities read %d times", n)
	}
}

func TestRatingConversion(t *testing.T) {
	for in, want := range map[float64]int32{0: 1, 0.4: 1, 5.5: 6, 7.4: 7, 9.95: 10, 10: 10} {
		if got := toSiloRating(in); got != want {
			t.Errorf("toSiloRating(%v) = %d, want %d", in, got, want)
		}
	}
}
