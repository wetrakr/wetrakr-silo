package provider

import (
	"context"
	"net/http"
	"testing"

	pluginv1 "github.com/Silo-Server/silo-plugin-sdk/pkg/pluginproto/silo/plugin/v1"
)

func apply(t *testing.T, server *Server, auth *pluginv1.WatchSyncAuthenticatedContext, events ...*pluginv1.WatchSyncEvent) *pluginv1.WatchSyncApplyEventsResponse {
	t.Helper()
	resp, err := server.ApplyEvents(context.Background(), &pluginv1.WatchSyncApplyEventsRequest{Context: auth, Events: events})
	if err != nil {
		t.Fatal(err)
	}
	return resp
}

func TestWatchedPlaysAreBatchedWithTheirDates(t *testing.T) {
	fake, server := newFake(t)
	fake.handle("POST", "/sync/tracking", jsonReply(200, map[string]any{
		"added": map[string]any{"total": 2},
		"errored": map[string]any{
			"movies": []map[string]any{{"ids": map[string]any{"tmdb": 603}, "status": "watched", "error": "Already added! A play within the hour counts as the same watch"}},
		},
		"notFound": map[string]any{
			"movies": []map[string]any{{"ids": map[string]any{"tmdb": 999}, "status": "watched"}},
		},
	}))
	watched := pluginv1.WatchSyncOperation_WATCH_SYNC_OPERATION_MARK_WATCHED
	resp := apply(t, server, authContext(),
		movieEvent("m1", watched, "155"),
		movieEvent("m2", watched, "603"),
		movieEvent("m3", watched, "999"),
		episodeEvent("e1", watched, map[string]string{"tvdb": "349232"}, "81189", 1, 1),
		episodeEvent("e2", watched, nil, "81189", 2, 3),
		movieEvent("m1", watched, "155"),
	)
	got := statuses(resp.GetResults())
	want := []pluginv1.WatchSyncApplyStatus{applied, noChange, rejected, applied, applied, applied}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("statuses = %v, want %v", got, want)
		}
	}
	calls := fake.calls("POST", "/sync/tracking")
	if len(calls) != 1 {
		t.Fatalf("%d tracking writes, want one batch", len(calls))
	}
	call := calls[0]
	if call.body["allow_rewatch"] != true {
		t.Fatalf("allow_rewatch = %v", call.body["allow_rewatch"])
	}
	if call.header.Get("Idempotency-Key") == "" {
		t.Fatal("batched writes need an idempotency key")
	}
	movies := call.body["movies"].([]any)
	if len(movies) != 3 {
		t.Fatalf("movies = %v (the repeated event must not be sent twice)", movies)
	}
	first := movies[0].(map[string]any)
	if first["status"] != "watched" || first["tracked_at"] != "2026-10-01T21:00:00.000Z" || mustJSON(t, first["ids"]) != `{"tmdb":155}` {
		t.Fatalf("movie item = %v", first)
	}
	episodes := call.body["episodes"].([]any)
	if len(episodes) != 1 || mustJSON(t, episodes[0].(map[string]any)["ids"]) != `{"tvdb":349232}` {
		t.Fatalf("an episode with its own ids goes flat: %v", episodes)
	}
	shows := call.body["shows"].([]any)
	nested := mustJSON(t, shows[0])
	want2 := `{"ids":{"tvdb":81189},"seasons":[{"episodes":[{"number":3,"status":"watched","tracked_at":"2026-10-01T22:00:00.000Z"}],"number":2}]}`
	if nested != want2 {
		t.Fatalf("nested episode = %s\nwant %s", nested, want2)
	}
}

func TestLogRewatchesOffSendsAllowRewatchFalse(t *testing.T) {
	fake, server := newFake(t)
	fake.handle("POST", "/sync/tracking", jsonReply(200, map[string]any{
		"errored": map[string]any{"movies": []map[string]any{{"ids": map[string]any{"tmdb": 155}, "error": "Already watched! Send allow_rewatch: true to add another play"}}},
	}))
	auth := authContext()
	auth.ConnectionSettings = map[string]string{"log_rewatches": "false"}
	resp := apply(t, server, auth, movieEvent("m1", pluginv1.WatchSyncOperation_WATCH_SYNC_OPERATION_MARK_WATCHED, "155"))
	if fake.calls("POST", "/sync/tracking")[0].body["allow_rewatch"] != false || resp.GetResults()[0].GetStatus() != noChange {
		t.Fatalf("response %v", resp)
	}
}

func TestUnwatchedClearsEveryPlayAndConverges(t *testing.T) {
	fake, server := newFake(t)
	fake.handle("POST", "/sync/tracking/remove/all", jsonReply(200, map[string]any{
		"notFound": map[string]any{"movies": []map[string]any{{"ids": map[string]any{"tmdb": 999}}}},
		"errored":  map[string]any{"movies": []map[string]any{{"ids": map[string]any{"tmdb": 603}, "error": "Nothing to remove"}}},
	}))
	unwatched := pluginv1.WatchSyncOperation_WATCH_SYNC_OPERATION_MARK_UNWATCHED
	resp := apply(t, server, authContext(), movieEvent("a", unwatched, "155"), movieEvent("b", unwatched, "603"), movieEvent("c", unwatched, "999"))
	got := statuses(resp.GetResults())
	if got[0] != applied || got[1] != noChange || got[2] != noChange {
		t.Fatalf("statuses = %v", got)
	}
	if item := fake.calls("POST", "/sync/tracking/remove/all")[0].body["movies"].([]any)[0].(map[string]any); item["status"] != "watched" {
		t.Fatalf("item = %v", item)
	}
}

func TestWatchlistAndDroppedMapToTrackingLists(t *testing.T) {
	fake, server := newFake(t)
	fake.handle("POST", "/sync/tracking", jsonReply(200, map[string]any{}))
	fake.handle("POST", "/sync/tracking/remove", jsonReply(200, map[string]any{}))
	series := func(id string, operation pluginv1.WatchSyncOperation) *pluginv1.WatchSyncEvent {
		return &pluginv1.WatchSyncEvent{EventId: id, Operation: operation, Media: &pluginv1.WatchSyncMedia{
			MediaType: pluginv1.WatchSyncMediaType_WATCH_SYNC_MEDIA_TYPE_SERIES, ExternalIds: map[string]string{"tvdb": "81189"},
		}}
	}
	resp := apply(t, server, authContext(),
		movieEvent("w1", pluginv1.WatchSyncOperation_WATCH_SYNC_OPERATION_ADD_TO_WATCHLIST, "155"),
		series("d1", pluginv1.WatchSyncOperation_WATCH_SYNC_OPERATION_MARK_DROPPED),
		series("u1", pluginv1.WatchSyncOperation_WATCH_SYNC_OPERATION_UNMARK_DROPPED),
		movieEvent("d2", pluginv1.WatchSyncOperation_WATCH_SYNC_OPERATION_MARK_DROPPED, "155"),
	)
	got := statuses(resp.GetResults())
	if got[0] != applied || got[1] != applied || got[2] != applied || got[3] != rejected {
		t.Fatalf("statuses = %v (a movie cannot be a dropped show)", got)
	}
	adds := fake.calls("POST", "/sync/tracking")
	if adds[0].body["movies"].([]any)[0].(map[string]any)["status"] != "planning" ||
		adds[1].body["shows"].([]any)[0].(map[string]any)["status"] != "dropped" {
		t.Fatalf("adds = %v", adds)
	}
	removes := fake.calls("POST", "/sync/tracking/remove")
	if len(removes) != 1 || removes[0].body["shows"].([]any)[0].(map[string]any)["status"] != "dropped" {
		t.Fatalf("removes = %v", removes)
	}
}

func TestRatingsAndFavorites(t *testing.T) {
	fake, server := newFake(t)
	fake.handle("POST", "/sync/ratings", jsonReply(200, map[string]any{}))
	fake.handle("POST", "/sync/ratings/remove", jsonReply(200, map[string]any{
		"notFound": map[string]any{"movies": []map[string]any{{"ids": map[string]any{"tmdb": 155}}}},
	}))
	fake.handle("POST", "/sync/favorites/remove", jsonReply(200, map[string]any{
		"errored": map[string]any{"movies": []map[string]any{{"ids": map[string]any{"tmdb": 155}, "error": "Already removed!"}}},
	}))
	rate := movieEvent("r1", pluginv1.WatchSyncOperation_WATCH_SYNC_OPERATION_SET_RATING, "155")
	rate.Rating = 8
	bad := movieEvent("r2", pluginv1.WatchSyncOperation_WATCH_SYNC_OPERATION_SET_RATING, "603")
	resp := apply(t, server, authContext(), rate, bad,
		movieEvent("r3", pluginv1.WatchSyncOperation_WATCH_SYNC_OPERATION_REMOVE_RATING, "155"),
		movieEvent("f1", pluginv1.WatchSyncOperation_WATCH_SYNC_OPERATION_REMOVE_FAVORITE, "155"),
	)
	got := statuses(resp.GetResults())
	if got[0] != applied || got[1] != rejected || got[2] != noChange || got[3] != noChange {
		t.Fatalf("statuses = %v", got)
	}
	if item := fake.calls("POST", "/sync/ratings")[0].body["movies"].([]any)[0].(map[string]any); item["rating"] != float64(8) {
		t.Fatalf("rating item = %v", item)
	}
}

func TestRemovalByProviderKeyOnly(t *testing.T) {
	fake, server := newFake(t)
	fake.handle("POST", "/sync/favorites/remove", jsonReply(200, map[string]any{}))
	event := &pluginv1.WatchSyncEvent{
		EventId: "f1", Operation: pluginv1.WatchSyncOperation_WATCH_SYNC_OPERATION_REMOVE_FAVORITE,
		ProviderItemKey: "wetrakr:movie:126",
		Media:           &pluginv1.WatchSyncMedia{MediaType: pluginv1.WatchSyncMediaType_WATCH_SYNC_MEDIA_TYPE_MOVIE},
	}
	resp := apply(t, server, authContext(), event)
	if resp.GetResults()[0].GetStatus() != applied {
		t.Fatalf("response %v", resp)
	}
	if item := fake.calls("POST", "/sync/favorites/remove")[0].body["movies"].([]any)[0].(map[string]any); item["id"] != float64(126) {
		t.Fatalf("item = %v", item)
	}
}

func TestServerErrorsAreRetried(t *testing.T) {
	fake, server := newFake(t)
	fake.handle("POST", "/sync/favorites", jsonReply(http.StatusBadGateway, map[string]any{}))
	resp := apply(t, server, authContext(), movieEvent("f1", pluginv1.WatchSyncOperation_WATCH_SYNC_OPERATION_ADD_FAVORITE, "155"))
	if resp.GetFault() != nil || resp.GetResults()[0].GetStatus() != retry {
		t.Fatalf("response %v", resp)
	}
}

func TestScrobbleFollowsSilosCompletion(t *testing.T) {
	fake, server := newFake(t)
	fake.handle("POST", "/scrobble/start", jsonReply(201, map[string]any{"action": "start"}))
	fake.handle("POST", "/scrobble/stop", jsonReply(201, map[string]any{"action": "scrobble"}))
	start := episodeEvent("s1", pluginv1.WatchSyncOperation_WATCH_SYNC_OPERATION_SCROBBLE_START, map[string]string{"tvdb": "1"}, "81189", 1, 2)
	start.PositionSeconds, start.DurationSeconds = 300, 3000
	stop := episodeEvent("s2", pluginv1.WatchSyncOperation_WATCH_SYNC_OPERATION_SCROBBLE_STOP, nil, "81189", 1, 2)
	stop.PositionSeconds, stop.DurationSeconds, stop.Completed = 2100, 3000, true
	early := movieEvent("s3", pluginv1.WatchSyncOperation_WATCH_SYNC_OPERATION_SCROBBLE_STOP, "155")
	early.PositionSeconds, early.DurationSeconds = 2550, 3000
	resp := apply(t, server, authContext(), start, stop, early)
	for _, result := range resp.GetResults() {
		if result.GetStatus() != applied {
			t.Fatalf("results = %v", resp.GetResults())
		}
	}
	body := fake.calls("POST", "/scrobble/start")[0].body
	if body["progress"] != float64(10) || mustJSON(t, body["show"]) != `{"ids":{"tvdb":81189}}` || mustJSON(t, body["episode"]) != `{"number":2,"season":1}` {
		t.Fatalf("start body = %v", body)
	}
	stops := fake.calls("POST", "/scrobble/stop")
	if stops[0].query["min_progress"][0] != "70" {
		t.Fatalf("a stop Silo counts complete at 70%% needs min_progress=70, got %v", stops[0].query)
	}
	if stops[1].query["min_progress"][0] != "100" {
		t.Fatalf("a stop Silo does not count complete must stay paused, got %v", stops[1].query)
	}
}

func TestScrobbleOfUnknownTitleIsRejected(t *testing.T) {
	fake, server := newFake(t)
	fake.handle("POST", "/scrobble/pause", jsonReply(404, map[string]any{"error": "NOT_FOUND"}))
	resp := apply(t, server, authContext(), movieEvent("p", pluginv1.WatchSyncOperation_WATCH_SYNC_OPERATION_SCROBBLE_PAUSE, "1"))
	if resp.GetFault() != nil || resp.GetResults()[0].GetStatus() != rejected {
		t.Fatalf("response %v", resp)
	}
}
