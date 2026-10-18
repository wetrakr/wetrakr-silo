package provider

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"unicode/utf8"

	pluginv1 "github.com/Silo-Server/silo-plugin-sdk/pkg/pluginproto/silo/plugin/v1"
)

// Write paths. WeTrakr's batched writes take movies, shows, seasons and
// episodes in one body and answer which items it could not find (notFound),
// could not apply (errored, with the reason) or already had (existing).
const (
	trackingPath        = "/sync/tracking"
	trackingRemovePath  = "/sync/tracking/remove"
	trackingRemoveAll   = "/sync/tracking/remove/all"
	favoritesPath       = "/sync/favorites"
	favoritesRemovePath = "/sync/favorites/remove"
	ratingsPath         = "/sync/ratings"
	ratingsRemovePath   = "/sync/ratings/remove"
)

// writeItem is one event's item in a batched write.
type writeItem struct {
	index  int
	kind   string
	ids    wtIDs
	id     int
	show   wtIDs
	season int
	number int
	// nested is set for an episode without ids of its own, sent under its
	// show by season and number.
	nested bool
	fields map[string]any
}

// itemFor resolves the event's media to a WeTrakr write item, or explains why
// it cannot be written.
func itemFor(index int, event *pluginv1.WatchSyncEvent, allowed ...string) (writeItem, string) {
	media := event.GetMedia()
	kind := mediaKind(media)
	if !containsKind(allowed, kind) {
		return writeItem{}, "WeTrakr does not take this media type for this operation"
	}
	item := writeItem{index: index, kind: kind, ids: idsFrom(media.GetExternalIds()), fields: map[string]any{}}
	if item.ids.empty() {
		// A removal may only carry the key WeTrakr returned for the title.
		item.ids = idsFromKey(event.GetProviderItemKey())
		item.id = wetrakrIDFromKey(event.GetProviderItemKey(), kind)
	}
	if kind == kindEpisode && item.ids.empty() && item.id == 0 {
		item.show = idsFrom(media.GetSeriesExternalIds())
		item.season = int(media.GetSeasonNumber())
		item.number = int(media.GetEpisodeNumber())
		if item.show.empty() || item.number <= 0 {
			return writeItem{}, "Episode needs its own IMDb, TMDB or TVDB ID, or its show's ID with season and episode numbers"
		}
		item.nested = true
		return item, ""
	}
	if item.ids.empty() && item.id == 0 {
		return writeItem{}, "Watch event needs an IMDb, TMDB or TVDB ID"
	}
	return item, ""
}

func containsKind(kinds []string, kind string) bool {
	for _, allowed := range kinds {
		if allowed == kind && kind != "" {
			return true
		}
	}
	return false
}

// writeBody builds the batched body: flat movies, shows and episodes, and
// nested shows for episodes without ids of their own.
func writeBody(items []writeItem, extra map[string]any) map[string]any {
	body := map[string]any{}
	var movies, shows, episodes []map[string]any
	nested := map[string]map[string]any{}
	var nestedOrder []string
	for _, item := range items {
		entry := map[string]any{}
		for key, value := range item.fields {
			entry[key] = value
		}
		if item.nested {
			key := showRefKey(item.show)
			show, ok := nested[key]
			if !ok {
				show = map[string]any{"ids": item.show, "seasons": []map[string]any{}}
				nested[key] = show
				nestedOrder = append(nestedOrder, key)
			}
			entry["number"] = item.number
			show["seasons"] = appendEpisode(show["seasons"].([]map[string]any), item.season, entry)
			continue
		}
		if item.id > 0 {
			entry["id"] = item.id
		} else {
			entry["ids"] = item.ids
		}
		switch item.kind {
		case kindMovie:
			movies = append(movies, entry)
		case kindSeries:
			shows = append(shows, entry)
		case kindEpisode:
			episodes = append(episodes, entry)
		}
	}
	for _, key := range nestedOrder {
		shows = append(shows, nested[key])
	}
	if len(movies) > 0 {
		body["movies"] = movies
	}
	if len(shows) > 0 {
		body["shows"] = shows
	}
	if len(episodes) > 0 {
		body["episodes"] = episodes
	}
	for key, value := range extra {
		body[key] = value
	}
	return body
}

func appendEpisode(seasons []map[string]any, season int, episode map[string]any) []map[string]any {
	for _, existing := range seasons {
		if existing["number"] == season {
			existing["episodes"] = append(existing["episodes"].([]map[string]any), episode)
			return seasons
		}
	}
	return append(seasons, map[string]any{"number": season, "episodes": []map[string]any{episode}})
}

func showRefKey(ids wtIDs) string {
	return ids.IMDb + "|" + strconv.Itoa(int(ids.TMDB)) + "|" + strconv.Itoa(int(ids.TVDB))
}

// idempotencyKey names one batched write, so a retry of the same events is
// replayed by WeTrakr instead of written twice.
func idempotencyKey(path string, events []*pluginv1.WatchSyncEvent, items []writeItem) string {
	ids := make([]string, 0, len(items))
	for _, item := range items {
		ids = append(ids, events[item.index].GetEventId())
	}
	sort.Strings(ids)
	sum := sha256.Sum256([]byte(path + "\n" + strings.Join(ids, "\n")))
	return "silo-" + hex.EncodeToString(sum[:20])
}

// writeMode says how a batched write's answers map to results. A removal of
// something already absent converges, so notFound is no change there.
type writeMode int

const (
	modeAdd writeMode = iota
	modeRemove
)

// sendBatch sends items to path and records a result for each. A fault that
// concerns the whole connection is returned instead.
func (s *Server) sendBatch(ctx context.Context, client *apiClient, group eventGroup, path string, items []writeItem, extra map[string]any, mode writeMode) *pluginv1.WatchSyncFault {
	if len(items) == 0 {
		return nil
	}
	indexes := make([]int, 0, len(items))
	for _, item := range items {
		indexes = append(indexes, item.index)
	}
	var result wtWriteResult
	_, fault := client.do(ctx, request{
		method:         http.MethodPost,
		path:           path,
		body:           writeBody(items, extra),
		idempotencyKey: idempotencyKey(path, group.events, items),
	}, &result)
	if fault != nil {
		return group.fail(indexes, fault)
	}
	for _, item := range items {
		status, message := classify(item, result, mode)
		switch {
		case message != "":
			group.reject(item.index, message)
		default:
			group.set(item.index, status)
		}
	}
	return nil
}

// classify finds an item in the answer's echoes. WeTrakr echoes the item as it
// was sent, so it is matched on any shared id; nested episodes that it found
// come back with WeTrakr's own id and cannot be matched, so they count as
// applied unless WeTrakr reported them not found.
func classify(item writeItem, result wtWriteResult, mode writeMode) (pluginv1.WatchSyncApplyStatus, string) {
	applied := pluginv1.WatchSyncApplyStatus_WATCH_SYNC_APPLY_STATUS_APPLIED
	noChange := pluginv1.WatchSyncApplyStatus_WATCH_SYNC_APPLY_STATUS_NO_CHANGE
	if echo, ok := findEcho(item, result.NotFound); ok {
		_ = echo
		if mode == modeRemove {
			return noChange, ""
		}
		return 0, "WeTrakr could not find this title"
	}
	if echo, ok := findEcho(item, result.Errored); ok {
		if converged(echo.Error) {
			return noChange, ""
		}
		return 0, "WeTrakr refused this item: " + safeText(echo.Error)
	}
	if _, ok := findEcho(item, result.Existing); ok {
		return noChange, ""
	}
	return applied, ""
}

// converged reports whether an errored reason means the account already
// holds the desired state: the play or favorite is there, or there is nothing
// left to remove.
func converged(reason string) bool {
	reason = strings.ToLower(strings.TrimSpace(reason))
	return strings.HasPrefix(reason, "already") || strings.HasPrefix(reason, "nothing to remove")
}

func safeText(text string) string {
	text = strings.TrimSpace(text)
	if text == "" {
		return "no reason given"
	}
	if utf8.RuneCountInString(text) > 160 {
		runes := []rune(text)
		text = string(runes[:160]) + "…"
	}
	return text
}

func findEcho(item writeItem, echoes wtEchoes) (wtEcho, bool) {
	var list []wtEcho
	switch item.kind {
	case kindMovie:
		list = echoes.Movies
	case kindSeries:
		list = echoes.Shows
	case kindEpisode:
		list = echoes.Episodes
		if item.nested {
			// A nested episode WeTrakr could not find comes back as
			// { show, season, number }.
			for _, echo := range append(append([]wtEcho{}, echoes.Episodes...), echoes.Shows...) {
				if echo.Show != nil && sharesID(echo.Show.IDs, item.show) && echo.Season == item.season && echo.Number == item.number {
					return echo, true
				}
			}
			return wtEcho{}, false
		}
	}
	for _, echo := range list {
		if item.id > 0 && echo.ID == item.id || sharesID(echo.IDs, item.ids) {
			return echo, true
		}
	}
	return wtEcho{}, false
}

func sharesID(a, b wtIDs) bool {
	return a.IMDb != "" && a.IMDb == b.IMDb ||
		a.TMDB > 0 && a.TMDB == b.TMDB ||
		a.TVDB > 0 && a.TVDB == b.TVDB
}

// collect resolves the pending events into write items, rejecting the ones
// that cannot be written. fill adds each item's fields, or explains why the
// event cannot be written.
func collect(group eventGroup, allowed []string, fill func(event *pluginv1.WatchSyncEvent, item *writeItem) string) []writeItem {
	items := make([]writeItem, 0, len(group.pending))
	for _, index := range group.pending {
		event := group.events[index]
		item, problem := itemFor(index, event, allowed...)
		if problem == "" && fill != nil {
			problem = fill(event, &item)
		}
		if problem != "" {
			group.reject(index, problem)
			continue
		}
		items = append(items, item)
	}
	return items
}

// applyWatched logs completed plays. With Log rewatches on, a play of a title
// the account already watched is a new play, except one within an hour of a
// play WeTrakr already holds, which WeTrakr counts as the same watch: that is
// what keeps a play Silo sends after scrobbling it, or a retried event, from
// being logged twice. With it off, titles already watched are left alone.
func (s *Server) applyWatched(ctx context.Context, client *apiClient, settings connectionSettings, group eventGroup) *pluginv1.WatchSyncFault {
	items := collect(group, []string{kindMovie, kindEpisode}, func(event *pluginv1.WatchSyncEvent, item *writeItem) string {
		occurredAt := event.GetOccurredAt()
		if occurredAt == nil || occurredAt.CheckValid() != nil {
			return "Watched event needs a valid watch time"
		}
		item.fields["status"] = "watched"
		item.fields["tracked_at"] = isoTime(occurredAt.AsTime())
		return ""
	})
	return s.sendBatch(ctx, client, group, trackingPath, items, map[string]any{"allow_rewatch": settings.logRewatches}, modeAdd)
}

// applyUnwatched clears every play of each title, which is what unwatching a
// title in Silo means.
func (s *Server) applyUnwatched(ctx context.Context, client *apiClient, group eventGroup) *pluginv1.WatchSyncFault {
	items := collect(group, []string{kindMovie, kindEpisode, kindSeries}, func(_ *pluginv1.WatchSyncEvent, item *writeItem) string {
		item.fields["status"] = "watched"
		return ""
	})
	return s.sendBatch(ctx, client, group, trackingRemoveAll, items, nil, modeRemove)
}

func (s *Server) applyFavorites(ctx context.Context, client *apiClient, operation pluginv1.WatchSyncOperation, group eventGroup) *pluginv1.WatchSyncFault {
	items := collect(group, []string{kindMovie, kindSeries, kindEpisode}, func(_ *pluginv1.WatchSyncEvent, item *writeItem) string {
		if item.nested {
			return "WeTrakr favorites an episode by its own IMDb, TMDB or TVDB ID"
		}
		return ""
	})
	if operation == pluginv1.WatchSyncOperation_WATCH_SYNC_OPERATION_REMOVE_FAVORITE {
		return s.sendBatch(ctx, client, group, favoritesRemovePath, items, nil, modeRemove)
	}
	return s.sendBatch(ctx, client, group, favoritesPath, items, nil, modeAdd)
}

// applyListStatus maps the Silo watchlist to WeTrakr's planning list and
// dropped shows to its dropped list. Taking a title off either list returns it
// to the state it had before, as WeTrakr does when a list status is removed.
func (s *Server) applyListStatus(ctx context.Context, client *apiClient, operation pluginv1.WatchSyncOperation, group eventGroup) *pluginv1.WatchSyncFault {
	status, allowed, path, mode := "planning", []string{kindMovie, kindSeries}, trackingPath, modeAdd
	switch operation {
	case pluginv1.WatchSyncOperation_WATCH_SYNC_OPERATION_REMOVE_FROM_WATCHLIST:
		path, mode = trackingRemovePath, modeRemove
	case pluginv1.WatchSyncOperation_WATCH_SYNC_OPERATION_MARK_DROPPED:
		status, allowed = "dropped", []string{kindSeries}
	case pluginv1.WatchSyncOperation_WATCH_SYNC_OPERATION_UNMARK_DROPPED:
		status, allowed, path, mode = "dropped", []string{kindSeries}, trackingRemovePath, modeRemove
	}
	items := collect(group, allowed, func(_ *pluginv1.WatchSyncEvent, item *writeItem) string {
		item.fields["status"] = status
		return ""
	})
	return s.sendBatch(ctx, client, group, path, items, nil, mode)
}

// applyRatings sets and clears ratings. The contract's 1 to 10 integers are
// already on WeTrakr's 0 to 10 scale.
func (s *Server) applyRatings(ctx context.Context, client *apiClient, operation pluginv1.WatchSyncOperation, group eventGroup) *pluginv1.WatchSyncFault {
	remove := operation == pluginv1.WatchSyncOperation_WATCH_SYNC_OPERATION_REMOVE_RATING
	items := collect(group, []string{kindMovie, kindSeries, kindEpisode}, func(event *pluginv1.WatchSyncEvent, item *writeItem) string {
		if remove {
			return ""
		}
		rating := event.GetRating()
		if rating < 1 || rating > 10 {
			return "Rating must be an integer from 1 to 10"
		}
		item.fields["rating"] = rating
		return ""
	})
	if remove {
		return s.sendBatch(ctx, client, group, ratingsRemovePath, items, nil, modeRemove)
	}
	return s.sendBatch(ctx, client, group, ratingsPath, items, nil, modeAdd)
}
