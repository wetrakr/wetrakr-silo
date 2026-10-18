package provider

import (
	"context"
	"math"
	"net/http"
	"net/url"
	"strconv"

	pluginv1 "github.com/Silo-Server/silo-plugin-sdk/pkg/pluginproto/silo/plugin/v1"
)

// WeTrakr marks a stopped title watched at 80% unless the request names
// another threshold with min_progress, from 50 to 100.
const (
	defaultWatchedPercent = 80
	minWatchedPercent     = 50
)

// applyScrobble forwards one playback event. On a stop the plugin makes
// WeTrakr follow Silo's own verdict: Silo says whether the playback completed
// under its configured threshold, and the request sets min_progress so WeTrakr
// logs a play exactly when Silo counts one.
func (s *Server) applyScrobble(ctx context.Context, client *apiClient, operation pluginv1.WatchSyncOperation, group eventGroup) *pluginv1.WatchSyncFault {
	index := group.pending[0]
	event := group.events[index]
	payload, problem := scrobblePayload(event)
	if problem != "" {
		group.reject(index, problem)
		return nil
	}
	path := "/scrobble/start"
	query := url.Values{}
	switch operation {
	case pluginv1.WatchSyncOperation_WATCH_SYNC_OPERATION_SCROBBLE_PAUSE:
		path = "/scrobble/pause"
	case pluginv1.WatchSyncOperation_WATCH_SYNC_OPERATION_SCROBBLE_STOP:
		path = "/scrobble/stop"
		progress := payload["progress"].(float64)
		threshold := stopThreshold(progress, event.GetCompleted())
		if threshold != defaultWatchedPercent {
			query.Set("min_progress", strconv.Itoa(threshold))
		}
		if event.GetCompleted() && progress < float64(threshold) {
			// Silo counted a play below WeTrakr's lowest threshold.
			payload["progress"] = float64(threshold)
		}
	}
	resp, fault := client.do(ctx, request{method: http.MethodPost, path: path, query: query, body: payload}, nil)
	if fault != nil {
		// A title WeTrakr cannot resolve answers 404 and changes nothing.
		if resp.status == http.StatusNotFound {
			group.reject(index, "WeTrakr could not find this title")
			return nil
		}
		return group.fail([]int{index}, fault)
	}
	group.set(index, pluginv1.WatchSyncApplyStatus_WATCH_SYNC_APPLY_STATUS_APPLIED)
	return nil
}

// stopThreshold is the min_progress that makes WeTrakr log a play when, and
// only when, Silo reports the playback completed.
func stopThreshold(progress float64, completed bool) int {
	if completed {
		if progress >= defaultWatchedPercent {
			return defaultWatchedPercent
		}
		return max(int(math.Floor(progress)), minWatchedPercent)
	}
	if progress >= defaultWatchedPercent {
		// Not complete by Silo's threshold, which is higher than WeTrakr's:
		// keep it paused.
		return 100
	}
	return defaultWatchedPercent
}

// scrobblePayload names a movie by its ids, and an episode by its show's ids
// with its season and episode numbers, which is how WeTrakr's scrobble
// resolves episodes.
func scrobblePayload(event *pluginv1.WatchSyncEvent) (map[string]any, string) {
	media := event.GetMedia()
	progress := event.GetCompletionPercent()
	if event.GetDurationSeconds() > 0 {
		progress = event.GetPositionSeconds() / event.GetDurationSeconds() * 100
	}
	progress = math.Round(math.Min(math.Max(progress, 0), 100)*100) / 100
	payload := map[string]any{"progress": progress, "app_version": "Silo"}
	switch mediaKind(media) {
	case kindMovie:
		ids := idsFrom(media.GetExternalIds())
		if ids.empty() {
			return nil, "Movie needs an IMDb, TMDB or TVDB ID"
		}
		movie := map[string]any{"ids": ids}
		addTitle(movie, media.GetTitle(), media.GetYear())
		payload["movie"] = movie
	case kindEpisode:
		show := idsFrom(media.GetSeriesExternalIds())
		if show.empty() || media.GetEpisodeNumber() <= 0 {
			return nil, "Episode needs its show's IMDb, TMDB or TVDB ID and its season and episode numbers"
		}
		showPayload := map[string]any{"ids": show}
		addTitle(showPayload, media.GetSeriesTitle(), media.GetSeriesYear())
		payload["show"] = showPayload
		payload["episode"] = map[string]any{"season": media.GetSeasonNumber(), "number": media.GetEpisodeNumber()}
	default:
		return nil, "WeTrakr scrobbles movies and episodes only"
	}
	return payload, ""
}

// addTitle adds the title and year, which WeTrakr uses when the ids miss.
func addTitle(target map[string]any, title string, year int32) {
	if title != "" {
		target["title"] = title
	}
	if year > 0 {
		target["year"] = year
	}
}
