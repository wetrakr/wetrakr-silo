package provider

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	pluginv1 "github.com/Silo-Server/silo-plugin-sdk/pkg/pluginproto/silo/plugin/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// How the plugin reads WeTrakr, following its sync guide: every traversal
// starts from GET /sync/last_activities and reads only the lists whose
// timestamps moved since the last traversal. Lists are read in compact mode,
// a few ids and a date per row, walked with WeTrakr's cursor. A traversal's
// durable cursor is the set of activity timestamps it saw.

const (
	// activityCacheTTL lets the state families of one sync run share one read
	// of last activities. It is short enough that the next scheduled run
	// always reads them again.
	activityCacheTTL = 90 * time.Second

	// fromDateOverlap re-reads a little before the previous mark, so a row
	// written in the same instant as the mark is never missed. Imports are
	// idempotent, so the overlap costs nothing but a few rows.
	fromDateOverlap = 2 * time.Minute

	maxCompactPage = 5000

	// maxProgressPercent keeps a resume point inside the contract's [0, 100)
	// range.
	maxProgressPercent = 99.9
)

// Activity stamps a traversal records, by state family.
var familyStamps = map[pluginv1.WatchSyncRemoteStateKind][]string{
	pluginv1.WatchSyncRemoteStateKind_WATCH_SYNC_REMOTE_STATE_KIND_WATCHED:   {"movies.watched", "episodes.watched"},
	pluginv1.WatchSyncRemoteStateKind_WATCH_SYNC_REMOTE_STATE_KIND_PROGRESS:  {"movies.playback", "episodes.playback"},
	pluginv1.WatchSyncRemoteStateKind_WATCH_SYNC_REMOTE_STATE_KIND_WATCHLIST: {"movies.planning", "movies.removed", "shows.planning", "shows.removed"},
	pluginv1.WatchSyncRemoteStateKind_WATCH_SYNC_REMOTE_STATE_KIND_FAVORITE:  {"favorites.all"},
	pluginv1.WatchSyncRemoteStateKind_WATCH_SYNC_REMOTE_STATE_KIND_RATING:    {"ratings.all"},
	pluginv1.WatchSyncRemoteStateKind_WATCH_SYNC_REMOTE_STATE_KIND_DROPPED:   {"shows.dropped", "shows.removed"},
}

func stampValue(activities wtActivities, name string) string {
	switch name {
	case "movies.watched":
		return activities.Movies.WatchedAt
	case "episodes.watched":
		return activities.Episodes.WatchedAt
	case "movies.playback":
		return activities.Movies.PlaybackAt
	case "episodes.playback":
		return activities.Episodes.PlaybackAt
	case "movies.planning":
		return activities.Movies.PlanningAt
	case "movies.removed":
		return activities.Movies.RemovedAt
	case "shows.planning":
		return activities.Shows.PlanningAt
	case "shows.removed":
		return activities.Shows.RemovedAt
	case "shows.dropped":
		return activities.Shows.DroppedAt
	case "favorites.all":
		return activities.Favorites.All
	case "ratings.all":
		return activities.Ratings.All
	}
	return ""
}

// step is one list a traversal walks.
type step struct {
	// Read is what the rows of this list become: watched, progress,
	// watchlist, favorite, rating or dropped.
	Read string `json:"r"`
	Path string `json:"p"`
	// From is the from_date of an incremental read, empty for a full one.
	From string `json:"f,omitempty"`
}

// pageToken carries a traversal between pages. The host keeps the original
// cursor and sends this back for the next page only.
type pageToken struct {
	Kind     int32             `json:"k"`
	Steps    []step            `json:"s"`
	Step     int               `json:"i"`
	After    string            `json:"a,omitempty"`
	Complete bool              `json:"c,omitempty"`
	Next     map[string]string `json:"n,omitempty"`
}

func encodePageToken(token *pageToken) string {
	data, _ := json.Marshal(token)
	return base64.RawURLEncoding.EncodeToString(data)
}

func decodePageToken(value string) (*pageToken, bool) {
	data, err := base64.RawURLEncoding.DecodeString(value)
	if err != nil {
		return nil, false
	}
	var token pageToken
	if json.Unmarshal(data, &token) != nil {
		return nil, false
	}
	return &token, true
}

func decodeCursor(cursor string) map[string]string {
	stamps := map[string]string{}
	if strings.TrimSpace(cursor) == "" {
		return stamps
	}
	_ = json.Unmarshal([]byte(cursor), &stamps)
	return stamps
}

func encodeCursor(stamps map[string]string) string {
	if len(stamps) == 0 {
		return ""
	}
	data, _ := json.Marshal(stamps)
	return string(data)
}

func (s *Server) ListRemoteState(ctx context.Context, req *pluginv1.WatchSyncListRemoteStateRequest) (*pluginv1.WatchSyncListRemoteStateResponse, error) {
	client, fault := s.authenticatedClient(req.GetContext())
	if fault != nil {
		return &pluginv1.WatchSyncListRemoteStateResponse{Fault: fault}, nil
	}
	ctx, cancel, stopAt := s.window(ctx)
	defer cancel()

	var token *pageToken
	if value := strings.TrimSpace(req.GetPageToken()); value != "" {
		decoded, ok := decodePageToken(value)
		if !ok {
			return &pluginv1.WatchSyncListRemoteStateResponse{Fault: invalidRequestFault("WeTrakr page token is invalid")}, nil
		}
		token = decoded
	} else {
		kind, fault := requestedKind(req.GetStateKinds())
		if fault != nil {
			return &pluginv1.WatchSyncListRemoteStateResponse{Fault: fault}, nil
		}
		activities, fault := s.lastActivities(ctx, client)
		if fault != nil {
			return &pluginv1.WatchSyncListRemoteStateResponse{Fault: fault}, nil
		}
		token = planTraversal(kind, decodeCursor(req.GetCursor()), activities)
	}

	pageSize := int(req.GetPageSize())
	if pageSize <= 0 || pageSize > maxCompactPage {
		pageSize = 1000
	}
	response := &pluginv1.WatchSyncListRemoteStateResponse{CompleteSnapshot: token.Complete}
	var warnings warningCounter
	encodedBytes := 0
	for token.Step < len(token.Steps) {
		if !s.clock().Before(stopAt) {
			// Hand back what we have; the next page resumes where we stopped.
			break
		}
		current := token.Steps[token.Step]
		rows, after, fault := s.readCompactPage(ctx, client, current, token.After, pageSize)
		if fault != nil {
			return &pluginv1.WatchSyncListRemoteStateResponse{Fault: fault}, nil
		}
		for _, row := range rows {
			state, warning := remoteState(current.Read, row)
			if warning != "" {
				warnings.add(warning)
				continue
			}
			if state == nil {
				continue
			}
			response.Items = append(response.Items, state)
			encodedBytes += proto.Size(state)
		}
		if after != "" {
			token.After = after
		} else {
			token.Step++
			token.After = ""
		}
		// One upstream page per response once there is something to hand
		// over, and never more than the byte budget.
		if len(response.Items) > 0 || encodedBytes >= s.responseBytes {
			break
		}
	}
	response.Warnings = warnings.list()
	if token.Step < len(token.Steps) {
		response.NextPageToken = encodePageToken(token)
		return response, nil
	}
	response.NextCursor = encodeCursor(token.Next)
	return response, nil
}

func requestedKind(kinds []pluginv1.WatchSyncRemoteStateKind) (pluginv1.WatchSyncRemoteStateKind, *pluginv1.WatchSyncFault) {
	if len(kinds) != 1 {
		// Hosts ask for one family per traversal; a cursor only makes sense
		// for one.
		return 0, invalidRequestFault("WeTrakr reads one state family per traversal")
	}
	if _, ok := familyStamps[kinds[0]]; !ok {
		return 0, invalidRequestFault("WeTrakr does not support the requested state family")
	}
	return kinds[0], nil
}

// planTraversal decides which lists a traversal reads. A family whose stamps
// match the cursor reads nothing: an empty, incremental traversal that leaves
// the host's state as it is.
func planTraversal(kind pluginv1.WatchSyncRemoteStateKind, previous map[string]string, activities wtActivities) *pageToken {
	token := &pageToken{Kind: int32(kind), Next: map[string]string{}}
	changed := map[string]bool{}
	anyChanged := false
	for _, name := range familyStamps[kind] {
		value := stampValue(activities, name)
		if value != "" {
			token.Next[name] = value
		} else if prev := previous[name]; prev != "" {
			token.Next[name] = prev
		}
		// A family never read before, or one whose stamp moved, is read.
		if previous[name] == "" || value != "" && value != previous[name] {
			changed[name] = true
			anyChanged = true
		}
	}
	fresh := len(previous) == 0
	from := func(name string) string {
		if fresh || previous[name] == "" {
			return ""
		}
		parsed, err := time.Parse(time.RFC3339Nano, previous[name])
		if err != nil {
			return ""
		}
		return isoTime(parsed.Add(-fromDateOverlap))
	}
	switch kind {
	case pluginv1.WatchSyncRemoteStateKind_WATCH_SYNC_REMOTE_STATE_KIND_WATCHED:
		// Watched rows only ever add plays in Silo, so incremental reads are
		// enough: from_date brings back every title with a new play.
		if changed["movies.watched"] {
			token.Steps = append(token.Steps, step{Read: readWatched, Path: "/sync/tracking/watched/movies", From: from("movies.watched")})
		}
		if changed["episodes.watched"] {
			token.Steps = append(token.Steps, step{Read: readWatched, Path: "/sync/tracking/watched/episodes", From: from("episodes.watched")})
		}
	case pluginv1.WatchSyncRemoteStateKind_WATCH_SYNC_REMOTE_STATE_KIND_PROGRESS:
		// The playing list is small and has no from_date of its own meaning,
		// so a moved stamp reads it whole. Resume points are only ever newer.
		if changed["movies.playback"] {
			token.Steps = append(token.Steps, step{Read: readProgress, Path: "/sync/tracking/playing/movies"})
		}
		if changed["episodes.playback"] {
			token.Steps = append(token.Steps, step{Read: readProgress, Path: "/sync/tracking/playing/episodes"})
		}
	case pluginv1.WatchSyncRemoteStateKind_WATCH_SYNC_REMOTE_STATE_KIND_WATCHLIST:
		// A tracking list gives no removal rows, so the watchlist is read whole
		// and handed over as a complete snapshot: a title absent from it left
		// the planning list on WeTrakr.
		if anyChanged {
			token.Complete = true
			token.Steps = []step{
				{Read: readWatchlist, Path: "/sync/tracking/planning/movies"},
				{Read: readWatchlist, Path: "/sync/tracking/planning/shows"},
			}
		}
	case pluginv1.WatchSyncRemoteStateKind_WATCH_SYNC_REMOTE_STATE_KIND_DROPPED:
		if anyChanged {
			token.Complete = true
			token.Steps = []step{{Read: readDropped, Path: "/sync/tracking/dropped/shows"}}
		}
	case pluginv1.WatchSyncRemoteStateKind_WATCH_SYNC_REMOTE_STATE_KIND_FAVORITE:
		// Favorites and ratings return removals as tombstones with from_date,
		// so after the first full read they are read incrementally.
		if anyChanged {
			since := from("favorites.all")
			token.Complete = since == ""
			token.Steps = []step{
				{Read: readFavorite, Path: "/sync/favorites/movies", From: since},
				{Read: readFavorite, Path: "/sync/favorites/shows", From: since},
			}
		}
	case pluginv1.WatchSyncRemoteStateKind_WATCH_SYNC_REMOTE_STATE_KIND_RATING:
		if anyChanged {
			since := from("ratings.all")
			token.Complete = since == ""
			token.Steps = []step{
				{Read: readRating, Path: "/sync/ratings/movies", From: since},
				{Read: readRating, Path: "/sync/ratings/shows", From: since},
			}
		}
	}
	return token
}

// What the rows of a step become.
const (
	readWatched   = "watched"
	readProgress  = "progress"
	readWatchlist = "watchlist"
	readFavorite  = "favorite"
	readRating    = "rating"
	readDropped   = "dropped"
)

// readCompactPage reads one compact page of a step, returning its rows and
// the cursor of the next page, if any.
func (s *Server) readCompactPage(ctx context.Context, client *apiClient, current step, after string, limit int) ([]wtCompactRow, string, *pluginv1.WatchSyncFault) {
	query := url.Values{}
	query.Set("compact", "true")
	query.Set("limit", strconv.Itoa(limit))
	if current.From != "" {
		query.Set("from_date", current.From)
	}
	if after != "" {
		query.Set("after", after)
	}
	var rows []wtCompactRow
	resp, fault := client.do(ctx, request{method: http.MethodGet, path: current.Path, query: query}, &rows)
	if fault != nil {
		return nil, "", fault
	}
	next := nextCursor(resp.header)
	if next == after {
		// A cursor that does not move would loop forever.
		next = ""
	}
	return rows, next, nil
}

// lastActivities reads the account's activity stamps, sharing one read
// between the state families of a sync run.
func (s *Server) lastActivities(ctx context.Context, client *apiClient) (wtActivities, *pluginv1.WatchSyncFault) {
	if cached, ok := s.activities.get(client.token); ok {
		return cached, nil
	}
	var activities wtActivities
	if _, fault := client.do(ctx, request{method: http.MethodGet, path: "/sync/last_activities"}, &activities); fault != nil {
		return wtActivities{}, fault
	}
	s.activities.put(client.token, activities)
	return activities, nil
}

func (c *activityCache) get(token string) (wtActivities, bool) {
	key := sha256.Sum256([]byte(token))
	c.mu.Lock()
	defer c.mu.Unlock()
	entry, ok := c.entries[key]
	if !ok || c.now().Sub(entry.at) > activityCacheTTL {
		return wtActivities{}, false
	}
	return entry.activities, true
}

func (c *activityCache) put(token string, activities wtActivities) {
	key := sha256.Sum256([]byte(token))
	now := c.now()
	c.mu.Lock()
	defer c.mu.Unlock()
	for k, entry := range c.entries {
		if now.Sub(entry.at) > activityCacheTTL {
			delete(c.entries, k)
		}
	}
	c.entries[key] = activityEntry{at: now, activities: activities}
}

func (c *activityCache) forget(token string) {
	key := sha256.Sum256([]byte(token))
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.entries, key)
}

// remoteState maps one compact row. A row Silo cannot match is skipped with a
// warning; a row that carries nothing for this family is skipped silently.
func remoteState(read string, row wtCompactRow) (*pluginv1.WatchSyncRemoteState, string) {
	kind := rowKind(row.Type)
	media, key := rowMedia(kind, row)
	removed := row.Status == "removed"
	if key == "" {
		return nil, fmt.Sprintf("wetrakr %s %s skipped because it has no usable external id", read, row.Type)
	}
	state := &pluginv1.WatchSyncRemoteState{ProviderItemKey: key, Media: media}
	switch read {
	case readWatched:
		if kind != kindMovie && kind != kindEpisode {
			return nil, ""
		}
		at := firstTime(row.LastWatched, row.WatchedAt, row.TrackedAt)
		if at.IsZero() {
			return nil, ""
		}
		state.Watched = &pluginv1.WatchSyncRemoteWatchedState{PlayCount: int32(max(row.Plays, 1)), LastWatchedAt: timestamppb.New(at)}
	case readProgress:
		if kind != kindMovie && kind != kindEpisode || row.Progress <= 0 {
			return nil, ""
		}
		progress := &pluginv1.WatchSyncRemoteProgressState{ProgressPercent: math.Min(row.Progress, maxProgressPercent)}
		if !row.TrackedAt.IsZero() {
			progress.PausedAt = timestamppb.New(row.TrackedAt.Time)
		}
		state.Progress = progress
	case readWatchlist:
		state.Watchlist = listState(row.TrackedAt, false)
	case readDropped:
		if kind != kindSeries {
			return nil, ""
		}
		state.Dropped = listState(row.TrackedAt, false)
	case readFavorite:
		state.Favorite = listState(firstTimeValue(row.FavoritedAt, row.RemovedAt), removed)
	case readRating:
		if removed {
			state.Rating = &pluginv1.WatchSyncRemoteRatingState{Removed: true}
			if !row.RemovedAt.IsZero() {
				state.Rating.RatedAt = timestamppb.New(row.RemovedAt.Time)
			}
			break
		}
		state.Rating = &pluginv1.WatchSyncRemoteRatingState{Rating: toSiloRating(row.Rating)}
		if !row.RatedAt.IsZero() {
			state.Rating.RatedAt = timestamppb.New(row.RatedAt.Time)
		}
	default:
		return nil, ""
	}
	return state, ""
}

func listState(at wtTime, removed bool) *pluginv1.WatchSyncRemoteListState {
	state := &pluginv1.WatchSyncRemoteListState{Removed: removed}
	if !at.IsZero() {
		state.ListedAt = timestamppb.New(at.Time)
	}
	return state
}

func rowKind(rowType string) string {
	switch rowType {
	case "movie":
		return kindMovie
	case "show":
		return kindSeries
	case "episode":
		return kindEpisode
	default:
		return ""
	}
}

func rowMedia(kind string, row wtCompactRow) (*pluginv1.WatchSyncMedia, string) {
	switch kind {
	case kindMovie:
		return &pluginv1.WatchSyncMedia{
			MediaType:   pluginv1.WatchSyncMediaType_WATCH_SYNC_MEDIA_TYPE_MOVIE,
			ExternalIds: externalIDs(row.IDs),
		}, movieKey(row.IDs, 0)
	case kindSeries:
		return &pluginv1.WatchSyncMedia{
			MediaType:   pluginv1.WatchSyncMediaType_WATCH_SYNC_MEDIA_TYPE_SERIES,
			ExternalIds: externalIDs(row.IDs),
		}, showKey(row.IDs, 0)
	case kindEpisode:
		if row.ShowIDs.empty() && row.IDs.empty() || row.Number <= 0 && row.IDs.empty() {
			return nil, ""
		}
		return &pluginv1.WatchSyncMedia{
			MediaType:         pluginv1.WatchSyncMediaType_WATCH_SYNC_MEDIA_TYPE_EPISODE,
			ExternalIds:       externalIDs(row.IDs),
			SeriesExternalIds: externalIDs(row.ShowIDs),
			SeasonNumber:      int32(row.SeasonNumber),
			EpisodeNumber:     int32(row.Number),
		}, episodeKey(row.IDs, row.ShowIDs, row.SeasonNumber, row.Number, 0)
	}
	return nil, ""
}

// toSiloRating converts WeTrakr's 0 to 10 scale (one decimal) to the
// contract's integers from 1 to 10, rounding half up.
func toSiloRating(value float64) int32 {
	rounded := int32(math.Floor(value + 0.5))
	return min(max(rounded, 1), 10)
}

func firstTime(values ...wtTime) time.Time {
	for _, value := range values {
		if !value.IsZero() {
			return value.Time
		}
	}
	return time.Time{}
}

func firstTimeValue(values ...wtTime) wtTime {
	for _, value := range values {
		if !value.IsZero() {
			return value
		}
	}
	return wtTime{}
}

// warningCounter combines warnings of the same text into one with a count.
type warningCounter map[string]int

func (w *warningCounter) add(message string) {
	if *w == nil {
		*w = warningCounter{}
	}
	(*w)[message]++
}

func (w warningCounter) list() []string {
	if len(w) == 0 {
		return nil
	}
	out := make([]string, 0, len(w))
	for message, count := range w {
		if count > 1 {
			message = fmt.Sprintf("%s (%d items)", message, count)
		}
		out = append(out, message)
	}
	sort.Strings(out)
	return out
}
