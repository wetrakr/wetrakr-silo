package provider

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	pluginv1 "github.com/Silo-Server/silo-plugin-sdk/pkg/pluginproto/silo/plugin/v1"
)

// Silo item kinds. TMDB and TVDB number movies, shows and episodes
// independently, so ids are always read together with their kind.
const (
	kindMovie   = "movie"
	kindEpisode = "episode"
	kindSeries  = "series"
)

// External id namespaces, as the host names them and as WeTrakr's ids object
// names them.
const (
	idIMDb = "imdb"
	idTMDB = "tmdb"
	idTVDB = "tvdb"
)

// flexInt decodes a JSON number or a numeric string. WeTrakr sends TMDB and
// TVDB ids as numbers, but a string never breaks a sync.
type flexInt int

func (v *flexInt) UnmarshalJSON(data []byte) error {
	text := strings.Trim(string(data), `"`)
	if text == "" || text == "null" {
		*v = 0
		return nil
	}
	parsed, err := strconv.ParseFloat(text, 64)
	if err != nil {
		// An id we cannot read is an absent id, not a failed page.
		*v = 0
		return nil
	}
	*v = flexInt(int(parsed))
	return nil
}

// wtIDs is WeTrakr's ids object. Other ids (letterboxd, anime ids) are
// ignored: Silo matches on IMDb, TMDB and TVDB.
type wtIDs struct {
	IMDb string  `json:"imdb,omitempty"`
	TMDB flexInt `json:"tmdb,omitempty"`
	TVDB flexInt `json:"tvdb,omitempty"`
}

func (ids wtIDs) empty() bool {
	return ids.IMDb == "" && ids.TMDB == 0 && ids.TVDB == 0
}

// MarshalJSON writes only the ids that are set, as WeTrakr expects them:
// numbers for TMDB and TVDB.
func (ids wtIDs) MarshalJSON() ([]byte, error) {
	out := make(map[string]any, 3)
	if ids.IMDb != "" {
		out[idIMDb] = ids.IMDb
	}
	if ids.TMDB > 0 {
		out[idTMDB] = int(ids.TMDB)
	}
	if ids.TVDB > 0 {
		out[idTVDB] = int(ids.TVDB)
	}
	return json.Marshal(out)
}

// idsFrom converts host external ids to WeTrakr ids.
func idsFrom(external map[string]string) wtIDs {
	return wtIDs{
		IMDb: normalizeIMDb(external[idIMDb]),
		TMDB: flexInt(parseInt(external[idTMDB])),
		TVDB: flexInt(parseInt(external[idTVDB])),
	}
}

func normalizeIMDb(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return ""
	}
	if !strings.HasPrefix(value, "tt") {
		if _, err := strconv.Atoi(value); err == nil {
			return "tt" + value
		}
	}
	return value
}

// externalIDs converts WeTrakr ids to the external id namespaces the host
// matches on.
func externalIDs(ids wtIDs) map[string]string {
	out := make(map[string]string, 3)
	if ids.IMDb != "" {
		out[idIMDb] = ids.IMDb
	}
	if value := intString(int(ids.TMDB)); value != "" {
		out[idTMDB] = value
	}
	if value := intString(int(ids.TVDB)); value != "" {
		out[idTVDB] = value
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

// Provider item keys are stored by the host for every synced item. They use
// the host's own scheme for local plays (providerItemKeyForLocalPlay), so a
// title imported from WeTrakr and the same title played in Silo share a key.
// A title WeTrakr holds without any external id falls back to its WeTrakr id.

func movieKey(ids wtIDs, wetrakrID int) string {
	switch {
	case ids.IMDb != "":
		return "imdb:" + ids.IMDb
	case ids.TMDB > 0:
		return "tmdb:" + strconv.Itoa(int(ids.TMDB))
	case ids.TVDB > 0:
		return "tvdb:" + strconv.Itoa(int(ids.TVDB))
	case wetrakrID > 0:
		return "wetrakr:movie:" + strconv.Itoa(wetrakrID)
	default:
		return ""
	}
}

func showKey(ids wtIDs, wetrakrID int) string {
	switch {
	case ids.TVDB > 0:
		return "tvdb:" + strconv.Itoa(int(ids.TVDB))
	case ids.TMDB > 0:
		return "tmdb:" + strconv.Itoa(int(ids.TMDB))
	case ids.IMDb != "":
		return "imdb:" + ids.IMDb
	case wetrakrID > 0:
		return "wetrakr:show:" + strconv.Itoa(wetrakrID)
	default:
		return ""
	}
}

func episodeKey(own wtIDs, show wtIDs, season, episode int, wetrakrID int) string {
	switch {
	case own.TVDB > 0:
		return "tvdb:" + strconv.Itoa(int(own.TVDB))
	case own.TMDB > 0:
		return "tmdb:" + strconv.Itoa(int(own.TMDB))
	case show.TVDB > 0:
		return fmt.Sprintf("show:tvdb:%d:s%d:e%d", int(show.TVDB), season, episode)
	case show.TMDB > 0:
		return fmt.Sprintf("show:tmdb:%d:s%d:e%d", int(show.TMDB), season, episode)
	case show.IMDb != "":
		return fmt.Sprintf("show:imdb:%s:s%d:e%d", show.IMDb, season, episode)
	case wetrakrID > 0:
		return "wetrakr:episode:" + strconv.Itoa(wetrakrID)
	default:
		return ""
	}
}

// wetrakrIDFromKey returns the WeTrakr id of a key the plugin built for a
// title without external ids, so a removal can still address it.
func wetrakrIDFromKey(key, kind string) int {
	rest, ok := strings.CutPrefix(key, "wetrakr:")
	if !ok {
		return 0
	}
	keyKind, value, ok := strings.Cut(rest, ":")
	if !ok {
		return 0
	}
	switch {
	case kind == kindMovie && keyKind == "movie",
		kind == kindSeries && keyKind == "show",
		kind == kindEpisode && keyKind == "episode":
		return parseInt(value)
	}
	return 0
}

// idsFromKey recovers external ids from a key the plugin or the host built,
// for removals whose media arrive without ids.
func idsFromKey(key string) wtIDs {
	prefix, value, ok := strings.Cut(key, ":")
	if !ok || value == "" {
		return wtIDs{}
	}
	switch prefix {
	case idIMDb:
		return wtIDs{IMDb: value}
	case idTMDB:
		return wtIDs{TMDB: flexInt(parseInt(value))}
	case idTVDB:
		return wtIDs{TVDB: flexInt(parseInt(value))}
	default:
		return wtIDs{}
	}
}

// mediaKind maps a host media type to the plugin's item kind.
func mediaKind(media *pluginv1.WatchSyncMedia) string {
	switch media.GetMediaType() {
	case pluginv1.WatchSyncMediaType_WATCH_SYNC_MEDIA_TYPE_MOVIE:
		return kindMovie
	case pluginv1.WatchSyncMediaType_WATCH_SYNC_MEDIA_TYPE_EPISODE:
		return kindEpisode
	case pluginv1.WatchSyncMediaType_WATCH_SYNC_MEDIA_TYPE_SERIES:
		return kindSeries
	default:
		return ""
	}
}

// wtTime decodes an ISO 8601 date, or leaves the zero time for a value that
// does not parse.
type wtTime struct{ time.Time }

func (t *wtTime) UnmarshalJSON(data []byte) error {
	text := strings.Trim(string(data), `"`)
	if text == "" || text == "null" {
		t.Time = time.Time{}
		return nil
	}
	for _, layout := range []string{time.RFC3339Nano, "2006-01-02T15:04:05.000Z", "2006-01-02"} {
		if parsed, err := time.Parse(layout, text); err == nil {
			t.Time = parsed.UTC()
			return nil
		}
	}
	t.Time = time.Time{}
	return nil
}

// isoTime formats a time the way WeTrakr writes its own dates.
func isoTime(t time.Time) string {
	return t.UTC().Format("2006-01-02T15:04:05.000Z")
}
