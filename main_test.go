package main

import (
	"testing"

	pluginv1 "github.com/Silo-Server/silo-plugin-sdk/pkg/pluginproto/silo/plugin/v1"
	"github.com/Silo-Server/silo-plugin-sdk/pkg/pluginsdk/manifest"
)

func TestManifestIsValid(t *testing.T) {
	parsed, err := manifest.Load(manifestJSON)
	if err != nil {
		t.Fatalf("load manifest: %v", err)
	}
	if err := manifest.Validate(parsed); err != nil {
		t.Fatalf("validate manifest: %v", err)
	}
	capabilities := parsed.GetCapabilities()
	if len(capabilities) != 1 || capabilities[0].GetId() != "wetrakr" {
		t.Fatalf("capabilities = %v", capabilities)
	}
	descriptor := capabilities[0].GetWatchSyncProvider()
	if methods := descriptor.GetAuthMethods(); len(methods) != 1 || methods[0] != pluginv1.WatchSyncAuthMethod_WATCH_SYNC_AUTH_METHOD_DEVICE_CODE {
		t.Fatalf("auth methods = %v, want device code", methods)
	}
	flags := map[string]bool{
		"import_watched":    descriptor.GetImportWatched(),
		"import_progress":   descriptor.GetImportProgress(),
		"export_watched":    descriptor.GetExportWatched(),
		"export_unwatched":  descriptor.GetExportUnwatched(),
		"import_favorites":  descriptor.GetImportFavorites(),
		"export_favorites":  descriptor.GetExportFavorites(),
		"remove_favorites":  descriptor.GetRemoveFavorites(),
		"import_watchlist":  descriptor.GetImportWatchlist(),
		"export_watchlist":  descriptor.GetExportWatchlist(),
		"remove_watchlist":  descriptor.GetRemoveWatchlist(),
		"scrobble_playback": descriptor.GetScrobblePlayback(),
		"import_ratings":    descriptor.GetImportRatings(),
		"export_ratings":    descriptor.GetExportRatings(),
		"sync_dropped":      descriptor.GetSyncDropped(),
	}
	for flag, set := range flags {
		if !set {
			t.Errorf("%s is not advertised", flag)
		}
	}
	if descriptor.GetProvidesWatchlistOrder() {
		t.Error("WeTrakr's planning list has no order to provide")
	}
	settings := descriptor.GetConnectionSettings()
	if len(settings) != 1 || settings[0].GetKey() != "log_rewatches" || !settings[0].GetDefaultValue().GetBoolValue() {
		t.Fatalf("connection settings = %v, want an on-by-default log_rewatches switch", settings)
	}
}
