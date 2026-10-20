# WeTrakr for Silo

Syncs [Silo](https://siloserver.org) profiles with their [WeTrakr](https://wetrakr.com) accounts through Silo's `watch_sync_provider.v1` plugin contract, and scrobbles what they play.

## What it syncs

| | WeTrakr to Silo | Silo to WeTrakr |
| --- | --- | --- |
| Watched movies and episodes | Imported, with play count and last watch time | Completed plays, with their watch time |
| Unwatched titles | | Clears every play of the title |
| Resume progress | Movies and episodes left halfway (WeTrakr's playing list) | Live playback: start, pause, stop |
| Watchlist | WeTrakr's planning list, movies and shows | Added to and removed from planning |
| Favorites | Movies and shows | Movies, shows and episodes |
| Ratings | Movies and shows, converted to 1 to 10 | Movies, shows and episodes |
| Dropped shows | WeTrakr's dropped list | Dropped and undropped |

Undropping a show, or taking a title off the watchlist, returns it to the state it had on WeTrakr before.

## Install

1. In Silo, open **Admin → Plugins → Repositories** and add `https://wetrakr.github.io/wetrakr-silo/manifest.json`.
2. Install **WeTrakr** from the catalog. There is nothing to configure on the server.
3. Each profile opens its watch-provider settings and connects WeTrakr. Silo shows a six-character code: enter it at [wetrakr.com/activate](https://wetrakr.com/activate) and allow Silo.

The plugin works with any Silo server built on `silo-plugin-sdk` v0.25.0 or later. It keeps watch-sync batches at 100 items, the most those builds accept.

The plugin signs in through WeTrakr's own Silo app. An administrator who wants to use an app of their own can enter its client ID in the plugin settings; profiles connected through one app have to reconnect when it changes.

## How it reads WeTrakr

It follows WeTrakr's [sync guide](https://api.wetrakr.com/#/sync). Every sync first reads `/sync/last_activities`, and then reads only the lists whose timestamps moved since the last sync:

- Watched history is read with `from_date`, so a sync brings only the titles with new plays. The first sync reads the whole history.
- Ratings and favorites are read with `from_date` too, and removals come back as tombstones. The first sync reads them whole.
- The watchlist and dropped shows are read whole whenever they moved, and handed to Silo as complete lists, so a title that left them on WeTrakr leaves them in Silo.
- Resume progress is read whenever the playing list moved.

Lists come in compact pages of 100 rows, the batch size Silo asks for, walked with WeTrakr's cursor. An account that did not change since the last sync costs one request.

## How it writes to WeTrakr

Writes are batched: one request per kind of change, up to 100 items. Each carries an `Idempotency-Key`, so a retried batch is replayed by WeTrakr instead of written twice.

- **Plays.** With **Log rewatches** on (the default), a play of a title the account already watched is logged as a rewatch. A play within an hour of one WeTrakr already holds counts as the same watch, which is what keeps a play scrobbled live and then exported by Silo from being logged twice. With it off, titles already watched are left alone.
- **Episodes** are sent by their own IMDb, TMDB or TVDB id, or by their show's id with season and episode numbers.
- **Scrobbling.** On a stop, the plugin tells WeTrakr Silo's own verdict through `min_progress`: WeTrakr logs a play exactly when Silo counts the playback as completed.

## Limits

The plugin paces each account to stay inside WeTrakr's free-plan rate limits: one write per second and bursts of reads. When WeTrakr asks it to wait ten seconds or less it waits and retries; a daily quota pauses that profile's sync until midnight UTC.

## Development

Requires Go 1.26 or later. The plugin builds against `silo-plugin-sdk` v0.27.0.

```bash
make test
make build CLIENT_ID=<wetrakr app client id>
./plugin manifest
```

`make build-all` builds static binaries for the platforms in `manifest.json`. Release builds take the app key from the `WETRAKR_CLIENT_ID` repository variable. Pushing a `v*` tag whose version matches `manifest.json` publishes the release and adds it to the repository index on GitHub Pages.

## License

AGPL-3.0-only. Parts of the plugin follow Silo's first-party Trakt and Simkl watch-provider plugins, which are AGPL-3.0 as well.
