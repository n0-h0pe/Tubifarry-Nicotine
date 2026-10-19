# Tubifarry API plugin for Nicotine+

This Nicotine+ plugin lets Lidarr search the Soulseek network and download music through
[Nicotine+](https://nicotine-plus.org/). It does this by running a small HTTP API inside Nicotine+.

The API copies the part of the [slskd](https://github.com/slskd/slskd) REST API that Tubifarry
uses. Because of that, Tubifarry's **Nicotine+** indexer and download client run the same code as
the Slskd ones. Search strategies, result scoring, retries, the ignore list and rate limits all
behave the same way.

Tested with Nicotine+ 3.3.10. It needs only the Python standard library, so nothing extra has to
be installed.

## Install

1. Copy the `tubifarry_api` folder into your Nicotine+ plugin folder:
   - Windows: `%AppData%\nicotine\plugins\`
   - Linux / macOS: `~/.local/share/nicotine/plugins/`
   - Flatpak: `~/.var/app/org.nicotine_plus.Nicotine/data/nicotine/plugins/`
2. In Nicotine+, open **Preferences → Plugins** and enable **Tubifarry API**.
3. Select the plugin and click **Settings**. Copy the **API key**, which is generated on first
   start. You can also set your own key here.
4. Make sure Nicotine+ is logged in to Soulseek. The API only searches and downloads while
   Nicotine+ is connected.

### Settings

| Setting | Default | Notes |
| --- | --- | --- |
| Listen address | `127.0.0.1` | Use `0.0.0.0` when Lidarr runs on another machine or in Docker. |
| Listen port | `5031` | Different from slskd's 5030, so both can run side by side. |
| API key | generated | Lidarr sends it in the `X-API-KEY` header. |
| Max concurrent searches | `5` | Any search above this limit gets HTTP 429, and Tubifarry retries it later. |
| Respect ignore list | on | Drops search results from users and IP addresses you ignore in Nicotine+. |

Changes to the address or port take effect within a few seconds. You don't need to restart.

## Configure Lidarr

1. **Settings → Indexers → Add → Nicotine+**: set the URL (`http://localhost:5031`) and the API key.
2. **Settings → Download Clients → Add → Nicotine+**: same URL and API key.
3. If Lidarr runs in Docker or on another machine, add a **Remote Path Mapping**. It maps the
   Nicotine+ download folder, as Nicotine+ sees it, to the same folder as Lidarr sees it.

Leave the indexer's **Concurrent Search Limit** low (1–2). The Soulseek server can temporarily
ban clients that send too many searches.

## How it maps onto Nicotine+

- **Searches** go out as normal Nicotine+ global searches, but no search tab opens in the UI.
  A search ends when no new results have arrived for the timeout set in Tubifarry. It also ends
  when it reaches the response or file limit.
- **Downloads** use the normal Nicotine+ download queue, so they show up in the Downloads tab.
  Files go to `<Nicotine+ download folder>/<destination>/` when Tubifarry sends a batch
  destination, for example `Artist - Album` or merged multi-disc folders. Otherwise they go to
  `<Nicotine+ download folder>/<remote folder name>/`, the default slskd layout. The plugin
  ignores the "username subfolders" option for API downloads.
- **Status**: Nicotine+ statuses are reported as slskd states. A download that fails (user went
  offline, connection timeout, rejected by the peer) is reported as failed, and Tubifarry then
  runs its retry and failure handling.
- **Paused** downloads appear as queued in Lidarr.
- **Downloads cleared from the list**: when Nicotine+ clears a finished download (for example
  with auto-clear), the plugin keeps reporting it as finished for up to a week, so Lidarr can
  still import it. If you clear a download from the Nicotine+ list before it finishes, Lidarr
  sees it as cancelled.
- The plugin saves its own download records in `tubifarry_api.json` in the Nicotine+ data
  folder, so Lidarr keeps track of downloads after a Nicotine+ restart.

## Supported endpoints

All endpoints need the `X-API-KEY` header.

| Method | Path |
| --- | --- |
| GET | `/api/v0/application`, `/api/v0/server`, `/api/v0/options`, `/api/v0/events` |
| GET, POST | `/api/v0/searches` |
| GET, PUT, DELETE | `/api/v0/searches/{id}` (`?includeResponses=true`) |
| GET | `/api/v0/searches/{id}/responses` |
| POST | `/api/v0/users/{username}/directory` |
| GET | `/api/v0/transfers/downloads` |
| GET, POST | `/api/v0/transfers/downloads/{username}` |
| POST | `/api/v0/transfers/downloads/batches` (with `options.destination`) |
| GET | `/api/v0/transfers/downloads/batches/{id}` |
| GET, DELETE | `/api/v0/transfers/downloads/{username}/{id}` (`?remove=true`) |
| GET | `/api/v0/transfers/downloads/{username}/{id}/position` |
| DELETE | `/api/v0/transfers/downloads/all/completed` |

## Tests

`tests/integration_test.py` starts a real Nicotine+ core in the test process. It loads the plugin
and simulates Soulseek peers. The test needs a Nicotine+ source checkout that matches your version:

```bash
git clone --depth 1 --branch 3.3.10 https://github.com/nicotine-plus/nicotine-plus.git
python NicotinePlugin/tests/integration_test.py nicotine-plus
```

Set `TUBIFARRY_API_DUMP_DIR` to also save the raw API responses as JSON files.
