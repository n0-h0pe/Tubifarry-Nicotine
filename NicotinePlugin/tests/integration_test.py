"""End-to-end test for the Tubifarry API plugin against a real (offline) Nicotine+ core.

The Nicotine+ core runs in-process without a network thread. Soulseek peers are simulated by
injecting the same messages the network thread would emit, so the plugin is exercised through
the real event system, search, download and plugin code paths of Nicotine+.

Usage:
    python integration_test.py <path to nicotine-plus source checkout>

The source checkout should match the Nicotine+ version you run (e.g. git tag 3.3.10).
"""

import json
import os
import shutil
import sys
import tempfile
import threading
import time
import traceback
import urllib.error
import urllib.request

NICOTINE_SRC = os.path.abspath(sys.argv[1] if len(sys.argv) > 1 else os.environ.get("NICOTINE_SRC", ""))
PLUGIN_SRC = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "tubifarry_api")
API_KEY = "test-api-key"
PORT = int(os.environ.get("TUBIFARRY_API_TEST_PORT", "50931"))
BASE_URL = f"http://127.0.0.1:{PORT}"

# Optionally save raw API responses, e.g. to feed them to Tubifarry's C# parsers
DUMP_DIR = os.environ.get("TUBIFARRY_API_DUMP_DIR")

if not os.path.isfile(os.path.join(NICOTINE_SRC, "pynicotine", "__init__.py")):
    sys.exit("Pass the path to a nicotine-plus source checkout")

sys.path.insert(0, NICOTINE_SRC)

TEMP_DIR = tempfile.mkdtemp(prefix="tubifarry_api_test_")
DATA_DIR = os.path.join(TEMP_DIR, "data")
DOWNLOAD_DIR = os.path.join(TEMP_DIR, "downloads")
INCOMPLETE_DIR = os.path.join(TEMP_DIR, "incomplete")

for folder in (DATA_DIR, DOWNLOAD_DIR, INCOMPLETE_DIR):
    os.makedirs(folder)

shutil.copytree(PLUGIN_SRC, os.path.join(DATA_DIR, "plugins", "tubifarry_api"),
                ignore=shutil.ignore_patterns("__pycache__"))

with open(os.path.join(TEMP_DIR, "config"), "w", encoding="utf-8") as config_file:
    config_file.write(f"""[server]
login = tester
auto_connect_startup = False

[transfers]
downloaddir = {DOWNLOAD_DIR}
incompletedir = {INCOMPLETE_DIR}
usernamesubfolders = True
shared = []

[plugins]
enable = True
enabled = ['tubifarry_api']
tubifarry_api = {{'listen_address': '127.0.0.1', 'listen_port': {PORT}, 'api_key': '{API_KEY}', \
'max_concurrent_searches': 2, 'respect_ignore_list': True}}
""")

from pynicotine.config import config  # noqa: E402  pylint: disable=wrong-import-position

config.set_config_file(os.path.join(TEMP_DIR, "config"))
config.set_data_folder(DATA_DIR)

from pynicotine.core import core  # noqa: E402  pylint: disable=wrong-import-position
from pynicotine.events import events  # noqa: E402  pylint: disable=wrong-import-position
from pynicotine import slskmessages  # noqa: E402  pylint: disable=wrong-import-position
from pynicotine.transfers import TransferStatus  # noqa: E402  pylint: disable=wrong-import-position

core.init_components(
    enabled_components={
        "shares", "users", "notifications", "network_filter", "statistics", "search", "downloads", "uploads",
        "userbrowse", "userinfo", "buddies", "chatrooms", "privatechat", "pluginhandler"
    },
    isolated_mode=True
)

SENT_MESSAGES = []
events.connect("queue-network-message", SENT_MESSAGES.append)

FAILURES = []
CHECKS = 0


# Helpers #


def check(condition, description):
    global CHECKS  # pylint: disable=global-statement
    CHECKS += 1

    if condition:
        print(f"  ok   {description}")
    else:
        print(f"  FAIL {description}")
        FAILURES.append(description)


def request(method, path, body=None, api_key=API_KEY):

    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(BASE_URL + path, data=data, method=method)

    if api_key is not None:
        req.add_header("X-API-KEY", api_key)

    if data is not None:
        req.add_header("Content-Type", "application/json")

    try:
        with urllib.request.urlopen(req, timeout=30) as response:
            raw = response.read()
            return response.status, (json.loads(raw) if raw else None), dict(response.headers)

    except urllib.error.HTTPError as error:
        raw = error.read()
        return error.code, (json.loads(raw) if raw else None), dict(error.headers)


def dump(name, body):

    if DUMP_DIR:
        os.makedirs(DUMP_DIR, exist_ok=True)

        with open(os.path.join(DUMP_DIR, f"{name}.json"), "w", encoding="utf-8") as file_handle:
            json.dump(body, file_handle, indent=2)


def on_main(func, *args):

    done = threading.Event()
    box = {}

    def callback():
        try:
            box["result"] = func(*args)
        finally:
            done.set()

    events.invoke_main_thread(callback)
    done.wait(10)
    return box.get("result")


def wait_for(predicate, timeout=10):

    end = time.time() + timeout

    while time.time() < end:
        if predicate():
            return True

        time.sleep(0.1)

    return False


def emit_peer_message(event_name, msg, username, addr=("10.0.0.1", 2234)):
    msg.username = username
    msg.addr = addr
    events.emit_main_thread(event_name, msg)


FLAC_ATTRS = {1: 215, 4: 44100, 5: 16}
MP3_ATTRS = {0: 320, 1: 215, 2: 0}


# Tests #


def test_auth_and_application():

    print("auth / application")

    status, _body, _headers = request("GET", "/api/v0/application", api_key=None)
    check(status == 401, "missing API key is rejected")

    status, _body, _headers = request("GET", "/api/v0/application", api_key="wrong")
    check(status == 401, "wrong API key is rejected")

    status, body, _headers = request("GET", "/api/v0/application")
    check(status == 200, "application endpoint responds")
    check(body["server"]["state"] == "Disconnected", "server state is Disconnected while offline")
    check("Connected" not in body["server"]["state"], "offline state never contains 'Connected'")

    status, body, _headers = request("POST", "/api/v0/searches", {"SearchText": "anything"})
    check(status == 409, "search while offline returns 409 (Tubifarry backs off)")

    status, body, _headers = request("GET", "/api/v0/options")
    check(status == 200 and body["directories"]["downloads"] == os.path.normpath(DOWNLOAD_DIR),
          "options report the Nicotine+ download folder")

    status, _body, _headers = request("GET", "/api/v0/nope")
    check(status == 404, "unknown route is 404")


def go_online():

    def login():
        core.users.login_status = slskmessages.UserStatus.ONLINE
        core.users.login_username = "tester"

    on_main(login)

    status, body, _headers = request("GET", "/api/v0/application")
    check(status == 200 and body["server"]["state"] == "Connected, LoggedIn", "server state is Connected, LoggedIn")
    dump("application", body)
    dump("options", request("GET", "/api/v0/options")[1])


def test_search():

    print("search")

    search_id = "3f7cbd57-1d3e-4c55-9f5d-0d3d3c8a1b01"
    status, body, _headers = request("POST", "/api/v0/searches", {
        "Id": search_id,
        "FileLimit": 10000,
        "FilterResponses": True,
        "MaximumPeerQueueLength": 1000000,
        "MinimumPeerUploadSpeed": 0,
        "MinimumResponseFileCount": 1,
        "ResponseLimit": 100,
        "SearchText": "Artist - Album (2020)",
        "SearchTimeout": 2000
    })
    check(status == 200, "search created")
    check(body["id"] == search_id and body["state"] == "InProgress", "search is InProgress with the requested id")

    token = body["token"]
    file_searches = [msg for msg in SENT_MESSAGES if isinstance(msg, slskmessages.FileSearch)]
    check(file_searches and file_searches[-1].token == token, "FileSearch message sent to the server")
    check(file_searches and file_searches[-1].searchterm == "Artist Album 2020",
          "search term sanitized like the Nicotine+ UI")
    check(token not in core.search.searches, "API search does not create a Nicotine+ UI search tab")

    # Peer with a full album, one locked file
    response = slskmessages.FileSearchResponse(
        search_username="peer1", token=token, freeulslots=True, ulspeed=1048576, inqueue=2,
        shares=[
            (1, "Music\\Artist\\Album (2020)\\01 - One.flac", 30000000, "", FLAC_ATTRS),
            (1, "Music\\Artist\\Album (2020)\\02 - Two.flac", 31000000, "", FLAC_ATTRS),
            (1, "Music\\Artist\\Album (2020)\\cover.jpg", 100000, "", {})
        ],
        private_shares=[(1, "Music\\Artist\\Album (2020)\\03 - Three.flac", 32000000, "", FLAC_ATTRS)]
    )
    emit_peer_message("file-search-response", response, "peer1")

    # Peer filtered out by queue length is still accepted (limit is 1000000), MP3 peer
    response = slskmessages.FileSearchResponse(
        search_username="peer2", token=token, freeulslots=False, ulspeed=50000, inqueue=40,
        shares=[(1, "share\\Artist - Album\\01 One.mp3", 9000000, "", MP3_ATTRS)])
    emit_peer_message("file-search-response", response, "peer2")

    # Ignored user must be dropped
    on_main(core.network_filter.ignore_user, "badpeer")
    response = slskmessages.FileSearchResponse(
        search_username="badpeer", token=token, freeulslots=True, ulspeed=1, inqueue=0,
        shares=[(1, "x\\y\\z.flac", 1, "", {})])
    emit_peer_message("file-search-response", response, "badpeer")

    # Response for an unrelated token is ignored
    response = slskmessages.FileSearchResponse(
        search_username="peer3", token=token + 99999, freeulslots=True, ulspeed=1, inqueue=0,
        shares=[(1, "x\\y\\z.flac", 1, "", {})])
    emit_peer_message("file-search-response", response, "peer3")

    check(wait_for(lambda: request("GET", f"/api/v0/searches/{search_id}")[1]["responseCount"] == 2),
          "two responses collected")

    status, body, _headers = request("GET", f"/api/v0/searches/{search_id}")
    check(body["responses"] == [], "responses omitted unless includeResponses=true")
    check(body["fileCount"] == 4 and body["lockedFileCount"] == 1, "file counts aggregated")

    check(wait_for(lambda: request("GET", f"/api/v0/searches/{search_id}")[1]["state"] != "InProgress", 8),
          "search completes after the inactivity timeout")

    status, body, _headers = request("GET", f"/api/v0/searches/{search_id}?includeResponses=True")
    check(body["state"] == "Completed, TimedOut" and body["isComplete"], "search state is Completed, TimedOut")
    dump("search", body)
    check(token not in slskmessages.SEARCH_TOKENS_ALLOWED, "search token no longer accepted after completion")

    peer1 = next(item for item in body["responses"] if item["username"] == "peer1")
    check(peer1["hasFreeUploadSlot"] is True and peer1["uploadSpeed"] == 1048576 and peer1["queueLength"] == 2,
          "peer stats mapped")
    check(peer1["fileCount"] == 3 and peer1["lockedFileCount"] == 1, "peer file counts mapped")

    flac = peer1["files"][0]
    check(flac["filename"] == "Music\\Artist\\Album (2020)\\01 - One.flac", "full remote path kept")
    check(flac["size"] == 30000000 and flac["length"] == 215 and flac["sampleRate"] == 44100
          and flac["bitDepth"] == 16 and flac["extension"] == "flac", "FLAC attributes mapped")
    check(peer1["lockedFiles"][0]["isLocked"] is True, "locked files flagged")

    peer2 = next(item for item in body["responses"] if item["username"] == "peer2")
    check(peer2["files"][0]["bitRate"] == 320 and peer2["files"][0]["isVariableBitRate"] is False,
          "MP3 bitrate mapped")

    status, _body, _headers = request("DELETE", f"/api/v0/searches/{search_id}")
    check(status == 204, "search deleted")
    status, _body, _headers = request("GET", f"/api/v0/searches/{search_id}")
    check(status == 404, "deleted search is gone")


def test_search_limits():

    print("search limits / filters")

    status, body, _headers = request("POST", "/api/v0/searches", {
        "SearchText": "limit test", "ResponseLimit": 1, "MinimumResponseFileCount": 2, "SearchTimeout": 60000})
    search_id, token = body["id"], body["token"]

    # Filtered: only one file (minimum is 2)
    emit_peer_message("file-search-response", slskmessages.FileSearchResponse(
        search_username="small", token=token, freeulslots=True, ulspeed=1, inqueue=0,
        shares=[(1, "a\\b\\1.flac", 1, "", {})]), "small")
    emit_peer_message("file-search-response", slskmessages.FileSearchResponse(
        search_username="big", token=token, freeulslots=True, ulspeed=1, inqueue=0,
        shares=[(1, "a\\b\\1.flac", 1, "", {}), (1, "a\\b\\2.flac", 1, "", {})]), "big")

    check(wait_for(lambda: request("GET", f"/api/v0/searches/{search_id}")[1]["state"] != "InProgress"),
          "response limit completes the search")
    _status, body, _headers = request("GET", f"/api/v0/searches/{search_id}?includeResponses=true")
    check(body["state"] == "Completed, ResponseLimitReached", "state is Completed, ResponseLimitReached")
    check([item["username"] for item in body["responses"]] == ["big"], "responses below file minimum filtered")

    # Concurrency limit (2 in this test config)
    ids = []

    for index in range(2):
        status, body, _headers = request("POST", "/api/v0/searches", {"SearchText": f"busy {index}",
                                                                       "SearchTimeout": 60000})
        ids.append(body["id"])

    status, _body, _headers = request("POST", "/api/v0/searches", {"SearchText": "one too many"})
    check(status == 429, "concurrent search limit returns 429")

    status, _body, _headers = request("PUT", f"/api/v0/searches/{ids[0]}")
    check(status == 204, "search stopped")
    _status, body, _headers = request("GET", f"/api/v0/searches/{ids[0]}")
    check(body["state"] == "Completed, Cancelled", "stopped search is Completed, Cancelled")

    for search_id in ids:
        request("DELETE", f"/api/v0/searches/{search_id}")

    request("DELETE", f"/api/v0/searches/{body['id']}")


def test_browse_directory():

    print("folder browsing")

    folder = "Music\\Artist\\Album (2020)"

    def reply():
        if not wait_for(lambda: any(isinstance(msg, slskmessages.FolderContentsRequest) for msg in SENT_MESSAGES)):
            return

        emit_peer_message("folder-contents-response", slskmessages.FolderContentsResponse(
            directory=folder, token=1, shares={folder: [
                (1, "01 - One.flac", 30000000, "", FLAC_ATTRS),
                (1, "02 - Two.flac", 31000000, "", FLAC_ATTRS),
                (1, "03 - Three.flac", 32000000, "", FLAC_ATTRS)
            ]}), "peer1")

    threading.Thread(target=reply, daemon=True).start()
    status, body, _headers = request("POST", "/api/v0/users/peer1/directory", {"directory": folder})

    sent = [msg for msg in SENT_MESSAGES if isinstance(msg, slskmessages.FolderContentsRequest)]
    check(sent and sent[-1].dir == folder and sent[-1].username == "peer1", "FolderContentsRequest sent to peer")
    check(status == 200 and len(body) == 1 and body[0]["fileCount"] == 3, "folder contents returned")
    check(body and body[0]["files"][0]["filename"] == "01 - One.flac", "folder file names are basenames")
    dump("directory", body)


def transfer_of(username, virtual_path):
    return core.downloads.transfers.get(username + virtual_path)


def simulate_progress(username, virtual_path, done_bytes):

    def progress():
        transfer = transfer_of(username, virtual_path)
        core.downloads._dequeue_transfer(transfer)  # pylint: disable=protected-access
        core.downloads._activate_transfer(transfer, token=4242)  # pylint: disable=protected-access
        transfer.status = TransferStatus.TRANSFERRING
        transfer.start_time = time.monotonic() - 4
        transfer.time_elapsed = 4
        transfer.current_byte_offset = done_bytes
        transfer.speed = 250000
        transfer.time_left = 10
        core.downloads._update_transfer(transfer)  # pylint: disable=protected-access

    on_main(progress)


def simulate_finish(username, virtual_path):

    def finish():
        transfer = transfer_of(username, virtual_path)
        incomplete_path = core.downloads.get_incomplete_download_file_path(username, virtual_path)

        with open(incomplete_path, "wb") as file_handle:
            file_handle.write(b"\0" * transfer.size)

        transfer.file_handle = open(incomplete_path, "ab")  # pylint: disable=consider-using-with
        core.downloads._finish_transfer(transfer)  # pylint: disable=protected-access

    on_main(finish)


def find_file(user_transfers, filename):
    for directory in user_transfers["directories"]:
        for file_data in directory["files"]:
            if file_data["filename"] == filename:
                return directory, file_data

    return None, None


def test_downloads():

    print("downloads")

    folder = "Music\\Artist\\Album (2020)"
    files = [
        {"Filename": f"{folder}\\01 - One.flac", "Size": 1000},
        {"Filename": f"{folder}\\02 - Two.flac", "Size": 2000}
    ]

    status, body, _headers = request("POST", "/api/v0/transfers/downloads/peer1", files)
    check(status == 201, "enqueue returns 201 Created")
    check(len(body["enqueued"]) == 2, "both files enqueued")

    queue_uploads = [msg for msg in SENT_MESSAGES if isinstance(msg, slskmessages.QueueUpload)]
    check(len(queue_uploads) == 2 and queue_uploads[0].username == "peer1", "QueueUpload sent to the peer")

    expected_folder = os.path.join(os.path.normpath(DOWNLOAD_DIR), "Album (2020)")
    check(transfer_of("peer1", files[0]["Filename"]).folder_path == expected_folder,
          "download goes to <downloads>/<remote folder name> (slskd layout, ignores username subfolders)")

    status, body, _headers = request("GET", "/api/v0/transfers/downloads/peer1")
    directory, one = find_file(body, files[0]["Filename"])
    check(status == 200 and directory["directory"] == folder and directory["fileCount"] == 2,
          "user transfers grouped by remote directory")
    check(one["state"] == "Queued, Remotely" and one["direction"] == "Download", "queued state mapped")
    check(one["enqueuedAt"].endswith("Z") and one["startedAt"] is None, "timestamps serialized")

    status, all_body, _headers = request("GET", "/api/v0/transfers/downloads")
    check(status == 200 and [user["username"] for user in all_body] == ["peer1"], "all transfers listed")

    status, _body, _headers = request("GET", "/api/v0/transfers/downloads/nobody")
    check(status == 404, "user without downloads is 404")

    # Progress
    simulate_progress("peer1", files[0]["Filename"], 400)
    status, body, _headers = request("GET", f"/api/v0/transfers/downloads/peer1/{one['id']}")
    check(status == 200 and body["state"] == "InProgress", "in-progress state mapped")
    check(body["bytesTransferred"] == 400 and body["bytesRemaining"] == 600 and body["percentComplete"] == 40.0,
          "progress bytes mapped")
    dump("transfers_in_progress", request("GET", "/api/v0/transfers/downloads/peer1")[1])
    check(body["averageSpeed"] == 250000.0 and body["startedAt"] is not None and body["elapsedTime"] == "00:00:04.000",
          "speed / start time / elapsed mapped")

    status, body, _headers = request("GET", f"/api/v0/transfers/downloads/peer1/{one['id']}/position")
    check(status == 200 and body == 0, "queue position endpoint")

    # Complete the first file
    simulate_finish("peer1", files[0]["Filename"])
    _status, body, _headers = request("GET", f"/api/v0/transfers/downloads/peer1/{one['id']}")
    check(body["state"] == "Completed, Succeeded" and body["bytesRemaining"] == 0, "completed state mapped")
    check(os.path.isfile(os.path.join(expected_folder, "01 - One.flac")), "file moved into the expected folder")

    status, events_body, headers = request("GET", "/api/v0/events?offset=0&limit=50")
    check(status == 200 and headers.get("X-Total-Count") == "1", "events endpoint reports total count")
    check(events_body[0]["type"] == "DownloadFileComplete"
          and json.loads(events_body[0]["data"])["transfer"]["username"] == "peer1", "DownloadFileComplete event")

    # Second file fails with a timeout, then is retried by re-enqueueing (what Tubifarry's retry does)
    def fail():
        transfer = transfer_of("peer1", files[1]["Filename"])
        core.downloads._abort_transfer(transfer, status=TransferStatus.CONNECTION_TIMEOUT)  # pylint: disable=W0212

    on_main(fail)
    _status, body, _headers = request("GET", "/api/v0/transfers/downloads/peer1")
    _directory, two = find_file(body, files[1]["Filename"])
    check(two["state"] == "Completed, TimedOut" and two["endedAt"] is not None, "timeout maps to Completed, TimedOut")

    status, _body, _headers = request("POST", "/api/v0/transfers/downloads/peer1", [files[1]])
    _status, body, _headers = request("GET", "/api/v0/transfers/downloads/peer1")
    _directory, two = find_file(body, files[1]["Filename"])
    check(status == 201 and two["state"] == "Queued, Remotely" and two["endedAt"] is None,
          "re-enqueue retries a failed download")

    # Finish the album with auto-clear enabled: record must survive Nicotine+ clearing it
    config.sections["transfers"]["autoclear_downloads"] = True
    simulate_finish("peer1", files[1]["Filename"])
    check(transfer_of("peer1", files[1]["Filename"]) is None, "Nicotine+ auto-cleared the finished download")

    _status, body, _headers = request("GET", "/api/v0/transfers/downloads/peer1")
    _directory, two = find_file(body, files[1]["Filename"])
    check(two is not None and two["state"] == "Completed, Succeeded", "auto-cleared download still reported")
    dump("transfers_completed", body)

    _status, events_body, _headers = request("GET", "/api/v0/events?offset=1&limit=50")
    types = [item["type"] for item in events_body]
    check(types == ["DownloadFileComplete", "DownloadDirectoryComplete"], "DownloadDirectoryComplete event emitted")
    check(json.loads(events_body[-1]["data"])["remoteDirectoryName"] == folder, "directory event names the folder")
    dump("events", request("GET", "/api/v0/events?offset=0&limit=50")[1])
    config.sections["transfers"]["autoclear_downloads"] = False

    # Rejected by peer
    rejected = {"Filename": "other\\Rare\\01.flac", "Size": 10}
    request("POST", "/api/v0/transfers/downloads/peer2", [rejected])

    def reject():
        transfer = transfer_of("peer2", rejected["Filename"])
        core.downloads._abort_transfer(transfer, status="File not shared.")  # pylint: disable=protected-access

    on_main(reject)
    _status, body, _headers = request("GET", "/api/v0/transfers/downloads/peer2")
    _directory, rare = find_file(body, rejected["Filename"])
    check(rare["state"] == "Completed, Rejected" and rare["exception"] == "File not shared.",
          "peer rejection maps to Completed, Rejected")
    dump("transfers_rejected", body)

    # Cancel, then remove (what Tubifarry does when Lidarr removes a queue item)
    cancel = {"Filename": "other\\Rare\\02.flac", "Size": 10}
    _status, body, _headers = request("POST", "/api/v0/transfers/downloads/peer2", [cancel])
    cancel_id = body["enqueued"][0]["id"]

    status, _body, _headers = request("DELETE", f"/api/v0/transfers/downloads/peer2/{cancel_id}")
    _status, body, _headers = request("GET", f"/api/v0/transfers/downloads/peer2/{cancel_id}")
    check(status == 204 and body["state"] == "Completed, Cancelled", "DELETE cancels the download")

    status, _body, _headers = request("DELETE", f"/api/v0/transfers/downloads/peer2/{cancel_id}?remove=true")
    check(status == 204 and transfer_of("peer2", cancel["Filename"]) is None, "DELETE ?remove=true clears it")
    status, _body, _headers = request("GET", f"/api/v0/transfers/downloads/peer2/{cancel_id}")
    check(status == 404, "removed download is gone")

    # Downloads added in the Nicotine+ UI are listed too (Tubifarry "Inclusive" mode)
    on_main(core.downloads.enqueue_download, "uiuser", "ui\\Folder\\song.mp3", None, 5)
    _status, all_body, _headers = request("GET", "/api/v0/transfers/downloads")
    check("uiuser" in [user["username"] for user in all_body], "downloads made in the UI are listed")

    status, _body, _headers = request("DELETE", "/api/v0/transfers/downloads/all/completed")
    _status, body, _headers = request("GET", "/api/v0/transfers/downloads/peer1")
    check(status == 204 and _status == 404, "DELETE all/completed clears finished downloads")

    # Persistence of API records across restarts
    request("POST", "/api/v0/transfers/downloads/peer3", [{"Filename": "p\\q\\r.flac", "Size": 5}])
    plugin = core.pluginhandler.enabled_plugins["tubifarry_api"]
    on_main(plugin._save_state, True)  # pylint: disable=protected-access

    with open(os.path.join(DATA_DIR, "tubifarry_api.json"), encoding="utf-8") as file_handle:
        saved = json.load(file_handle)

    check(any(item["username"] == "peer3" for item in saved["downloads"]), "API downloads persisted to disk")


def test_disable_enable():

    print("disable / enable")

    on_main(core.pluginhandler.disable_plugin, "tubifarry_api")
    check(wait_for(lambda: _port_closed()), "API stops listening when the plugin is disabled")

    callbacks = events._callbacks["file-search-response"]  # pylint: disable=protected-access
    check(all(getattr(callback, "__self__", None).__class__.__name__ != "Plugin" for callback in callbacks),
          "search hook removed on disable")

    on_main(core.pluginhandler.enable_plugin, "tubifarry_api")
    check(wait_for(lambda: not _port_closed()), "API listens again after re-enabling")


def _port_closed():
    try:
        request("GET", "/api/v0/application")
        return False
    except OSError:
        return True


def run_tests():

    try:
        if not wait_for(lambda: not _port_closed(), 15):
            check(False, "plugin API started")
            return

        test_auth_and_application()
        go_online()
        test_search()
        test_search_limits()
        test_browse_directory()
        test_downloads()
        test_disable_enable()

    except Exception:  # pylint: disable=broad-except
        traceback.print_exc()
        FAILURES.append("exception")

    finally:
        events.invoke_main_thread(core.quit)


def main():

    threading.Thread(target=run_tests, daemon=True).start()
    core.start()

    while events.process_thread_events():
        time.sleep(0.05)

    shutil.rmtree(TEMP_DIR, ignore_errors=True)
    print(f"\n{CHECKS - len(FAILURES)}/{CHECKS} checks passed")
    return 1 if FAILURES else 0


if __name__ == "__main__":
    sys.exit(main())
