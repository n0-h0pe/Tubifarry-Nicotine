# Tubifarry API plugin for Nicotine+
#
# Exposes the subset of the slskd REST API (v0) that the Tubifarry Lidarr plugin
# uses, backed by the running Nicotine+ instance. This lets Lidarr search the
# Soulseek network, browse folders and queue downloads through Nicotine+.
#
# GNU GENERAL PUBLIC LICENSE Version 3

import hmac
import json
import os
import re
import secrets
import threading
import time
import uuid

from collections import defaultdict
from datetime import datetime
from datetime import timezone
from http.server import BaseHTTPRequestHandler
from http.server import ThreadingHTTPServer
from urllib.parse import parse_qs
from urllib.parse import unquote
from urllib.parse import urlsplit

from pynicotine.config import config
from pynicotine.events import events
from pynicotine.pluginsystem import BasePlugin
from pynicotine.slskmessages import FileListMessage
from pynicotine.slskmessages import FileSearch
from pynicotine.slskmessages import FolderContentsRequest
from pynicotine.slskmessages import increment_token
from pynicotine.slskmessages import PlaceInQueueRequest
from pynicotine.slskmessages import UserStatus
from pynicotine.transfers import TransferStatus
from pynicotine.utils import clean_file

API_VERSION = "1.1.1"
TRANSFER_ID_NAMESPACE = uuid.UUID("0b5e7f0e-2a51-4c56-9b0e-7d1f3f6c9a11")
STATE_FILE_NAME = "tubifarry_api.json"

MAIN_THREAD_TIMEOUT = 15
SEARCH_RETENTION = 60 * 60          # Keep finished searches for one hour
RECORD_RETENTION = 7 * 24 * 60 * 60  # Keep finished API downloads for a week
MAX_EVENTS = 2000
FOLDER_REQUEST_TIMEOUT = 12
FOLDER_CACHE_DURATION = 15 * 60


# Helpers #


def utc_iso(timestamp):
    if not timestamp:
        return None

    return datetime.fromtimestamp(timestamp, tz=timezone.utc).isoformat(timespec="milliseconds").replace(
        "+00:00", "Z")


def timespan(seconds):
    """Format seconds like a .NET TimeSpan ([d.]hh:mm:ss.fff)."""

    seconds = max(0.0, float(seconds or 0))
    days, remainder = divmod(seconds, 86400)
    hours, remainder = divmod(remainder, 3600)
    minutes, secs = divmod(remainder, 60)
    text = f"{int(hours):02d}:{int(minutes):02d}:{secs:06.3f}"

    return f"{int(days)}.{text}" if days else text


def transfer_id(username, virtual_path):
    return str(uuid.uuid5(TRANSFER_ID_NAMESPACE, f"{username}\0{virtual_path}"))


def remote_directory(virtual_path):
    return virtual_path.rpartition("\\")[0]


def folder_key(username, folder_path):
    """Folder replies are matched ignoring case, slash style and trailing separators."""
    return username, (folder_path or "").replace("/", "\\").rstrip("\\").lower()


def lower_keys(data):
    return {str(key).lower(): value for key, value in data.items()} if isinstance(data, dict) else {}


def file_info_to_dict(fileinfo, is_locked=False, basename_only=False):
    """Convert a Nicotine+ (code, path, size, ext, attrs) tuple to an slskd file object."""

    code, file_path, size, _ext, attrs, *_unused = fileinfo
    bitrate, length, vbr, sample_rate, bit_depth = FileListMessage.parse_file_attributes(attrs)
    basename = file_path.rpartition("\\")[2]
    _name, dot, extension = basename.rpartition(".")

    return {
        "code": code,
        "filename": basename if basename_only else file_path,
        "size": size,
        "extension": extension.lower() if dot else "",
        "bitRate": bitrate,
        "bitDepth": bit_depth,
        "sampleRate": sample_rate,
        "length": length,
        "isVariableBitRate": bool(vbr) if vbr is not None else None,
        "isLocked": is_locked
    }


class DownloadState:
    """slskd TransferStates strings (flags enum, comma separated)."""

    REQUESTED = "Requested"
    QUEUED_REMOTELY = "Queued, Remotely"
    QUEUED_LOCALLY = "Queued, Locally"
    INITIALIZING = "Initializing"
    IN_PROGRESS = "InProgress"
    SUCCEEDED = "Completed, Succeeded"
    CANCELLED = "Completed, Cancelled"
    TIMED_OUT = "Completed, TimedOut"
    ERRORED = "Completed, Errored"
    REJECTED = "Completed, Rejected"

    STATUS_MAP = {
        None: REQUESTED,
        TransferStatus.QUEUED: QUEUED_REMOTELY,
        TransferStatus.GETTING_STATUS: INITIALIZING,
        TransferStatus.TRANSFERRING: IN_PROGRESS,
        TransferStatus.PAUSED: QUEUED_LOCALLY,
        TransferStatus.FINISHED: SUCCEEDED,
        TransferStatus.CANCELLED: CANCELLED,
        TransferStatus.FILTERED: REJECTED,
        TransferStatus.USER_LOGGED_OFF: ERRORED,
        TransferStatus.CONNECTION_CLOSED: ERRORED,
        TransferStatus.CONNECTION_TIMEOUT: TIMED_OUT,
        TransferStatus.DOWNLOAD_FOLDER_ERROR: ERRORED,
        TransferStatus.LOCAL_FILE_ERROR: ERRORED
    }

    @classmethod
    def from_status(cls, status):
        # Anything else is a rejection reason sent by the remote peer ("File not shared.", "Banned", ...)
        return cls.STATUS_MAP.get(status, cls.REJECTED)

    @staticmethod
    def is_complete(state):
        return state.startswith("Completed")


# Downloads #


class DownloadRecord:
    """Last known state of a download, kept even after Nicotine+ clears it from its list."""

    __slots__ = ("id", "username", "filename", "size", "is_api", "state", "requested_at", "enqueued_at",
                 "started_at", "ended_at", "bytes_transferred", "average_speed", "elapsed", "remaining",
                 "place_in_queue", "local_path", "exception", "batch_id")

    PERSISTED = ("id", "username", "filename", "size", "state", "requested_at", "enqueued_at", "started_at",
                 "ended_at", "bytes_transferred", "local_path", "exception", "batch_id")

    def __init__(self, username, filename, size=0, is_api=False):

        now = time.time()

        self.id = transfer_id(username, filename)
        self.username = username
        self.filename = filename
        self.size = size or 0
        self.is_api = is_api
        self.state = DownloadState.REQUESTED
        self.requested_at = now
        self.enqueued_at = now
        self.started_at = None
        self.ended_at = None
        self.bytes_transferred = 0
        self.average_speed = 0
        self.elapsed = 0
        self.remaining = 0
        self.place_in_queue = None
        self.local_path = None
        self.exception = None
        self.batch_id = None

    @property
    def directory(self):
        return remote_directory(self.filename)

    def restart(self, size=None, batch_id=None):

        now = time.time()

        if size:
            self.size = size

        self.batch_id = batch_id
        self.is_api = True
        self.state = DownloadState.REQUESTED
        self.requested_at = self.enqueued_at = now
        self.started_at = self.ended_at = None
        self.bytes_transferred = 0
        self.average_speed = self.elapsed = self.remaining = 0
        self.exception = None

    def update_from(self, transfer):

        now = time.time()
        state = DownloadState.from_status(transfer.status)
        previous_state = self.state

        if transfer.size:
            self.size = transfer.size

        if state == DownloadState.IN_PROGRESS:
            if self.started_at is None:
                self.started_at = now - (transfer.time_elapsed or 0)

            self.bytes_transferred = transfer.current_byte_offset or 0
            self.average_speed = transfer.speed or transfer.avg_speed or 0
            self.elapsed = transfer.time_elapsed or 0
            self.remaining = transfer.time_left or 0

        elif state == DownloadState.SUCCEEDED:
            self.bytes_transferred = self.size
            self.average_speed = transfer.avg_speed or self.average_speed
            self.elapsed = transfer.time_elapsed or self.elapsed
            self.remaining = 0

        else:
            self.average_speed = 0
            self.remaining = 0

        if DownloadState.is_complete(state):
            if self.ended_at is None or not DownloadState.is_complete(previous_state):
                self.ended_at = now

            self.exception = None if state == DownloadState.SUCCEEDED else transfer.status

        else:
            if DownloadState.is_complete(previous_state):
                # Download was retried
                self.enqueued_at = now
                self.started_at = None

            self.ended_at = None
            self.exception = None

        self.place_in_queue = transfer.queue_position or None
        self.state = state

    def mark_removed(self):
        """The transfer disappeared from Nicotine+ (cleared by the user, or auto-cleared)."""

        if not DownloadState.is_complete(self.state):
            self.state = DownloadState.CANCELLED
            self.exception = "Removed from Nicotine+"
            self.ended_at = self.ended_at or time.time()

        self.average_speed = 0
        self.remaining = 0
        self.place_in_queue = None

    def to_dict(self):

        size = self.size or 0
        transferred = min(self.bytes_transferred or 0, size) if size else (self.bytes_transferred or 0)

        return {
            "id": self.id,
            "username": self.username,
            "direction": "Download",
            "filename": self.filename,
            "size": size,
            "startOffset": 0,
            "state": self.state,
            "requestedAt": utc_iso(self.requested_at),
            "enqueuedAt": utc_iso(self.enqueued_at),
            "startedAt": utc_iso(self.started_at),
            "endedAt": utc_iso(self.ended_at),
            "bytesTransferred": transferred,
            "averageSpeed": float(self.average_speed or 0),
            "bytesRemaining": max(0, size - transferred),
            "elapsedTime": timespan(self.elapsed) if self.started_at else None,
            "percentComplete": round(transferred / size * 100, 2) if size else 0.0,
            "remainingTime": timespan(self.remaining) if self.remaining else None,
            "placeInQueue": self.place_in_queue,
            "exception": self.exception,
            "batchId": self.batch_id,
            "localFilename": self.local_path
        }

    def to_json(self):
        return {key: getattr(self, key) for key in self.PERSISTED}

    @classmethod
    def from_json(cls, data):

        record = cls(data["username"], data["filename"], data.get("size", 0), is_api=True)

        for key in cls.PERSISTED:
            if key in data:
                setattr(record, key, data[key])

        return record


# Searches #


class ApiSearch:

    __slots__ = ("id", "text", "token", "started_at", "ended_at", "last_activity", "state", "timeout", "file_limit",
                 "response_limit", "filter_responses", "minimum_response_file_count",
                 "maximum_peer_queue_length", "minimum_peer_upload_speed", "responses", "file_count",
                 "locked_file_count")

    def __init__(self, search_id, text, token, options):

        self.id = search_id
        self.text = text
        self.token = token
        self.started_at = self.last_activity = time.time()
        self.ended_at = None
        self.state = "InProgress"
        self.timeout = max(1.0, float(options.get("searchtimeout") or 15000) / 1000)
        self.file_limit = int(options.get("filelimit") or 10000)
        self.response_limit = int(options.get("responselimit") or 100)
        self.filter_responses = bool(options.get("filterresponses", True))
        self.minimum_response_file_count = int(options.get("minimumresponsefilecount") or 1)
        self.maximum_peer_queue_length = int(options.get("maximumpeerqueuelength") or 1000000)
        self.minimum_peer_upload_speed = int(options.get("minimumpeeruploadspeed") or 0)
        self.responses = []
        self.file_count = 0
        self.locked_file_count = 0

    @property
    def is_complete(self):
        return self.state != "InProgress"

    def accepts(self, response):

        if not self.filter_responses:
            return True

        return (response["fileCount"] >= self.minimum_response_file_count
                and response["queueLength"] <= self.maximum_peer_queue_length
                and response["uploadSpeed"] >= self.minimum_peer_upload_speed)

    def add_response(self, response):
        """Returns a completion state if a limit was reached."""

        self.responses.append(response)
        self.last_activity = time.time()
        self.file_count += response["fileCount"]
        self.locked_file_count += response["lockedFileCount"]

        if self.file_count >= self.file_limit:
            return "Completed, FileLimitReached"

        if len(self.responses) >= self.response_limit:
            return "Completed, ResponseLimitReached"

        return None

    def has_timed_out(self, now):
        # Like slskd, the timeout is measured from the last response received, with an upper bound
        return (now - self.last_activity >= self.timeout
                or now - self.started_at >= max(60.0, self.timeout * 4))

    def complete(self, state):

        if self.is_complete:
            return False

        self.state = state
        self.ended_at = time.time()
        return True

    def to_dict(self, include_responses=False):
        return {
            "id": self.id,
            "searchText": self.text,
            "token": self.token,
            "state": self.state,
            "isComplete": self.is_complete,
            "startedAt": utc_iso(self.started_at),
            "endedAt": utc_iso(self.ended_at),
            "fileCount": self.file_count,
            "lockedFileCount": self.locked_file_count,
            "responseCount": len(self.responses),
            "responses": list(self.responses) if include_responses else []
        }


class FolderRequest:

    __slots__ = ("username", "folder_path", "done", "result", "error", "legacy_attempt")

    def __init__(self, username, folder_path):
        self.username = username
        self.folder_path = folder_path
        self.done = threading.Event()
        self.result = None
        self.error = None
        self.legacy_attempt = False

    def fail(self, error):
        self.error = error
        self.done.set()


# HTTP #


class ApiError(Exception):

    def __init__(self, status, message):
        super().__init__(message)
        self.status = status
        self.message = message


class ApiResponse:

    __slots__ = ("status", "body", "headers")

    def __init__(self, body=None, status=200, headers=None):
        self.status = status
        self.body = body
        self.headers = headers or {}


def build_handler(api):

    class Handler(BaseHTTPRequestHandler):

        server_version = f"TubifarryNicotineApi/{API_VERSION}"
        protocol_version = "HTTP/1.1"

        def log_message(self, *_args):  # pylint: disable=arguments-differ
            # Keep the Nicotine+ log clean
            pass

        def _handle(self, method):

            try:
                body = self._read_body()
                response = api.handle_request(method, self.path, self.headers, body)

            except ApiError as error:
                response = ApiResponse({"error": error.message}, status=error.status)

            except Exception as error:  # pylint: disable=broad-except
                api.log_error("Unhandled error for %s %s: %s", (method, self.path, error))
                response = ApiResponse({"error": str(error)}, status=500)

            self._send(response)

        def _read_body(self):

            length = int(self.headers.get("Content-Length") or 0)

            if length <= 0:
                return None

            raw = self.rfile.read(length)

            try:
                return json.loads(raw.decode("utf-8"))

            except ValueError as error:
                raise ApiError(400, "Request body is not valid JSON") from error

        def _send(self, response):

            if response.status == 204 or response.body is None:
                payload = b""
            else:
                payload = json.dumps(response.body).encode("utf-8")

            self.send_response(response.status)

            if payload:
                self.send_header("Content-Type", "application/json; charset=utf-8")

            self.send_header("Content-Length", str(len(payload)))
            self.send_header("Connection", "close")
            self.close_connection = True

            for key, value in response.headers.items():
                self.send_header(key, value)

            self.end_headers()

            if payload and self.command != "HEAD":
                self.wfile.write(payload)

        def do_GET(self):  # pylint: disable=invalid-name
            self._handle("GET")

        def do_POST(self):  # pylint: disable=invalid-name
            self._handle("POST")

        def do_PUT(self):  # pylint: disable=invalid-name
            self._handle("PUT")

        def do_DELETE(self):  # pylint: disable=invalid-name
            self._handle("DELETE")

    return Handler


class ApiServer(ThreadingHTTPServer):
    daemon_threads = True
    block_on_close = False


# Plugin #


class Plugin(BasePlugin):

    def __init__(self, *args, **kwargs):

        super().__init__(*args, **kwargs)

        self.settings = {
            "listen_address": "127.0.0.1",
            "listen_port": 5031,
            "api_key": "",
            "max_concurrent_searches": 5,
            "respect_ignore_list": True
        }
        self.metasettings = {
            "listen_address": {
                "description": ("Address the API listens on. Use 127.0.0.1 when Lidarr runs on this machine, "
                                "or 0.0.0.0 to allow other machines / Docker containers"),
                "type": "string"
            },
            "listen_port": {
                "description": "Port the API listens on (use it in the Lidarr indexer and download client URL)",
                "type": "int", "minimum": 1, "maximum": 65535
            },
            "api_key": {
                "description": "API key Lidarr must send (X-API-KEY). Generated automatically if left empty",
                "type": "string"
            },
            "max_concurrent_searches": {
                "description": "Maximum number of API searches running at the same time",
                "type": "int", "minimum": 1, "maximum": 10
            },
            "respect_ignore_list": {
                "description": "Drop search results from users and IP addresses ignored in Nicotine+",
                "type": "bool"
            }
        }

        self._lock = threading.RLock()
        self._server = None
        self._server_thread = None
        self._server_binding = None
        self._settings_timer_id = None
        self._save_timer_id = None
        self._maintenance_stop = threading.Event()
        self._maintenance_thread = None

        self._searches = {}             # search id -> ApiSearch
        self._searches_by_token = {}    # search token -> ApiSearch
        self._records = {}              # transfer id -> DownloadRecord
        self._batches = {}              # batch id -> slskd batch object (API enqueues)
        self._records_dirty = False
        self._completed_folders = set()  # (username, remote folder) already reported complete
        self._folder_requests = defaultdict(list)  # folder_key -> [FolderRequest]
        self._folder_cache = {}                    # folder_key -> (expiry, folders or None, error)
        self._folder_token = int(time.time()) & 0x7FFFFFFF
        self._events = []
        self._events_offset = 0          # Absolute index of self._events[0]

        self._routes = [
            ("GET", r"/api/v0/application", self._get_application),
            ("GET", r"/api/v0/server", self._get_server),
            ("GET", r"/api/v0/options", self._get_options),
            ("GET", r"/api/v0/events", self._get_events),
            ("GET", r"/api/v0/searches", self._list_searches),
            ("POST", r"/api/v0/searches", self._create_search),
            ("GET", r"/api/v0/searches/(?P<search_id>[^/]+)", self._get_search),
            ("GET", r"/api/v0/searches/(?P<search_id>[^/]+)/responses", self._get_search_responses),
            ("PUT", r"/api/v0/searches/(?P<search_id>[^/]+)", self._stop_search),
            ("DELETE", r"/api/v0/searches/(?P<search_id>[^/]+)", self._delete_search),
            ("POST", r"/api/v0/users/(?P<username>[^/]+)/directory", self._browse_directory),
            ("GET", r"/api/v0/transfers/downloads", self._list_downloads),
            ("POST", r"/api/v0/transfers/downloads/batches", self._create_batch),
            ("GET", r"/api/v0/transfers/downloads/batches/(?P<batch_id>[^/]+)", self._get_batch),
            ("DELETE", r"/api/v0/transfers/downloads/all/completed", self._clear_completed_downloads),
            ("GET", r"/api/v0/transfers/downloads/(?P<username>[^/]+)", self._get_user_downloads),
            ("POST", r"/api/v0/transfers/downloads/(?P<username>[^/]+)", self._enqueue_downloads),
            ("GET", r"/api/v0/transfers/downloads/(?P<username>[^/]+)/(?P<tid>[^/]+)", self._get_download),
            ("GET", r"/api/v0/transfers/downloads/(?P<username>[^/]+)/(?P<tid>[^/]+)/position",
             self._get_download_position),
            ("DELETE", r"/api/v0/transfers/downloads/(?P<username>[^/]+)/(?P<tid>[^/]+)",
             self._delete_download)
        ]
        self._routes = [(method, re.compile(f"^{pattern}/?$"), func) for method, pattern, func in self._routes]

    # Lifecycle #

    def loaded_notification(self):

        if not self.settings["api_key"]:
            self.settings["api_key"] = secrets.token_urlsafe(24)
            config.write_configuration()
            self.log("Generated a new API key, see the plugin preferences to copy it into Lidarr")

        self._load_state()

        events.connect("folder-contents-response", self._folder_contents_response)
        events.connect("peer-connection-error", self._peer_connection_error)
        events.connect("peer-connection-closed", self._peer_connection_closed)
        events.connect("update-download", self._update_download)
        events.connect("abort-download", self._update_download)
        events.connect("clear-download", self._clear_download)
        events.connect("server-disconnect", self._server_disconnect)

        # Nicotine+ discards search responses for tokens it doesn't know about before
        # other handlers run, so our handler has to go first
        try:
            events._callbacks["file-search-response"].insert(  # pylint: disable=protected-access
                0, self._file_search_response)
        except AttributeError:
            events.connect("file-search-response", self._file_search_response)

        self._settings_timer_id = events.schedule(delay=3, callback=self._check_settings, repeat=True)
        self._save_timer_id = events.schedule(delay=60, callback=self._save_state, repeat=True)

        self._maintenance_stop.clear()
        self._maintenance_thread = threading.Thread(
            target=self._maintenance_loop, name="TubifarryApiMaintenance", daemon=True)
        self._maintenance_thread.start()

        self._start_server()

    def disable(self):
        self._shutdown()

    def shutdown_notification(self):
        self._shutdown()

    def _shutdown(self):

        self._stop_server()
        self._maintenance_stop.set()

        for timer_id in (self._settings_timer_id, self._save_timer_id):
            if timer_id is not None:
                events.cancel_scheduled(timer_id)

        self._settings_timer_id = self._save_timer_id = None

        for event_name, callback in (
            ("file-search-response", self._file_search_response),
            ("folder-contents-response", self._folder_contents_response),
            ("peer-connection-error", self._peer_connection_error),
            ("peer-connection-closed", self._peer_connection_closed),
            ("update-download", self._update_download),
            ("abort-download", self._update_download),
            ("clear-download", self._clear_download),
            ("server-disconnect", self._server_disconnect)
        ):
            try:
                events.disconnect(event_name, callback)
            except (KeyError, ValueError):
                pass

        with self._lock:
            searches = list(self._searches.values())
            self._searches.clear()
            self._searches_by_token.clear()

        for search in searches:
            self.core.search.remove_allowed_token(search.token)

        self._save_state(force=True)

    def _start_server(self):

        address = str(self.settings["listen_address"] or "127.0.0.1").strip()
        port = int(self.settings["listen_port"])

        try:
            server = ApiServer((address, port), build_handler(self))

        except OSError as error:
            self.log("Unable to listen on %s:%s: %s", (address, port, error))
            self._server_binding = (address, port)
            return

        self._server = server
        self._server_binding = (address, port)
        self._server_thread = threading.Thread(
            target=server.serve_forever, kwargs={"poll_interval": 0.5}, name="TubifarryApiServer", daemon=True)
        self._server_thread.start()

        self.log("slskd-compatible API listening on http://%s:%s", (address, port))

    def _stop_server(self):

        server, self._server = self._server, None

        if server is None:
            return

        server.shutdown()
        server.server_close()
        self._server_thread = None

    def _check_settings(self):
        """Nicotine+ has no settings-changed hook, restart the server if the binding changed."""

        binding = (str(self.settings["listen_address"] or "127.0.0.1").strip(), int(self.settings["listen_port"]))

        if binding != self._server_binding:
            self._stop_server()
            self._start_server()

    def log_error(self, msg, msg_args=None):
        self.log(msg, msg_args)

    # Threading #

    def _call_main(self, func, *args):
        """Run func on the Nicotine+ main thread and return its result."""

        done = threading.Event()
        box = {}

        def callback():
            try:
                box["result"] = func(*args)
            except Exception as error:  # pylint: disable=broad-except
                box["error"] = error
            finally:
                done.set()

        events.invoke_main_thread(callback)

        if not done.wait(MAIN_THREAD_TIMEOUT):
            raise ApiError(503, "Nicotine+ did not respond in time")

        if "error" in box:
            raise box["error"]

        return box.get("result")

    def _maintenance_loop(self):

        while not self._maintenance_stop.wait(0.25):
            now = time.time()
            expired_tokens = []

            with self._lock:
                for search in list(self._searches.values()):
                    if not search.is_complete and search.has_timed_out(now):
                        search.complete("Completed, TimedOut")
                        expired_tokens.append(search.token)

                    elif search.is_complete and now - search.ended_at >= SEARCH_RETENTION:
                        self._forget_search(search)

            for token in expired_tokens:
                events.invoke_main_thread(self.core.search.remove_allowed_token, token)

    # Request routing #

    def handle_request(self, method, raw_path, headers, body):

        expected_key = str(self.settings["api_key"] or "")
        provided_key = headers.get("X-API-KEY") or ""

        if not expected_key or not hmac.compare_digest(provided_key.encode(), expected_key.encode()):
            raise ApiError(401, "Invalid or missing X-API-KEY header")

        url = urlsplit(raw_path)
        query = {key: values[-1] for key, values in parse_qs(url.query).items()}

        # Match on the raw path so escaped slashes inside usernames survive
        path = url.path.rstrip("/") or "/"
        path_allowed = False

        for route_method, pattern, func in self._routes:
            match = pattern.match(path)

            if match is None:
                continue

            path_allowed = True

            if route_method != method:
                continue

            params = {key: unquote(value) for key, value in match.groupdict().items()}
            result = func(query=query, body=body, **params)
            return result if isinstance(result, ApiResponse) else ApiResponse(result)

        if path_allowed:
            raise ApiError(405, "Method not allowed")

        raise ApiError(404, "Not found")

    # Application #

    def _is_logged_in(self):
        return self.core.users.login_status != UserStatus.OFFLINE

    def _server_state(self):

        logged_in = self._is_logged_in()
        server_host, server_port = config.sections["server"]["server"]

        return {
            "address": f"{server_host}:{server_port}",
            "state": "Connected, LoggedIn" if logged_in else "Disconnected",
            "isConnected": logged_in,
            "isLoggedIn": logged_in,
            "isTransitioning": False
        }

    def _get_application(self, **_kwargs):

        def snapshot():
            from pynicotine import __version__ as nicotine_version

            return {
                "version": {
                    "current": f"Nicotine+ {nicotine_version} (Tubifarry API {API_VERSION})",
                    "full": f"{nicotine_version}+tubifarry-api.{API_VERSION}",
                    "isUpdateAvailable": False
                },
                "server": self._server_state(),
                "user": {
                    "username": self.core.users.login_username
                },
                "client": "Nicotine+"
            }

        return self._call_main(snapshot)

    def _get_server(self, **_kwargs):
        return self._call_main(self._server_state)

    def _get_options(self, **_kwargs):

        def snapshot():
            return {
                "directories": {
                    "downloads": self.core.downloads.get_default_download_folder(),
                    "incomplete": self.core.downloads.get_incomplete_download_folder()
                }
            }

        return self._call_main(snapshot)

    def _get_events(self, query, **_kwargs):

        offset = max(0, int(query.get("offset") or 0))
        limit = max(1, min(500, int(query.get("limit") or 100)))

        # Newest first, like slskd: Tubifarry only reads the first page to find new events
        with self._lock:
            records = self._events[::-1][offset:offset + limit]
            total = self._events_offset + len(self._events)

        return ApiResponse(records, headers={"X-Total-Count": str(total)})

    def _add_event(self, event_type, data):

        with self._lock:
            self._events.append({
                "id": str(uuid.uuid4()),
                "timestamp": utc_iso(time.time()),
                "type": event_type,
                "version": 0,
                "data": json.dumps(data)
            })

            overflow = len(self._events) - MAX_EVENTS

            if overflow > 0:
                del self._events[:overflow]
                self._events_offset += overflow

    # Searches #

    def _get_search_or_404(self, search_id):

        with self._lock:
            search = self._searches.get(search_id)

        if search is None:
            raise ApiError(404, f"Search {search_id} not found")

        return search

    def _list_searches(self, **_kwargs):
        with self._lock:
            return [search.to_dict() for search in self._searches.values()]

    def _create_search(self, body, **_kwargs):

        options = lower_keys(body)
        text = str(options.get("searchtext") or "").strip()

        if not text:
            raise ApiError(400, "SearchText is required")

        search_id = str(options.get("id") or uuid.uuid4())

        def start():
            if not self._is_logged_in():
                # Tubifarry treats 409 as "client not connected" and backs off
                raise ApiError(409, "Nicotine+ is not connected to the Soulseek server")

            with self._lock:
                if search_id in self._searches:
                    raise ApiError(400, f"Search {search_id} already exists")

                running = sum(1 for search in self._searches.values() if not search.is_complete)

                if running >= int(self.settings["max_concurrent_searches"]):
                    raise ApiError(429, "Too many concurrent searches")

            core_search = self.core.search
            _term, term_transmitted, _included, _excluded = core_search.sanitize_search_term(text)

            # Share the token counter with Nicotine+ to avoid clashing with searches made in the UI
            core_search.token = token = increment_token(core_search.token)
            search = ApiSearch(search_id, text, token, options)

            with self._lock:
                self._searches[search_id] = search
                self._searches_by_token[token] = search

            core_search.add_allowed_token(token)
            self.core.send_message_to_server(FileSearch(token, term_transmitted))
            return search.to_dict()

        return self._call_main(start)

    def _get_search(self, search_id, query, **_kwargs):

        include_responses = str(query.get("includeResponses", "false")).lower() == "true"
        search = self._get_search_or_404(search_id)

        with self._lock:
            return search.to_dict(include_responses=include_responses)

    def _get_search_responses(self, search_id, **_kwargs):

        search = self._get_search_or_404(search_id)

        with self._lock:
            return list(search.responses)

    def _stop_search(self, search_id, **_kwargs):

        search = self._get_search_or_404(search_id)

        with self._lock:
            stopped = search.complete("Completed, Cancelled")

        if stopped:
            events.invoke_main_thread(self.core.search.remove_allowed_token, search.token)

        return ApiResponse(status=204)

    def _delete_search(self, search_id, **_kwargs):

        search = self._get_search_or_404(search_id)

        with self._lock:
            self._forget_search(search)

        events.invoke_main_thread(self.core.search.remove_allowed_token, search.token)
        return ApiResponse(status=204)

    def _forget_search(self, search):
        self._searches.pop(search.id, None)
        self._searches_by_token.pop(search.token, None)

    def _file_search_response(self, msg):
        """Peer code 9, runs on the main thread before Nicotine+'s own handler."""

        with self._lock:
            search = self._searches_by_token.get(msg.token)

        if search is None or search.is_complete:
            return

        username = msg.username

        if self.settings["respect_ignore_list"]:
            network_filter = self.core.network_filter
            ip_address = msg.addr[0] if msg.addr else None

            if network_filter.is_user_ignored(username) or network_filter.is_user_ip_ignored(username, ip_address):
                return

        files = [file_info_to_dict(fileinfo) for fileinfo in (msg.list or [])]
        locked_files = [file_info_to_dict(fileinfo, is_locked=True) for fileinfo in (msg.privatelist or [])]

        response = {
            "username": username,
            "token": msg.token,
            "hasFreeUploadSlot": bool(msg.freeulslots),
            "uploadSpeed": msg.ulspeed or 0,
            "queueLength": msg.inqueue or 0,
            "fileCount": len(files),
            "lockedFileCount": len(locked_files),
            "files": files,
            "lockedFiles": locked_files
        }

        completed_state = None

        with self._lock:
            if search.is_complete or not search.accepts(response):
                return

            completed_state = search.add_response(response)

            if completed_state is not None:
                search.complete(completed_state)

        if completed_state is not None:
            self.core.search.remove_allowed_token(search.token)

    def _server_disconnect(self, *_args):

        with self._lock:
            searches = [search for search in self._searches.values() if not search.is_complete]

            for search in searches:
                search.complete("Completed, Errored")

        for search in searches:
            self.core.search.remove_allowed_token(search.token)

    # Folder browsing #

    def _browse_directory(self, username, body, **_kwargs):

        folder_path = str(lower_keys(body).get("directory") or "").replace("/", "\\").rstrip("\\")

        if not folder_path:
            raise ApiError(400, "Directory is required")

        key = folder_key(username, folder_path)
        now = time.time()

        with self._lock:
            for cached_key, (expiry, _folders, _error) in list(self._folder_cache.items()):
                if expiry < now:
                    del self._folder_cache[cached_key]

            cached = self._folder_cache.get(key)

        if cached is not None:
            _expiry, folders, error = cached

            if folders is None:
                raise ApiError(404, f"{error} (cached)")

            return self._folders_to_dict(folders, folder_path)

        request = FolderRequest(username, folder_path)

        with self._lock:
            self._folder_requests[key].append(request)

        try:
            self._call_main(self._send_folder_request, request)

            if not request.done.wait(FOLDER_REQUEST_TIMEOUT):
                request.error = f"{username} did not send the contents of {folder_path} within {FOLDER_REQUEST_TIMEOUT}s"
                self.log("Folder request timed out: %s", request.error)

        finally:
            with self._lock:
                pending = self._folder_requests.get(key)

                if pending and request in pending:
                    pending.remove(request)

                if not pending:
                    self._folder_requests.pop(key, None)

        folders = request.result or None

        if folders is None and request.error is None:
            request.error = f"{username} returned no files for {folder_path}"

        with self._lock:
            self._folder_cache[key] = (time.time() + FOLDER_CACHE_DURATION, folders, request.error)

        if folders is None:
            raise ApiError(504 if "within" in request.error else 404, request.error)

        return self._folders_to_dict(folders, folder_path)

    @staticmethod
    def _folders_to_dict(folders, folder_path):

        # Requested folder first
        requested = folder_key("", folder_path)[1]
        ordered = sorted(folders.items(), key=lambda item: folder_key("", item[0])[1] != requested)

        return [
            {
                "name": folder,
                "fileCount": len(files),
                "files": [file_info_to_dict(fileinfo, basename_only=True) for fileinfo in files]
            }
            for folder, files in ordered
        ]

    def _send_folder_request(self, request):

        if not self._is_logged_in():
            raise ApiError(409, "Nicotine+ is not connected to the Soulseek server")

        self._folder_token = increment_token(self._folder_token)
        self.core.send_message_to_peer(
            request.username,
            FolderContentsRequest(request.folder_path, self._folder_token, legacy_client=request.legacy_attempt)
        )

    def _folder_contents_response(self, msg, *_args):
        """Peer code 37."""

        key = folder_key(msg.username, msg.dir)

        with self._lock:
            requests = list(self._folder_requests.get(key, []))
            other_pending = [] if requests else [
                request.folder_path for pending_key, pending in self._folder_requests.items()
                if pending_key[0] == msg.username for request in pending
            ]

        if other_pending:
            self.log("Folder reply from %s for '%s' does not match the pending request(s) %s",
                     (msg.username, msg.dir, other_pending))
            return

        for request in requests:
            if request.done.is_set():
                continue

            if not msg.list and not request.legacy_attempt:
                # Same fallback as Nicotine+: retry with latin-1 encoding for legacy clients
                request.legacy_attempt = True
                self._send_folder_request(request)
                continue

            request.result = msg.list or {}
            request.done.set()

    def _peer_connection_error(self, username, _conn_type, msgs, is_offline=False, is_timeout=True):
        """Fail folder requests right away when the peer can't be reached, instead of waiting for the timeout."""

        if not msgs:
            return

        reason = "is offline" if is_offline else ("could not be reached (timeout)" if is_timeout
                                                   else "closed the connection")

        for msg in msgs:
            if not isinstance(msg, FolderContentsRequest):
                continue

            with self._lock:
                requests = list(self._folder_requests.get(folder_key(username, msg.dir), []))

            for request in requests:
                if not request.done.is_set():
                    request.fail(f"{username} {reason}, could not browse {request.folder_path}")

    def _peer_connection_closed(self, username, conn_type, msgs=None):
        self._peer_connection_error(username, conn_type, msgs, is_timeout=False)

    # Downloads #

    def _get_record(self, transfer, is_api=False):

        tid = transfer_id(transfer.username, transfer.virtual_path)
        record = self._records.get(tid)

        if record is None:
            record = self._records[tid] = DownloadRecord(
                transfer.username, transfer.virtual_path, transfer.size, is_api=is_api)

        return record

    def _update_download(self, transfer, *_args):
        """Keep timestamps accurate between API polls (main thread)."""

        tid = transfer_id(transfer.username, transfer.virtual_path)
        record = self._records.get(tid)

        if record is not None:
            record.update_from(transfer)
            self._records_dirty = record.is_api or self._records_dirty

    def _clear_download(self, transfer, *_args):

        tid = transfer_id(transfer.username, transfer.virtual_path)
        record = self._records.get(tid)

        if record is None:
            return

        if not record.is_api:
            del self._records[tid]
            return

        record.update_from(transfer)
        record.mark_removed()
        self._records_dirty = True

    def download_finished_notification(self, user, virtual_path, real_path):

        tid = transfer_id(user, virtual_path)
        record = self._records.get(tid)

        if record is None or not record.is_api:
            return

        record.state = DownloadState.SUCCEEDED
        record.bytes_transferred = record.size
        record.ended_at = record.ended_at or time.time()
        record.local_path = real_path
        self._records_dirty = True

        self._add_event("DownloadFileComplete", {
            "localFilename": real_path,
            "remoteFilename": virtual_path,
            "transfer": record.to_dict()
        })

        folder = record.directory
        folder_key = (user, folder)
        folder_records = [
            other for other in self._records.values()
            if other.is_api and other.username == user and other.directory == folder
        ]

        if folder_key in self._completed_folders:
            return

        if all(other.state == DownloadState.SUCCEEDED for other in folder_records):
            self._completed_folders.add(folder_key)
            self._add_event("DownloadDirectoryComplete", {
                "localDirectoryName": os.path.dirname(real_path),
                "remoteDirectoryName": folder,
                "username": user
            })

    def _snapshot_downloads(self, username=None):
        """Returns {username: {remote folder: [record]}} (main thread)."""

        grouped = defaultdict(lambda: defaultdict(list))
        seen = set()

        for transfer in list(self.core.downloads.transfers.values()):
            if username is not None and transfer.username != username:
                continue

            record = self._get_record(transfer)
            record.update_from(transfer)
            seen.add(record.id)
            grouped[record.username][record.directory].append(record)

        for tid, record in list(self._records.items()):
            if tid in seen or (username is not None and record.username != username):
                continue

            if not record.is_api:
                # Not ours, and Nicotine+ no longer has it
                del self._records[tid]
                continue

            record.mark_removed()
            grouped[record.username][record.directory].append(record)

        return {
            user: {
                folder: [record.to_dict() for record in records]
                for folder, records in folders.items()
            }
            for user, folders in grouped.items()
        }

    @staticmethod
    def _user_transfers_to_dict(username, folders):
        return {
            "username": username,
            "directories": [
                {
                    "directory": folder,
                    "fileCount": len(files),
                    "files": files
                }
                for folder, files in folders.items()
            ]
        }

    def _list_downloads(self, **_kwargs):

        grouped = self._call_main(self._snapshot_downloads)
        return [self._user_transfers_to_dict(username, folders) for username, folders in grouped.items()]

    def _get_user_downloads(self, username, **_kwargs):

        grouped = self._call_main(self._snapshot_downloads, username)

        if username not in grouped:
            raise ApiError(404, f"No downloads from {username}")

        return self._user_transfers_to_dict(username, grouped[username])

    def _find_download(self, username, tid):

        grouped = self._snapshot_downloads(username)

        for files in grouped.get(username, {}).values():
            for file_data in files:
                if file_data["id"] == tid:
                    return file_data

        return None

    def _get_download(self, username, tid, **_kwargs):

        file_data = self._call_main(self._find_download, username, tid)

        if file_data is None:
            raise ApiError(404, f"Download {tid} not found")

        return file_data

    def _get_download_position(self, username, tid, **_kwargs):

        def lookup():
            record = self._records.get(tid)

            if record is None:
                return None

            transfer = self.core.downloads.transfers.get(record.username + record.filename)

            if transfer is not None and transfer.status == TransferStatus.QUEUED and self._is_logged_in():
                # Ask for a fresh position, the reply updates the transfer
                self.core.send_message_to_peer(
                    record.username, PlaceInQueueRequest(record.filename, legacy_client=transfer.legacy_attempt))
                return transfer.queue_position or 0

            return record.place_in_queue or 0

        position = self._call_main(lookup)

        if position is None:
            raise ApiError(404, f"Download {tid} not found")

        return position

    @staticmethod
    def _parse_files(items):

        files = []

        for item in items if isinstance(items, list) else []:
            item = lower_keys(item)
            filename = str(item.get("filename") or "").replace("/", "\\")

            if filename:
                files.append((filename, int(item.get("size") or 0)))

        if not files:
            raise ApiError(400, "No valid files to enqueue")

        return files

    def _enqueue_downloads(self, username, body, **_kwargs):

        files = self._parse_files([body] if isinstance(body, dict) else body)
        result = self._call_main(self._enqueue_files, username, files)
        return ApiResponse(result, status=201)

    def _create_batch(self, body, **_kwargs):
        """slskd batch enqueue: all files of one grab, with an optional destination subfolder."""

        data = lower_keys(body)
        username = str(data.get("username") or "").strip()

        if not username:
            raise ApiError(400, "Username is required")

        files = self._parse_files(data.get("files"))
        options = lower_keys(data.get("options"))
        batch_id = str(data.get("id") or uuid.uuid4())
        destination = options.get("destination") or None
        batch = {
            "id": batch_id,
            "username": username,
            "requestedAt": utc_iso(time.time()),
            "options": {
                "externalId": options.get("externalid"),
                "destination": destination
            }
        }

        def enqueue():
            result = self._enqueue_files(username, files, batch_id=batch_id, destination=destination)
            self._batches[batch_id] = batch
            return dict(batch, files=result["enqueued"], failures=result["failed"])

        return ApiResponse(self._call_main(enqueue), status=201)

    def _get_batch(self, batch_id, **_kwargs):

        def lookup():
            batch = self._batches.get(batch_id)

            if batch is None:
                return None

            files = [record.to_dict() for record in self._records.values() if record.batch_id == batch_id]
            return dict(batch, files=files)

        batch = self._call_main(lookup)

        if batch is None:
            raise ApiError(404, f"Batch {batch_id} not found")

        return batch

    @staticmethod
    def _destination_folder(download_folder, virtual_path, destination=None):

        if destination:
            destination = str(destination)

            if os.path.isabs(destination) or re.match(r"^[A-Za-z]:[\\/]", destination):
                return destination

            segments = [clean_file(segment) for segment in re.split(r"[\\/]+", destination)
                        if segment and segment not in {".", ".."}]

            if segments:
                return os.path.join(download_folder, *segments)

        # slskd default layout: <downloads>/<remote folder name>/<file>
        folder_name = remote_directory(virtual_path).rpartition("\\")[2]
        return os.path.join(download_folder, clean_file(folder_name)) if folder_name else download_folder

    def _enqueue_files(self, username, files, batch_id=None, destination=None):
        """Main thread."""

        if not self._is_logged_in():
            raise ApiError(409, "Nicotine+ is not connected to the Soulseek server")

        downloads = self.core.downloads
        download_folder = downloads.get_default_download_folder()
        enqueued = []

        for virtual_path, size in files:
            tid = transfer_id(username, virtual_path)
            record = self._records.get(tid)

            if record is None:
                record = self._records[tid] = DownloadRecord(username, virtual_path, size, is_api=True)

            record.restart(size, batch_id)
            self._completed_folders.discard((username, remote_directory(virtual_path)))

            transfer = downloads.transfers.get(username + virtual_path)

            if transfer is None or transfer.status == TransferStatus.FINISHED:
                downloads.enqueue_download(
                    username, virtual_path, size=size,
                    folder_path=self._destination_folder(download_folder, virtual_path, destination))
            else:
                # Existing failed/paused download: retry it (no-op for active downloads)
                downloads.retry_download(transfer)

            transfer = downloads.transfers.get(username + virtual_path)

            if transfer is not None:
                record.update_from(transfer)

            enqueued.append(record.to_dict())

        self._records_dirty = True
        return {"enqueued": enqueued, "failed": []}

    def _delete_download(self, username, tid, query, **_kwargs):

        remove = str(query.get("remove", "false")).lower() == "true"

        def delete():
            record = self._records.get(tid)
            transfer = None

            if record is not None:
                transfer = self.core.downloads.transfers.get(record.username + record.filename)
            else:
                for candidate in list(self.core.downloads.transfers.values()):
                    if candidate.username == username and transfer_id(
                            candidate.username, candidate.virtual_path) == tid:
                        transfer = candidate
                        break

            if record is None and transfer is None:
                return False

            if transfer is not None:
                if remove:
                    self.core.downloads.clear_downloads([transfer])

                elif transfer.status != TransferStatus.FINISHED:
                    self.core.downloads.abort_downloads([transfer], status=TransferStatus.CANCELLED)

            if remove:
                self._records.pop(tid, None)

            elif record is not None and transfer is not None:
                record.update_from(transfer)

            self._records_dirty = True
            return True

        if not self._call_main(delete):
            raise ApiError(404, f"Download {tid} not found")

        return ApiResponse(status=204)

    def _clear_completed_downloads(self, **_kwargs):

        def clear():
            completed = [
                transfer for transfer in list(self.core.downloads.transfers.values())
                if DownloadState.is_complete(DownloadState.from_status(transfer.status))
            ]

            if completed:
                self.core.downloads.clear_downloads(completed)

            for tid, record in list(self._records.items()):
                if DownloadState.is_complete(record.state):
                    del self._records[tid]

            self._records_dirty = True

        self._call_main(clear)
        return ApiResponse(status=204)

    # Persistence #

    def _state_file_path(self):
        return os.path.join(config.data_folder_path, STATE_FILE_NAME)

    def _load_state(self):

        path = self._state_file_path()

        if not os.path.isfile(path):
            return

        try:
            with open(path, encoding="utf-8") as file_handle:
                data = json.load(file_handle)

            for item in data.get("downloads", []):
                record = DownloadRecord.from_json(item)
                self._records[record.id] = record

            self._batches = {batch["id"]: batch for batch in data.get("batches", [])}

        except (OSError, ValueError, KeyError, TypeError) as error:
            self.log("Unable to load saved download state: %s", error)

    def _save_state(self, force=False):

        if not self._records_dirty and not force:
            return

        now = time.time()
        records = [
            record.to_json() for record in self._records.values()
            if record.is_api and not (record.ended_at and now - record.ended_at > RECORD_RETENTION)
        ]
        batch_ids = {record["batch_id"] for record in records if record.get("batch_id")}
        batches = [batch for batch_id, batch in self._batches.items() if batch_id in batch_ids]
        path = self._state_file_path()
        temp_path = path + ".tmp"

        try:
            with open(temp_path, "w", encoding="utf-8") as file_handle:
                json.dump({"version": 2, "downloads": records, "batches": batches}, file_handle)

            os.replace(temp_path, path)
            self._records_dirty = False

        except OSError as error:
            self.log("Unable to save download state: %s", error)
