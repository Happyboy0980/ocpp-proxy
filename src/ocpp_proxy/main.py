import asyncio
import csv
import io
import json
import logging
import os
import uuid
from typing import Any

from aiohttp import WSCloseCode, WSMsgType, web

from .backend_manager import BackendManager
from .charge_point_factory import ChargePointFactory
from .config import Config
from .ha_bridge import HABridge
from .logger import EventLogger
from .ocpp_service_manager import OCPPServiceManager

_LOGGER = logging.getLogger(__name__)


class _WebSocketClosed(Exception):
    pass


class AiohttpWSAdapter:
    """Adapt aiohttp WebSocketResponse to the recv/send interface expected by the ocpp library.

    Also acts as the transparent proxy layer:
    - Every CALL (type 2) from the charger is forwarded raw to all registered service callbacks.
    - Every CALL from a backend service is forwarded raw to the charger with a new unique ID;
      the charger's CALLRESULT/CALLERROR is intercepted and returned to the originating service
      using the original ID, keeping the ocpp library unaware of these passthrough calls.
    """

    def __init__(self, ws: web.WebSocketResponse) -> None:
        self._ws = ws
        self._queue: asyncio.Queue = asyncio.Queue()
        # Callbacks invoked with every raw CALL (type 2) received from the charger
        self._raw_forward_cbs: list = []
        # Map: proxy-generated unique_id → (service_connection, original_unique_id)
        self._service_call_map: dict[str, tuple] = {}

    async def recv(self) -> str:
        msg = await self._queue.get()
        if msg is None:
            raise _WebSocketClosed()
        return msg

    async def send(self, data: str) -> None:
        await self._ws.send_str(data)

    def add_raw_forward_cb(self, cb: Any) -> None:
        """Register a coroutine callback to receive every raw CALL from the charger."""
        self._raw_forward_cbs.append(cb)

    async def proxy_call_to_charger(
        self, service_conn: Any, original_id: str, action: str, payload: dict
    ) -> None:
        """Send a CALL to the charger on behalf of a backend service.

        Generates a fresh unique ID, stores the ID mapping, then sends the raw
        OCPP message.  When the charger replies the response is intercepted in
        _read_loop and returned to *service_conn* using *original_id*.
        """
        new_id = str(uuid.uuid4())
        self._service_call_map[new_id] = (service_conn, original_id)
        await self._ws.send_str(json.dumps([2, new_id, action, payload]))

    async def _read_loop(self) -> None:
        try:
            async for msg in self._ws:
                if msg.type == WSMsgType.TEXT:
                    intercepted = False
                    try:
                        parsed = json.loads(msg.data)
                        if isinstance(parsed, list) and len(parsed) >= 2:
                            msg_type = parsed[0]
                            unique_id = parsed[1]
                            # CALLRESULT (3) or CALLERROR (4) for a service-proxied call
                            if msg_type in (3, 4) and unique_id in self._service_call_map:
                                service_conn, orig_id = self._service_call_map.pop(unique_id)
                                # Return to service with the original ID
                                response = [msg_type, orig_id] + list(parsed[2:])
                                try:
                                    await service_conn.send(json.dumps(response))
                                except Exception:
                                    pass
                                intercepted = True
                            # Forward every CALL from the charger to backend services
                            if msg_type == 2:
                                for cb in self._raw_forward_cbs:
                                    asyncio.create_task(cb(msg.data))
                    except Exception:
                        pass
                    if not intercepted:
                        await self._queue.put(msg.data)
                elif msg.type in (WSMsgType.CLOSE, WSMsgType.CLOSING, WSMsgType.ERROR):
                    break
        finally:
            await self._queue.put(None)


async def charger_handler(request: web.Request) -> web.WebSocketResponse:
    """Handle WebSocket connection from the EV charger (CSMS role)."""
    ws = web.WebSocketResponse(protocols=("ocpp1.6", "ocpp2.0.1"))
    await ws.prepare(request)

    # Derive charge point ID from the URL path (e.g. /EVB-P20286478/EVB-P20286478)
    path_segments = [p for p in request.path.split("/") if p]
    cp_id = path_segments[-1] if path_segments else "CP-1"

    config = request.app["config"]
    adapter = AiohttpWSAdapter(ws)

    # Register service manager so every charger CALL is forwarded raw to backend services
    ocpp_service_manager = request.app.get("ocpp_service_manager")
    if ocpp_service_manager:
        adapter.add_raw_forward_cb(ocpp_service_manager.forward_raw_to_services)

    cp = ChargePointFactory.create_charge_point(
        cp_id,
        adapter,
        version=config.ocpp_version,
        manager=request.app["backend_manager"],
        ha_bridge=request.app["ha_bridge"],
        event_logger=request.app["event_logger"],
        auto_detect=config.auto_detect_ocpp_version,
    )
    # store active charge point for proxying control requests
    request.app["charge_point"] = cp
    _LOGGER.info(f"Charger connected using OCPP {cp.ocpp_version}")
    read_task = asyncio.ensure_future(adapter._read_loop())
    try:
        await cp.start()
    except _WebSocketClosed:
        pass
    except Exception:
        _LOGGER.exception("Charger handler error")
    finally:
        read_task.cancel()
        try:
            await read_task
        except asyncio.CancelledError:
            pass
        await ws.close(code=WSCloseCode.GOING_AWAY)
    return ws


async def sessions_json(request: web.Request) -> web.Response:
    """Return all charging sessions as JSON."""
    sessions = request.app["event_logger"].get_sessions()
    return web.json_response(sessions)


async def sessions_csv(request: web.Request) -> web.Response:
    """Return all charging sessions as CSV."""
    sessions = request.app["event_logger"].get_sessions()
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["timestamp", "backend_id", "duration_s", "energy_kwh", "revenue"])
    for s in sessions:
        writer.writerow(
            [s["timestamp"], s["backend_id"], s["duration_s"], s["energy_kwh"], s["revenue"]]
        )
    return web.Response(text=output.getvalue(), content_type="text/csv")


async def override_handler(request: web.Request) -> web.Response:
    """Manually override the active control owner."""
    try:
        data = await request.json()
    except ValueError:
        return web.Response(status=400, text="Invalid JSON")

    backend_id = data.get("backend_id")
    manager = request.app["backend_manager"]
    manager.release_control()
    ok = await manager.request_control(backend_id)
    return web.json_response({"success": ok, "owner": manager._lock_owner})


async def status_handler(request: web.Request) -> web.Response:
    """Get current control owner status and backend information."""
    backend_manager = request.app["backend_manager"]
    status = backend_manager.get_backend_status()
    return web.json_response(status)


async def welcome_handler(_request: web.Request) -> web.Response:
    """Serve a simple welcome page for browser access."""
    html_content = """\
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>EV Charger Proxy</title>
    <link
      href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/css/bootstrap.min.css"
      rel="stylesheet"
      integrity="sha384-ENjdO4Dr2bkBIFxQpeoL2m0U5p3YhN9j+S8E6eE/d2RH+8abtTE1Pi6jizoU3m1G"
      crossorigin="anonymous"
    />
</head>
<body class="bg-light">
  <div class="container py-5">
    <div class="text-center mb-4">
      <h1 class="display-4">EV Charger Proxy</h1>
      <p class="lead">Proxy your EV charger to multiple backends and log charging sessions.</p>
    </div>
    <div class="card">
      <div class="card-header">
        Available Endpoints
      </div>
      <ul class="list-group list-group-flush">
        <li class="list-group-item"><a href="/charger">/charger</a> (WebSocket for charger)</li>
        <li class="list-group-item">
          <a href="/backend?id=your_backend_id">/backend?id=your_backend_id</a>
          (WebSocket for backend)
        </li>
        <li class="list-group-item"><a href="/sessions">/sessions</a> (JSON session data)</li>
        <li class="list-group-item">
          <a href="/sessions.csv">/sessions.csv</a> (CSV session data)
        </li>
        <li class="list-group-item">
          <a href="/status">/status</a> (backend status and control owner)
        </li>
        <li class="list-group-item">
          <a href="/override">/override</a> (POST to override control owner)
        </li>
      </ul>
    </div>
  </div>
</body>
</html>
"""
    return web.Response(text=html_content, content_type="text/html")


async def backend_handler(request: web.Request) -> web.WebSocketResponse:
    """Handle WebSocket connections from backend service clients."""
    ws = web.WebSocketResponse()
    await ws.prepare(request)
    backend_id = request.query.get("id", "unknown")
    manager: BackendManager = request.app["backend_manager"]
    manager.subscribe(backend_id, ws)
    _LOGGER.info("Backend %s connected", backend_id)
    try:
        async for msg in ws:
            if msg.type == web.WSMsgType.TEXT:
                data = msg.json()
                action = data.get("action")
                cp = request.app.get("charge_point")
                # Remote start request
                if action == "RemoteStartTransaction" and cp:
                    allowed = await manager.request_control(backend_id)
                    if not allowed:
                        await ws.send_json({"error": "control_locked"})
                        continue
                    req = await cp.send_remote_start_transaction(
                        connector_id=data.get("connector_id", 1), id_tag=data.get("id_tag")
                    )
                    await ws.send_json({"action": "RemoteStartTransaction", "result": req})
                # Remote stop request
                elif action == "RemoteStopTransaction" and cp:
                    req = await cp.send_remote_stop_transaction(
                        transaction_id=data.get("transaction_id")
                    )
                    await ws.send_json({"action": "RemoteStopTransaction", "result": req})
                else:
                    await ws.send_json({"error": "unknown_action"})
    except asyncio.CancelledError:
        pass
    finally:
        manager.unsubscribe(backend_id)
        await ws.close(code=WSCloseCode.GOING_AWAY)
        _LOGGER.info("Backend %s disconnected", backend_id)
    return ws


async def init_app() -> web.Application:
    """Initialize application components and routes."""
    config = Config()
    ha_url = os.getenv("HA_URL")
    ha_token = os.getenv("HA_TOKEN")
    ha = HABridge(ha_url, ha_token) if ha_url and ha_token else None

    # Initialize OCPP service manager
    ocpp_service_manager = OCPPServiceManager(config)

    app = web.Application()
    app["config"] = config
    app["backend_manager"] = BackendManager(config, ha, ocpp_service_manager)
    app["ha_bridge"] = ha
    app["event_logger"] = EventLogger(db_path=os.getenv("LOG_DB_PATH", "usage_log.db"))
    app["ocpp_service_manager"] = ocpp_service_manager
    # Pre-initialise so handlers can update it without triggering aiohttp deprecation warnings
    app["charge_point"] = None

    # Set app reference for backend manager
    app["backend_manager"].set_app_reference(app)

    app.add_routes(
        [
            web.get("/", welcome_handler),
            web.get("/charger", charger_handler),
            web.get("/backend", backend_handler),
            web.get("/sessions", sessions_json),
            web.get("/sessions.csv", sessions_csv),
            web.get("/status", status_handler),
            web.post("/override", override_handler),
            web.get("/{path_info:.*}", charger_handler),
        ]
    )

    # on_startup runs inside web.run_app()'s event loop, so service tasks survive
    app.on_startup.append(startup_app)
    app.on_cleanup.append(cleanup_app)
    return app


async def startup_app(app: web.Application) -> None:
    """Start outbound OCPP service connections after the web server is running."""
    await app["ocpp_service_manager"].start_services()


async def cleanup_app(app: web.Application) -> None:
    """Cleanup function to properly close OCPP service connections."""
    if "ocpp_service_manager" in app:
        await app["ocpp_service_manager"].stop_all_services()


def main() -> None:
    """Entrypoint for the proxy server."""
    logging.basicConfig(level=logging.INFO)
    app = asyncio.get_event_loop().run_until_complete(init_app())
    web.run_app(app, port=int(os.getenv("PORT", 9000)))


if __name__ == "__main__":
    main()
