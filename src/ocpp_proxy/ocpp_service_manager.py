import asyncio
import json
import logging
import uuid
from typing import TYPE_CHECKING, Any, cast

import websockets

if TYPE_CHECKING:
    from websockets.typing import Subprotocol
else:
    Subprotocol = str

_LOGGER = logging.getLogger(__name__)


class RawOCPPServiceClient:
    """Outbound OCPP client — true transparent proxy between the charger and a backend OCPP server.

    Charger → service : every raw CALL from the charger is forwarded unchanged by
    OCPPServiceManager.forward_raw_to_services(), which calls send_raw() on each client.

    Service → charger : every CALL from the service is forwarded to the charger via
    AiohttpWSAdapter.proxy_call_to_charger(), which remaps the unique ID so the charger's
    CALLRESULT is intercepted and returned to this service using the original ID.
    """

    def __init__(
        self,
        service_id: str,
        connection: Any,
        version: str = "1.6",
        service_manager: Any = None,
    ) -> None:
        self.service_id = service_id
        self._connection = connection
        self.version = version
        self.connected = True
        self._service_manager = service_manager

    def _get_adapter(self) -> Any:
        """Return the AiohttpWSAdapter for the currently connected charger, or None."""
        try:
            cp = self._service_manager.backend_manager._app["state"]["charge_point"]
            return cp.connection  # ChargePointBase stores adapter as self.connection
        except Exception:
            return None

    async def send_raw(self, raw_msg: str) -> None:
        """Forward a raw OCPP message (from the charger) to this backend service."""
        try:
            _LOGGER.debug("[%s] charger→service: %s", self.service_id, raw_msg)
            await self._connection.send(raw_msg)
        except Exception:
            _LOGGER.exception(f"[{self.service_id}] Failed to forward raw message")
            self.connected = False

    async def start(self) -> None:
        """Listen for incoming messages from the backend OCPP server.

        Type-2 (CALL) messages are forwarded to the physical charger via the adapter's
        proxy_call_to_charger(), which handles ID remapping and response routing.
        Type-3/4 messages (CALLRESULT/CALLERROR) arriving here are responses the backend
        sent to our forwarded charger events — they are silently discarded since the proxy
        already replied to those events on the charger side.
        """
        try:
            async for raw_msg in self._connection:
                try:
                    _LOGGER.debug("[%s] service→proxy: %s", self.service_id, raw_msg)
                    msg = json.loads(raw_msg)
                    if not isinstance(msg, list) or len(msg) < 2:
                        continue
                    msg_type = msg[0]
                    if msg_type == 2:  # CALL from backend → forward to charger
                        unique_id = msg[1]
                        action = msg[2] if len(msg) > 2 else ""
                        payload = msg[3] if len(msg) > 3 else {}
                        _LOGGER.info(f"[{self.service_id}] Backend command: {action}")
                        adapter = self._get_adapter()
                        if adapter:
                            await adapter.proxy_call_to_charger(
                                self._connection, unique_id, action, payload
                            )
                        else:
                            # Charger not yet connected — return a CALLRESULT with status
                            # "Rejected" (valid OCPP response) instead of a CALLERROR so
                            # the backend service handles it gracefully.
                            _LOGGER.debug(
                                "[%s] charger not connected, rejecting %s", self.service_id, action
                            )
                            await self._connection.send(
                                json.dumps([3, unique_id, {"status": "Rejected"}])
                            )
                    # msg_type 3 / 4: responses to our forwarded events — discard silently
                except Exception:
                    _LOGGER.debug(f"[{self.service_id}] Error parsing backend message")
        except Exception:
            _LOGGER.info(f"[{self.service_id}] Connection closed")
        finally:
            self.connected = False


class OCPPServiceManager:
    """
    Manages outbound connections to OCPP services.
    Handles authentication, connection lifecycle, and message routing.
    Supports both OCPP 1.6 and 2.0.1 versions.
    """

    def __init__(self, config: Any, backend_manager: Any = None) -> None:
        self.config = config
        self.backend_manager = backend_manager
        self.services: dict[str, Any] = {}
        self._connection_tasks: dict[str, asyncio.Task[Any]] = {}

    async def start_services(self) -> None:
        """Start connections to all configured OCPP services (with auto-reconnect)."""
        if not hasattr(self.config, "ocpp_services"):
            _LOGGER.info("No OCPP services configured")
            return

        for service_config in self.config.ocpp_services:
            service_id = service_config.get("id")
            if service_id and service_config.get("enabled", True):
                task = asyncio.create_task(
                    self._run_service_with_reconnect(service_id, service_config)
                )
                self._connection_tasks[service_id] = task

    async def _run_service_with_reconnect(
        self, service_id: str, service_config: dict[str, Any]
    ) -> None:
        """Persistent reconnect loop — keeps reconnecting until the task is cancelled."""
        retry_delay = 5
        max_retry_delay = 60

        while True:
            try:
                await self.connect_service(service_id, service_config)
                retry_delay = 5  # reset backoff after a successful (clean) disconnect
            except asyncio.CancelledError:
                _LOGGER.info("[%s] Service task cancelled", service_id)
                return
            except Exception as exc:
                _LOGGER.warning("[%s] Connection error: %s", service_id, exc)

            if service_id in self.services:
                self.services[service_id].connected = False

            _LOGGER.info("[%s] Reconnecting in %ds...", service_id, retry_delay)
            try:
                await asyncio.sleep(retry_delay)
            except asyncio.CancelledError:
                _LOGGER.info("[%s] Service task cancelled during reconnect wait", service_id)
                return
            retry_delay = min(retry_delay * 2, max_retry_delay)

    async def connect_service(self, service_id: str, service_config: dict[str, Any]) -> None:
        """Connect to a specific OCPP service and block until the connection closes."""
        url = service_config.get("url")
        if not url:
            _LOGGER.error("No URL configured for OCPP service %s", service_id)
            return

        version = service_config.get("version", "1.6")

        auth_headers: dict[str, str] = {}
        if service_config.get("auth_type") == "basic":
            username = service_config.get("username")
            password = service_config.get("password")
            if username and password:
                import base64
                credentials = base64.b64encode(f"{username}:{password}".encode()).decode()
                auth_headers["Authorization"] = f"Basic {credentials}"
        elif service_config.get("auth_type") == "token":
            token = service_config.get("token")
            if token:
                auth_headers["Authorization"] = f"Bearer {token}"

        subprotocols: list[Subprotocol] = []
        if version == "1.6":
            subprotocols = [cast("Subprotocol", "ocpp1.6")]
        elif version == "2.0.1":
            subprotocols = [cast("Subprotocol", "ocpp2.0.1")]

        # connect() raises on failure — caller (_run_service_with_reconnect) retries
        connection = await websockets.connect(
            url,
            additional_headers=auth_headers,
            subprotocols=subprotocols,
            ping_interval=30,
            ping_timeout=10,
        )

        client = RawOCPPServiceClient(service_id, connection, version, service_manager=self)
        self.services[service_id] = client

        _LOGGER.info("Connecting to OCPP %s service %s at %s", version, service_id, url)

        # Replay last known charger state so the backend can initialise its entities
        try:
            cp = self.backend_manager._app["state"].get("charge_point")
            if cp and getattr(cp, "last_boot_payload", None):
                await client.send_raw(
                    json.dumps([2, str(uuid.uuid4()), "BootNotification", cp.last_boot_payload])
                )
            if cp and getattr(cp, "last_status_payload", None):
                await client.send_raw(
                    json.dumps([2, str(uuid.uuid4()), "StatusNotification", cp.last_status_payload])
                )
        except Exception:
            _LOGGER.debug("[%s] Could not replay boot/status to new service", service_id)

        try:
            await client.start()  # blocks until the connection closes
        finally:
            client.connected = False
            _LOGGER.info("Disconnected from OCPP service %s", service_id)
            try:
                await connection.close()
            except Exception:
                pass

    async def disconnect_service(self, service_id: str) -> None:
        """Disconnect from a specific OCPP service and stop its reconnect loop."""
        # Cancel the reconnect loop task — this also stops any active connection
        if service_id in self._connection_tasks:
            self._connection_tasks[service_id].cancel()
            del self._connection_tasks[service_id]

        if service_id in self.services:
            client = self.services[service_id]
            client.connected = False
            # Close the underlying WebSocket if still open
            if hasattr(client, "_connection"):
                try:
                    await client._connection.close()
                except Exception:
                    pass
            del self.services[service_id]
            _LOGGER.info("Disconnected from OCPP service %s", service_id)

    async def request_control_from_service(
        self, service_id: str, action: str, params: dict[str, Any]
    ) -> bool:
        """Handle control request from an OCPP service."""
        if not self.backend_manager:
            return False

        # Treat OCPP services as special backend clients
        success = await self.backend_manager.request_control(f"ocpp_service_{service_id}")

        if success and hasattr(self.backend_manager, "_app"):
            # Forward the request to the charge point
            cp = self.backend_manager._app["state"].get("charge_point")
            if cp:
                try:
                    if action == "RemoteStartTransaction":
                        result = await cp.send_remote_start_transaction(
                            connector_id=params.get("connector_id", 1), id_tag=params.get("id_tag")
                        )
                        return bool(result)
                    if action == "RemoteStopTransaction":
                        result = await cp.send_remote_stop_transaction(
                            transaction_id=params.get("transaction_id")
                        )
                        return bool(result)
                except Exception:
                    _LOGGER.exception(f"Error forwarding {action} from service {service_id}")

        return False

    async def replay_to_connected_services(self, cp: Any) -> None:
        """Replay the last known BootNotification, StatusNotification and MeterValues to
        every service that is currently connected.  Called when the charger (re)connects so
        services that came online before the charger can initialise their entities."""
        boot = getattr(cp, "last_boot_payload", None)
        status = getattr(cp, "last_status_payload", None)
        meter = getattr(cp, "last_meter_payload", None)
        if not boot:
            return
        for service_id, client in list(self.services.items()):
            if not client.connected:
                continue
            try:
                await client.send_raw(json.dumps([2, str(uuid.uuid4()), "BootNotification", boot]))
                if status:
                    await client.send_raw(
                        json.dumps([2, str(uuid.uuid4()), "StatusNotification", status])
                    )
                if meter:
                    await client.send_raw(
                        json.dumps([2, str(uuid.uuid4()), "MeterValues", meter])
                    )
                _LOGGER.info("[%s] Replayed boot/status/meter to already-connected service", service_id)
            except Exception:
                _LOGGER.debug("[%s] Could not replay to service on charger connect", service_id)

    async def forward_raw_to_services(self, raw_msg: str) -> None:
        """Forward a raw OCPP message from the charger to every connected backend service."""
        for client in list(self.services.values()):
            if getattr(client, "connected", False):
                asyncio.create_task(client.send_raw(raw_msg))

    async def stop_all_services(self) -> None:
        """Stop all OCPP service connections."""
        for service_id in list(self.services.keys()):
            await self.disconnect_service(service_id)

    def get_service_status(self) -> dict[str, dict[str, Any]]:
        """Get status of all OCPP services."""
        status = {}
        for service_id, client in self.services.items():
            status[service_id] = {
                "connected": getattr(client, "connected", False),
                "authenticated": getattr(client, "authenticated", False),
                "version": getattr(client, "ocpp_version", "unknown"),
            }
        return status
