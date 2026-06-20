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
                            await self._connection.send(
                                json.dumps([4, unique_id, "InternalError", "No charger connected", {}])
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
        """Start connections to all configured OCPP services."""
        if not hasattr(self.config, "ocpp_services"):
            _LOGGER.info("No OCPP services configured")
            return

        for service_config in self.config.ocpp_services:
            service_id = service_config.get("id")
            if service_id and service_config.get("enabled", True):
                await self.connect_service(service_id, service_config)

    async def connect_service(self, service_id: str, service_config: dict[str, Any]) -> None:
        """Connect to a specific OCPP service."""
        try:
            url = service_config.get("url")
            if not url:
                _LOGGER.error(f"No URL configured for OCPP service {service_id}")
                return

            # Determine OCPP version (default to 1.6 if not specified)
            version = service_config.get("version", "1.6")

            # Handle authentication
            auth_headers = {}
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

            # Set WebSocket subprotocol based on version
            subprotocols: list[Subprotocol] = []
            if version == "1.6":
                subprotocols = [cast("Subprotocol", "ocpp1.6")]
            elif version == "2.0.1":
                subprotocols = [cast("Subprotocol", "ocpp2.0.1")]

            # Create WebSocket connection
            connection = await websockets.connect(
                url,
                additional_headers=auth_headers,
                subprotocols=subprotocols,
                ping_interval=30,
                ping_timeout=10,
            )

            # Create raw OCPP service client
            client = RawOCPPServiceClient(service_id, connection, version, service_manager=self)
            self.services[service_id] = client

            # Start the client in a background task
            task = asyncio.create_task(client.start())
            self._connection_tasks[service_id] = task

            _LOGGER.info(f"Connecting to OCPP {version} service {service_id} at {url}")

            # If charger is already online, replay its last BootNotification and
            # StatusNotification so the backend service initialises correctly
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
                _LOGGER.debug(f"[{service_id}] Could not replay boot/status to new service")

        except Exception:
            _LOGGER.exception(f"Failed to connect to OCPP service {service_id}")

    async def disconnect_service(self, service_id: str) -> None:
        """Disconnect from a specific OCPP service."""
        if service_id in self.services:
            client = self.services[service_id]

            # Cancel connection task
            if service_id in self._connection_tasks:
                self._connection_tasks[service_id].cancel()
                del self._connection_tasks[service_id]

            # Close WebSocket connection
            if hasattr(client, "_connection"):
                await client._connection.close()

            del self.services[service_id]
            _LOGGER.info(f"Disconnected from OCPP service {service_id}")

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
