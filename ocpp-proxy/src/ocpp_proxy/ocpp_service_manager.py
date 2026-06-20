import asyncio
import datetime
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
    """Outbound OCPP client that forwards charger events as raw OCPP JSON messages."""

    def __init__(self, service_id: str, connection: Any, version: str = "1.6") -> None:
        self.service_id = service_id
        self._connection = connection
        self.version = version
        self.connected = True

    def _call_msg(self, action: str, payload: dict) -> str:
        return json.dumps([2, str(uuid.uuid4()), action, payload])

    async def send_call(self, action: str, payload: dict) -> None:
        """Send an OCPP CALL message to the backend (fire-and-forget)."""
        try:
            await self._connection.send(self._call_msg(action, payload))
        except Exception:
            _LOGGER.exception(f"[{self.service_id}] Failed to send {action}")
            self.connected = False

    async def start(self) -> None:
        """Listen for incoming commands from the backend OCPP server."""
        try:
            async for raw_msg in self._connection:
                try:
                    msg = json.loads(raw_msg)
                    if not isinstance(msg, list) or len(msg) < 3:
                        continue
                    if msg[0] == 2:  # CALL from backend — acknowledge with empty response
                        await self._connection.send(json.dumps([3, msg[1], {}]))
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
        self._background_tasks: set[asyncio.Task[Any]] = set()

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
                extra_headers=auth_headers,
                subprotocols=subprotocols,
                ping_interval=30,
                ping_timeout=10,
            )

            # Create raw OCPP service client
            client = RawOCPPServiceClient(service_id, connection, version)
            self.services[service_id] = client

            # Start the client in a background task
            task = asyncio.create_task(client.start())
            self._connection_tasks[service_id] = task

            _LOGGER.info(f"Connecting to OCPP {version} service {service_id} at {url}")

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
            cp = self.backend_manager._app.get("charge_point")
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

    def broadcast_event_to_services(self, event: dict[str, Any]) -> None:
        """Broadcast charger events to all connected OCPP services."""
        for client in self.services.values():
            if hasattr(client, "connected") and client.connected:
                task = asyncio.create_task(self._send_event_to_service(client, event))
                # Store reference to prevent task being garbage collected
                self._background_tasks.add(task)
                task.add_done_callback(self._background_tasks.discard)

    async def _send_event_to_service(self, client: Any, event: dict[str, Any]) -> None:
        """Forward a charger event to an outbound OCPP service as an OCPP CALL."""
        event_type = event.get("type")
        try:
            if event_type == "boot":
                await client.send_call("BootNotification", {
                    "chargePointVendor": event.get("vendor", "Unknown"),
                    "chargePointModel": event.get("model", "Unknown"),
                })
            elif event_type == "status":
                await client.send_call("StatusNotification", {
                    "connectorId": event.get("connector_id", 0),
                    "errorCode": event.get("error_code", "NoError"),
                    "status": event.get("status", "Available"),
                    "timestamp": datetime.datetime.now(datetime.UTC).isoformat(),
                })
            elif event_type == "meter":
                await client.send_call("MeterValues", {
                    "connectorId": event.get("connector_id", 0),
                    "meterValue": event.get("values", []),
                })
            elif event_type == "heartbeat":
                await client.send_call("Heartbeat", {})
            elif event_type == "transaction_started":
                await client.send_call("StartTransaction", {
                    "connectorId": event.get("connector_id", 1),
                    "idTag": event.get("id_tag", ""),
                    "meterStart": event.get("meter_start", 0),
                    "timestamp": event.get("timestamp", ""),
                })
            elif event_type == "transaction_stopped":
                await client.send_call("StopTransaction", {
                    "transactionId": event.get("transaction_id", 0),
                    "meterStop": event.get("meter_stop", 0),
                    "timestamp": event.get("timestamp", ""),
                })
        except Exception:
            _LOGGER.exception(
                f"Error forwarding {event_type} to service {getattr(client, 'service_id', 'unknown')}"
            )

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
