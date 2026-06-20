# Changelog

All notable changes to this Home Assistant add-on will be documented in this file.

## [0.1.20] - 2026-06-20

### Fixed
- Send `TriggerMessage(BootNotification)` and `TriggerMessage(StatusNotification)` to the
  charger 2 seconds after every (re)connect. This restarts HA's full configuration chain
  (GetConfiguration → ChangeConfiguration for measurands and sample interval) so all sensor
  entities (Vendor, Model, Status, Voltage, Current, Power, etc.) populate correctly.

## [0.1.19] - 2026-06-20

### Added
- DEBUG-level logging for every message flowing through the proxy in both directions:
  charger→services, services→charger, and proxied CALLRESULT responses.
- `LOG_LEVEL` environment variable (default `DEBUG`) to control log verbosity.

## [0.1.18] - 2026-06-20

### Fixed
- Persist last BootNotification, StatusNotification and MeterValues payloads to
  `/data/ocpp_proxy_state.json` so they survive proxy restarts.
- Replay persisted payloads to all already-connected backend services when the charger
  (re)connects, ensuring HA entities initialise immediately after a proxy restart.
- Added `replay_to_connected_services()` method to `OCPPServiceManager`.

## [0.1.17] - 2026-06-20

### Fixed
- Replay last BootNotification and StatusNotification to backend services that connect
  after the charger has already sent them (service-connects-first scenario).

## [0.1.16] - 2026-06-20

### Fixed
- Read add-on options from `/data/options.json` (Home Assistant Supervisor format) with
  fallback to YAML; configuration was always empty before this fix.
- Use `additional_headers` instead of deprecated `extra_headers` for websockets 14+
  compatibility when connecting to backend OCPP services.
- Move `start_services()` into `on_startup` handler so outbound service connections share
  the correct asyncio event loop with the web server.
- Use `asyncio.run()` instead of `get_event_loop()` for Python 3.14 compatibility.
- Store app state in a mutable `app["state"]` dict to avoid aiohttp DeprecationWarning.

## [0.1.0] - 2024-01-XX

### Added
- Initial release of OCPP Proxy Home Assistant Add-on
- Support for OCPP 1.6 and 2.0.1 protocols
- Automatic OCPP version detection
- Multi-backend subscription and control arbitration
- Home Assistant API integration
- Session tracking and revenue logging
- WebSocket and REST API endpoints
- Provider whitelist/blacklist support
- Rate limiting and safety controls
- OCPP service client connections
- Web-based status interface

### Features
- Single charger, multiple backend support
- Smart control arbitration with user override
- Real-time event broadcasting
- SQLite session persistence
- Home Assistant sensor integration
- Presence-based charging control
- Manual override controls
- CSV export functionality

### Technical
- Built on python-ocpp library
- aiohttp WebSocket server
- SQLite database for session logging
- Poetry dependency management
- Comprehensive test suite (85% coverage)
- Docker containerization
- Multi-architecture support (amd64, armv7, aarch64, armhf)