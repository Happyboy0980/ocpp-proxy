"""
EVBox ELVI / G4E-WBO  –  evbStatusNotification DataTransfer parser
====================================================================

The charger sends a proprietary OCPP DataTransfer every ~30 s:
    vendorId  = "EV-BOX"
    messageId = "evbStatusNotification"
    data      = "<comma-separated with {group} tokens>"

Usage
-----
    from ocpp_proxy.evbox_datatransfer import parse_evb_status

    parsed = parse_evb_status(data_string)
    if parsed:
        print(parsed["status"])          # "Charging"
        print(parsed["total_energy_wh"]) # 21921036
        print(parsed["pilot_state"])     # "C"

The returned dict is always JSON-serialisable so you can forward it over
REST, WebSocket, MQTT or store it anywhere.

Format reference  (verified with live captures June 2026)
----------------------------------------------------------
Two firmware variants are auto-detected (both have 25 top-level tokens):

  OLD firmware (pre-P0424, e.g. W6.0.0):
    1  connectorId
    2  status
    3  errorCode
    4  info
    5  vendorErrorCode
    6  ledColor
    7  ledOn
    8  {offeredCurrent_x4, maxPower_W, cpDutyCyclePct}
    9  {totalLifetimeEnergy_Wh, sessionEnergy_Wh}
    10 {pilotState_char, internalSupply_mV, pilotPosPeak_mV, pilotNegPeak_mV}
    11 unknown1
    12 unknown2
    13 gridVoltage_V  (L-L)
    14 timestamp
    15 transactionId
    16 cellularSignal_bars  <-- integer 1-5
    17 {0,0,0,0,0,0,0,0,0} (meter group, all zero if no energy-meter installed)
    18..25  config params

  NEW firmware (P0424+, W7.x.x, e.g. W7.1.0-020):
    1..15  identical
    16 wifiRssi_dBm         <-- integer 30-90 (magnitude, sign negative)
    17 {L1_V, internalTemp_C?, unk, unk, 0, 0, activePower_W?, 0, 0}
    18 clockAlignedInterval_s
    19 sessionDuration_min  (0 when idle)
    20 cellularSignal_bars
    21 internalParam        (slowly changing, NOT current limit)
    22 unknown
    23 clockAlignedInterval_s2?
    24 ocppCurrentLimit_da  (e.g. 80 = 8.0 A from SetChargingProfile)
    25 firmwareParam        (constant 5004)

Pilot state / CP voltage mapping (IEC 61851 / SAE J1772):
    A  no vehicle connected   pilot ≈ 12 V   ASCII 65
    B  vehicle connected      pilot ≈  9 V   ASCII 66
    C  vehicle charging       pilot ≈  6 V   ASCII 67
    D  charging + ventilation pilot ≈  3 V   ASCII 68

Confirmed field meanings (cross-validated with live captures at 8 A, 16 A, 32 A, Available):
  power group [0]  = hardware max current A (constant for this charger; 0 when no car)
  power group [1]  = rated max power W  (≈4800 W for this charger)
  power group [2]  = CP PWM duty cycle %  (13% → 13×0.6 = 7.8 A ≈ 8 A ✓)
  energy group [0] = lifetime energy meter Wh  (matches StopTransaction meterStop)
  energy group [1] = session energy Wh  (0 when no car)
  pilot group [1]  = internal 12 V supply in mV  (~12100-12200)
  pilot group [2]  = CP pilot positive peak mV  (~12 V=A, ~9 V=B, ~6 V=C)
  pilot group [3]  = CP pilot negative peak mV  (~12 V when car connected, large default when no car)
  new FW token[16] = WiFi RSSI magnitude (78 → -78 dBm)
  new FW token[17] = meter group:
    [0] = L1 phase voltage V
    [1] = internal temp °C (tentative)
    [3] = measured current dA (73=7.3 A, 152=15.2 A, 310=31.0 A ✓)
    [6] = power factor × 1000 (993=0.993, 997=0.997, 999=0.999 ✓)
  new FW token[18] = clock aligned interval s
  new FW token[19] = session duration minutes  (0 when idle)
  new FW token[20] = cellular signal bars
  new FW token[24] = OCPP current limit in dA  (80=8.0 A, 160=16.0 A, 320=32.0 A via SetChargingProfile)

Note: in the old firmware the letter ASCII value coincidentally equals the
absolute WiFi RSSI (A=65 → -65 dBm).  In the new firmware the RSSI is an
explicit integer at position 16.
"""

from __future__ import annotations

import logging
from typing import Any

_LOGGER = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Tokeniser
# ---------------------------------------------------------------------------

def _tokenize(data: str) -> list[str]:
    """Split on top-level commas; treat {...} as a single token."""
    tokens: list[str] = []
    depth = 0
    buf: list[str] = []
    for ch in data:
        if ch == "{":
            depth += 1
            buf.append(ch)
        elif ch == "}":
            depth -= 1
            buf.append(ch)
        elif ch == "," and depth == 0:
            tokens.append("".join(buf).strip())
            buf = []
        else:
            buf.append(ch)
    if buf:
        tokens.append("".join(buf).strip())
    return tokens


def _parse_group(token: str) -> list[Any]:
    """Convert '{a,b,c}' → [a, b, c] with auto int/str typing."""
    inner = token.strip("{}").strip()
    if not inner:
        return []
    parts = [p.strip() for p in inner.split(",")]
    result: list[Any] = []
    for p in parts:
        try:
            result.append(int(p))
        except ValueError:
            result.append(p)
    return result


def _int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


# ---------------------------------------------------------------------------
# Public parser
# ---------------------------------------------------------------------------

def parse_evb_status(data: str) -> dict[str, Any] | None:
    """Parse an evbStatusNotification data string.

    Returns a flat dict with all decoded fields, or None on parse error.

    All values are primitive Python types (str / int / float / bool / list)
    so the result is always JSON-serialisable.
    """
    tokens = _tokenize(data)
    if len(tokens) < 25:
        _LOGGER.warning("evbStatusNotification: expected ≥25 tokens, got %d", len(tokens))
        return None

    try:
        # ---- shared fields 1-15 ----
        connector_id   = _int(tokens[0])
        status         = tokens[1]
        error_code     = tokens[2]
        info           = tokens[3]
        vendor_err     = _int(tokens[4])
        led_color      = tokens[5]
        led_on         = bool(_int(tokens[6]))

        power_grp      = _parse_group(tokens[7])   # {P_W, Pmax_W, dutyCycle_%}
        energy_grp     = _parse_group(tokens[8])   # {total_Wh, reactive_Wh}
        pilot_grp      = _parse_group(tokens[9])   # {state, intV_mV, pilotV_mV, cableCal}

        unknown1       = _int(tokens[10])
        unknown2       = _int(tokens[11])
        grid_voltage   = _int(tokens[12])
        timestamp      = tokens[13]
        transaction_id = _int(tokens[14])

        # ---- firmware version detection ----
        # position 16 (index 15):  old FW = phases (1-3),  new FW = RSSI (30-90)
        token16 = _int(tokens[15])
        new_firmware = token16 > 3

        if new_firmware:
            wifi_rssi_dbm         = -token16              # e.g. 78 → -78 dBm
            meter_grp             = _parse_group(tokens[16])  # 9 values
            unknown3              = _int(tokens[17])      # unknown: 270 idle, 310 charging
            session_duration_min  = _int(tokens[18])   # 0 when idle
            cellular_bars         = _int(tokens[19])   # 1-5 bars
            internal_param        = _int(tokens[20])   # slowly changing, identity unknown
            unknown5              = _int(tokens[21])
            clock_aligned_s       = _int(tokens[22])
            ocpp_current_limit_da = _int(tokens[23])   # 80 = 8.0 A from SetChargingProfile; HeartbeatInterval when idle
            firmware_param        = _int(tokens[24])   # constant 5004
        else:
            cellular_bars         = token16            # signal bars 1-5
            meter_grp             = _parse_group(tokens[16])  # 9 values
            unknown3              = _int(tokens[17])
            session_duration_min  = _int(tokens[18])   # tentative
            unknown5              = _int(tokens[19])
            internal_param        = _int(tokens[20])
            unknown6              = _int(tokens[21])
            clock_aligned_s       = _int(tokens[22])
            ocpp_current_limit_da = _int(tokens[23])
            firmware_param        = _int(tokens[24])
            # derive RSSI from pilot state ASCII (old FW trick)
            pilot_char = pilot_grp[0] if pilot_grp else "A"
            wifi_rssi_dbm = -ord(pilot_char) if isinstance(pilot_char, str) and len(pilot_char) == 1 else 0

        # ---- pilot group ----
        pilot_state         = str(pilot_grp[0]) if len(pilot_grp) > 0 else "?"
        internal_supply_mv  = _int(pilot_grp[1]) if len(pilot_grp) > 1 else 0  # ~12100 mV = 12.1 V
        pilot_pos_peak_mv   = _int(pilot_grp[2]) if len(pilot_grp) > 2 else 0  # 12V=A, 9V=B, 6V=C
        pilot_neg_peak_mv   = _int(pilot_grp[3]) if len(pilot_grp) > 3 else 0  # ~12 V when car connected

        # ---- power group ----
        # [0] = hardware max current A (constant for this charger, 0 when no car)
        # [1] = rated max power W
        # [2] = CP PWM duty cycle % (13% × 0.6 = 7.8 A ≈ 8 A)
        hardware_max_current_a = _int(power_grp[0]) if len(power_grp) > 0 else 0
        max_power_w            = _int(power_grp[1]) if len(power_grp) > 1 else 0
        cp_duty_cycle          = _int(power_grp[2]) if len(power_grp) > 2 else 0

        # ---- energy group ----
        total_energy_wh   = _int(energy_grp[0]) if len(energy_grp) > 0 else 0
        session_energy_wh = _int(energy_grp[1]) if len(energy_grp) > 1 else 0

        # ---- meter group ----
        # [0] = L1 phase voltage V
        # [3] = measured current dA (e.g. 73 = 7.3 A; confirmed 8/16/32 A captures)
        # [6] = power factor × 1000 (e.g. 993 = 0.993; ~1.0 for resistive EV load)
        l1_voltage_v      = _int(meter_grp[0]) if len(meter_grp) > 0 else None
        internal_temp_c   = _int(meter_grp[1]) if len(meter_grp) > 1 else None  # tentative
        measured_i_da     = _int(meter_grp[3]) if len(meter_grp) > 3 else 0
        pf_raw            = _int(meter_grp[6]) if len(meter_grp) > 6 else 0
        measured_current_a: float | None = round(measured_i_da / 10.0, 1) if measured_i_da else None
        power_factor: float | None = round(pf_raw / 1000.0, 3) if pf_raw else None
        active_power_w: float | None = (
            round(l1_voltage_v * (measured_i_da / 10.0) * (pf_raw / 1000.0), 0)
            if l1_voltage_v and measured_i_da and pf_raw else None
        )

        # ---- derived/friendly values ----
        # Offered current via CP duty cycle (IEC 61851: I = duty% × 0.6 for 10-85%)
        offered_current_a: float | None = None
        if 10 <= cp_duty_cycle <= 85:
            offered_current_a = round(cp_duty_cycle * 0.6, 1)

        # OCPP limit: only meaningful during an active session
        # (when idle, this field slot shows HeartbeatInterval instead)
        ocpp_current_limit_a = (ocpp_current_limit_da / 10.0
                                if transaction_id > 0 else None)

        return {
            # Core status
            "connector_id":         connector_id,
            "status":               status,           # Available / Charging / Preparing / SuspendedEVSE / SuspendedEV
            "error_code":           error_code,
            "info":                 info,
            "vendor_error_code":    vendor_err,
            "timestamp":            timestamp,
            "transaction_id":       transaction_id,

            # LED
            "led_color":            led_color,        # Green / Blue / Yellow / Red / Off
            "led_on":               led_on,

            # Pilot / IEC 61851 state
            "pilot_state":          pilot_state,      # A=no car, B=connected, C=charging
            "car_connected":        pilot_state in ("B", "C", "D"),
            "is_charging":          pilot_state == "C",
            "internal_supply_mv":   internal_supply_mv,   # ~12100 mV (12 V rail)
            "pilot_pos_peak_mv":    pilot_pos_peak_mv,    # 12 V=A, 9 V=B, 6 V=C
            "pilot_neg_peak_mv":    pilot_neg_peak_mv,    # ~12 V when car connected

            # Current / power
            "offered_current_a":    offered_current_a,    # from CP duty cycle (IEC 61851 × 0.6)
            "measured_current_a":   measured_current_a,   # energy meter reading (dA/10)
            "power_factor":         power_factor,         # meter_grp[6]/1000 (~0.993-0.999)
            "active_power_w":       active_power_w,       # V × I × PF (None when idle)
            "hardware_max_current_a": hardware_max_current_a,  # charger hw limit (constant)
            "cp_duty_cycle_pct":    cp_duty_cycle,        # raw duty cycle %
            "max_power_w":          max_power_w,          # rated charger max power
            "ocpp_current_limit_a": ocpp_current_limit_a, # from SetChargingProfile (None when idle)

            # Energy
            "total_energy_wh":      total_energy_wh,      # lifetime meter (matches meterStop)
            "total_energy_kwh":     round(total_energy_wh / 1000, 3),
            "session_energy_wh":    session_energy_wh,    # this session (0 when no car)
            "session_energy_kwh":   round(session_energy_wh / 1000, 3),

            # Grid
            "grid_voltage_v":       grid_voltage,         # L-L voltage (fluctuates with load)

            # Connectivity
            "wifi_rssi_dbm":        wifi_rssi_dbm,        # e.g. -72
            "cellular_signal_bars": cellular_bars,        # 1-5 bars

            # Meter values (energy meter required; all 0 if no meter installed)
            "l1_voltage_v":         l1_voltage_v,         # L1-to-N ~232 V
            "internal_temp_c":      internal_temp_c,      # tentative: charger temp °C
            "meter_group_raw":      meter_grp,            # all 9 raw values

            # Session
            "session_duration_min": session_duration_min, # 0 when idle

            # Meta
            "firmware_generation":  "new" if new_firmware else "old",
            "clock_aligned_interval_s": clock_aligned_s,
        }

    except Exception as exc:
        _LOGGER.warning("evbStatusNotification parse error: %s  data=%r", exc, data)
        return None


def is_evb_status(vendor_id: str, message_id: str) -> bool:
    """Return True if this DataTransfer is an EV-BOX status notification."""
    return vendor_id == "EV-BOX" and message_id == "evbStatusNotification"
