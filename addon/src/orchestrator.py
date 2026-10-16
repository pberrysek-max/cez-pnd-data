"""Runtime orchestrator for CEZ PND data polling.

Coordinates:
- Configurable polling schedule
- CEZ session management
- Bounded retries for assembly fetching
- Parsing and publishing sensor values to MQTT
- Detailed logging to diagnose missing or unavailable sensors
- Independent error handling for each meter and HDO data
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Awaitable, Callable, Union

from .auth import ServiceMaintenanceError
from .dip_client import DipMaintenanceError, DipTokenError
from .hdo_parser import parse_hdo_signals
from .parser import CezDataParser

logger = logging.getLogger(__name__)

_PARSER_KEY_TO_SENSOR_KEY: dict[str, str] = {
    "consumption_kw": "consumption",
    "production_kw": "production",
    "reactive_kw": "reactive",
    "reactive_import_inductive_kw": "reactive_import_inductive",
    "reactive_export_capacitive_kw": "reactive_export_capacitive",
    "reactive_export_inductive_kw": "reactive_export_inductive",
    "reactive_import_capacitive_kw": "reactive_import_capacitive",
    "daily_consumption_kwh": "daily_consumption",
    "daily_production_kwh": "daily_production",
    "register_consumption_kwh": "register_consumption",
    "register_production_kwh": "register_production",
    "register_low_tariff_kwh": "register_low_tariff",
    "register_high_tariff_kwh": "register_high_tariff",
}

CEZ_FETCH_ERROR = "CEZ_FETCH_ERROR"
MQTT_PUBLISH_ERROR = "MQTT_PUBLISH_ERROR"
SESSION_EXPIRED = "SESSION_EXPIRED"
NO_DATA_WARNING = "NO_DATA_AVAILABLE"
FETCH_ERROR = "ASSEMBLY_FETCH_ERROR"
HDO_FETCH_ERROR = "HDO_FETCH_ERROR"
HDO_TOKEN_ERROR = "HDO_TOKEN_ERROR"
DIP_MAINTENANCE = "DIP_MAINTENANCE"
PORTAL_MAINTENANCE = "PORTAL_MAINTENANCE"


class SessionExpiredError(Exception):
    """Raised when the CEZ session is expired (e.g. HTTP 401)."""


@dataclass(frozen=True)
class OrchestratorConfig:
    """Runtime configuration for the orchestrator loop."""

    electrometers: list[dict[str, str]]
    poll_interval_seconds: int = 900
    max_retries: int = 3
    retry_base_delay_seconds: float = 5.0
    email: str = ""

    @property
    def poll_interval(self) -> timedelta:
        return timedelta(seconds=self.poll_interval_seconds)

    @property
    def meter_id(self) -> str:
        """Return the first meter ID for backward compatibility."""
        if self.electrometers:
            return self.electrometers[0].get("electrometer_id", "unknown")
        return "unknown"

    @property
    def ean(self) -> str:
        """Return the first EAN for backward compatibility."""
        if self.electrometers:
            return self.electrometers[0].get("ean", "")
        return ""


FetcherCallable = Callable[..., Awaitable[dict[str, Any]]]
HdoFetcherCallable = Callable[..., Awaitable[dict[str, Any]]]
FetcherType = Union[FetcherCallable, Any]


ASSEMBLY_CONFIGS: list[dict[str, Any]] = [
    {"id": -1003, "name": "profile_all"},
    {"id": -1012, "name": "profile_consumption_reactive"},
    {"id": -1011, "name": "profile_production_reactive"},
    {"id": -1021, "name": "daily_consumption"},
    {"id": -1022, "name": "daily_production"},
    {
        "id": -1027,
        "name": "daily_registers",
        "fallback_yesterday": True,
    },
]


class Orchestrator:
    """Coordinates fetch, parse and publish cycles."""

    def __init__(
        self,
        config: OrchestratorConfig,
        auth_client: Any,
        fetcher: FetcherType,
        mqtt_publisher: Any,
        hdo_fetcher: HdoFetcherCallable | None = None,
    ) -> None:
        self._config = config
        self._auth = auth_client
        self._fetcher = fetcher
        self._hdo_fetcher = hdo_fetcher
        self._mqtt = mqtt_publisher

    async def run_loop(self) -> None:
        """Start the MQTT publisher and poll continuously."""
        logger.info(
            "Orchestrator starting — interval: %ds, meters: %d",
            self._config.poll_interval_seconds,
            len(self._config.electrometers),
        )

        try:
            self._mqtt.start()
            logger.info("MQTT publisher start requested")
        except Exception:
            logger.exception(
                "[%s] MQTT publisher failed to start",
                MQTT_PUBLISH_ERROR,
            )

        try:
            self._mqtt.publish_discovery()
            logger.info("MQTT discovery publication requested")
        except Exception:
            logger.exception(
                "[%s] MQTT discovery publication failed",
                MQTT_PUBLISH_ERROR,
            )

        while True:
            try:
                await self.run_once()
            except asyncio.CancelledError:
                logger.info("Orchestrator shutdown requested")
                raise
            except Exception:
                # An unexpected error must not permanently stop polling.
                logger.exception("Unexpected error in polling cycle")

            await asyncio.sleep(
                max(1, self._config.poll_interval_seconds)
            )

    async def run_once(self) -> None:
        """Execute one complete fetch, parse and publish cycle."""
        cycle_start = datetime.now()
        num_electrometers = len(self._config.electrometers)

        logger.info(
            "Starting poll cycle for %d meter(s)",
            num_electrometers,
        )

        if not self._config.electrometers:
            logger.warning("No electrometers configured; nothing to poll")
            return

        try:
            session = await self._auth.ensure_session()
        except ServiceMaintenanceError as exc:
            logger.warning(
                "[%s] CEZ portal maintenance: %s — skipping cycle",
                PORTAL_MAINTENANCE,
                exc,
            )
            return
        except Exception:
            logger.exception(
                "[%s] Unable to obtain CEZ session — skipping cycle",
                SESSION_EXPIRED,
            )
            return

        cookies = session.cookies
        state: dict[str, dict[str, Any]] = {}

        # Fetch and parse each meter independently.
        for electrometer in self._config.electrometers:
            meter_id = electrometer.get("electrometer_id", "unknown")

            try:
                assembly_data = await self._fetch_all_assemblies(
                    cookies,
                    meter_id,
                )
            except Exception:
                logger.exception(
                    "[%s] Unexpected fetch error for meter %s",
                    FETCH_ERROR,
                    meter_id,
                )
                continue

            if not assembly_data:
                logger.warning(
                    "[%s] No assembly data returned for meter %s",
                    NO_DATA_WARNING,
                    meter_id,
                )
                continue

            logger.info(
                "Meter %s: received %d assembly/assemblies: %s",
                meter_id,
                len(assembly_data),
                ", ".join(assembly_data.keys()),
            )

            parsed_values = 0

            for assembly_name, assembly_payload in assembly_data.items():
                try:
                    parser = CezDataParser(assembly_payload)
                    reading = parser.get_latest_reading_dict()
                except Exception:
                    logger.exception(
                        "Parser failed for meter %s, assembly %s",
                        meter_id,
                        assembly_name,
                    )
                    continue

                if not reading:
                    logger.warning(
                        "Parser returned no reading for meter %s, "
                        "assembly %s",
                        meter_id,
                        assembly_name,
                    )
                    continue

                logger.debug(
                    "Meter %s, assembly %s: parser keys: %s",
                    meter_id,
                    assembly_name,
                    list(reading.keys()),
                )

                reading_meter_id = (
                    reading.get("electrometer_id") or meter_id
                )
                meter_state = state.setdefault(reading_meter_id, {})

                for parser_key, value in reading.items():
                    if parser_key == "electrometer_id":
                        continue

                    sensor_key = _PARSER_KEY_TO_SENSOR_KEY.get(parser_key)

                    if sensor_key is None:
                        logger.debug(
                            "Unmapped parser key for meter %s: %s",
                            reading_meter_id,
                            parser_key,
                        )
                        continue

                    if value is None:
                        logger.debug(
                            "No value for sensor %s, meter %s",
                            sensor_key,
                            reading_meter_id,
                        )
                        continue

                    meter_state[sensor_key] = value
                    parsed_values += 1

            logger.info(
                "Meter %s: parsed %d usable sensor value(s)",
                meter_id,
                parsed_values,
            )

        # Publish parsed data and log what actually reached the publisher.
        if state:
            sensor_count = sum(len(values) for values in state.values())

            logger.info(
                "Parsed state ready for MQTT: %d meter(s), %d sensor value(s)",
                len(state),
                sensor_count,
            )
            logger.debug("Parsed sensor state: %s", state)

            try:
                self._mqtt.publish_state(state)
                logger.info(
                    "MQTT state publication requested for %d meter(s), "
                    "%d sensor value(s)",
                    len(state),
                    sensor_count,
                )
            except Exception:
                logger.exception(
                    "[%s] Failed to publish sensor state to MQTT",
                    MQTT_PUBLISH_ERROR,
                )
        else:
            logger.warning(
                "[%s] No usable sensor values parsed; "
                "nothing will be published to MQTT",
                NO_DATA_WARNING,
            )

        # HDO data is independent of the main PND sensor data.
        if self._hdo_fetcher:
            await self._poll_hdo(session)

        duration = (datetime.now() - cycle_start).total_seconds()
        logger.info(
            "Poll cycle completed in %.2fs for %d meter(s)",
            duration,
            num_electrometers,
        )

    async def _poll_hdo(self, session: Any) -> None:
        """Fetch and publish HDO signals without interrupting PND polling."""
        hdo_session = session

        try:
            if not hdo_session.has_live_context:
                logger.warning(
                    "Browser context is not alive; refreshing session for HDO"
                )
                hdo_session = await self._auth.ensure_session()
        except Exception:
            logger.exception(
                "[%s] Re-authentication for HDO failed",
                HDO_FETCH_ERROR,
            )
            return

        if not hdo_session.has_live_context:
            logger.warning(
                "[%s] No live browser context available; skipping HDO",
                HDO_FETCH_ERROR,
            )
            return

        context = hdo_session.context

        for electrometer in self._config.electrometers:
            meter_id = electrometer.get("electrometer_id", "unknown")
            ean = electrometer.get("ean", "")

            if not ean:
                logger.debug(
                    "No EAN configured for meter %s; skipping HDO",
                    meter_id,
                )
                continue

            try:
                hdo_raw = await self._hdo_fetcher(context, ean)
                hdo_data = parse_hdo_signals(hdo_raw)

                if not hdo_data:
                    logger.warning(
                        "HDO returned no parsed data for meter %s",
                        meter_id,
                    )
                    continue

                logger.info(
                    "Meter %s: parsed %d HDO value(s)",
                    meter_id,
                    len(hdo_data),
                )

                self._mqtt.publish_hdo_state(
                    hdo_data,
                    electrometer_id=meter_id,
                )
                logger.info(
                    "HDO MQTT publication requested for meter %s",
                    meter_id,
                )

            except DipMaintenanceError as exc:
                logger.warning(
                    "[%s] %s for meter %s — skipping HDO",
                    DIP_MAINTENANCE,
                    exc,
                    meter_id,
                )
            except DipTokenError as exc:
                logger.error(
                    "[%s] Token acquisition failed for meter %s: %s",
                    HDO_TOKEN_ERROR,
                    meter_id,
                    exc,
                )
            except Exception:
                logger.exception(
                    "[%s] HDO fetch, parse or publish failed for meter %s",
                    HDO_FETCH_ERROR,
                    meter_id,
                )

    async def _fetch_assembly(
        self,
        cookies: list[dict[str, Any]],
        meter_id: str,
        assembly_id: int,
        date_from: str,
        date_to: str,
    ) -> dict[str, Any] | None:
        """Fetch a single assembly from the PND API."""
        return await self._fetcher(
            cookies,
            electrometer_id=meter_id,
            assembly_id=assembly_id,
            date_from=date_from,
            date_to=date_to,
        )

    async def _fetch_assembly_with_fallback(
        self,
        cookies: list[dict[str, Any]],
        meter_id: str,
        config: dict[str, Any],
        date_from: str,
        date_to: str,
    ) -> dict[str, Any] | None:
        """Fetch an assembly, optionally falling back to yesterday."""
        payload = await self._fetch_assembly(
            cookies,
            meter_id,
            config["id"],
            date_from,
            date_to,
        )

        if payload is None:
            return None

        if (
            config.get("fallback_yesterday")
            and not payload.get("hasData", True)
        ):
            logger.warning(
                "[%s] Assembly %s has no data for today; "
                "retrying with yesterday's date range",
                NO_DATA_WARNING,
                config["name"],
            )

            date_obj = datetime.strptime(
                date_from.split()[0],
                "%d.%m.%Y",
            )
            yesterday = date_obj - timedelta(days=1)
            yesterday_from = yesterday.strftime("%d.%m.%Y")
            yesterday_to = date_from

            payload = await self._fetch_assembly(
                cookies,
                meter_id,
                config["id"],
                yesterday_from,
                yesterday_to,
            )

        return payload

    async def _fetch_all_assemblies(
        self,
        cookies: list[dict[str, Any]],
        meter_id: str = "unknown",
    ) -> dict[str, Any]:
        """Fetch all configured assemblies, retrying failed batch requests."""
        fetcher_obj = getattr(self._fetcher, "__self__", None)

        # Prefer the fetcher's batch implementation when available.
        if fetcher_obj is not None and hasattr(fetcher_obj, "fetch_all"):
            last_error: Exception | None = None

            for attempt in range(
                1,
                max(1, self._config.max_retries) + 1,
            ):
                try:
                    result = await fetcher_obj.fetch_all(
                        cookies,
                        meter_id,
                        ASSEMBLY_CONFIGS,
                    )

                    if result:
                        logger.debug(
                            "Batch fetch succeeded for meter %s "
                            "on attempt %d",
                            meter_id,
                            attempt,
                        )
                        return result

                    logger.warning(
                        "Batch fetch returned no assemblies for meter %s "
                        "(attempt %d/%d)",
                        meter_id,
                        attempt,
                        max(1, self._config.max_retries),
                    )

                except Exception as exc:
                    last_error = exc
                    logger.warning(
                        "[%s] Batch fetch failed for meter %s "
                        "(attempt %d/%d): %s",
                        FETCH_ERROR,
                        meter_id,
                        attempt,
                        max(1, self._config.max_retries),
                        exc,
                    )

                if attempt < max(1, self._config.max_retries):
                    delay = self._config.retry_base_delay_seconds * (
                        2 ** (attempt - 1)
                    )
                    await asyncio.sleep(delay)

            if last_error is not None:
                logger.error(
                    "[%s] Batch fetch exhausted retries for meter %s",
                    FETCH_ERROR,
                    meter_id,
                    exc_info=(
                        type(last_error),
                        last_error,
                        last_error.__traceback__,
                    ),
                )

            return {}

        # Fallback for fetchers that do not expose fetch_all().
        results: dict[str, Any] = {}
        today = datetime.now()
        date_from = today.strftime("%d.%m.%Y 00:00")
        date_to = today.strftime("%d.%m.%Y 23:59")

        for config in ASSEMBLY_CONFIGS:
            try:
                payload = await self._fetch_assembly_with_fallback(
                    cookies,
                    meter_id,
                    config,
                    date_from,
                    date_to,
                )

                if payload and payload.get("hasData"):
                    results[config["name"]] = payload
                    logger.debug(
                        "Assembly %s returned data for meter %s",
                        config["name"],
                        meter_id,
                    )
                else:
                    logger.warning(
                        "[%s] Assembly %s returned no data for meter %s",
                        FETCH_ERROR,
                        config["name"],
                        meter_id,
                    )

            except Exception:
                logger.exception(
                    "[%s] Assembly %s failed for meter %s",
                    FETCH_ERROR,
                    config["name"],
                    meter_id,
                )

        return results

    async def _fetch_with_retry(
        self,
        cookies: Any,
        *,
        _reauthed: bool = False,
    ) -> dict[str, Any] | None:
        """Fetch through a simple callable with retry and re-auth support."""
        last_error: Exception | None = None

        for attempt in range(
            1,
            max(1, self._config.max_retries) + 1,
        ):
            try:
                return await self._fetcher(cookies)

            except SessionExpiredError:
                if _reauthed:
                    logger.error(
                        "[%s] Session still expired after re-authentication",
                        SESSION_EXPIRED,
                    )
                    return None

                logger.warning(
                    "[%s] Session expired; attempting re-authentication",
                    SESSION_EXPIRED,
                )

                try:
                    session = await self._auth.ensure_session()
                    cookies = session.cookies
                except Exception:
                    logger.exception(
                        "[%s] Re-authentication failed",
                        SESSION_EXPIRED,
                    )
                    return None

                return await self._fetch_with_retry(
                    cookies,
                    _reauthed=True,
                )

            except Exception as exc:
                last_error = exc

                if attempt < max(1, self._config.max_retries):
                    delay = self._config.retry_base_delay_seconds * (
                        2 ** (attempt - 1)
                    )
                    logger.warning(
                        "[%s] Fetch failed (attempt %d/%d): %s; "
                        "retrying in %.1fs",
                        CEZ_FETCH_ERROR,
                        attempt,
                        max(1, self._config.max_retries),
                        exc,
                        delay,
                    )
                    await asyncio.sleep(delay)

        logger.error(
            "[%s] Fetch failed after retries: %s",
            CEZ_FETCH_ERROR,
            last_error,
        )
        return None
