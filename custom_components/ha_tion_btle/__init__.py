"""The Tion breezer component."""
from __future__ import annotations

import asyncio
import datetime
from functools import cached_property
import logging
import math
from time import monotonic

from bleak import BleakClient
from bleak.backends.device import BLEDevice
from bleak_retry_connector import BleakNotFoundError, establish_connection
from homeassistant.components import bluetooth
from homeassistant.components.bluetooth import BluetoothCallbackMatcher
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant, callback
from homeassistant.exceptions import ConfigEntryNotReady
from homeassistant.helpers.update_coordinator import DataUpdateCoordinator, UpdateFailed

from ._vendor import tion_btle
from ._vendor.tion_btle.tion import Tion
from .const import DOMAIN, TION_SCHEMA, CONF_KEEP_ALIVE, CONF_AWAY_TEMP, CONF_MAC, PLATFORMS

_LOGGER = logging.getLogger(__name__)

ALTERNATE_SOURCE_COOLDOWN_SECONDS = 60
SETUP_CLEANUP_TIMEOUT_SECONDS = 10
SHUTDOWN_TIMEOUT_SECONDS = 10
SETUP_TIMEOUT_SECONDS = 75


async def async_setup(hass, config):
    return True


async def async_setup_entry(hass, config_entry: ConfigEntry):
    _LOGGER.info("Setting up %s ", config_entry.unique_id)

    domain_data = hass.data.setdefault(DOMAIN, {})

    try:
        instance = TionInstance(hass, config_entry)
    except Exception:
        if not domain_data:
            hass.data.pop(DOMAIN)
        raise
    entry_key = config_entry.unique_id or config_entry.entry_id
    domain_data[entry_key] = instance
    config_entry.async_on_unload(
        bluetooth.async_register_callback(
            hass=hass,
            callback=instance.update_btle_device,
            match_dict=BluetoothCallbackMatcher(
                address=instance.config[CONF_MAC],
                connectable=True,
            ),
            mode=bluetooth.BluetoothScanningMode.ACTIVE,
        )
    )

    refresh_task = hass.async_create_task(instance.async_config_entry_first_refresh())
    try:
        done, _pending = await asyncio.wait(
            {refresh_task},
            timeout=SETUP_TIMEOUT_SECONDS,
        )
        if not done:
            _cancel_and_detach_task(refresh_task)
            raise ConfigEntryNotReady(
                f"Timed out setting up Tion after {SETUP_TIMEOUT_SECONDS} seconds"
            )
        refresh_task.result()
    except ConfigEntryNotReady:
        await _async_finish_failed_setup(hass, domain_data, entry_key, instance)
        raise
    except asyncio.CancelledError:
        _cancel_and_detach_task(refresh_task)
        await asyncio.shield(
            _async_cleanup_failed_setup(hass, domain_data, entry_key, instance)
        )
        raise
    except Exception:
        _cancel_and_detach_task(refresh_task)
        await _async_finish_failed_setup(hass, domain_data, entry_key, instance)
        raise
    config_entry.async_on_unload(
        config_entry.add_update_listener(_async_reload_entry)
    )

    await hass.config_entries.async_forward_entry_setups(config_entry, PLATFORMS)
    return True


def _consume_task_result(task: asyncio.Task) -> None:
    """Consume completion from a detached task without hiding setup errors."""
    try:
        task.result()
    except (asyncio.CancelledError, Exception):
        pass


def _cancel_and_detach_task(task: asyncio.Task) -> None:
    """Request cancellation without waiting for a resistant coroutine."""
    if not task.done():
        task.cancel()
    task.add_done_callback(_consume_task_result)


async def _async_cleanup_failed_setup(
    hass: HomeAssistant,
    domain_data: dict,
    entry_key: str,
    instance: TionInstance,
) -> None:
    """Remove setup state without letting resistant shutdown block setup."""
    domain_data.pop(entry_key, None)
    if not domain_data:
        hass.data.pop(DOMAIN, None)

    shutdown_task = hass.async_create_task(instance.async_shutdown())
    try:
        await asyncio.wait_for(
            asyncio.shield(shutdown_task),
            timeout=SETUP_CLEANUP_TIMEOUT_SECONDS,
        )
    except TimeoutError:
        _LOGGER.warning(
            "Timed out cleaning up %s after failed setup; cleanup continues "
            "in the background",
            instance.name,
        )
    except asyncio.CancelledError:
        # Preserve setup cancellation. The HA-owned shutdown task keeps
        # cleaning up independently and will be awaited by HA at stop.
        raise
    except Exception as err:
        _LOGGER.debug(
            "Error cleaning up %s after failed setup: %s",
            instance.name,
            err,
        )


async def _async_finish_failed_setup(
    hass: HomeAssistant,
    domain_data: dict,
    entry_key: str,
    instance: TionInstance,
) -> None:
    """Run cleanup as a task so outer cancellation cannot interrupt it."""
    cleanup_task = hass.async_create_task(
        _async_cleanup_failed_setup(hass, domain_data, entry_key, instance)
    )
    try:
        await asyncio.shield(cleanup_task)
    except asyncio.CancelledError:
        _cancel_and_detach_task(cleanup_task)
        raise


async def _async_reload_entry(hass: HomeAssistant, config_entry: ConfigEntry) -> None:
    """Reload a Tion entry when its options change."""
    await hass.config_entries.async_reload(config_entry.entry_id)


async def async_unload_entry(hass: HomeAssistant, config_entry: ConfigEntry) -> bool:
    """Unload platforms and release the coordinator and BLE connection."""
    unload_ok = await hass.config_entries.async_unload_platforms(
        config_entry,
        PLATFORMS,
    )
    if not unload_ok:
        return False

    entry_key = config_entry.unique_id or config_entry.entry_id
    instance = hass.data.get(DOMAIN, {}).pop(entry_key, None)
    if instance is not None:
        await instance.async_shutdown()

    if not hass.data.get(DOMAIN):
        hass.data.pop(DOMAIN, None)

    return True


class TionInstance(DataUpdateCoordinator):
    def __init__(self, hass: HomeAssistant, config_entry: ConfigEntry):

        self._config_entry: ConfigEntry = config_entry

        assert self.config[CONF_MAC] is not None
        # https://developers.home-assistant.io/docs/network_discovery/#fetching-the-bleak-bledevice-from-the-address
        btle_device = bluetooth.async_ble_device_from_address(hass, self.config[CONF_MAC], connectable=True)
        if btle_device is None:
            raise ConfigEntryNotReady

        self.__keep_alive: int = 60
        try:
            self.__keep_alive = self.config[CONF_KEEP_ALIVE]
        except KeyError:
            pass

        self.__tion: Tion = self.getTion(self.model, btle_device)
        self.__tion.set_client_factory(self._async_create_client)
        self.__tion.set_alternate_client_factory(self._async_create_alternate_client)
        self.__tion.set_session_failure_callback(self._handle_session_failure)
        # A successful coordinator refresh stores the complete state required
        # by Tion's full-state SET command. Reuse it for up to two polling
        # intervals so normal controls need only SET + its response; cold,
        # stale, or uncertain state still falls back to GET + SET.
        self.__tion.set_state_cache_ttl(max(self.__keep_alive * 2, 60))
        self.__keep_alive = datetime.timedelta(seconds=self.__keep_alive)
        self.rssi: int = 0
        self._last_connection_source: str | None = None
        self._failed_connection_source: str | None = None
        self._failed_connection_at: float | None = None
        self._tion_shutdown = False

        if self._config_entry.unique_id is None:
            _LOGGER.critical(f"Unique id is None for {self._config_entry.title}! "
                             f"Will fix it by using {self.unique_id}")
            hass.config_entries.async_update_entry(
                entry=self._config_entry,
                unique_id=self.unique_id,
            )
            _LOGGER.critical("Done! Please restart Home Assistant.")

        super().__init__(
            name=self.config['name'] if 'name' in self.config else TION_SCHEMA['name']['default'],
            hass=hass,
            logger=_LOGGER,
            config_entry=config_entry,
            update_interval=self.__keep_alive,
            update_method=self.async_update_state,
        )

    @property
    def config(self) -> dict:
        try:
            data = dict(self._config_entry.data or {})
        except AttributeError:
            data = {}

        try:
            options = self._config_entry.options or {}
            data.update(options)
        except AttributeError:
            pass
        return data

    @staticmethod
    def _decode_state(state: str) -> bool:
        return True if state == "on" else False

    def _prepare_response(self, response: dict) -> dict:
        """Convert a tion-btle response to Home Assistant state."""
        response = response.copy()
        response["is_on"] = self._decode_state(response["state"])
        response["heater"] = self._decode_state(response["heater"])
        response["is_heating"] = self._decode_state(response["heating"])
        response["filter_remain"] = math.ceil(response["filter_remain"])
        response["fan_speed"] = int(response["fan_speed"])
        response["rssi"] = self.rssi
        return response

    @staticmethod
    def _device_source(device: BLEDevice) -> str | None:
        """Return the Home Assistant scanner source embedded in a BLEDevice."""
        details = getattr(device, "details", None)
        if isinstance(details, dict):
            source = details.get("source")
            return str(source) if source is not None else None
        return None

    def _handle_session_failure(self, err: Exception) -> None:
        """Temporarily deprioritize the scanner used by a failed BLE session."""
        self._failed_connection_source = self._last_connection_source
        self._failed_connection_at = monotonic()
        _LOGGER.debug(
            "BLE session for %s failed through source %s: %s",
            self.name,
            self._failed_connection_source or "unknown",
            err,
        )

    def _connection_candidates(self) -> list[tuple[BLEDevice, str | None, int]]:
        """Return one current connectable BLEDevice per scanner, best RSSI first."""
        address = self.config[CONF_MAC]
        by_source: dict[str, tuple[BLEDevice, str | None, int]] = {}
        preferred_device = bluetooth.async_ble_device_from_address(
            self.hass,
            address,
            connectable=True,
        )
        preferred_source = (
            self._device_source(preferred_device)
            if preferred_device is not None
            else None
        )

        try:
            scanner_devices = bluetooth.async_scanner_devices_by_address(
                self.hass,
                address,
                connectable=True,
            )
        except (AttributeError, TypeError):
            scanner_devices = []

        for scanner_device in scanner_devices:
            device = scanner_device.ble_device
            source = getattr(scanner_device.scanner, "source", None)
            source = str(source) if source is not None else self._device_source(device)
            advertisement = scanner_device.advertisement
            rssi = int(getattr(advertisement, "rssi", -127))
            key = source or repr(getattr(device, "details", None))
            current = by_source.get(key)
            if current is None or rssi > current[2]:
                by_source[key] = (device, source, rssi)

            if (
                preferred_source is None
                and preferred_device is not None
                and (
                    device is preferred_device
                    or getattr(device, "details", None)
                    == getattr(preferred_device, "details", None)
                )
            ):
                preferred_source = source

        if not by_source:
            if preferred_device is not None:
                by_source[
                    preferred_source
                    or repr(getattr(preferred_device, "details", None))
                ] = (
                    preferred_device,
                    preferred_source,
                    self.rssi,
                )

        return sorted(
            by_source.values(),
            key=lambda candidate: (
                candidate[1] == preferred_source,
                candidate[2],
            ),
            reverse=True,
        )

    async def _async_connect_candidate(
        self,
        device: BLEDevice,
        source: str | None,
        rssi: int,
        route: str,
    ) -> BleakClient:
        """Connect to one explicit Home Assistant Bluetooth source."""
        _LOGGER.info(
            "Connecting %s through %s source %s (RSSI %d)",
            self.name,
            route,
            source or "unknown",
            rssi,
        )
        self._last_connection_source = source
        try:
            client = await establish_connection(
                BleakClient,
                device,
                self.name,
                max_attempts=3,
            )
        except Exception:
            self._failed_connection_source = source
            self._failed_connection_at = monotonic()
            raise

        return client

    async def _async_create_client(self, _device: str | BLEDevice) -> BleakClient:
        """Connect through the best current source, avoiding a recent failure."""
        candidates = self._connection_candidates()
        if not candidates:
            raise BleakNotFoundError(
                f"Could not find connectable Tion {self.config[CONF_MAC]}"
            )

        if (
            self._failed_connection_source is not None
            and self._failed_connection_at is not None
            and monotonic() - self._failed_connection_at
            < ALTERNATE_SOURCE_COOLDOWN_SECONDS
        ):
            candidates.sort(
                key=lambda candidate: candidate[1] == self._failed_connection_source
            )

        return await self._async_connect_candidate(*candidates[0], route="primary")

    async def _async_create_alternate_client(
        self, _device: str | BLEDevice
    ) -> BleakClient:
        """Connect through a different scanner after a full BLE session failure."""
        candidates = self._connection_candidates()
        if not candidates:
            raise BleakNotFoundError(
                f"Could not find connectable Tion {self.config[CONF_MAC]}"
            )

        alternate = next(
            (
                candidate
                for candidate in candidates
                if candidate[1] != self._last_connection_source
            ),
            candidates[0],
        )
        return await self._async_connect_candidate(*alternate, route="alternate")

    async def async_update_state(self):
        self.logger.info("Tion instance update started")
        response: dict[str, str | bool | int] = {}

        try:
            response = await self.__tion.get()
        except Exception as err:
            raise UpdateFailed(f"Unable to update Tion: {err}") from err

        response = self._prepare_response(response)
        self.logger.debug(f"Result is {response}")
        return response

    @property
    def away_temp(self) -> int:
        """Temperature for away mode"""
        return self.config[CONF_AWAY_TEMP] if CONF_AWAY_TEMP in self.config else TION_SCHEMA[CONF_AWAY_TEMP]['default']

    async def set(self, **kwargs):
        if "fan_speed" in kwargs:
            kwargs["fan_speed"] = int(kwargs["fan_speed"])

        if "is_on" in kwargs:
            kwargs["state"] = "on" if kwargs["is_on"] else "off"
            del kwargs["is_on"]
        if "heater" in kwargs:
            kwargs["heater"] = "on" if kwargs["heater"] else "off"

        args = ', '.join('%s=%r' % x for x in kwargs.items())
        _LOGGER.info("Need to set: " + args)
        try:
            response = await self.__tion.set(kwargs)
        except Exception as err:
            self.async_set_update_error(err)
            raise
        self.async_set_updated_data(self._prepare_response(response))

    async def async_shutdown(self) -> None:
        """Stop coordinator work and release an open BLE connection."""
        if self._tion_shutdown:
            return
        self._tion_shutdown = True
        await super().async_shutdown()
        try:
            async with asyncio.timeout(SHUTDOWN_TIMEOUT_SECONDS):
                await self.__tion.async_close()
        except Exception as err:
            _LOGGER.debug("Error closing %s during unload: %s", self.name, err)

    @staticmethod
    def getTion(model: str, mac: str | BLEDevice) -> tion_btle.TionS3 | tion_btle.TionLite | tion_btle.TionS4:
        if model == 'S3':
            from ._vendor.tion_btle.s3 import TionS3 as Breezer
        elif model == 'S4':
            from ._vendor.tion_btle.s4 import TionS4 as Breezer
        elif model == 'Lite':
            from ._vendor.tion_btle.lite import TionLite as Breezer
        else:
            raise NotImplementedError("Model '%s' is not supported!" % model)
        return Breezer(mac)

    @property
    def device_info(self):
        info = {"identifiers": {(DOMAIN, self.unique_id)}, "name": self.name, "manufacturer": "Tion",
                "model": self.data.get("model")}
        if self.data.get("fw_version") is not None:
            info['sw_version'] = self.data.get("fw_version")
        return info

    @cached_property
    def unique_id(self):
        return self.config[CONF_MAC]

    @cached_property
    def supported_air_sources(self) -> list[str]:
        if self.model == "S3":
            return ["outside", "mixed", "recirculation"]
        else:
            return ["outside", "recirculation"]

    @cached_property
    def model(self) -> str:
        try:
            model = self.config['model']
        except KeyError:
            _LOGGER.warning(f"Model was not found in config. "
                            f"Please update integration settings! Config is {self.config}")
            _LOGGER.warning("Assume that model is S3")
            model = 'S3'
        return model

    @callback
    def update_btle_device(
            self,
            service_info: bluetooth.BluetoothServiceInfoBleak,
            _change: bluetooth.BluetoothChange
    ) -> None:
        if service_info.device is not None:
            self.rssi = service_info.rssi
            self.__tion.update_btle_device(service_info.device)
