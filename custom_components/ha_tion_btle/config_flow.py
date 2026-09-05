"""Adds config flow for Tion custom component."""
from __future__ import annotations

import logging
import datetime
import asyncio

import bleak
from bleak import BleakClient
from bleak.backends.device import BLEDevice
from bleak_retry_connector import BleakNotFoundError, establish_connection
import voluptuous as vol

from homeassistant import config_entries
from homeassistant.components import bluetooth
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import callback, async_get_hass
from ._vendor import tion_btle
from ._vendor.tion_btle.tion import Tion

from .const import (
    BLUETOOTH_SOURCE_AUTO,
    CONF_BLUETOOTH_SOURCE,
    CONF_MAC,
    CONF_REPAIR,
    CONF_STRICT_BLUETOOTH_SOURCE,
    DOMAIN,
    TION_SCHEMA,
)

_LOGGER = logging.getLogger(__name__)

TION_OPTIONS_SCHEMA = TION_SCHEMA.copy()
del TION_OPTIONS_SCHEMA["pair"]
del TION_OPTIONS_SCHEMA[CONF_MAC]
del TION_OPTIONS_SCHEMA["model"]


class TionFlow:
    def __init__(self):
        self._data: dict = {}
        self._config_entry: ConfigEntry = {}
        self._retry: bool = False

    @staticmethod
    def __get_my_platform(config: dict):
        for i in config:
            if i['platform'] == DOMAIN:
                return i

    @staticmethod
    def __add_default_value(config: dict, key: str) -> dict:
        result = {}
        if key in config:
            if key == 'pair':
                # don't suggest pairing if device already configured
                result['default'] = False
            else:
                result['default'] = config[key]
        elif 'default' in TION_SCHEMA[key].keys():
            result['default'] = TION_SCHEMA[key]['default']

        return result

    @staticmethod
    def __add_value_from_saved_settings(config: dict, key: str) -> dict:
        result = {}
        try:
            value = config[key].seconds if isinstance(config[key], datetime.timedelta) else config[key]
            result['description'] = {"suggested_value": value}
        except (TypeError, KeyError):
            # TypeError -- config is not dict (have no climate in config, for example)
            # KeyError -- config have no key (have climate, but have no Tion)
            pass

        return result

    def get_schema(self, schema_description: dict = None) -> vol.Schema:
        schema = vol.Schema({})
        if schema_description is None:
            schema_description = {}

        for k in schema_description.keys():
            type = vol.Required if TION_SCHEMA[k]['required'] else vol.Optional
            options = {}
            options.update(self.__add_default_value(self.config, k))
            options.update(self.__add_value_from_saved_settings(self.config, k))
            if self._retry:
                options.update(self.__add_value_from_saved_settings(self._data, k))
            schema = schema.extend({type(k, **options): TION_SCHEMA[k]['type']})
        return schema

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
    def _device_source(device: BLEDevice) -> str | None:
        """Return the Home Assistant scanner source embedded in a BLEDevice."""
        details = getattr(device, "details", None)
        if isinstance(details, dict):
            source = details.get("source")
            return str(source) if source is not None else None
        return None

    @classmethod
    def _scanner_source(cls, scanner_device) -> str | None:
        """Return a stable source id for a Home Assistant scanner device."""
        source = getattr(scanner_device.scanner, "source", None)
        if source is None:
            source = cls._device_source(scanner_device.ble_device)
        return str(source) if source is not None else None

    @classmethod
    def _ble_device_from_source(
        cls,
        mac: str,
        source: str = BLUETOOTH_SOURCE_AUTO,
    ) -> BLEDevice | None:
        """Return the BLEDevice advertised by one explicit scanner source."""
        hass = async_get_hass()
        if source == BLUETOOTH_SOURCE_AUTO:
            return bluetooth.async_ble_device_from_address(
                hass=hass,
                address=mac,
                connectable=True,
            )

        try:
            scanner_devices = bluetooth.async_scanner_devices_by_address(
                hass,
                mac,
                connectable=True,
            )
        except (AttributeError, TypeError):
            scanner_devices = []

        return next(
            (
                scanner_device.ble_device
                for scanner_device in scanner_devices
                if cls._scanner_source(scanner_device) == source
            ),
            None,
        )

    def _source_choices(self, mac: str) -> dict[str, str]:
        """Return currently visible scanner sources for the options form."""
        choices = {BLUETOOTH_SOURCE_AUTO: "Automatic (all available sources)"}
        try:
            scanner_devices = bluetooth.async_scanner_devices_by_address(
                async_get_hass(),
                mac,
                connectable=True,
            )
        except (AttributeError, TypeError):
            scanner_devices = []

        best_by_source: dict[str, tuple[str, int]] = {}
        for scanner_device in scanner_devices:
            source = self._scanner_source(scanner_device)
            if source is None:
                continue
            scanner_name = str(
                getattr(scanner_device.scanner, "name", None) or source
            )
            rssi = int(getattr(scanner_device.advertisement, "rssi", -127))
            current = best_by_source.get(source)
            if current is None or rssi > current[1]:
                best_by_source[source] = (scanner_name, rssi)

        for source, (scanner_name, rssi) in sorted(best_by_source.items()):
            if scanner_name == source:
                choices[source] = f"{source} (RSSI {rssi})"
            else:
                choices[source] = f"{scanner_name} — {source} (RSSI {rssi})"

        configured_source = str(
            self.config.get(CONF_BLUETOOTH_SOURCE, BLUETOOTH_SOURCE_AUTO)
        )
        if configured_source not in choices:
            choices[configured_source] = (
                f"{configured_source} (configured, not currently visible)"
            )
        return choices

    def get_options_schema(self) -> vol.Schema:
        """Build options with the scanner sources currently seeing this Tion."""
        schema = self.get_schema(TION_OPTIONS_SCHEMA)
        source = str(
            self.config.get(CONF_BLUETOOTH_SOURCE, BLUETOOTH_SOURCE_AUTO)
        )
        strict = bool(self.config.get(CONF_STRICT_BLUETOOTH_SOURCE, False))
        return schema.extend(
            {
                vol.Optional(CONF_BLUETOOTH_SOURCE, default=source): vol.In(
                    self._source_choices(self.config[CONF_MAC])
                ),
                vol.Optional(
                    CONF_STRICT_BLUETOOTH_SOURCE,
                    default=strict,
                ): bool,
                vol.Optional(CONF_REPAIR, default=False): bool,
            }
        )

    @classmethod
    def getTion(
        cls,
        model: str,
        mac: str,
        source: str = BLUETOOTH_SOURCE_AUTO,
    ) -> tion_btle.TionS3 | tion_btle.TionLite | tion_btle.TionS4:

        btle_device = cls._ble_device_from_source(mac, source)
        if btle_device is None:
            message = f"Could not find device with {mac=} through source {source}"
            _LOGGER.critical("getTion: %s", message)
            raise bleak.BleakError(message)

        if model == 'S3':
            from ._vendor.tion_btle.s3 import TionS3 as Breezer
        elif model == 'S4':
            from ._vendor.tion_btle.s4 import TionS4 as Breezer
        elif model == 'Lite':
            from ._vendor.tion_btle.lite import TionLite as Breezer
        else:
            raise NotImplementedError("Model '%s' is not supported!" % model)
        tion = Breezer(btle_device)

        async def create_client(_device: str | BLEDevice) -> BleakClient:
            device = cls._ble_device_from_source(mac, source)
            if device is None:
                raise BleakNotFoundError(
                    f"Could not find connectable Tion {mac} through source {source}"
                )
            _LOGGER.info(
                "Pairing Tion %s through Bluetooth source %s",
                mac,
                cls._device_source(device) or source,
            )
            return await establish_connection(
                BleakClient,
                device,
                f"Tion {model}",
                max_attempts=3,
            )

        tion.set_client_factory(create_client)
        return tion


class TionConfigFlow(TionFlow, config_entries.ConfigFlow, domain=DOMAIN):
    """Initial setup."""
    VERSION = 1
    CONNECTION_CLASS = config_entries.CONN_CLASS_CLOUD_POLL

    def __init__(self):
        super().__init__()

    @staticmethod
    @callback
    def async_get_options_flow(config_entry):
        return TionOptionsFlowHandler(config_entry)

    async def _create_entry(self, data, title, step: str):
        if step in ["user", "pair"]:
            await self.async_set_unique_id(data["mac"])
            self._abort_if_unique_id_configured()
        return self.async_create_entry(title=title, data=data)

    async def async_step_user(self, input=None):
        """user initiates a flow via the user interface."""

        if input is not None:
            result = {}
            self._data = input
            if input['pair']:
                _LOGGER.debug("Showing pair info")
                return self.async_show_form(step_id="pair")
            else:
                _LOGGER.debug("Going create entry with name %s" % input['name'])
                _LOGGER.debug(input)
                try:
                    _tion: Tion = self.getTion(input['model'], input['mac'])
                    result = await _tion.get()
                except Exception as e:
                    _LOGGER.error("Could not get data from breezer. result is %s, error: %s" % (result, str(e)))
                    return self.async_show_form(step_id='add_failed')

                return await self._create_entry(title=input['name'], data=input, step="user")

        return self.async_show_form(step_id="user", data_schema=self.get_schema(TION_SCHEMA))

    async def async_step_pair(self, input):
        """Pair host and breezer"""
        _LOGGER.debug("Real pairing step")
        try:
            _LOGGER.debug(self._data)
            _tion: Tion = self.getTion(
                self._data['model'],
                self._data['mac'],
            )
            await _tion.pair()
        except Exception as e:
            _LOGGER.error(
                "Cannot pair device. Data is %s; %s: %s",
                self._data,
                type(e).__name__,
                str(e),
            )
            return self.async_show_form(step_id='pair_failed')

        return await self._create_entry(title=self._data['name'], data=self._data, step="pair")

    async def async_step_add_failed(self, input):
        _LOGGER.debug("Add failed. Returning to first step")
        self._retry = True
        return await self.async_step_user(None)

    async def async_step_pair_failed(self, input):
        _LOGGER.debug("Pair failed. Returning to first step")
        self._retry = True
        return await self.async_step_user(None)


class TionOptionsFlowHandler(TionConfigFlow, config_entries.OptionsFlow):
    """Change options dialog."""

    def __init__(self, config_entry):
        """Initialize Shelly options flow."""
        super().__init__()
        self._config_entry = config_entry
        self._entry_id = config_entry.entry_id

        # config_entry.add_update_listener(update_listener)

    @staticmethod
    def _normalize_options(input: dict) -> dict:
        """Remove the one-shot action and normalize source routing options."""
        options = dict(input)
        options.pop(CONF_REPAIR, None)
        if options.get(CONF_BLUETOOTH_SOURCE) == BLUETOOTH_SOURCE_AUTO:
            options[CONF_STRICT_BLUETOOTH_SOURCE] = False
        return options

    async def _async_setup_entry(self, options: dict | None = None) -> None:
        """Restore the entry after an exclusive pairing attempt."""
        if options is not None:
            self.hass.config_entries.async_update_entry(
                self._config_entry,
                options=options,
            )
        if self._config_entry.disabled_by is None:
            await self.hass.config_entries.async_setup(self._entry_id)

    async def async_step_init(self, input=None):
        if input is not None:
            repair = bool(input.get(CONF_REPAIR, False))
            self._data = self._normalize_options(input)
            if repair:
                if self._data.get(CONF_BLUETOOTH_SOURCE) == BLUETOOTH_SOURCE_AUTO:
                    return self.async_show_form(
                        step_id="init",
                        data_schema=self.get_options_schema(),
                        errors={"base": "pair_source_required"},
                    )
                return self.async_show_form(step_id="pair")
            return self.async_create_entry(title="", data=self._data)

        return self.async_show_form(
            step_id="init",
            data_schema=self.get_options_schema(),
        )

    async def async_step_pair(self, input):
        """Pair the existing Tion exclusively through the selected source."""
        source = str(self._data[CONF_BLUETOOTH_SOURCE])
        unload_ok = await self.hass.config_entries.async_unload(self._entry_id)
        if not unload_ok:
            return self.async_show_form(
                step_id="pair",
                errors={"base": "unload_failed"},
            )

        try:
            # Give the previous GATT session a brief moment to release the
            # breezer before opening the exclusive pairing connection.
            await asyncio.sleep(1)
            tion: Tion = self.getTion(
                self.config["model"],
                self.config[CONF_MAC],
                source,
            )
            await tion.pair()
        except asyncio.CancelledError:
            await asyncio.shield(self._async_setup_entry())
            raise
        except Exception as err:
            _LOGGER.error(
                "Cannot re-pair %s through source %s; %s: %s",
                self.config[CONF_MAC],
                source,
                type(err).__name__,
                err,
            )
            await self._async_setup_entry()
            return self.async_show_form(step_id="pair_failed")

        await self._async_setup_entry(self._data)
        return self.async_abort(reason="pair_successful")

    async def async_step_pair_failed(self, input):
        """Return to options after a failed re-pair attempt."""
        self._retry = True
        return await self.async_step_init()
