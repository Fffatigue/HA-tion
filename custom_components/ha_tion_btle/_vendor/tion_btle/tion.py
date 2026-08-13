from __future__ import annotations

import abc
import asyncio
import inspect
import logging
from asyncio import Semaphore
from typing import Any, Awaitable, Callable, Dict, List, Optional, Tuple, Union, final
from time import localtime, monotonic, strftime

from bleak import BleakClient
from bleak import exc
from bleak.backends.device import BLEDevice

_LOGGER = logging.getLogger(__name__)

BluetoothDevice = Union[str, BLEDevice]
ClientFactory = Callable[[BluetoothDevice], Awaitable[BleakClient]]
SessionFailureCallback = Callable[[Exception], None]

FULL_SESSION_ATTEMPTS = 2
FULL_SESSION_RETRY_DELAY_SECONDS = 2
GET_SESSION_ATTEMPTS = 2
GET_SESSION_RETRY_DELAY_SECONDS = 2
RESPONSE_TIMEOUT_SECONDS = 10.0
DISCONNECT_TIMEOUT_SECONDS = 5.0


class MaxTriesExceededError(Exception):
    pass


def _consume_background_task(task: asyncio.Task) -> None:
    """Retrieve a detached cleanup task's eventual result."""
    try:
        task.result()
    except asyncio.CancelledError:
        pass
    except Exception:
        _LOGGER.debug("Detached BLE cleanup task failed", exc_info=True)


def retry(retries: int = 2, delay: int = 0):
    def decor(f: Callable):
        async def wrapper(*args, **kwargs):
            last_info_exception = None
            last_warning_exception = None
            for i in range(retries+1):
                try:
                    _LOGGER.debug("Trying %d/%d: %s(args=%s,kwargs=%s)", i, retries, f.__name__, args, kwargs)
                    if inspect.iscoroutinefunction(f):
                        return await f(*args, **kwargs)
                    return f(*args, **kwargs)
                except (exc.BleakError, exc.BleakDBusError) as _e:
                    next_message = "Will try again" if i < retries else "Will not try again"
                    _LOGGER.warning("Got exception: %s. %s", str(_e), next_message)
                    last_warning_exception = _e
                    if delay > 0:
                        await asyncio.sleep(delay)

            _LOGGER.critical("Retry limit (%d) exceeded for %s(%s, %s)", retries, f.__name__, args, kwargs)
            if _LOGGER.level > logging.INFO and last_info_exception is not None:
                _LOGGER.critical(f"Last exception was {last_info_exception}")
            elif _LOGGER.level > logging.WARNING and last_warning_exception is not None:
                _LOGGER.critical(f"Last exception was {last_warning_exception}")

            raise MaxTriesExceededError

        return wrapper
    return decor


class TionDelegation:
    def __init__(self):
        self._data: List[bytearray] = []
        self._generation = 0

    def handleNotification(self, handle: int, data: bytearray):
        self._data.append(data)
        _LOGGER.debug(f"Got data in {handle} response {bytes(data).hex()}")
        _LOGGER.debug(f"{self._data=}")

    @property
    def data(self) -> bytearray:
        return self._data.pop(0)

    @property
    def haveNewData(self) -> bool:
        return len(self._data) > 0

    def clear(self) -> None:
        """Discard all queued notifications."""
        self._data.clear()

    def begin_session(self) -> Callable[[int, bytearray], None]:
        """Start a new notification generation and return its callback."""
        self._generation += 1
        generation = self._generation
        self.clear()

        def handle_notification(handle: int, data: bytearray) -> None:
            if generation != self._generation:
                _LOGGER.debug(
                    "Ignoring notification from stale BLE session generation %d",
                    generation,
                )
                return
            self.handleNotification(handle, data)

        return handle_notification

    def end_session(self) -> None:
        """Invalidate the active notification callback and clear its queue."""
        self._generation += 1
        self.clear()


class TionException(Exception):
    def __init__(self, expression, message):
        self.expression = expression
        self.message = message


class Tion:
    statuses = ['off', 'on']
    modes = ['recirculation', 'mixed']  # 'recirculation', 'mixed' and 'outside', as Index exception
    uuid_notify: str = ""
    uuid_write: str = ""

    def __init__(self, mac: BluetoothDevice):
        self._mac = mac
        self._btle: Optional[BleakClient] = None
        self._btle_device: BluetoothDevice = mac
        self._next_btle_device: Optional[BluetoothDevice] = None
        self._client_factory: Optional[ClientFactory] = None
        self._alternate_client_factory: Optional[ClientFactory] = None
        self._session_failure_callback: Optional[SessionFailureCallback] = None
        self._delegation = TionDelegation()
        self._fan_speed = 0
        self._model: str = self.__class__.__name__
        self._data: bytearray = bytearray()
        """Data from breezer response at request state command"""
        # states
        self._in_temp: int = 0
        self._out_temp: int = 0
        self._heater_temp: int = 0
        self._fan_speed: int = 0
        self._mode: int = 0
        self._state: bool = False
        self._heater: bool = False
        self._sound: bool = False
        self._filter_remain: float = 0.0
        self._error_code: int = 0
        self.__failed_connects: int = 0
        self.__connections_count: int = 0
        self.__notifications_enabled: bool = False
        self.have_breezer_state: bool = False
        # ``have_breezer_state`` belongs to the response currently being
        # collected and is intentionally reset for every connection/request.
        # Keep the last decoded full device state separately so a SET can
        # preserve unchanged fields without issuing a preliminary GET.
        self._state_cache_updated_at: Optional[float] = None
        self._state_cache_ttl: float = 0.0
        self._connection_lock = Semaphore(1)
        self._operation_lock = asyncio.Lock()
        # Keep at most one SET transaction waiting behind the active BLE
        # operation. Slider controls may submit many intermediate values while
        # a weak connection is still busy; merge those values so the next
        # transaction applies the latest requested state instead of replaying
        # a stale queue for minutes.
        self._pending_set_requests: List[
            Tuple[Dict[str, Any], asyncio.Future]
        ] = []
        self._active_set_waiters: List[asyncio.Future] = []
        self._set_worker_task: Optional[asyncio.Task] = None

    @abc.abstractmethod
    async def _send_request(self, request: bytearray):
        """ Send request to device

        Args:
          request : array of bytes to send to device
        Returns:
          array of bytes with device response
        """
        pass

    @abc.abstractmethod
    def _decode_response(self, response: bytearray) -> dict:
        """ Decode response from device

        Args:
          response: array of bytes with data from device, taken from _send_request
        Returns:
          dictionary with device response
        """
        pass

    @abc.abstractmethod
    def _encode_request(self, request: dict) -> bytearray:
        """ Encode dictionary of request to byte array

        Args:
          request: dictionary with request
        Returns:
          Byte array for sending to device
        """
        pass

    @abc.abstractmethod
    def _generate_model_specific_json(self) -> dict:
        """
        Generates dict with model-specific parameters based on class variables
        :return: dict of model specific properties
        """
        raise NotImplementedError()

    def __generate_common_json(self) -> dict:
        """
        Generates dict with common parameters based on class properties
        :return: dict of common properties
        """
        return {
            "state": self.state,
            "heater": self.heater,
            "heating": self.heating,
            "sound": self.sound,
            "mode": self.mode,
            "out_temp": self.out_temp,
            "in_temp": self.in_temp,
            "heater_temp": self._heater_temp,
            "fan_speed": self.fan_speed,
            "filter_remain": self.filter_remain,
            "time": strftime("%H:%M", localtime()),
            "request_error_code": self._error_code,
            "model": self.model,
        }

    @final
    @property
    def heating(self) -> str:
        """Tries to guess is heater working right now."""
        if self.heater == "off":
            return "off"

        if self.heater_temp - self.in_temp > 3 and self.out_temp > self.in_temp:
            return "on"

        return "off"

    @final
    async def get_state_from_breezer(self) -> None:
        """
        Get current state from breezer
        :return: None
        """
        for attempt in range(GET_SESSION_ATTEMPTS):
            connected = False
            try:
                await self.connect(prefer_alternate=attempt > 0)
                connected = True
                self._reset_response_state()
                await self._try_write(request=self.command_getStatus)
                response = await self._get_data_from_breezer()
                self._decode_response(response)
                self._mark_state_cache_updated()
                return
            except asyncio.CancelledError:
                self._invalidate_state_cache()
                raise
            except Exception as err:
                self._invalidate_state_cache()
                self._report_session_failure(err)
                if attempt + 1 >= GET_SESSION_ATTEMPTS:
                    raise
                _LOGGER.info(
                    "GET session attempt %d/%d failed for %s: %s. "
                    "Retrying through a fresh session",
                    attempt + 1,
                    GET_SESSION_ATTEMPTS,
                    self.mac,
                    err,
                )
            finally:
                if connected:
                    await self._release_connection_after_operation()

            await asyncio.sleep(GET_SESSION_RETRY_DELAY_SECONDS)

    @final
    async def get(self, skip_update: bool = False) -> dict:
        """
        Report current breezer state
        :param skip_update: may we skip requesting data from breezer or not
        :return:
          dictionary with device state
        """
        async with self._operation_lock:
            return await self._get(skip_update)

    async def _get(self, skip_update: bool = False) -> dict:
        """Get the state while the caller holds the operation lock."""
        if skip_update and self._has_fresh_state_cache():
            _LOGGER.debug(
                "Skipping GET because the last confirmed full state is still fresh"
            )
        else:
            await self.get_state_from_breezer()
        common = self.__generate_common_json()
        model_specific_data = self._generate_model_specific_json()

        return {**common, **model_specific_data}

    @final
    def _set_internal_state_from_request(self, request: dict) -> None:
        """
        Set internal parameters based on user request
        :param request: changed breezer parameter from set request
        :return: None
        """
        for p in ['fan_speed', 'heater_temp', 'heater', 'sound', 'mode', 'state']:
            # ToDo: lite have additional parameters to set: "light" and "co2_auto_control", so we should get this
            #  list from class
            try:
                setattr(self, p, request[p])
            except KeyError:
                pass

    @final
    async def set(self, new_settings=None) -> dict:
        """
        Set new breezer state, coalescing requests that have not started yet.

        One BLE transaction may already be in progress. Any SET calls received
        before it completes are merged into a single following transaction;
        for the same field, the most recent value wins. All callers in that
        batch receive the result of the merged transaction.

        :param new_settings: json with new state
        :return: confirmed breezer state
        """
        new_settings = dict(new_settings or {})

        try:
            if new_settings["fan_speed"] == 0:
                del new_settings["fan_speed"]
                new_settings["state"] = "off"
        except KeyError:
            pass

        waiter = asyncio.get_running_loop().create_future()
        self._pending_set_requests.append((new_settings, waiter))

        if self._set_worker_task is None or self._set_worker_task.done():
            self._set_worker_task = asyncio.create_task(self._drain_pending_sets())

        return await waiter

    async def _drain_pending_sets(self) -> None:
        """Execute one active SET and one latest-value pending SET at a time."""
        try:
            while self._pending_set_requests:
                requests = self._pending_set_requests
                self._pending_set_requests = []
                requests = [
                    (settings, waiter)
                    for settings, waiter in requests
                    if not waiter.cancelled()
                ]
                if not requests:
                    continue

                settings: Dict[str, Any] = {}
                for requested_settings, _waiter in requests:
                    settings.update(requested_settings)
                self._active_set_waiters = [
                    waiter for _settings, waiter in requests
                ]

                if len(self._active_set_waiters) > 1:
                    _LOGGER.debug(
                        "Coalesced %d pending SET requests into %s",
                        len(self._active_set_waiters),
                        settings,
                    )

                try:
                    result = await self._set(settings)
                except Exception as err:
                    for waiter in self._active_set_waiters:
                        if not waiter.done():
                            waiter.set_exception(err)
                else:
                    for waiter in self._active_set_waiters:
                        if not waiter.done():
                            waiter.set_result(result)
                self._active_set_waiters = []
        except asyncio.CancelledError:
            for waiter in self._active_set_waiters:
                if not waiter.done():
                    waiter.cancel()
            for _settings, waiter in self._pending_set_requests:
                if not waiter.done():
                    waiter.cancel()
            self._pending_set_requests = []
            raise
        finally:
            for waiter in self._active_set_waiters:
                if not waiter.done():
                    waiter.cancel()
            self._active_set_waiters = []
            self._set_worker_task = None

    async def _set(self, new_settings: dict) -> dict:
        """Execute a single SET transaction."""
        async with self._operation_lock:
            connected = False
            try:
                # A stale cache requires an idempotent GET first, before the
                # exactly-once SET owns any connection reference. Otherwise a
                # nested GET retry cannot replace the primary physical client
                # with an alternate route while the outer SET lease is held.
                current_settings = None
                if not self._has_fresh_state_cache():
                    current_settings = await self._get(skip_update=False)

                await self.connect()
                connected = True
                if current_settings is None:
                    current_settings = await self._get(skip_update=True)

                merged_settings = {**current_settings, **new_settings}

                encoded_request = self._encode_request(merged_settings)
                _LOGGER.debug("Will write %s", encoded_request)
                self._reset_response_state()
                await self._send_request(encoded_request)
                response = await self._get_data_from_breezer()
                self._decode_response(response)
                self._mark_state_cache_updated()
                return await self._get(skip_update=True)
            except BaseException:
                # The write may have reached the breezer while its response was
                # lost. Do not trust the old full-state cache on the next SET.
                self._invalidate_state_cache()
                self._reset_response_state()
                raise
            finally:
                if connected:
                    await self._release_connection_after_operation()

    async def _release_connection_after_operation(self) -> None:
        """Release an operation's reference even while it is being cancelled."""
        disconnect_task = asyncio.create_task(self.disconnect())
        try:
            await asyncio.shield(disconnect_task)
        except asyncio.CancelledError:
            try:
                await disconnect_task
            finally:
                raise

    @final
    def set_state_cache_ttl(self, seconds: float) -> None:
        """Enable the confirmed-state fast SET path for ``seconds``."""
        self._state_cache_ttl = max(float(seconds), 0.0)

    @final
    def _mark_state_cache_updated(self) -> None:
        """Remember when a full state was confirmed by the breezer."""
        self._state_cache_updated_at = monotonic()

    @final
    def _invalidate_state_cache(self) -> None:
        """Force the next SET to refresh the full state first."""
        self._state_cache_updated_at = None

    @final
    def _has_fresh_state_cache(self) -> bool:
        """Return whether the cached full state is safe to reuse for SET."""
        if self._state_cache_ttl <= 0 or self._state_cache_updated_at is None:
            return False
        return monotonic() - self._state_cache_updated_at <= self._state_cache_ttl

    def _report_session_failure(self, err: Exception) -> None:
        """Notify the host about a failed BLE session without breaking retry."""
        if self._session_failure_callback is None:
            return
        try:
            self._session_failure_callback(err)
        except Exception:
            _LOGGER.exception("Ignoring exception from BLE session failure callback")

    def _reset_message_assembly(self) -> None:
        """Reset model-specific partial-frame state before another response."""

    def _reset_response_state(self) -> None:
        """Discard notifications and partial frames from an earlier request."""
        self.have_breezer_state = False
        self._delegation.clear()
        self._data = bytearray()
        self._reset_message_assembly()

    @final
    @property
    def mac(self):
        return self._mac.address if isinstance(self._mac, BLEDevice) else self._mac

    @staticmethod
    def decode_temperature(raw: int) -> int:
        """ Converts temperature from bytes with addition code to int
        Args:
          raw: raw temperature value from Tion
        Returns:
          Integer value for temperature
        """
        barrier = 0b10000000
        return raw if raw < barrier else -(~(raw - barrier) + barrier + 1)

    @final
    def _process_status(self, code: int) -> str:
        try:
            status = self.statuses[code]
        except IndexError:
            status = 'unknown'
        return status

    @final
    @property
    def connection_status(self):
        status = "connected" if self._btle is not None and self._btle.is_connected else "disc"
        return status

    @final
    async def _try_connect(self, use_alternate: bool = False) -> bool:
        """Create a fresh client and connect it to the latest device."""
        device = self.set_new_btle_device()
        client = None
        try:
            if use_alternate:
                client_factory = (
                    self._alternate_client_factory or self._client_factory
                )
            else:
                client_factory = (
                    self._client_factory or self._alternate_client_factory
                )
            if client_factory is not None:
                client = await client_factory(device)
            else:
                client = BleakClient(device)
                await client.connect()
        except BaseException:
            if client is not None and client.is_connected:
                try:
                    await self._bounded_disconnect_client(
                        client, "cancelled or failed connection"
                    )
                except Exception as disconnect_err:
                    _LOGGER.debug(
                        "Ignoring client cleanup error for %s: %s",
                        self.mac,
                        disconnect_err,
                    )
            raise

        if not client.is_connected:
            raise exc.BleakError("Client factory returned a disconnected client")
        self._btle = client
        return self._btle.is_connected

    @final
    @retry(retries=1, delay=2)
    async def _try_connect_with_retries(self) -> bool:
        """Connect with the legacy retry policy for standalone callers."""
        return await self._try_connect()

    @final
    async def _connect(
        self,
        need_notifications: bool = True,
        prefer_alternate: bool = False,
    ):
        _LOGGER.debug(f"Connecting. {self.connection_status=}.")
        if self.connection_status != "disc":
            _LOGGER.debug(f"_connect done. {self.connection_status=}.")
            return

        for attempt in range(FULL_SESSION_ATTEMPTS):
            use_alternate = (
                self._alternate_client_factory is not None
                and (prefer_alternate != (attempt > 0))
            )
            try:
                if (
                    self._client_factory is not None
                    or self._alternate_client_factory is not None
                ):
                    await self._try_connect(use_alternate=use_alternate)
                else:
                    await self._try_connect_with_retries()
                if need_notifications:
                    await self._enable_notifications()
                else:
                    _LOGGER.debug("Notifications was not requested")
                break
            except Exception as err:
                self._report_session_failure(err)
                # A client that connected but failed during GATT setup is not
                # safe to reuse. Drop it before retrying the complete session,
                # including service discovery and notification subscription.
                try:
                    await self._disconnect()
                except Exception as disconnect_err:
                    # A broken BlueZ/GATT session may also fail while closing.
                    # That must not prevent the next attempt from using the
                    # freshly cleared client reference.
                    _LOGGER.debug(
                        "Ignoring disconnect error while recovering %s: %s",
                        self.mac,
                        disconnect_err,
                    )

                if attempt + 1 >= FULL_SESSION_ATTEMPTS:
                    _LOGGER.warning(
                        "Bluetooth session failed after %d attempts for %s: %s",
                        FULL_SESSION_ATTEMPTS,
                        self.mac,
                        err,
                    )
                    raise

                retry_route = (
                    " with the alternate client factory"
                    if self._alternate_client_factory is not None
                    else ""
                )
                _LOGGER.info(
                    "Bluetooth session attempt %d/%d failed for %s: %s. "
                    "Retrying the complete session%s",
                    attempt + 1,
                    FULL_SESSION_ATTEMPTS,
                    self.mac,
                    err,
                    retry_route,
                )
                await asyncio.sleep(FULL_SESSION_RETRY_DELAY_SECONDS)
        _LOGGER.debug(f"_connect done. {self.connection_status=}.")

    @final
    async def _disconnect(self):
        _LOGGER.debug(f"Disconnecting. {self.connection_status=}.")
        client = self._btle
        self._btle = None
        self.__notifications_enabled = False
        self._delegation.end_session()
        self._reset_message_assembly()
        self.have_breezer_state = False
        try:
            if client is not None and client.is_connected:
                await self._bounded_disconnect_client(client, "session cleanup")
        finally:
            self.set_new_btle_device()

        _LOGGER.debug(f"_disconnect done. {self.connection_status=}")

    async def _bounded_disconnect_client(
        self, client: BleakClient, reason: str
    ) -> None:
        """Disconnect one physical client without allowing cleanup to hang."""
        disconnect_task = asyncio.create_task(client.disconnect())
        try:
            done, _pending = await asyncio.wait(
                [disconnect_task], timeout=DISCONNECT_TIMEOUT_SECONDS
            )
        except asyncio.CancelledError:
            disconnect_task.cancel()
            disconnect_task.add_done_callback(_consume_background_task)
            raise

        if disconnect_task not in done:
            _LOGGER.warning(
                "Timed out after %.1fs disconnecting %s during %s",
                DISCONNECT_TIMEOUT_SECONDS,
                self.mac,
                reason,
            )
            disconnect_task.cancel()
            disconnect_task.add_done_callback(_consume_background_task)
            return

        # Preserve the old behavior for a completed disconnect: real transport
        # errors still reach the caller, while only a hung cleanup is detached.
        disconnect_task.result()

    @final
    async def _try_write(self, request: bytearray):
        """Write exactly once.

        A failed GATT write is ambiguous: the breezer may have received it
        even when the acknowledgement was lost. In particular, retrying a SET
        blindly can replay an old command after a newer user action.
        """
        _LOGGER.debug(f"Writing {bytes(request).hex()} to {self.uuid_write}, {self.connection_status=}")
        if self._btle is None:
            raise exc.BleakError("Tion is not connected")
        return await self._btle.write_gatt_char(
            self.uuid_write,
            request,
            False
        )

    @final
    async def _enable_notifications(self):
        _LOGGER.debug(f"Enabling notification. {self.connection_status=}")
        if self._btle is None:
            raise exc.BleakError("Tion is not connected")
        try:
            notification_callback = self._delegation.begin_session()
            self._reset_message_assembly()
            await self._btle.start_notify(self.uuid_notify, notification_callback)
        except exc.BleakError as e:
            self._delegation.end_session()
            self._reset_message_assembly()
            _LOGGER.warning("Got exception %s while enabling notifications!" % str(e))
            raise e

        self.__notifications_enabled = True
        _LOGGER.debug(f"_enable_notifications done")
        return

    @final
    @property
    def fan_speed(self):
        return self._fan_speed

    @fan_speed.setter
    def fan_speed(self, new_speed: int):
        if 0 <= new_speed <= 6:
            self._fan_speed = new_speed

        else:
            _LOGGER.warning("Incorrect new fan speed. Will use 1 instead")
            self._fan_speed = 1

        # self.set({"fan_speed": new_speed})

    @final
    def _process_mode(self, mode_code: int) -> str:
        try:
            mode = self.modes[mode_code]
        except IndexError:
            mode = 'outside'
        return mode

    @staticmethod
    def _decode_state(state: bool) -> str:
        return "on" if state else "off"

    @staticmethod
    def _encode_state(state: str) -> bool:
        return state == "on"

    @final
    @property
    def state(self) -> str:
        return self._decode_state(self._state)

    @final
    @state.setter
    def state(self, new_state: str):
        self._state = self._encode_state(new_state)

    @final
    @property
    def heater(self) -> str:
        return self._decode_state(self._heater)

    @final
    @heater.setter
    def heater(self, new_state: str):
        self._heater = self._encode_state(new_state)

    @final
    @property
    def heater_temp(self) -> int:
        return self._heater_temp

    @final
    @heater_temp.setter
    def heater_temp(self, new_temp: int):
        self._heater_temp = new_temp

    @final
    @property
    def target_temp(self) -> int:
        return self.heater_temp

    @final
    @target_temp.setter
    def target_temp(self, new_temp: int):
        self.heater_temp = new_temp

    @final
    @property
    def in_temp(self):
        """Income air temperature"""
        return self._in_temp

    @final
    @property
    def out_temp(self):
        """Outcome air temperature"""
        return self._out_temp

    @final
    @property
    def sound(self) -> str:
        return self._decode_state(self._sound)

    @final
    @sound.setter
    def sound(self, new_state: str):
        self._sound = self._encode_state(new_state)

    @final
    @property
    def filter_remain(self) -> float:
        return self._filter_remain

    @final
    @property
    def mode(self):
        return self._process_mode(self._mode)

    @final
    @mode.setter
    def mode(self, new_state: str):
        self._mode = self._encode_mode(new_state)

    @final
    @property
    def model(self) -> str:
        return (
            self._model[len("Tion"):]
            if self._model.startswith("Tion")
            else self._model
        )

    @final
    def _encode_status(self, status: str) -> int:
        """
        Encode string status () to int
        :param status: one of:  "on", "off"
        :return: integer equivalent of state
        """
        return self.statuses.index(status) if status in self.statuses else 0

    @final
    def _encode_mode(self, mode: str) -> int:
        """
        Encode string mode to integer
        :param mode: one of self.modes + any other as outside
        :return: integer equivalent of mode
        """
        return self.modes.index(mode) if mode in self.modes else 2

    @final
    async def pair(self):
        async with self._operation_lock:
            _LOGGER.debug("Pairing")
            connected = False
            try:
                await self.connect(need_notifications=False)
                connected = True
                _LOGGER.debug("Connected. BT pairing ...")
                if self._btle is None:
                    raise exc.BleakError("Tion is not connected")
                await self._btle.pair()
                # device-specific pairing
                _LOGGER.debug("Device-specific pairing ...")
                await self._pair()
                _LOGGER.debug("Device pair is done")
            except Exception as e:
                _LOGGER.critical(f"Got exception while pair {type(e).__name__}: {str(e)}")
                raise TionException('pair', f"{type(e).__name__}: {str(e)}")
            finally:
                if connected:
                    await self._release_connection_after_operation()
                _LOGGER.debug("Pair operation released its connection")

    @abc.abstractmethod
    async def _pair(self):
        """Perform model-specific pair steps"""

    @final
    async def connect(
        self,
        prefer_alternate: bool = False,
        need_notifications: bool = True,
    ):
        async with self._connection_lock:
            if self.__connections_count == 0:
                self.have_breezer_state = False
                try:
                    await self._connect(
                        need_notifications=need_notifications,
                        prefer_alternate=prefer_alternate,
                    )
                except BaseException:
                    # Cancellation is not an Exception on supported Python
                    # versions. Still close a client that may already have
                    # connected before propagating it to the caller.
                    try:
                        await self._disconnect_completely()
                    except Exception as cleanup_err:
                        _LOGGER.debug(
                            "Ignoring connection cleanup error for %s: %s",
                            self.mac,
                            cleanup_err,
                        )
                    raise
            self.__connections_count += 1

    @final
    async def disconnect(self):
        async with self._connection_lock:
            if self.__connections_count == 0:
                return
            self.__connections_count -= 1
            if self.__connections_count == 0:
                await self._disconnect_completely()

    @final
    async def async_close(self) -> None:
        """Stop background work and force the BLE transport closed.

        Home Assistant calls this while unloading an integration. Unlike
        :meth:`disconnect`, shutdown must not trust the public connection
        reference count: an operation may have been cancelled between
        acquiring a reference and releasing it.
        """
        worker = self._set_worker_task
        if worker is not None and worker is not asyncio.current_task():
            if not worker.done():
                worker.cancel()
            try:
                await worker
            except asyncio.CancelledError:
                pass
            except Exception:
                _LOGGER.exception("SET worker failed while closing %s", self.mac)

        # Normally the worker cancellation path handles both its active batch
        # and the pending batch. Repeat the cleanup here so shutdown also
        # covers a worker that had already exited unexpectedly.
        for _settings, waiter in self._pending_set_requests:
            if not waiter.done():
                waiter.cancel()
        for waiter in self._active_set_waiters:
            if not waiter.done():
                waiter.cancel()
        self._pending_set_requests = []
        self._active_set_waiters = []
        self._set_worker_task = None

        async with self._connection_lock:
            self.__connections_count = 0
            await self._disconnect_completely()

    async def _disconnect_completely(self) -> None:
        """Finish physical cleanup even if the calling task is cancelled."""
        disconnect_task = asyncio.create_task(self._disconnect())
        try:
            await asyncio.shield(disconnect_task)
        except asyncio.CancelledError:
            # The outer task has already consumed its cancellation. Waiting
            # here prevents a ghost BlueZ connection from surviving it.
            try:
                await disconnect_task
            finally:
                raise

    @property
    @abc.abstractmethod
    def command_getStatus(self) -> bytearray:
        raise NotImplementedError()

    @abc.abstractmethod
    def _collect_message(self, package: bytearray) -> bool:
        """
        Collects message from several package
        Must set self._data

        :param package: single package from breezer
        :return: Have we full response from breezer or not
        """
        raise NotImplementedError()

    @final
    async def _get_data_from_breezer(self) -> bytearray:
        """ Get byte array with breezer response on state request

        :returns:
          breezer response
        """
        _LOGGER.debug("Collecting data")
        deadline = monotonic() + RESPONSE_TIMEOUT_SECONDS
        try:
            while monotonic() < deadline:
                if self._delegation.haveNewData:
                    byte_response = self._delegation.data
                    if self._collect_message(byte_response):
                        self.have_breezer_state = True
                        return self._data
                    continue

                await asyncio.sleep(min(0.1, max(deadline - monotonic(), 0)))

            _LOGGER.debug("Waiting too long for data")
            raise TionException(
                "_get_data_from_breezer", "Could not get breezer state"
            )
        except BaseException:
            self._reset_response_state()
            raise

    @final
    def update_btle_device(self, new_device: BluetoothDevice):
        if new_device is None:
            _LOGGER.info(f"Skipping update due to {new_device= }!")
            return
        self._next_btle_device = new_device

    @final
    def set_new_btle_device(self) -> BluetoothDevice:
        if self._next_btle_device is not None:
            _LOGGER.debug(f"Updating BLE device from {self._btle_device} to {self._next_btle_device}")
            self._btle_device = self._next_btle_device
            self._next_btle_device = None
        return self._btle_device

    @final
    def set_client_factory(self, client_factory: ClientFactory) -> None:
        """Set an async factory that returns an already connected fresh client."""
        self._client_factory = client_factory

    @final
    def set_alternate_client_factory(self, client_factory: ClientFactory) -> None:
        """Set a client factory used by the second full connection attempt."""
        self._alternate_client_factory = client_factory

    @final
    def set_session_failure_callback(
        self, callback: SessionFailureCallback
    ) -> None:
        """Set a callback invoked whenever a complete BLE session fails."""
        self._session_failure_callback = callback
