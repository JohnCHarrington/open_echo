import asyncio
from typing import Callable, Coroutine
import numpy as np
import serial.tools.list_ports
import struct
import logging
import time
import serial_asyncio_fast as aserial

from settings import Medium, Settings

log = logging.getLogger("uvicorn")


class EchoReader:
    def __init__(
        self,
        data_callback: Callable[[dict], Coroutine],
        depth_callback: Callable[[dict], Coroutine],
        settings: Settings | None = None,
    ):
        self.settings = settings
        self._restart_event = asyncio.Event()
        self.data_callback = data_callback
        self.depth_callback = depth_callback
        self._task: asyncio.Task | None = None

    def update_settings(self, new_settings: Settings):
        log.info("EchoReader updating settings...")
        self.settings = new_settings
        self._restart_event.set()  # Signal restart

    def __enter__(self):
        self._task = asyncio.create_task(self.run_forever())
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        if self._task:
            self._task.cancel()
            self._task = None

        if exc_type is not None:
            log.error(f"Error in EchoReader: {exc_value}")

    async def read_packet(self, reader: asyncio.StreamReader):
        while True:
            header = await reader.readexactly(1)
            if header != b"\xaa":
                continue  # Wait for the start byte

            payload = await reader.readexactly(
                8 + 2 * self.settings.num_samples
            )  # Read payload
            checksum = await reader.readexactly(1)

            if len(payload) != 8 + 2 * self.settings.num_samples or len(checksum) != 1:
                continue  # Incomplete packet

            # Verify checksum
            calc_checksum = 0
            for byte in payload:
                calc_checksum ^= byte
            if calc_checksum != checksum[0]:
                log.warning("⚠️ Checksum mismatch")
                continue

            # Unpack payload
            depth, temp_scaled, vDrv_scaled, res_scaled = struct.unpack(
                ">HhHh", payload[:8]
            )
            depth = min(depth, self.settings.num_samples)

            samples = struct.unpack(f">{self.settings.num_samples}H", payload[8:])

            temperature = temp_scaled / 100.0
            drive_voltage = vDrv_scaled / 100.0
            resolution = res_scaled / 100.0
            values = np.array(samples)

            return values, depth, temperature, drive_voltage, resolution

    @staticmethod
    def get_serial_ports():
        """Retrieve a list of available serial ports."""
        return [port.device for port in serial.tools.list_ports.comports()][::-1]

    @staticmethod
    async def wait_for_ack(reader, ack_byte=b"\xac", timeout=2.0):
        start = time.monotonic()
        while time.monotonic() - start < timeout:
            try:
                byte = await asyncio.wait_for(reader.readexactly(1), timeout=0.2)
                if byte == ack_byte:
                    return True
            except asyncio.TimeoutError:
                continue
        return False

    async def update_arduino_settings(
        self,
        settings: Settings,
        writer: asyncio.StreamWriter,
        reader: asyncio.StreamReader,
    ):
        log.info("Applying new settings to Arduino...")
        try:
            # Prepare settings packet
            # 1 byte: ENABLE_DYNAMIC_RESOLUTION
            # 2 bytes: NUM_SAMPLES (high, low)
            # 2 bytes: BLINDZONE_SAMPLE_END (high, low)
            # 1 byte: MEDIUM (0 for WATER, 1 for AIR)
            packet = bytearray()
            packet.append(0xA5)  # Start byte

            data = bytearray()
            data.append(int(bool(settings.dynamic_resolution)))

            num_samples = int(settings.num_samples)
            data.append((num_samples >> 8) & 0xFF)
            data.append(num_samples & 0xFF)

            blindzone = int(settings.blindzone_sample_end)
            data.append((blindzone >> 8) & 0xFF)
            data.append(blindzone & 0xFF)

            log.info(int(settings.medium == Medium.AIR))
            data.append(int(settings.medium == Medium.AIR))

            data.append(int(settings.threshold_value) & 0xFF)
            packet.extend(data)

            checksum = 0
            for b in data:
                checksum ^= b & 0xFF

            packet.append(checksum)

            # Send the packet three times to ensure it is received
            for _ in range(3):
                writer.write(packet)
                await writer.drain()
                await asyncio.sleep(0.5)  # Wait for Arduino to process

        except Exception as e:
            log.warning(f"Could not send settings to Arduino: {e}")

    async def aread_echo(self, reader: asyncio.StreamReader):
        result = await self.read_packet(reader)
        if result:
            values, depth_index, temperature, drive_voltage, resolution = result
            depth = depth_index * (resolution / 100)  # Convert to meters
            try:
                data = {
                    "spectrogram": values.tolist(),
                    "measured_depth": depth,
                    "temperature": temperature,
                    "drive_voltage": drive_voltage,
                    "resolution": resolution,
                }
                log.info("Resolution: %s cm", resolution)
                await self.data_callback(data)
            except Exception as e:
                log.error(f"❌ Error sending data: {e}", exc_info=e)

            try:
                self.depth_callback(depth)
            except Exception as e:
                log.error(f"❌ Error sending depth: {e}", exc_info=e)

        await asyncio.sleep(0.1)  # Allow time for other tasks

    async def run_forever(self):
        """Continuously read serial data and emit processed arrays. Supports live settings update and restart."""
        while True:
            if self.settings is None:
                log.warning("Settings not initialized, waiting...")
                await asyncio.sleep(1)
                continue

            self._restart_event.clear()
            try:
                reader, writer = await aserial.open_serial_connection(
                    url=self.settings.serial_port,
                    baudrate=self.settings.baud_rate,
                    timeout=1,
                )
                await self.update_arduino_settings(self.settings, writer, reader)
                log.info("Connected to serial port: %s", str(self.settings.serial_port))
                log.info("Connected to serial port: %s", str(self.settings.serial_port))
                while not self._restart_event.is_set():
                    await self.aread_echo(reader)

            except serial.SerialException as e:
                log.error(f"❌ Serial Error: {e}")

            await self._restart_event.wait()
