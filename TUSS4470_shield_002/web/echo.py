import asyncio
import time
from typing import Callable, Coroutine
import numpy as np
import serial
import serial.tools.list_ports
import struct
import logging
from pydantic_settings import BaseSettings
from enum import StrEnum

log = logging.getLogger(__name__)


class Medium(StrEnum):
    WATER = "water"
    AIR = "air"


speed_of_sound_map = {
    Medium.WATER: 1500,  # meters per second in water
    Medium.AIR: 330,  # meters per second in air
}


class EchoSettings(BaseSettings):
    serial_port: str
    baud_rate: int = 250000
    num_samples: int = 1800
    medium: Medium = Medium.WATER
    max_rows: int = 300  # Number of time steps (Y-axis)
    y_label_distance: int = 50  # distance between labels in cm

    @property
    def packet_size(self) -> int:
        return 1 + 6 + 2 * self.num_samples + 1


settings = EchoSettings()


class EchoReader:
    def __init__(self, port: str = None):
        self._running = False
        self.port = port or settings.serial_port

    def __enter__(self):
        self._running = True
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self._running = False
        if exc_type is not None:
            log.error(f"Error in EchoReader: {exc_value}")

    @staticmethod
    def read_packet(ser):
        while True:
            header = ser.read(1)
            if header != b"\xaa":
                continue  # Wait for the start byte

            payload = ser.read(8 + 2 * settings.num_samples)  # Read payload
            checksum = ser.read(1)

            if len(payload) != 8 + 2 * settings.num_samples or len(checksum) != 1:
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
            depth = min(depth, settings.num_samples)

            # log.info(depth)

            samples = struct.unpack(f">{settings.num_samples}H", payload[8:])

            temperature = temp_scaled / 100.0
            drive_voltage = vDrv_scaled / 100.0
            resolution = res_scaled / 100.0
            values = np.array(samples)

            return values, depth, temperature, drive_voltage, resolution

    @staticmethod
    def get_serial_ports():
        """Retrieve a list of available serial ports."""
        return [port.device for port in serial.tools.list_ports.comports()][::-1]

    def read_echo(self, callback: Callable[[dict], None]):
        """Continuously read serial data and emit processed arrays."""
        if self.port == "test":
            # For testing purposes, generate dummy data
            while self._running:
                values = np.random.rand(settings.num_samples) * 256
                measured_depth = 5
                temperature = 25.0
                drive_voltage = 5.0
                resolution = 1

                data = {
                    "spectrogram": values.tolist(),
                    "measured_depth": measured_depth,
                    "temperature": temperature,
                    "drive_voltage": drive_voltage,
                    "resolution": resolution,
                }
                callback(data)
                time.sleep(0.1)  # Simulate delay
        try:
            with serial.Serial(self.port, settings.baud_rate, timeout=1) as ser:
                # log.info("Connected to serial port: ", str(settings.serial_port))
                while self._running:
                    result = self.read_packet(ser)
                    if result:
                        values, depth_index, temperature, drive_voltage, resolution = (
                            result
                        )
                        try:
                            data = {
                                "spectrogram": values.tolist(),
                                "measured_depth": depth_index
                                * (resolution / 100),  # Convert to meters
                                "temperature": temperature,
                                "drive_voltage": drive_voltage,
                                "resolution": resolution,
                            }
                            callback(data)

                        except Exception as e:
                            log.error(f"❌ Error sending data: {e}")
                            break

        except serial.SerialException as e:
            log.error(f"❌ Serial Error: {e}")

    async def aread_echo(self, callback: Callable[[dict], Coroutine]):
        """Continuously read serial data and emit processed arrays."""
        if self.port == "test":
            # For testing purposes, generate dummy data
            while self._running:
                values = np.random.rand(settings.num_samples) * 256
                measured_depth = 5
                temperature = 25.0
                drive_voltage = 5.0
                resolution = 1  # cm/sample

                data = {
                    "spectrogram": values.tolist(),
                    "measured_depth": measured_depth,
                    "temperature": temperature,
                    "drive_voltage": drive_voltage,
                    "resolution": resolution,
                }
                await callback(data)
                await asyncio.sleep(0.1)  # Simulate delay
        try:
            with serial.Serial(self.port, settings.baud_rate, timeout=1) as ser:
                # log.info("Connected to serial port: ", str(settings.serial_port))
                while self._running:
                    result = self.read_packet(ser)
                    if result:
                        values, depth_index, temperature, drive_voltage, resolution = (
                            result
                        )
                        try:
                            data = {
                                "spectrogram": values.tolist(),
                                "measured_depth": depth_index
                                * (resolution / 100),  # Convert to meters
                                "temperature": temperature,
                                "drive_voltage": drive_voltage,
                                "resolution": resolution,
                            }
                            await callback(data)

                        except Exception as e:
                            log.error(f"❌ Error sending data: {e}")
                            break
                    await asyncio.sleep(0.1)  # Allow time for other tasks

        except serial.SerialException as e:
            log.error(f"❌ Serial Error: {e}")
