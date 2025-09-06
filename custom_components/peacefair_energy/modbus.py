import logging
import asyncio
from pymodbus.client import ModbusTcpClient, ModbusUdpClient
from pymodbus.exceptions import ModbusIOException
from pymodbus.framer import FramerType
import threading

try:
    from homeassistant.const import (
        DEVICE_CLASS_VOLTAGE,
        DEVICE_CLASS_CURRENT,
        DEVICE_CLASS_POWER,
        DEVICE_CLASS_ENERGY,
        DEVICE_CLASS_POWER_FACTOR
    )
except ImportError:
    DEVICE_CLASS_VOLTAGE = "voltage"
    DEVICE_CLASS_CURRENT = "current"
    DEVICE_CLASS_POWER = "power"
    DEVICE_CLASS_ENERGY = "energy"
    DEVICE_CLASS_POWER_FACTOR = "power_factor"

from .const import (
    DEVICE_CLASS_FREQUENCY
)

HPG_SENSOR_TYPES = [
    DEVICE_CLASS_VOLTAGE,
    DEVICE_CLASS_CURRENT,
    DEVICE_CLASS_POWER,
    DEVICE_CLASS_ENERGY,
    DEVICE_CLASS_POWER_FACTOR,
    DEVICE_CLASS_FREQUENCY
]

_LOGGER = logging.getLogger(__name__)


class ModbusHub:
    def __init__(self, protocol, host, port, slave):
        self._lock = threading.Lock()
        self._slave = slave
        if protocol == "rtuovertcp":
            self._client = ModbusTcpClient(
                host=host,
                port=port,
                framer=FramerType.RTU,
                timeout=2
            )
        elif protocol == "rtuoverudp":
            self._client = ModbusUdpClient(
                host=host,
                port=port,
                framer=FramerType.RTU,
                timeout=2
            )

    def connect(self):
        with self._lock:
            self._client.connect()

    def close(self):
        with self._lock:
            self._client.close()

    # 新增同步版本，供线程池调用
    def read_input_registers_sync(self, address, count):
        with self._lock:
            # kwargs = {"unit": self._slave, "count": count}
            kwargs = {"count": count}
            return self._client.read_input_registers(address, **kwargs)

    # 异步版本，交给线程池执行，避免阻塞事件循环
    async def read_input_registers(self, address, count):
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(
            None,
            self.read_input_registers_sync,
            address,
            count
        )

    def reset_energy(self):
        with self._lock:
            kwargs = {"unit": self._slave}
#             request = ModbusResetEnergyRequest(**kwargs)
#             self._client.execute(request)

    # 异步版本info_gather
    async def info_gather(self):
        data = {}
        try:
            result = await self.read_input_registers(0, 9)
            if result is not None and type(result) is not ModbusIOException \
                    and result.registers is not None and len(result.registers) == 9:
                data[DEVICE_CLASS_VOLTAGE] = result.registers[0] / 10
                data[DEVICE_CLASS_CURRENT] = ((result.registers[2] << 16) + result.registers[1]) / 1000
                data[DEVICE_CLASS_POWER] = ((result.registers[4] << 16) + result.registers[3]) / 10
                data[DEVICE_CLASS_ENERGY] = ((result.registers[6] << 16) + result.registers[5]) / 1000
                data[DEVICE_CLASS_FREQUENCY] = result.registers[7] / 10
                data[DEVICE_CLASS_POWER_FACTOR] = result.registers[8] / 100
            else:
                _LOGGER.debug("Error in gathering, timed out")
        except Exception as e:
            _LOGGER.error(f"Error in gathering, {e}")
        return data

    # 如果有别的同步读写函数，也建议用类似方式异步包装
