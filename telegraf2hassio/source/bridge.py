from cmath import log
import json
import logging
import hashlib
import time
from copy import deepcopy

VERSION = "0.1a"
HA_PREFIX = "homeassistant/sensor"
STATE_PREFIX = "telegraf2ha"

logging.basicConfig(
    format='[%(asctime)s] %(levelname)-2s %(message)s',
    level=logging.INFO,
    datefmt='%H:%M:%S')


def cleanout_string(string):
    return string.replace("-", "_").replace("/", "_")

class calc_measurement():
    def __init__(self, uid):
        self.id = uid
        self.__prev_value = 0
        self.__prev_t = 0.0

    def set_name(self, name):
        self.name = name
        self.name_calc = f"{self.name}_dt"

    def get_rate(self, value, time):
        delta = value - self.__prev_value
        rate = float(delta) / (time - self.__prev_t)

        self.__prev_value = value
        self.__prev_t = time

        # First time being called
        # no previous known value
        if value == delta:
            rate = 0.0

        return rate


class telegraf_mqtt_bridge():
    def __init__(self, transmit_callback, cm_str_list) -> None:
        self.hosts = {}
        self.cm_dict = {}
        self.transmit_callback = transmit_callback
        
        for uid in cm_str_list.split(","):
            # Initialize a dict with the desired calculated values UIDs
            self.cm_dict[uid] = calc_measurement(uid)

    def __get_host_name(self, jdata):
        # Build the host name of the current meassage
        return jdata['tags']['host']

    def __simplify_chip_name(self, chip_name):
        """Extract meaningful part from chip name for better readability"""
        if not chip_name:
            return ""
        
        # Remove common suffixes that don't add value
        chip_name = chip_name.replace("-isa-0000", "").replace("-acpi-0", "").replace("-virtual-0", "")
        
        # Extract the main chip identifier (first part before any remaining dashes)
        main_part = chip_name.split('-')[0]
        
        # Common chip name mappings for better readability
        chip_mappings = {
            'coretemp': 'cpu',
            'acpitz': 'acpi',
            'soc_dts1': 'soc',
            'nouveau': 'gpu_nouveau',
            'amdgpu': 'gpu_amd',
            'nvidia': 'gpu_nvidia'
        }
        
        return chip_mappings.get(main_part, main_part)

    def __get_sensor_name(self, jdata):
        # Build up the sensor name with improved readability
        base_name = jdata['name']
        
        # Use properties names to differentiate measurements with same name
        if len(jdata['tags']) > 1:
            chip = jdata['tags'].get('chip', "")
            device = jdata['tags'].get('device', "")
            interface = jdata['tags'].get('interface', "")
            feature = jdata['tags'].get('feature', "")
            
            # Start with a more meaningful name than the generic 'sensors'
            if chip:
                simplified_chip = self.__simplify_chip_name(chip)
                if simplified_chip and base_name == "sensors":
                    # Replace generic 'sensors' with meaningful chip name
                    sensor_name = simplified_chip
                else:
                    sensor_name = base_name + '_' + simplified_chip
            else:
                sensor_name = base_name
                
            # Add other tags if they provide meaningful information
            if device and device != chip:  # Don't duplicate if device same as chip
                sensor_name += '_' + device
            if interface:
                sensor_name += '_' + interface
            if feature:
                sensor_name += '_' + feature
        else:
            sensor_name = base_name

        # Append this unique suffix to differ same-sensor-named topics
        # that contain different tags, that confuse hassio
        uid = hashlib.sha1(str(self.jdata_recv['fields'].keys()).encode()).hexdigest()[0:2]
        sensor_name += f"_{uid}"

        return cleanout_string(sensor_name)

    def __get_unique_id(self, jdata, measurement_name):
            host_name = self.__get_host_name(jdata)
            sensor_name = self.__get_sensor_name(jdata)

            return cleanout_string(f"{host_name}_{sensor_name}_{measurement_name}")

    def __get_measurements_list(self, jdata):
        return jdata['fields'].keys()

    def add_calc(self, jdata_o):
        jdata = deepcopy(jdata_o)
        for measurement_name in self.__get_measurements_list(jdata_o):

            uid = self.__get_unique_id(jdata, measurement_name)

            # Add calc sensor and calculated value
            if uid in self.cm_dict.keys():
                self.cm_dict[uid].set_name(measurement_name)

                value = jdata["fields"][self.cm_dict[uid].name]
                t = jdata["timestamp"]

                jdata["fields"][self.cm_dict[uid].name_calc] = self.cm_dict[uid].get_rate(value, t)

        return jdata


    def announce_new(self, host_name, sensor_name, jdata) -> int:
        # Add current host if unknown
        current_host, is_new_h = self.add_host(host_name)
        # Add unknown sensors to host
        current_sensor, is_new_s = current_host.add_sensor(sensor_name)
        # Add unknown measurements to each sensor 
        for measurement_name in self.__get_measurements_list(jdata):
            measurement_obj, is_new_m = current_sensor.add_measurement(measurement_name)
            
            if is_new_m:
                uid = self.__get_unique_id(jdata, measurement_name)
                logging.info(f"Added measurement UID: {uid}")

        return (is_new_s | is_new_h | is_new_m)

    def send(self, data):
        # Once all the unknown sensors are announced,
        # start sending their data only
        
        try:
            decoded_data = data.payload.decode()
            self.jdata_recv = json.loads(decoded_data)
        except Exception as e:
            logging.error(f"Failed to decode data payload. Ignoring message. Error description: {e}")
            return False

        jdata = self.add_calc(self.jdata_recv)

        host_name = self.__get_host_name(jdata)
        sensor_name = self.__get_sensor_name(jdata)

        is_new = self.announce_new(host_name, sensor_name, jdata)

        topic_data = f"{STATE_PREFIX}/{host_name}/{sensor_name}/data"

        self.transmit_callback(topic_data, json.dumps(jdata['fields']))

        if is_new:
            logging.info(f"Added sensor: {self.print(jdata)}")

        return is_new

    def print(self, jdata):
        # jdata = json.loads(data.payload.decode())
        host_name = self.__get_host_name(jdata)
        sensor_name = self.__get_sensor_name(jdata)
        measurements = ""

        for measurement in self.__get_measurements_list(jdata):
            measurements += f"{measurement},"
        measurements = measurements.rstrip(",")

        return f"{STATE_PREFIX}/{host_name}/{sensor_name}/[{measurements}]" 

    def add_host(self, host_name):
        current_host = self.hosts.get(host_name)
        if current_host is None:
            current_host = host(self, host_name)
            self.hosts[host_name] = current_host
            return current_host, True

        return current_host, False

class host():
    def __init__(self, parent_listener, name) -> None:
        self.name = name
        self.sensors = {}
        self.parent_listener = parent_listener

        self.info = {}
        self.info["identifiers"] = [f"telegraf2ha_{self.name}"]  # Unique identifier per host
        self.info["model"] = "Telegraf Host"
        self.info["name"] = self.name  # Use actual host name (e.g., "GB-BXBT")
        self.info["sw_version"] = VERSION
        self.info["manufacturer"] = "telegraf2ha"

    def add_sensor(self, sensor_name):
        # To create the sensor name, also check for extra tags (for the case of disks for example)
        current_sensor = self.sensors.get(sensor_name)
        if current_sensor is None:
            current_sensor = sensor(self, sensor_name)
            self.sensors[sensor_name] = current_sensor
            return current_sensor, True

        return current_sensor, False


class sensor():
    def __init__(self, parent_host, name) -> None:
        self.name = name
        self.measurements = {}
        self.parent_host = parent_host

    def add_measurement(self, measurement_name):
        current_measurement = self.measurements.get(measurement_name)
        if current_measurement is None:
            current_measurement = measurement(self, measurement_name)
            self.measurements[measurement_name] = current_measurement
            return current_measurement, True
        
        return current_measurement, False

class measurement():    
    def __init__(self, parent_sensor, name) -> None:
        self.name = name
        self.parent_sensor = parent_sensor
        
        # Construct a clean name and unique ID without redundant hostnames
        base_sensor_name = self.parent_sensor.name.rsplit('_', 1)[0] # Removes the _xx UID suffix
        self.clean_name = f"{base_sensor_name}_{self.name}"
        self.uid = f"telegraf2ha_{self.parent_sensor.parent_host.name}_{self.clean_name}"
        
        self.topic = f"{HA_PREFIX}/{self.parent_sensor.parent_host.name}/{self.clean_name}"

        config_payload = {
            # "~": self.topic,
            "name": self.clean_name.replace("_", " ").title(), # Create a nice friendly name
            "state_topic": f"{STATE_PREFIX}/{self.parent_sensor.parent_host.name}/{self.parent_sensor.name}/data",
            "unit_of_measurement": "",
            "device": self.parent_sensor.parent_host.info,
            "unique_id": self.uid,
            "platform": "mqtt",
            # Make the template such that we can use the telegraph topic straight
            "value_template": f"{{{{ value_json.{self.name} | round(2) }}}}",
        }

        # If it is a new measumente, announce it to hassio
        discovery_topic = f"{self.topic}/config"
        self.parent_sensor.parent_host.parent_listener.transmit_callback(discovery_topic, json.dumps(config_payload), retain=True)
