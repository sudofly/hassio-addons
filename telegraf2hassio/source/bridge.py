from cmath import log
import json
import logging
import hashlib
import time
from copy import deepcopy

VERSION = "0.1"
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
        
        # Track discovery topics for cleanup
        self.active_discovery_topics = set()
        self.last_seen_discovery_topics = {}  # topic -> timestamp
        self.discovery_cleanup_interval = 300  # 5 minutes in seconds

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
            
            # Update discovery topic timestamp for both new and existing measurements
            discovery_topic = f"{HA_PREFIX}/{host_name}/{sensor_name}_{measurement_name}/config"
            self.register_discovery_topic(discovery_topic)
            
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

        # Periodically cleanup old discovery topics (every 10 messages approximately)
        if hasattr(self, '_message_count'):
            self._message_count += 1
        else:
            self._message_count = 1
            
        if self._message_count % 10 == 0:
            self.cleanup_old_discovery_topics()

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

    def register_discovery_topic(self, discovery_topic):
        """Register a discovery topic as active"""
        self.active_discovery_topics.add(discovery_topic)
        self.last_seen_discovery_topics[discovery_topic] = time.time()

    def cleanup_old_discovery_topics(self, max_age_seconds=None):
        """Remove old discovery topics that haven't been seen recently"""
        if max_age_seconds is None:
            max_age_seconds = self.discovery_cleanup_interval
            
        current_time = time.time()
        topics_to_remove = []
        
        for topic, last_seen in self.last_seen_discovery_topics.items():
            if current_time - last_seen > max_age_seconds:
                topics_to_remove.append(topic)
        
        for topic in topics_to_remove:
            # Send empty payload to remove from Home Assistant
            self.transmit_callback(topic, "", retain=True)
            self.active_discovery_topics.discard(topic)
            del self.last_seen_discovery_topics[topic]
            logging.info(f"Removed old discovery topic: {topic}")
        
        if topics_to_remove:
            logging.info(f"Cleaned up {len(topics_to_remove)} old discovery topics")
        
        return len(topics_to_remove)

    def force_cleanup_all_discovery_topics(self):
        """Force removal of all tracked discovery topics (useful for testing or major changes)"""
        removed_count = 0
        for topic in list(self.active_discovery_topics):
            self.transmit_callback(topic, "", retain=True)
            removed_count += 1
            
        self.active_discovery_topics.clear()
        self.last_seen_discovery_topics.clear()
        logging.info(f"Force removed all {removed_count} discovery topics")
        return removed_count

class host():
    def __init__(self, parent_listener, name) -> None:
        self.name = name
        self.sensors = {}
        self.parent_listener = parent_listener

        self.info = {}
        self.info["identifiers"] = "bridge"
        self.info["model"] = "your_bridge"
        self.info["name"] = self.name
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
        self.topic = f"{HA_PREFIX}/{self.parent_sensor.parent_host.name}/{self.parent_sensor.name}_{self.name}"
        self.uid = f"{self.parent_sensor.parent_host.name}_{self.parent_sensor.name}_{self.name}"

        config_payload = {
            # "~": self.topic,
            "name": f"{self.parent_sensor.parent_host.name}_{self.parent_sensor.name[0:-3]}_{self.name}",
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
        
        # Register this discovery topic for tracking and cleanup
        self.parent_sensor.parent_host.parent_listener.register_discovery_topic(discovery_topic)
