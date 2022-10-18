# Copyright 2013-2022 Matthew Wall
# Distributed under the terms of the GNU Public License (GPLv3)
"""
Upload data to MQTT server

This service requires the python bindings for mqtt:

   pip install paho-mqtt

Minimal configuration:

[StdRestful]
    [[MQTT]]
        server_url = mqtt://username:password@localhost:1883/
        topic = weather
        unit_system = METRIC

Other MQTT options can be specified:

[StdRestful]
    [[MQTT]]
        ...
        qos = 1        # options are 0, 1, 2
        retain = true  # options are true or false

The observations can be sent individually, or in an aggregated packet:

[StdRestful]
    [[MQTT]]
        ...
        aggregation = individual, aggregate # individual, aggregate, or both

Bind to loop packets or archive records:

[StdRestful]
    [[MQTT]]
        ...
        binding = loop # options are loop or archive

Use the inputs map to customize name, format, or unit for any observation.
Note that starting with v0.24, option 'units' was renamed to 'unit', although
either will be accepted.

[StdRestful]
    [[MQTT]]
        ...
        unit_system = METRIC # default to metric
        [[[inputs]]]
            [[[[outTemp]]]]
                name = inside_temperature  # use a label other than outTemp
                format = %.2f              # two decimal places of precision
                unit = degree_F            # convert outTemp to F, others in C
            [[[[windSpeed]]]]
                unit = knot  # convert the wind speed to knots

To change the data binding:

[StdRestful]
    [[MQTT]]
        ...
        data_binding = wx_binding # or any other valid data binding

Use TLS to encrypt connection to broker.  The TLS options will be passed to
Paho client tls_set method.  Refer to Paho client documentation for details:

  https://eclipse.org/paho/clients/python/docs/

[StdRestful]
    [[MQTT]]
        ...
        [[[tls]]]
            # CA certificates file (mandatory)
            ca_certs = /etc/ssl/certs/ca-certificates.crt
            # PEM encoded client certificate file (optional)
            certfile = /home/user/.ssh/id.crt
            # private key file (optional)
            keyfile = /home/user/.ssh/id.key
            # Certificate requirements imposed on the broker (optional).
            #   Options are 'none', 'optional' or 'required'.
            #   Default is 'required'.
            cert_reqs = required
            # SSL/TLS protocol (optional).
            #   Options include sslv2, sslv23, sslv3, tls, tlsv1, tlsv11,
            #   tlsv12.
            #   Default is 'tlsv12'
            #   Not all options are supported by all systems.
            #   OpenSSL version till 1.0.0.h supports sslv2, sslv3 and tlsv1
            #   OpenSSL >= 1.0.1 supports tlsv11 and tlsv12
            #   OpenSSL >= 1.1.1 support TLSv1.3 (use tls_version = tls)
            #   Check your OpenSSL protocol support with:
            #   openssl s_client -help 2>&1  > /dev/null | egrep "\-(ssl|tls)[^a-z]"
            tls_version = tlsv12
            # Allowable encryption ciphers (optional).
            #   To specify multiple cyphers, delimit with commas and enclose
            #   in quotes.
            #ciphers =
			
[StdRestful]
    [[MQTT]]
        ...
            ha_discovery = True # Options are True or False, default False
            # Activate mqtt_discovery for home automation systems (devices) supproting that feature
            ha_discovery_topic = homeassistant/sensor/weewx # default None
            # root topic for discovery of devices on the network. If left empty discovery will be disabled.
            # based on the MQTT Discovery implementation for Home Assitant - https://www.home-assistant.io/docs/mqtt/discovery/
            ha_device_name = 'My weewx device' # default None
            # Unique name of the weewx device for publishing all sensors as one multi-sensor device. Will be used also as the name of the device inside HomeAssitant
            # If left empty all sensors will be published separately. 
"""

try:
    import queue as Queue
except ImportError:
    import Queue

try:
    from urllib.parse import urlparse
except ImportError:
    from urlparse import urlparse

import paho.mqtt.client as mqtt
import random
import socket
import sys
import time

try:
    import cjson as json
    setattr(json, 'dumps', json.encode)
    setattr(json, 'loads', json.decode)
except (ImportError, AttributeError):
    try:
        import simplejson as json
    except ImportError:
        import json

import weewx
import weewx.restx
import weewx.units
from weeutil.weeutil import to_int, to_bool, accumulateLeaves

VERSION = "0.24"

if weewx.__version__ < "3":
    raise weewx.UnsupportedFeature("weewx 3 is required, found %s" %
                                   weewx.__version__)

try:
    # weewx4 logging
    import weeutil.logger
    import logging
    log = logging.getLogger(__name__)
    def logdbg(msg):
        log.debug(msg)
    def loginf(msg):
        log.info(msg)
    def logerr(msg):
        log.error(msg)
except ImportError:
    # old-style weewx logging
    import syslog
    def logmsg(level, msg):
        syslog.syslog(level, 'restx: MQTT: %s' % msg)
    def logdbg(msg):
        logmsg(syslog.LOG_DEBUG, msg)
    def loginf(msg):
        logmsg(syslog.LOG_INFO, msg)
    def logerr(msg):
        logmsg(syslog.LOG_ERR, msg)


def _compat(d, old_label, new_label):
    if old_label in d and new_label not in d:
        d.setdefault(new_label, d[old_label])
        d.pop(old_label)

def _obfuscate_password(url):
    parts = urlparse(url)
    if parts.password is not None:
        # split out the host portion manually. We could use
        # parts.hostname and parts.port, but then you'd have to check
        # if either part is None. The hostname would also be lowercased.
        host_info = parts.netloc.rpartition('@')[-1]
        parts = parts._replace(netloc='{}:xxx@{}'.format(
            parts.username, host_info))
        url = parts.geturl()
    return url

# some unit labels are rather lengthy.  this reduces them to something shorter.
UNIT_REDUCTIONS = {
    'degree_F': 'F',
    'degree_C': 'C',
    'inch': 'in',
    'mile_per_hour': 'mph',
    'mile_per_hour2': 'mph',
    'km_per_hour': 'kph',
    'km_per_hour2': 'kph',
    'knot': 'knot',
    'knot2': 'knot2',
    'meter_per_second': 'mps',
    'meter_per_second2': 'mps',
    'degree_compass': None,
    'watt_per_meter_squared': 'Wpm2',
    'uv_index': None,
    'percent': None,
    'unix_epoch': None,
    }
# The type of data delivered by a sensor, impacts how it is displayed in the frontend of HomeAssistant.
# Each sensor can have defined device_class and/or unit_of_measurement passed in configuration
# https://www.home-assistant.io/integrations/sensor/
HA_SENSOR_TYPE = {
    'degree_F': 'temperature',
    'degree_C': 'temperature',
    'mbar': 'pressure',	
    'inch': 'distance',
    'cm': 'distance',
    'meter': 'distance',
    'km': 'distance',
    'mile': 'distance',
    'minute': 'duration',
    'hour': 'duration',
    'mile_per_hour': 'speed',
    'mile_per_hour2': 'speed',
    'cm_per_hour': 'speed',
    'cm_per_hour2': 'speed',   
    'km_per_hour': 'speed',
    'km_per_hour2': 'speed',
    'knot': 'speed',
    'knot2': 'speed',
    'meter_per_second': 'speed',
    'meter_per_second2': 'speed',
    'degree_compass': None,
    'watt_per_meter_squared': 'power',
    'uv_index': None,
    'percent': 'humidity',
    'unix_epoch': 'timestamp',
    'volt' : 'voltage'
    }
#https://github.com/home-assistant/core/blob/dev/homeassistant/const.py
HA_SENSOR_UNIT = {
    'degree_F': '°F',
    'degree_C': '°C',
    'mbar': 'mbar',	
    'inch': 'in',
    'cm': 'cm',
    'meter': 'm',
    'km': 'km',
    'mile': 'mi',
    'minute': 'min',
    'hour': 'h',
    'mile_per_hour': 'mph',
    'mile_per_hour2': 'mph',
    'cm_per_hour': 'cm/h',
    'cm_per_hour2': 'cm/h',   
    'km_per_hour': 'km/h',
    'km_per_hour2': 'km/h',
    'knot': 'kn',
    'knot2': 'kn',
    'meter_per_second': 'm/s',
    'meter_per_second2': 'm/s',
    'degree_compass': '°',
    'watt_per_meter_squared': 'W/m²',
    'uv_index': 'UV index',
    'percent': '%',
    'unix_epoch': None,
    'volt' : 'V'
}	
# return the units label for an observation
def _get_units_label(obs, unit_system, unit_type=None):
    if unit_type is None:
        (unit_type, _) = weewx.units.getStandardUnitType(unit_system, obs)
    return UNIT_REDUCTIONS.get(unit_type, unit_type)

# get the template for an observation based on the observation key
def _get_template(obs_key, overrides, append_units_label, unit_system):
    tmpl_dict = dict()
    if append_units_label:
        unit_type = overrides.get('unit')
        label = _get_units_label(obs_key, unit_system, unit_type)
        if label is not None:
            tmpl_dict['name'] = "%s_%s" % (obs_key, label)
    for x in ['name', 'format', 'unit']:
        if x in overrides:
            tmpl_dict[x] = overrides[x]
    return tmpl_dict


class MQTT(weewx.restx.StdRESTbase):
    def __init__(self, engine, config_dict):
        """This service recognizes standard restful options plus the following:

        Required parameters:

        server_url: URL of the broker, e.g., something of the form
          mqtt://username:password@localhost:1883/
        Default is None

        Optional parameters:

        unit_system: one of US, METRIC, or METRICWX
        Default is None; units will be those of data in the database

        topic: the MQTT topic under which to post
        Default is 'weather'

        append_units_label: should units label be appended to name
        Default is True

        obs_to_upload: Which observations to upload.  Possible values are
        none or all.  When none is specified, only items in the inputs list
        will be uploaded.  When all is specified, all observations will be
        uploaded, subject to overrides in the inputs list.
        Default is all

        inputs: dictionary of weewx observation names with optional upload
        name, format, and units
        Default is None

        tls: dictionary of TLS parameters used by the Paho client to establish
        a secure connection with the broker.
        Default is None
        """
        super(MQTT, self).__init__(engine, config_dict)
        loginf("service version is %s" % VERSION)
        site_dict = weewx.restx.get_site_dict(config_dict, 'MQTT', 'server_url')
        if not site_dict:
            return

        # for backward compatibility: 'units' is now 'unit_system'
        _compat(site_dict, 'units', 'unit_system')

        site_dict.setdefault('client_id', '')
        site_dict.setdefault('topic', 'weather')
        site_dict.setdefault('append_units_label', True)
        site_dict.setdefault('augment_record', True)
        site_dict.setdefault('obs_to_upload', 'all')
        site_dict.setdefault('retain', False)
        site_dict.setdefault('qos', 0)
        site_dict.setdefault('aggregation', 'individual,aggregate')
        site_dict.setdefault('ha_discovery', False)
        site_dict.setdefault('ha_discovery_topic', None)
        site_dict.setdefault('ha_device_name', None)

        usn = site_dict.get('unit_system', None)
        if usn is not None:
            site_dict['unit_system'] = weewx.units.unit_constants[usn]

        if 'tls' in config_dict['StdRESTful']['MQTT']:
            site_dict['tls'] = dict(config_dict['StdRESTful']['MQTT']['tls'])

        if 'inputs' in config_dict['StdRESTful']['MQTT']:
            site_dict['inputs'] = dict(config_dict['StdRESTful']['MQTT']['inputs'])
            # In the 'inputs' section, option 'units' is now 'unit'.
            for obs_type in site_dict['inputs']:
                _compat(site_dict['inputs'][obs_type], 'units', 'unit')

        site_dict['append_units_label'] = to_bool(site_dict.get('append_units_label'))
        site_dict['augment_record'] = to_bool(site_dict.get('augment_record'))
        site_dict['retain'] = to_bool(site_dict.get('retain'))
        site_dict['qos'] = to_int(site_dict.get('qos'))
        site_dict['ha_discovery'] = to_bool(site_dict.get('ha_discovery'))
        if site_dict['ha_discovery']:
            ha_device_name = site_dict.get('ha_device_name', None)
            if site_dict['ha_discovery_topic'] is None:
                site_dict['ha_discovery'] = False
                loginf("ha_discovery is disabled, because discovery_topic is missing")
            elif ha_device_name is not None:
                device = dict()
                device['name'] = site_dict['ha_device_name']
                device['manufacturer'] = weewx.__name__
                device['model'] = config_dict['Station']['station_type']
                device['hw_version'] = "weewx_version:" + weewx.__version__
                device['sw_version'] = "weewx-mqtt:" + VERSION
                # remove spaces and special chars in name and substitute with '_'
                device['identifiers'] = [device['name'].translate ({ord(c): "_" for c in "!@#$%^&*()[]{};:,./<>?\|`~-=+ "})]
                site_dict['ha_device_name'] = device
        binding = site_dict.pop('binding', 'archive')
        loginf("binding to %s" % binding)
        data_binding = site_dict.pop('data_binding', 'wx_binding')
        loginf("data_binding is %s" % data_binding)

        # if we are supposed to augment the record with data from weather
        # tables, then get the manager dict to do it.  there may be no weather
        # tables, so be prepared to fail.
        try:
            if site_dict.get('augment_record'):
                _manager_dict = weewx.manager.get_manager_dict_from_config(
                    config_dict, data_binding)
                site_dict['manager_dict'] = _manager_dict
        except weewx.UnknownBinding:
            pass

        self.archive_queue = Queue.Queue()
        self.archive_thread = MQTTThread(self.archive_queue, **site_dict)
        self.archive_thread.start()

        if 'archive' in binding:
            self.bind(weewx.NEW_ARCHIVE_RECORD, self.new_archive_record)
        if 'loop' in binding:
            self.bind(weewx.NEW_LOOP_PACKET, self.new_loop_packet)

        if 'topic' in site_dict:
            loginf("topic is %s" % site_dict['topic'])
        if usn is not None:
            loginf("desired unit system is %s" % usn)
        loginf("data will be uploaded to %s" %
               _obfuscate_password(site_dict['server_url']))
        if 'tls' in site_dict:
            loginf("network encryption/authentication will be attempted")
        if 'ha_discovery' in site_dict:
            loginf("ha_discovery is %s" % site_dict['ha_discovery'])          

    def new_archive_record(self, event):
        self.archive_queue.put(event.record)

    def new_loop_packet(self, event):
        self.archive_queue.put(event.packet)


class TLSDefaults(object):
    def __init__(self):
        import ssl

        # Paho acceptable TLS options
        self.TLS_OPTIONS = [
            'ca_certs', 'certfile', 'keyfile',
            'cert_reqs', 'tls_version', 'ciphers'
            ]
        # map for Paho acceptable TLS cert request options
        self.CERT_REQ_OPTIONS = {
            'none': ssl.CERT_NONE,
            'optional': ssl.CERT_OPTIONAL,
            'required': ssl.CERT_REQUIRED
            }
        # Map for Paho acceptable TLS version options. Some options are
        # dependent on the OpenSSL install so catch exceptions
        self.TLS_VER_OPTIONS = dict()
        try:
            self.TLS_VER_OPTIONS['tls'] = ssl.PROTOCOL_TLS
        except AttributeError:
            pass
        try:
            # deprecated - use tls instead, or tlsv12 if python < 2.7.13
            self.TLS_VER_OPTIONS['tlsv1'] = ssl.PROTOCOL_TLSv1
        except AttributeError:
            pass
        try:
            # deprecated - use tls instead, or tlsv12 if python < 2.7.13
            self.TLS_VER_OPTIONS['tlsv11'] = ssl.PROTOCOL_TLSv1_1
        except AttributeError:
            pass
        try:
            # deprecated - use tls instead if python >= 2.7.13
            self.TLS_VER_OPTIONS['tlsv12'] = ssl.PROTOCOL_TLSv1_2
        except AttributeError:
            pass
        try:
            # SSLv2 is insecure - this protocol is deprecated
            self.TLS_VER_OPTIONS['sslv2'] = ssl.PROTOCOL_SSLv2
        except AttributeError:
            pass
        try:
            # deprecated - use tls instead, or tlsv12 if python < 2.7.13
            # (alias for PROTOCOL_TLS)
            self.TLS_VER_OPTIONS['sslv23'] = ssl.PROTOCOL_SSLv23
        except AttributeError:
            pass
        try:
            # SSLv3 is insecure - this protocol is deprecated
            self.TLS_VER_OPTIONS['sslv3'] = ssl.PROTOCOL_SSLv3
        except AttributeError:
            pass


class MQTTThread(weewx.restx.RESTThread):

    def __init__(self, queue, server_url,
                 client_id='', topic='', unit_system=None, skip_upload=False,
                 augment_record=True, retain=False, aggregation='individual',
                 ha_discovery=False, ha_device_name=None, ha_discovery_topic=None,
                 inputs={}, obs_to_upload='all', append_units_label=True,
                 manager_dict=None, tls=None, qos=0,
                 post_interval=None, stale=None,
                 log_success=True, log_failure=True,
                 timeout=60, max_tries=3, retry_wait=5,
                 max_backlog=sys.maxsize):
        super(MQTTThread, self).__init__(queue,
                                         protocol_name='MQTT',
                                         manager_dict=manager_dict,
                                         post_interval=post_interval,
                                         max_backlog=max_backlog,
                                         stale=stale,
                                         log_success=log_success,
                                         log_failure=log_failure,
                                         max_tries=max_tries,
                                         timeout=timeout,
                                         retry_wait=retry_wait)
        self.server_url = server_url
        self.client_id = client_id
        self.topic = topic
        self.upload_all = True if obs_to_upload.lower() == 'all' else False
        self.append_units_label = append_units_label
        self.tls_dict = {}
        if tls is not None:
            # we have TLS options so construct a dict to configure Paho TLS
            dflts = TLSDefaults()
            for opt in tls:
                if opt == 'cert_reqs':
                    if tls[opt] in dflts.CERT_REQ_OPTIONS:
                        self.tls_dict[opt] = dflts.CERT_REQ_OPTIONS.get(tls[opt])
                elif opt == 'tls_version':
                    if tls[opt] in dflts.TLS_VER_OPTIONS:
                        self.tls_dict[opt] = dflts.TLS_VER_OPTIONS.get(tls[opt])
                elif opt in dflts.TLS_OPTIONS:
                    self.tls_dict[opt] = tls[opt]
            logdbg("TLS parameters: %s" % self.tls_dict)
        self.inputs = inputs
        self.unit_system = unit_system
        self.augment_record = augment_record
        self.retain = retain
        self.qos = qos
        self.aggregation = aggregation
        self.templates = dict()
        self.skip_upload = skip_upload
        self.mc = None
        self.mc_try_time = 0
        self.ha_discovery = ha_discovery
        self.ha_device_name = ha_device_name
        self.ha_discovery_topic = ha_discovery_topic

    def get_mqtt_client(self):
        if self.mc:
            if self.ha_discovery:
                self.mc.publish(self.topic + '/availability', payload='online', retain=True)
            return
        if time.time() - self.mc_try_time < self.retry_wait:
            return
        client_id = self.client_id
        if not client_id:
            pad = "%032x" % random.getrandbits(128)
            client_id = 'weewx_%s' % pad[:8]
        mc = mqtt.Client(client_id=client_id)
        if self.ha_discovery:
            mc.will_set(self.topic + '/availability', payload='offline', retain=True)
        url = urlparse(self.server_url)
        if url.username is not None and url.password is not None:
            mc.username_pw_set(url.username, url.password)
        # if we have TLS opts configure TLS on our broker connection
        if len(self.tls_dict) > 0:
            mc.tls_set(**self.tls_dict)
        try:
            self.mc_try_time = time.time()
            mc.connect(url.hostname, url.port)
        except (socket.error, socket.timeout, socket.herror) as e:
            logerr('Failed to connect to MQTT server (%s): %s' %
                    (_obfuscate_password(self.server_url), str(e)))
            self.mc = None
            return
        if self.ha_discovery:
            mc.publish(self.topic + '/availability', payload='online', retain=True)
        mc.loop_start()
        loginf('client established for %s' %
               _obfuscate_password(self.server_url))
        self.mc = mc

    def filter_data(self, record):
        # if uploading everything, we must check the upload variables list
        # every time since variables may come and go in a record.  use the
        # inputs to override any generic template generation.
        if self.upload_all:
            for f in record:
                if f not in self.templates:
                    self.templates[f] = _get_template(f,
                                                      self.inputs.get(f, {}),
                                                      self.append_units_label,
                                                      record['usUnits'])

        # otherwise, create the list of upload variables once, based on the
        # user-specified list of inputs.
        elif not self.templates:
            for f in self.inputs:
                self.templates[f] = _get_template(f, self.inputs[f],
                                                  self.append_units_label,
                                                  record['usUnits'])

        # loop through the templates, populating them with data from the record
        data = dict()
        for k in self.templates:
            try:
                v = float(record.get(k))
                name = self.templates[k].get('name', k)
                fmt = self.templates[k].get('format', '%s')
                to_units = self.templates[k].get('unit')
                if to_units is not None:
                    (from_unit, from_group) = weewx.units.getStandardUnitType(
                        record['usUnits'], k)
                    from_t = (v, from_unit, from_group)
                    v = weewx.units.convert(from_t, to_units)[0]
                s = fmt % v
                data[name] = s
            except (TypeError, ValueError):
                pass
        # FIXME: generalize this
        if 'latitude' in data and 'longitude' in data:
            parts = [str(data['latitude']), str(data['longitude'])]
            if 'altitude_meter' in data:
                parts.append(str(data['altitude_meter']))
            elif 'altitude_foot' in data:
                parts.append(str(data['altitude_foot']))
            data['position'] = ','.join(parts)
        return data

    def filter_sensor_info(self, data, unit_system):
        # Use sensor type and units from HomeAssistant documentation
        sensor = dict()
        for f in data:
            overrides = self.inputs.get(f.partition("_")[0], {})
            unit_type = overrides.get('unit')
            if unit_type is None:
                (unit_type, _) = weewx.units.getStandardUnitType(unit_system, f.partition("_")[0])
            sensor[f] = dict()
            sensor[f]['type'] = HA_SENSOR_TYPE.get(unit_type, unit_type)
            sensor[f]['unit'] = HA_SENSOR_UNIT.get(unit_type, unit_type)
        return sensor
	
    def ha_discovery_send(self, data, sensor, topic_mode):
        if self.ha_device_name is not None:
            device_tracker = dict()
            device_tracker['name']= self.ha_device_name['name']
            device_tracker['unique_id']= self.ha_device_name['identifiers'][0] + "_tracker"
            device_tracker['state_topic']= self.topic + '/availability'
            device_tracker['availability_topic']= self.topic + '/availability'
            device_tracker['device'] = self.ha_device_name
            tpc = self.ha_discovery_topic.replace("sensor", "device_tracker") + 'config'
            (res, mid) = self.mc.publish(tpc,  json.dumps(device_tracker),
                                     retain=True, qos=self.qos)
        for key in data:
            conf = dict()
            tpc = self.ha_discovery_topic + key + '/config'
            conf['name']= key
            conf['unique_id']= self.ha_device_name['identifiers'][0] + '_' + key
            if sensor[key]['type'] is not None:
                conf['device_class']= sensor[key]['type']
            if sensor[key]['unit'] is not None:
                conf['unit_of_measurement']=sensor[key]['unit']
            if topic_mode == 'aggregate':
                conf['state_topic'] = self.topic + '/loop'
                if sensor[key]['type'] == 'timestamp':
                    conf['value_template'] = "{{ value_json." + key + " | int | as_datetime }}"
                else:
                    conf['value_template'] = "{{ value_json." + key + "  | float | round(1) }}"
            elif topic_mode == 'individual':
                conf['state_topic'] = self.topic + '/' + key
                if sensor[key]['type'] == 'timestamp':
                    conf['value_template'] = "{{ value | int | as_datetime }}"
                else:
                    conf['value_template'] = "{{ value | float | round(1) }}"
            conf['availability_topic'] = self.topic + '/availability' 
            if self.ha_device_name is not None:
                conf['device'] = self.ha_device_name
            (res, mid) = self.mc.publish(tpc, json.dumps(conf),
            # to avoid losing configuration on restart in HA retain = true
		                        retain=True, qos=self.qos)
            if res != mqtt.MQTT_ERR_SUCCESS:
                logerr("publish failed for %s: %s" %
                        (tpc, mqtt.error_string(res)))
	
    def process_record(self, record, dbm):
        if self.augment_record and dbm is not None:
            record = self.get_record(record, dbm)
        if self.unit_system is not None:
            record = weewx.units.to_std_system(record, self.unit_system)
        data = self.filter_data(record)
        if weewx.debug >= 2:
            logdbg("data: %s" % data)
        if self.skip_upload:
            loginf("skipping upload")
            return
        self.get_mqtt_client()
        if not self.mc:
            raise weewx.restx.FailedPost('MQTT client not available')
        if self.aggregation.find('aggregate') >= 0:
            tpc = self.topic + '/loop'
            (res, mid) = self.mc.publish(tpc, json.dumps(data),
                                         retain=self.retain, qos=self.qos)
            if res != mqtt.MQTT_ERR_SUCCESS:
                logerr("publish failed for %s: %s" %
                       (tpc, mqtt.error_string(res)))
        if self.aggregation.find('individual') >= 0:
            for key in data:
                tpc = self.topic + '/' + key
                (res, mid) = self.mc.publish(tpc, data[key],
                                             retain=self.retain)
                if res != mqtt.MQTT_ERR_SUCCESS:
                    logerr("publish failed for %s: %s" %
                           (tpc, mqtt.error_string(res)))
        if self.ha_discovery:
            if self.aggregation.find('aggregate') >= 0:
                topic_mode = 'aggregate'
            elif self.aggregation.find('individual') >= 0:
                topic_mode = 'individual'
            else:
                topic_mode = None
            if topic_mode is not None:
                sensor = self.filter_sensor_info(data, record['usUnits'])
                self.ha_discovery_send(data, sensor, topic_mode)
