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

Connection robustness.  The connection to the broker is verified before every
post and rebuilt if it has been lost, and a failed post is retried.  The
defaults suit an archive binding; if you bind to loop packets, lower the
timeouts so a broker outage cannot stall the posting thread between packets,
and set 'max_backlog' to bound the queue.

[StdRestful]
    [[MQTT]]
        ...
        # MQTT keepalive interval, in seconds.  The broker declares the client
        # dead if it hears nothing for 1.5x this.  Default is 60.
        keepalive = 60
        # How long to wait for the broker to acknowledge a connection
        # (CONNACK), in seconds.  Default is 10.
        connect_timeout = 10
        # How long to wait for the broker to confirm delivery of a message, in
        # seconds.  Only meaningful when qos is 1 or 2; set to 0 to publish
        # without waiting for confirmation.  Default is 10.
        publish_timeout = 10
        # Bounds on the delay between the client's own reconnect attempts, in
        # seconds.  The delay doubles from min to max.  Defaults are 1 and 120.
        reconnect_min_delay = 1
        reconnect_max_delay = 120
        # How many times to try posting a record, and how long to wait between
        # attempts, in seconds.  Defaults are 3 and 5.
        max_tries = 3
        retry_wait = 5

Note on 'client_id': an MQTT broker permits only one connection per client
identifier, and disconnects the older connection when a second one presents the
same id.  If you set 'client_id' explicitly, running 'weectl rest run MQTT' by
hand will therefore kick the running weewx daemon off the broker.  Leaving
'client_id' unset (the default) gives each connection a random id and avoids
this entirely.

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
import re
import socket
import sys
import threading
import time

try:
    # paho-mqtt 2.0 introduced a callback API version, and the callback
    # signatures differ between the two versions.  Probe once here so the rest
    # of the module can work with either.
    from paho.mqtt.enums import CallbackAPIVersion
    PAHO2 = True
except ImportError:
    PAHO2 = False

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
from weeutil.weeutil import to_int, to_float, to_bool, accumulateLeaves

VERSION = "0.26"

# How long to let the client finish a reconnect of its own before giving up on
# it and building a new one.  Short on purpose: a client that is genuinely
# reconnecting recovers well within this, and one that is not is dead and can
# never publish again, so there is nothing to gain by waiting longer.
RECONNECT_GRACE = 2.0

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

def _rc_string(rc):
    """Render a paho result code.

    paho 1.x reports plain ints, paho 2.x a ReasonCode object which is not an
    int and which error_string() renders as "Unknown error." -- unhelpfully
    hiding, for example, "Session taken over", the broker's way of saying
    another client connected with the same client_id.
    """
    if isinstance(rc, int):
        return mqtt.error_string(rc)
    return str(rc)


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


# ----------------------------------------------------------------------------
# Home Assistant MQTT discovery support
# ----------------------------------------------------------------------------
#
# Discovery describes the data *exactly as it is published*. Whether values are
# converted to metric/SI is left to the admin via the existing 'unit_system'
# option (US, METRIC, or METRICWX), which converts the whole record before it is
# published; discovery then reports whatever units that produced. So:
#   - leave unit_system unset  -> native units published, HA shows e.g. degree_F
#   - unit_system = METRICWX   -> SI-ish units published, HA shows degree_C, etc.
#
# GROUP_TO_DEVICE_CLASS maps a weewx unit group to a Home Assistant
# (device_class, state_class). Only observations whose group is listed here get a
# discovery entity. UNIT_TO_HA_UOM maps the weewx unit actually in use to a Home
# Assistant 'unit_of_measurement' string.
GROUP_TO_DEVICE_CLASS = {
    'group_temperature':  ('temperature',             'measurement'),
    'group_pressure':     ('atmospheric_pressure',    'measurement'),
    'group_pressurerate': (None,                      'measurement'),
    'group_rain':         ('precipitation',           'total'),
    'group_rainrate':     ('precipitation_intensity', 'measurement'),
    'group_speed':        ('wind_speed',              'measurement'),
    'group_percent':      (None,                      'measurement'),
    'group_direction':    (None,                      'measurement'),
    'group_radiation':    ('irradiance',              'measurement'),
    'group_distance':     ('distance',                'measurement'),
    'group_altitude':     ('distance',                'measurement'),
    'group_volt':         ('voltage',                 'measurement'),
    'group_power':        ('power',                   'measurement'),
    'group_energy':       ('energy',                  'total_increasing'),
    # A timestamp (e.g. dateTime, the time of the observation). state_class must
    # be None: Home Assistant rejects a state_class on a timestamp entity. The
    # epoch value is converted to a datetime in the discovery value_template.
    'group_time':         ('timestamp',               None),
}

# Map weewx unit names to Home Assistant 'unit_of_measurement' strings. weewx
# 'mbar' is reported to HA as 'hPa' (numerically identical). Units not listed are
# sent without a unit_of_measurement.
UNIT_TO_HA_UOM = {
    'degree_F': '°F', 'degree_C': '°C', 'degree_K': 'K',
    'inHg': 'inHg', 'mbar': 'hPa', 'hPa': 'hPa', 'mmHg': 'mmHg', 'kPa': 'kPa',
    'inHg_per_hour': 'inHg/h', 'mbar_per_hour': 'hPa/h', 'hPa_per_hour': 'hPa/h',
    'inch': 'in', 'cm': 'cm', 'mm': 'mm',
    'inch_per_hour': 'in/h', 'cm_per_hour': 'cm/h', 'mm_per_hour': 'mm/h',
    'mile_per_hour': 'mph', 'km_per_hour': 'km/h', 'meter_per_second': 'm/s',
    'knot': 'kn',
    'percent': '%', 'degree_compass': '°',
    'watt_per_meter_squared': 'W/m²',
    'volt': 'V', 'watt': 'W', 'watt_hour': 'Wh',
    'km': 'km', 'meter': 'm', 'mile': 'mi', 'foot': 'ft',
    'uv_index': None,
}

# Home Assistant only allows a fixed set of units for each device_class, and
# rejects the whole discovery message if the unit_of_measurement is not one of
# them (e.g. 'cm/h' is not valid for 'precipitation_intensity'). For each
# device_class we use, this lists the units HA accepts. If our published unit is
# not in the set, we drop the device_class rather than the (correct) unit.
# Device classes without a unit constraint (e.g. 'timestamp') are not listed.
DEVICE_CLASS_UNITS = {
    'temperature':             {'°C', '°F', 'K'},
    'atmospheric_pressure':    {'cbar', 'bar', 'hPa', 'mmHg', 'inHg', 'kPa',
                                'mbar', 'Pa', 'psi'},
    'humidity':                {'%'},
    'precipitation':           {'cm', 'in', 'mm'},
    'precipitation_intensity': {'in/d', 'in/h', 'mm/d', 'mm/h'},
    'wind_speed':              {'Beaufort', 'ft/s', 'km/h', 'kn', 'm/s', 'mph'},
    'irradiance':              {'W/m²', 'BTU/(h⋅ft²)'},
    'distance':                {'km', 'm', 'cm', 'mm', 'mi', 'nmi', 'yd',
                                'in', 'ft'},
    'voltage':                 {'V', 'mV', 'µV', 'kV', 'MV'},
    'power':                   {'mW', 'W', 'kW', 'MW', 'GW', 'TW', 'BTU/h'},
    'energy':                  {'J', 'kJ', 'MJ', 'GJ', 'mWh', 'Wh', 'kWh',
                                'MWh', 'GWh', 'TWh', 'cal', 'kcal', 'Mcal',
                                'Gcal'},
}

# Observations in group_percent that really represent relative humidity. These
# get the Home Assistant 'humidity' device_class; other percentages do not.
HUMIDITY_OBS = {'outHumidity', 'inHumidity', 'humidity'}

# Record keys that carry no real observation and must never become a sensor. The
# admin can exclude further fields with the 'skip_fields' option; these are always
# excluded regardless. (dateTime is intentionally NOT here: it is published as a
# proper timestamp entity, see group_time above.)
DEFAULT_SKIP_FIELDS = {'interval', 'usUnits'}

# Friendly display names for common observations. Anything not listed falls back
# to a humanized version of the weewx key (e.g. 'extraTemp1' -> 'Extra Temp 1').
OBS_FRIENDLY_NAMES = {
    'outTemp': 'Outside Temperature', 'inTemp': 'Inside Temperature',
    'outHumidity': 'Outside Humidity', 'inHumidity': 'Inside Humidity',
    'barometer': 'Barometer', 'pressure': 'Pressure', 'altimeter': 'Altimeter',
    'windSpeed': 'Wind Speed', 'windGust': 'Wind Gust',
    'windDir': 'Wind Direction', 'windGustDir': 'Wind Gust Direction',
    'rain': 'Rain', 'rainRate': 'Rain Rate', 'dewpoint': 'Dew Point',
    'windchill': 'Wind Chill', 'heatindex': 'Heat Index',
    'radiation': 'Solar Radiation', 'UV': 'UV Index',
    'ET': 'Evapotranspiration', 'appTemp': 'Apparent Temperature',
    # 'dateTime' is the time of the observation. Give it a descriptive name and
    # id so it does not show up in Home Assistant as a generic "Date Time" that
    # clashes with the many unrelated dateTime/timestamp entities other devices
    # publish.
    'dateTime': 'Observation Time',
}

# A few observations get a more descriptive id (used for unique_id/object_id, and
# hence the Home Assistant entity_id) than their terse weewx key.
OBS_ID_OVERRIDES = {'dateTime': 'observation_time'}


def _friendly_name(obs):
    """Return a human-friendly name for an observation key."""
    if obs in OBS_FRIENDLY_NAMES:
        return OBS_FRIENDLY_NAMES[obs]
    # Insert spaces at camelCase and letter/digit boundaries, then title-case.
    s = re.sub('([a-z])([A-Z])', r'\1 \2', obs)
    s = re.sub('([A-Za-z])([0-9])', r'\1 \2', s)
    s = s.replace('_', ' ')
    return s[:1].upper() + s[1:]


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
        # qos 1 (at least once) is the default: at qos 0 the client silently
        # drops anything published while the connection is down, so a momentary
        # outage loses the record outright.
        site_dict.setdefault('qos', 1)
        site_dict.setdefault('aggregation', 'individual,aggregate')
        site_dict.setdefault('keepalive', 60)
        site_dict.setdefault('connect_timeout', 10)
        site_dict.setdefault('publish_timeout', 10)
        site_dict.setdefault('reconnect_min_delay', 1)
        site_dict.setdefault('reconnect_max_delay', 120)

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
        site_dict['keepalive'] = to_int(site_dict.get('keepalive'))
        for opt in ('connect_timeout', 'publish_timeout',
                    'reconnect_min_delay', 'reconnect_max_delay'):
            site_dict[opt] = to_float(site_dict.get(opt))
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

        # Optional Home Assistant MQTT discovery. The nested [[[ha_discovery]]]
        # section is not part of the accumulated site_dict, so read it directly.
        mqtt_cfg = config_dict['StdRESTful']['MQTT']
        if 'ha_discovery' in mqtt_cfg and to_bool(mqtt_cfg['ha_discovery'].get('enable', False)):
            ha_cfg = mqtt_cfg['ha_discovery']
            device_cfg = ha_cfg.get('device', {})
            stn = getattr(self.engine, 'stn_info', None)
            # 'skip_fields' lists observations that get no discovery message.
            # They are still published as normal MQTT data; only the HA discovery
            # announcement is suppressed.
            # ConfigObj gives a string for a single value or a list for several.
            skip_fields = ha_cfg.get('skip_fields', [])
            if isinstance(skip_fields, str):
                skip_fields = [skip_fields]
            site_dict['ha_discovery'] = {
                'enable': True,
                'discovery_prefix': ha_cfg.get('discovery_prefix', 'homeassistant'),
                'node_id': ha_cfg.get('node_id', 'weewx'),
                'unique_id_prefix': ha_cfg.get('unique_id_prefix', 'weewx'),
                'skip_fields': skip_fields,
                'device': {
                    'name': device_cfg.get('name',
                                           getattr(stn, 'location', None) or 'WeeWX'),
                    'manufacturer': device_cfg.get('manufacturer', 'WeeWX'),
                    'model': device_cfg.get('model',
                                            getattr(stn, 'hardware', None) or 'Unknown'),
                    'identifiers': device_cfg.get('identifiers',
                                                  ha_cfg.get('node_id', 'weewx')),
                    'sw_version': weewx.__version__,
                },
            }
            loginf("Home Assistant discovery is enabled")

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
                 inputs={}, obs_to_upload='all', append_units_label=True,
                 manager_dict=None, tls=None, qos=1,
                 ha_discovery=None,
                 keepalive=60, connect_timeout=10, publish_timeout=10,
                 reconnect_min_delay=1, reconnect_max_delay=120,
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
        # ConfigObj parses a comma-separated 'aggregation' (e.g. "individual,
        # aggregate") into a list; normalize to a string so .find() works.
        if not isinstance(aggregation, str):
            aggregation = ','.join(aggregation)
        self.aggregation = aggregation
        self.templates = dict()
        self.skip_upload = skip_upload
        self.keepalive = to_int(keepalive)
        self.connect_timeout = to_float(connect_timeout)
        self.publish_timeout = to_float(publish_timeout)
        self.reconnect_min_delay = to_float(reconnect_min_delay)
        self.reconnect_max_delay = to_float(reconnect_max_delay)
        self.mc = None
        # Set by on_connect once the broker has acknowledged the connection,
        # cleared by on_disconnect.  This is what lets us wait for a CONNACK
        # instead of assuming a socket that has been opened is usable.
        self._connected = threading.Event()
        # Set when the broker answers at all, accepting or refusing.  A refusal
        # is final, so waiting on this rather than on _connected lets a bad
        # password fail immediately instead of sitting out connect_timeout while
        # the client retries in the background.
        self._connack = threading.Event()
        # The reason code from the most recent CONNACK, so a refused connection
        # can be reported as itself (e.g. "not authorised") rather than as a
        # mystery publish failure later on.
        self._connect_rc = None
        # Home Assistant discovery: config dict (or empty) and a one-time guard.
        self.ha = ha_discovery or {}
        device = self.ha.get('device')
        if device:
            # ConfigObj turns any comma-containing value (e.g. a location like
            # "Bronx, New York") into a list, but Home Assistant requires
            # these device fields to be plain strings -- it rejects the whole
            # discovery message otherwise. Coerce them.
            for key in ('name', 'manufacturer', 'model', 'sw_version'):
                if isinstance(device.get(key), (list, tuple)):
                    device[key] = ', '.join(str(x) for x in device[key])
            # 'identifiers' must be a list of strings.
            ident = device.get('identifiers')
            if isinstance(ident, str):
                device['identifiers'] = [ident]
            elif isinstance(ident, (list, tuple)):
                device['identifiers'] = [str(x) for x in ident]
        # Fields excluded from discovery: the mandatory non-observations plus any
        # the admin configured via 'skip_fields'.
        self.skip_fields = set(DEFAULT_SKIP_FIELDS) | set(self.ha.get('skip_fields', []))
        self._discovery_sent = False
        # Latches the "no usUnits" warning so a station that never emits it logs
        # once, not on every record.
        self._warned_no_usunits = False

    def _on_connect(self, rc):
        """Called from the client's network thread when a CONNACK arrives."""
        self._connect_rc = rc
        self._connack.set()
        # rc is 0 / Success on acceptance.  Anything else is a refusal (bad
        # credentials, not authorised, server unavailable, ...) and the client
        # is not usable.
        if rc == 0:
            self._connected.set()
            loginf('connected to %s' %
                   _obfuscate_password(self.server_url))
            # The broker may have been restarted without its retained message
            # store, which would leave Home Assistant with no discovery
            # configs.  Re-announcing on every connect is cheap and idempotent
            # (the messages are retained), so let the next record redo it.
            self._discovery_sent = False
        else:
            self._connected.clear()
            logerr('connection to %s refused: %s' %
                   (_obfuscate_password(self.server_url),
                    mqtt.connack_string(rc)))

    def _on_disconnect(self, rc):
        """Called from the client's network thread when the connection drops."""
        self._connected.clear()
        # Log an unexpected disconnect loudly.  Without this the first sign of
        # trouble is a failed publish, which for an archive binding may not
        # happen until the next archive interval.
        if rc == 0:
            loginf('disconnected from %s' %
                   _obfuscate_password(self.server_url))
        else:
            logerr('unexpectedly disconnected from %s: %s' %
                   (_obfuscate_password(self.server_url), _rc_string(rc)))

    def _new_paho_client(self, client_id):
        """Create a paho client, using whichever callback API is available."""
        if PAHO2:
            mc = mqtt.Client(CallbackAPIVersion.VERSION2, client_id=client_id)
            # paho 2.x: on_connect(client, userdata, flags, reason_code,
            # properties), on_disconnect(client, userdata, flags, reason_code,
            # properties).
            mc.on_connect = \
                lambda c, u, flags, rc, props=None: self._on_connect(rc)
            mc.on_disconnect = \
                lambda c, u, flags, rc, props=None: self._on_disconnect(rc)
        else:
            mc = mqtt.Client(client_id=client_id)
            # paho 1.x: on_connect(client, userdata, flags, rc),
            # on_disconnect(client, userdata, rc).
            mc.on_connect = lambda c, u, flags, rc: self._on_connect(rc)
            mc.on_disconnect = lambda c, u, rc: self._on_disconnect(rc)
        return mc

    def _teardown_client(self):
        """Discard the current client.  Safe to call in any state."""
        mc, self.mc = self.mc, None
        self._connected.clear()
        if mc is None:
            return
        # Both calls can raise if the socket is already gone; that is exactly
        # the situation we are cleaning up after, so it is not worth reporting.
        try:
            mc.disconnect()
        except Exception as e:
            logdbg('error while disconnecting client: %s' % e)
        try:
            mc.loop_stop()
        except Exception as e:
            logdbg('error while stopping client loop: %s' % e)

    def _new_client(self):
        """Connect a new client and wait for the broker to accept it.

        Raises FailedPost if a usable connection cannot be established.
        """
        client_id = self.client_id
        if not client_id:
            pad = "%032x" % random.getrandbits(128)
            client_id = 'weewx_%s' % pad[:8]
        mc = self._new_paho_client(client_id)
        mc.reconnect_delay_set(min_delay=self.reconnect_min_delay,
                               max_delay=self.reconnect_max_delay)
        url = urlparse(self.server_url)
        if url.username is not None and url.password is not None:
            mc.username_pw_set(url.username, url.password)
        # if we have TLS opts configure TLS on our broker connection
        if len(self.tls_dict) > 0:
            mc.tls_set(**self.tls_dict)
        port = url.port or (8883 if self.tls_dict else 1883)
        self._connected.clear()
        self._connack.clear()
        self._connect_rc = None
        try:
            mc.connect(url.hostname, port, keepalive=self.keepalive)
        except (socket.error, socket.timeout, socket.herror, OSError,
                ValueError) as e:
            # ValueError covers a malformed server_url (no host, bad port).
            # Reporting it per record is better than letting it propagate and
            # terminate the posting thread.
            raise weewx.restx.FailedPost(
                'cannot connect to MQTT server (%s): %s' %
                (_obfuscate_password(self.server_url), e))
        mc.loop_start()
        self.mc = mc
        # connect() only opens the socket and sends the CONNECT packet; the
        # broker's CONNACK is handled by the network thread started above.
        # Wait for it, so we never hand back a client that the broker has not
        # accepted -- otherwise a refused connection only shows up as a failed
        # publish much later.
        self._connack.wait(self.connect_timeout)
        if not self._connected.is_set():
            rc = self._connect_rc
            self._teardown_client()
            if rc is None:
                raise weewx.restx.FailedPost(
                    'timed out after %s seconds waiting for MQTT server (%s) '
                    'to acknowledge the connection' %
                    (self.connect_timeout,
                     _obfuscate_password(self.server_url)))
            raise weewx.restx.FailedPost(
                'MQTT server (%s) refused the connection: %s' %
                (_obfuscate_password(self.server_url),
                 mqtt.connack_string(rc)))
        logdbg('client established for %s' %
               _obfuscate_password(self.server_url))
        return mc

    def _ensure_client(self):
        """Return a connected client, rebuilding it if the connection is gone.

        Raises FailedPost if no connection can be established.
        """
        if self.mc is not None and self.mc.is_connected():
            return self.mc
        if self.mc is not None:
            # The client exists but is not connected.  It may be part way
            # through its own reconnect, so give that a moment to land before
            # throwing the client away -- but only a moment, because the client
            # may equally be permanently dead (paho stops its network thread
            # for good on some disconnect paths), and a dead client can never
            # publish again.
            if self._connected.wait(RECONNECT_GRACE) and self.mc.is_connected():
                return self.mc
            loginf('MQTT client is not connected; rebuilding it')
            self._teardown_client()
        return self._new_client()

    def _publish(self, messages, retain=None):
        """Publish a list of (topic, payload) and confirm delivery.

        Raises FailedPost if any message could not be handed to the broker.
        """
        if retain is None:
            retain = self.retain
        # Publish everything first and collect the message handles, then wait.
        # Waiting on each message before publishing the next would serialize a
        # broker round trip per observation; this way the acknowledgements come
        # back in parallel.
        pending = []
        for topic, payload in messages:
            info = self.mc.publish(topic, payload,
                                   retain=retain, qos=self.qos)
            if info.rc != mqtt.MQTT_ERR_SUCCESS:
                raise weewx.restx.FailedPost(
                    'publish failed for %s: %s' %
                    (topic, _rc_string(info.rc)))
            pending.append((topic, info))

        if not self.qos or not self.publish_timeout:
            return
        # At qos 0 publish() is fire and forget, so there is nothing to confirm.
        # At qos 1 or 2 the broker acknowledges each message, which is the only
        # real evidence the data arrived.
        deadline = time.time() + self.publish_timeout
        for topic, info in pending:
            try:
                info.wait_for_publish(max(0.0, deadline - time.time()))
                published = info.is_published()
            except (RuntimeError, ValueError) as e:
                # Raised when the message can no longer be delivered, e.g. the
                # connection dropped between publishing and waiting.
                raise weewx.restx.FailedPost(
                    'publish failed for %s: %s' % (topic, e))
            if not published:
                raise weewx.restx.FailedPost(
                    'timed out after %s seconds waiting for the MQTT server to '
                    'confirm %s' % (self.publish_timeout, topic))

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

    def _published_name_and_unit(self, obs, usUnits):
        """Return (published_name, weewx_unit) for an observation, matching exactly
        what filter_data() publishes: this honors append_units_label and any
        per-input 'name'/'unit' override, so the discovery state_topic/value_template
        line up with the data that is actually sent."""
        overrides = self.inputs.get(obs, {})
        tmpl = _get_template(obs, overrides, self.append_units_label, usUnits)
        name = tmpl.get('name', obs)
        unit = tmpl.get('unit')
        if unit is None:
            try:
                (unit, _group) = weewx.units.getStandardUnitType(usUnits, obs)
            except (KeyError, ValueError):
                unit = None
        return name, unit

    def _discovery_configs(self, record):
        """Build (config_topic, payload) pairs for Home Assistant discovery.

        A sensor is produced for each *published* observation whose unit group is
        known (GROUP_TO_DEVICE_CLASS) and that is not excluded by skip_fields. The
        reported units are those of the data as actually published -- i.e. after
        any 'unit_system' conversion -- so Home Assistant shows whatever units the
        station or admin chose. In aggregate mode the sensor reads the JSON loop
        packet via a value_template; otherwise it reads the field's own sub-topic.
        """
        configs = []
        # Units are interpreted via usUnits. If a record lacks it,
        # getStandardUnitType() below returns (None, None) and every observation
        # is skipped, so this naturally yields no configs. The caller decides what
        # to do with an empty result (warn and retry on a later record).
        usUnits = record.get('usUnits')
        prefix = self.ha.get('discovery_prefix', 'homeassistant')
        node = self.ha.get('node_id', 'weewx')
        uid_prefix = self.ha.get('unique_id_prefix', 'weewx')
        device = self.ha.get('device', {})
        aggregate = self.aggregation.find('aggregate') >= 0
        # Mirror filter_data's choice of which observations get published.
        candidates = record if self.upload_all else self.inputs
        for obs in candidates:
            if obs in self.skip_fields or obs not in record or record.get(obs) is None:
                continue
            try:
                float(record.get(obs))
            except (TypeError, ValueError):
                continue
            try:
                (_u, group) = weewx.units.getStandardUnitType(usUnits, obs)
            except (KeyError, ValueError):
                continue
            if group not in GROUP_TO_DEVICE_CLASS:
                continue
            device_class, state_class = GROUP_TO_DEVICE_CLASS[group]
            if group == 'group_percent' and obs in HUMIDITY_OBS:
                device_class = 'humidity'
            name, unit = self._published_name_and_unit(obs, usUnits)
            uom = UNIT_TO_HA_UOM.get(unit)
            # HA validates the unit against the device_class and rejects the whole
            # entity on a mismatch (e.g. 'cm/h' is not valid for
            # 'precipitation_intensity' when unit_system=METRIC). The unit is the
            # truth, so keep it and drop the device_class instead -- the sensor is
            # then a plain measurement with the correct unit.
            if (device_class in DEVICE_CLASS_UNITS
                    and uom not in DEVICE_CLASS_UNITS[device_class]):
                logdbg("HA discovery: unit %r not valid for device_class %r on "
                       "%s; publishing without a device_class"
                       % (uom, device_class, obs))
                device_class = None
            obs_id = OBS_ID_OVERRIDES.get(obs, obs)
            is_timestamp = device_class == 'timestamp'
            payload = {
                'name': _friendly_name(obs),
                'unique_id': "%s_%s" % (uid_prefix, obs_id),
                'object_id': "%s_%s" % (uid_prefix, obs_id),
                'device': device,
            }
            # Home Assistant forbids a state_class on a timestamp entity, so only
            # set it when present.
            if state_class is not None:
                payload['state_class'] = state_class
            if uom is not None:
                payload['unit_of_measurement'] = uom
            if device_class is not None:
                payload['device_class'] = device_class
            # Where the sensor reads its state, and the expression for the value.
            if aggregate:
                payload['state_topic'] = self.topic + '/loop'
                src = "value_json.%s" % name
            else:
                payload['state_topic'] = self.topic + '/' + name
                src = "value"
            # A timestamp is published as a Unix epoch, but HA's timestamp
            # device_class needs a datetime, so convert it. Other fields need a
            # template only in aggregate mode (to pull the value out of the JSON);
            # in individual mode the raw payload already is the value.
            if is_timestamp:
                payload['value_template'] = "{{ as_datetime(%s | float) }}" % src
            elif aggregate:
                payload['value_template'] = "{{ %s }}" % src
            topic = "%s/sensor/%s/%s/config" % (prefix, node, obs)
            configs.append((topic, payload))
        return configs

    def publish_ha_discovery(self, record):
        """Publish Home Assistant discovery configs (retained). Used both for the
        automatic once-before-first-packet trigger and the manual admin trigger
        (weectl rest run --discovery)."""
        if not self.ha.get('enable'):
            loginf("Home Assistant discovery is not enabled")
            return
        # Every observation's units are interpreted via the record's usUnits.
        # Some records may not carry it; without it the data cannot be described,
        # so skip *without* marking discovery as sent, leaving it to be retried on
        # a later record that does have usUnits.
        if record.get('usUnits') is None:
            logerr("Home Assistant discovery skipped: record has no 'usUnits'; "
                   "cannot determine units. Will retry on a later record.")
            return
        # Describe the data as it will actually be published: honor unit_system.
        # (Idempotent when process_record already converted the record.)
        if self.unit_system is not None:
            record = weewx.units.to_std_system(record, self.unit_system)
        configs = self._discovery_configs(record)
        if not configs:
            logdbg("Home Assistant discovery: record has no describable "
                   "observations; will retry on a later record.")
            return
        self._ensure_client()
        # Discovery messages are retained so Home Assistant sees them whenever it
        # (re)starts. A failure here propagates so the caller can retry, rather
        # than leaving HA with a half-announced device.
        self._publish([(topic, json.dumps(payload))
                       for topic, payload in configs], retain=True)
        loginf("published Home Assistant discovery for %d sensors" % len(configs))
        # Only now do we consider discovery done, so a first record that could not
        # be described does not permanently suppress it.
        self._discovery_sent = True

    def process_record(self, record, dbm):
        # Augmenting, converting and labeling all interpret the data via usUnits,
        # which is a WeeWX invariant: every loop packet and archive record carries
        # it. A record without it cannot be processed, so skip it instead of
        # raising KeyError -- which would kill the posting thread -- or publishing
        # unitless, inconsistent topics. We warn only once so a misbehaving source
        # that never emits usUnits does not flood the log every record.
        if record.get('usUnits') is None:
            if not self._warned_no_usunits:
                logerr("skipping record(s) with no 'usUnits': cannot determine "
                       "units. weewx-mqtt requires usUnits on every record.")
                self._warned_no_usunits = True
            return
        self._warned_no_usunits = False
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
        messages = []
        if self.aggregation.find('aggregate') >= 0:
            messages.append((self.topic + '/loop', json.dumps(data)))
        if self.aggregation.find('individual') >= 0:
            for key in data:
                messages.append((self.topic + '/' + key, data[key]))

        # Try the whole record, connection included, up to max_tries times.
        # weewx does not retry for us: RESTThread.run_loop only retries posts
        # made through post_with_retries(), which this thread does not use, so
        # a FailedPost that escapes here is the end of the road for the record.
        for attempt in range(self.max_tries):
            try:
                self._ensure_client()
                # Publish Home Assistant discovery before the first data packet,
                # and again after a reconnect (see _on_connect).
                if self.ha.get('enable') and not self._discovery_sent:
                    self.publish_ha_discovery(record)
                self._publish(messages)
                return
            except weewx.restx.FailedPost as e:
                # A failed publish almost always means the connection has gone,
                # so discard the client and let the next attempt build a fresh
                # one.  This is also what keeps the retry exactly-once: at qos 1
                # or 2 the client holds a message published while disconnected
                # and would redeliver it on reconnect, so republishing on a
                # reused client would send every observation twice.
                self._teardown_client()
                if attempt + 1 >= self.max_tries:
                    raise weewx.restx.FailedPost(
                        'failed after %d attempts: %s' % (self.max_tries, e))
                loginf('attempt %d of %d failed (%s); retrying in %s seconds' %
                       (attempt + 1, self.max_tries, e, self.retry_wait))
                time.sleep(self.retry_wait)
