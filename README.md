MQTT is a machine-to-machine (M2M)/"Internet of Things" connectivity protocol.
It was designed as an extremely lightweight publish/subscribe messaging
transport. It is useful for connections with remote locations where a small
code footprint is required and/or network bandwidth is at a premium. For
example, it has been used in sensors communicating to a broker via satellite
link, over occasional dial-up connections with healthcare providers, and in a
range of home automation and small device scenarios. It is also ideal for
mobile applications because of its small size, low power usage, minimised data
packets, and efficient distribution of information to one or many receivers.

This is an extension to weewx that uploads weather data to an MQTT broker
(server).


### Download

wget -O weewx-mqtt.zip https://github.com/matthewwall/weewx-mqtt/archive/master.zip

### How to Install

1.  Install the python modules that weewx-mqtt depends on (python-cjson is optional, and currently not supported with python3):

NOTE: the current version of paho-mqtt is not compatible with v0.24 of this extension. Until this extension is updated, force install a previous version of paho-mqtt.
```
# For a system with only python3:
sudo pip install paho-mqtt==1.6.1
sudo pip install python-cjson
```
**Or**
```
# For a system with both python2 and python3, and using weewx on python3:
sudo pip3 install paho-mqtt==1.6.1
```

2.  Run the extension installer:

```
sudo weectl extension install weewx-mqtt.zip
```

3.  Modify the weewx configuration file:

```
[StdRESTful]
    ...
    [[MQTT]]
        server_url = mqtt://username:password@localhost:1883/
        topic = weather
        unit_system = METRIC
```
```
[Engine]
    [[Services]]
        restful_services = ..., user.mqtt.MQTT
```

4.  Restart weewx

```
sudo systemctl stop weewx
sudo systemctl start weewx
```

### Options

_append_units_label_ - Indicates whether to append the units abbreviation to the variable name.  For example, `outTemp` would be called `outTemp_F` and `pressure` would be called `pressure_mbar`.  Note that / is not ok to use within a topic component and ^ is avoided, so e.g. solar radiation will be Wpm2.  Default is `True`.

_unit_system_ - Unit system to which values should be converted before uploading.  If nothing is specified, the units from StdConvert will be used.  Possible values are `US`, `METRIC`, or `METRICWX`.  Default is None.

_binding_ - Indicates whether to bind to LOOP packets ('loop'), archive records ('archive'), or both loop and archive.  Default is 'archive'.

_topic_ - The base MQTT topic under which to publish.  Messages are published to topics formed by concatenating the base topic and a name for the particular thing being published. Default is 'weather'.

_aggregation_ - How the observations should be grouped.  Options are `individual` and/or `aggregate`.  When `individual` is specified, each observation is sent as a separate MQTT message.  When `aggregate` is specified, all observations are sent in a single, JSON-formatted message.  Default is `individual, aggregate`, i.e., send a message for each observation as well as an aggregate message with all observations.

_retain_ - When set to `True`, the MQTT `retain` property is set for each message.  Default is `False`.

_client_id_ - Use this mqtt client ID (optional)

### Topic names

This extension allows renaming (see examples), but the default is to use individual topic names that match the weewx database.  if TOPIC is the base topic, this results in publishing to TOPIC/outTemp_F and TOPIC/dewpoint_F.  (The lack of "out" in dewpoint is not a typo; the non-symmetry with outTemp is how the default database schema is.)

For publishing aggregate data, the topic is TOPIC/loop, and the content is a json dictionary where the keys are what would be used for individual topics.  The dictionary will appear to be in an arbitrary order, but this is ok as dictionaries are not ordered.

Note that the choice of topic names (including unit suffixes) and units used in the contents forms a protocol and the configuration of this extension and of other programs that consume this data must match.


### Home Assistant discovery

This extension can announce its observations to [Home Assistant](https://www.home-assistant.io/)
using [MQTT discovery](https://www.home-assistant.io/integrations/mqtt/#mqtt-discovery),
so the sensors appear automatically without any manual entity configuration.

```
[StdRESTful]
    [[MQTT]]
        ...
        # To send metric/SI units to Home Assistant, convert the published data
        # with the standard unit_system option (US, METRIC, or METRICWX). If this
        # is not set, the station's native units are published and announced.
        unit_system = METRICWX
        [[[ha_discovery]]]
            # Turn on Home Assistant discovery
            enable = true
            # Topic prefix Home Assistant listens on (default: homeassistant)
            discovery_prefix = homeassistant
            # Identifier for this station's node/device (default: weewx)
            node_id = weewx
            # Prefix for the entities' unique ids (default: weewx)
            unique_id_prefix = weewx
            # Observations to leave OUT of Home Assistant discovery, comma-
            # separated. NOTE: this only suppresses the discovery message for
            # these fields -- they are STILL published as normal MQTT data. It
            # does not stop them from being sent. (usUnits and interval never get
            # a discovery message.)
            skip_fields = inTemp, inHumidity
            [[[[device]]]]
                # All values optional; sensible defaults are taken from [Station].
                name = My Weather Station
                manufacturer = WeeWX
                model = Vantage Pro2
                identifiers = weewx
```

Notes:

* Discovery describes the data **exactly as it is published**, including its
  units. Whether values are converted is entirely up to you, via the existing
  `unit_system` option: leave it unset to publish (and announce) the station's
  native units (e.g. `°F`, `inHg`), or set `unit_system = METRICWX` to publish
  and announce metric/SI units (`°C`, `hPa`, `mm`, `m/s`). This works for any
  station regardless of the units it natively reports.
* **`skip_fields` does not stop a field from being published over MQTT.** It only
  suppresses the Home Assistant *discovery* message for that field, so no HA
  entity is auto-created for it. The field's data is still sent on its normal
  topic / in the aggregate packet exactly as before; you can still subscribe to
  it or add it to Home Assistant manually. `usUnits` and `interval` never get a
  discovery message.
* `dateTime` (the time of the observation) is published as a Home Assistant
  `timestamp` entity named **Observation Time** (`weewx_observation_time`). Its
  Unix-epoch value is converted to a datetime in the discovery `value_template`.
* Discovery messages are published **once**, **retained**, just before the first
  data packet is sent. Because they are retained, Home Assistant will pick them
  up even if it (re)starts later, and re-sending them on each weewx restart is
  harmless.
* You can also (re)publish discovery on demand, without waiting for weewx to send
  a packet, using the `weectl rest` command:

  ```
  weectl rest run MQTT --discovery
  ```

  This publishes the discovery messages (using the most recent archive record to
  determine the available observations) and then exits.


### TLS Options

This extension supports the use of encrypted connections to the broker using TLS.  The TLS options will be passed to Paho client tls_set method.  Refer to Paho client documentation for details:

https://eclipse.org/paho/clients/python/docs/

This is how to specify the options in the MQTT extension configuration:
```
[StdRESTful]
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
            #   Options include sslv1, sslv2, sslv23, tls, tlsv1, tlsv12
            #   Default is 'tlsv1'
            #   Not all options are supported by all systems.
            tls_version = tlsv1
            # Allowable encryption ciphers (optional).
            #   To specify multiple cyphers, delimit with commas and enclose
            #   in quotes.
            #ciphers =
```

### Examples

This configuration will use the METRIC unit system for all observations,
whether the database is METRIC, METRICWX, or US:

```
[StdRESTful]
    [[MQTT]]
        unit_system = METRIC
```

This configuration will specialize the units of outTemp and the units, format,
and name of inTemp:

```
[StdRESTful]
    [[MQTT]]
        unit_system = METRICWX
        [[[inputs]]]
            [[[[outTemp]]]]
                units = degree_F             # use F instead of C
            [[[[inTemp]]]]
                units = degree_F             # use F instead of C
                format = %.2f                # use two decimal places of precision
                name = inside_temperature    # use label other than inTemp
            [[[[windSpeed]]]]
                units = knot                 # use knots instead of meter_per_second
```

The observation `outTemp` will be converted to degrees F and published to a topic formed from the configured topic and  `outTemp_F`.  The observation `inTemp` will be converted to degrees F and uploaded as `inside_temperature` with 2 decimal places of precision.  Since the `unit_system` is `METRICWX`, other temperatures (if they exist) will use degrees C.  For example `extraTemp1` will be published as `extraTemp1_C`.  The observation `windSpeed` will be converted to knots and published as `windSpeed_knot`.

### Interaction with offline archive records

When weewx starts after a period of time being off, then for any hardware with a data logger (such as the Davis  Vantage), archive records with timestamps and associated data are retrieved from the weather station and added to the database.  When the mqtt extension is configured, each of these archive records will be processed and result in MQTT messages; this can be viewed as MQTT catching up just as the main database catches up.  For aggregate messages, there will be a timestamp and data within a json object.  While MQTT isn't a database synchronization protocol, an MQTT listener that happens to be online at this time will receive the messages and could be storing them (e.g. in influxdb).

For individual messages, the values are sent disconnected from the timestamps, and this is not clearly useful, but also fairly clearly not harmful.

### How to verify

The default topic is `weather`, so connect to the broker and subscribe to everything in the `weather` topic:

```
weather/#
```

This will list all observations.

For example, if the uploader is configured to send individual observations you should see something like this (_Please note that this list may vary depending on your settings_):

```
weather/loop
weather/cloudbase_meter
weather/outHumidity
weather/pressure_mbar
weather/rain_cm
weather/barometer_mbar
weather/dewpoint_C
weather/rainTotal
weather/ptr
weather/illuminance
weather/heatindex_C
weather/dayRain_cm
weather/outTempBatteryStatus
weather/delay
weather/inDewpoint
weather/altimeter_mbar
weather/windchill_C
weather/appTemp_C
weather/outTemp_C
weather/status
weather/windGust_kph
weather/humidex_C
weather/rain24_cm
weather/rxCheckPercent
weather/hourRain_cm
weather/inTemp_C
weather/windSpeed_kph
weather/usUnits
weather/UV
weather/rainRate_cm_per_hour
weather/dateTime
weather/windDir
weather/inHumidity
weather/radiation_Wpm2
```

### Source repository
https://github.com/matthewwall/weewx-mqtt
