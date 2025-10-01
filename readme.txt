mqtt - weewx extension that sends data to an MQTT broker
Copyright 2014-2025 Matthew Wall
Distributed under the terms of the GNU Public License (GPLv3)

===============================================================================
Pre-Requisites

Install the MQTT python bindings:

  sudo apt install python3-paho-mqtt

If you are using a python virtual environment, use pip:

  pip install paho-mqtt

===============================================================================
Installation instructions

1) run the installer:

weectl extension install https://github.com/matthewwall/weewx-mqtt

2) enter MQTT broker URL in the WeeWX configuration file:

[StdRESTful]
    [[MQTT]]
        server_url = mqtt://username:password@example.com:1883

3) restart weewx

sudo systemctl restart weewx


===============================================================================
Configuration options

Minimal configuration:
```
[StdRestful]
    [[MQTT]]
        server_url = mqtt://username:password@localhost:1883/
        topic = weather
        unit_system = METRIC
```

Other MQTT options can be specified:
```
[StdRestful]
    [[MQTT]]
        ...
        qos = 1        # options are 0, 1, 2
        retain = true  # options are true or false
```

The observations can be sent individually, or in an aggregated packet:
```
[StdRestful]
    [[MQTT]]
        ...
        aggregation = individual, aggregate # individual, aggregate, or both
```

Bind to loop packets or archive records:
```
[StdRestful]
    [[MQTT]]
        ...
        binding = loop # options are loop or archive
```

Use the inputs map to customize name, format, or unit for any observation.
Note that starting with v0.24, option 'units' was renamed to 'unit', although
either will be accepted.
```
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
```

To change the data binding:
```
[StdRestful]
    [[MQTT]]
        ...
        data_binding = wx_binding # or any other valid data binding
```

Use TLS to encrypt connection to broker.  The TLS options will be passed to
Paho client tls_set method.  Refer to Paho client documentation for details:
```
  https://eclipse.org/paho/clients/python/docs/
```
```
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
```
