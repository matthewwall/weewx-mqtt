mqtt - weewx extension that sends data to an MQTT broker
Copyright 2014-2026 Matthew Wall
Distributed under the terms of the GNU Public License (GPLv3)

===============================================================================
Pre-Requisites

This extension uses the paho-mqtt python module.

For debian systems:

  sudo apt install python3-paho-mqtt

For pip installs:

  sudo pip3 install paho-mqtt


If running python3 on Debian based system,
and weewx is not running in a separate python environment:

  sudo apt-get install python3-paho-mqtt

===============================================================================
Installation instructions:

1) install the driver

weectl extension install https://github.com/matthewwall/weewx-mqtt/archive/master.zip

2) modify weewx.conf to include the MQTT server URL:

[StdRESTful]
    [[MQTT]]
        server_url = mqtt://username:password@example.com:1883

3) restart weewx

sudo systemctl restart weewx


===============================================================================
Options

For configuration options and details, see the comments in mqtt.py
