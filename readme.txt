mqtt - weewx extension that sends data to an MQTT broker
Copyright 2014-2020 Matthew Wall
Distributed under the terms of the GNU Public License (GPLv3)

===============================================================================
Pre-Requisites

Install the MQTT python bindings

For python3:

  sudo pip3 install paho-mqtt

For python2:

  sudo pip install paho-mqtt

===============================================================================
Installation instructions:

1) download

wget -O weewx-mqtt.zip https://github.com/matthewwall/weewx-mqtt/archive/master.zip

2) run the installer:

wee_extension --install weewx-mqtt.zip

3) modify weewx.conf:

[StdRESTful]
    [[MQTT]]
        server_url = mqtt://username:password@example.com:1883

4) restart weewx

sudo /etc/init.d/weewx stop
sudo /etc/init.d/weewx start


===============================================================================
Options

For configuration options and details, see the comments in mqtt.py
