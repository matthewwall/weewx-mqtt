mqtt - weewx extension that sends data to an MQTT broker
Copyright 2014-2020 Matthew Wall
Distributed under the terms of the GNU Public License (GPLv3)

Installation instructions:

0) install the MQTT python bindings

sudo pip install paho-mqtt

1) run the installer:

wee_extension --install weewx-mqtt.tgz

2) modify weewx.conf:

[StdRESTful]
    [[MQTT]]
        server_url = mqtt://username:password@example.com:1883

3) restart weewx

sudo /etc/init.d/weewx stop
sudo /etc/init.d/weewx start
