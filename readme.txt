mqtt - weewx extension that sends data to an MQTT server
Copyright 2014 Matthew Wall

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
