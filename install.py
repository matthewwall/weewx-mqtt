# installer for MQTT
# Copyright 2014-2020 Matthew Wall
# Distributed under the terms of the GNU Public License (GPLv3)

from weecfg.extension import ExtensionInstaller

def loader():
    return MQTTInstaller()

class MQTTInstaller(ExtensionInstaller):
    def __init__(self):
        super(MQTTInstaller, self).__init__(
            version="0.25",
            name='mqtt',
            description='Upload weather data to MQTT server.',
            author="Matthew Wall",
            author_email="mwall@users.sourceforge.net",
            restful_services='user.mqtt.MQTT',
            config={
                'StdRESTful': {
                    'MQTT': {
                        'server_url': 'INSERT_SERVER_URL_HERE',
                        # Home Assistant MQTT discovery. Set enable = true to have
                        # weewx announce its sensors to Home Assistant. Discovery
                        # reports whatever units are published; to send metric/SI
                        # units to HA, set 'unit_system = METRICWX' above.
                        # 'skip_fields' only suppresses the discovery message for
                        # the listed fields -- those fields are STILL published as
                        # normal MQTT data; it does not stop them being sent.
                        'ha_discovery': {
                            'enable': 'false',
                            'discovery_prefix': 'homeassistant',
                            'node_id': 'weewx',
                            'unique_id_prefix': 'weewx',
                            'skip_fields': '',
                            'device': {
                                'manufacturer': 'WeeWX',
                            }}}}},
            files=[('bin/user', ['bin/user/mqtt.py'])]
            )
