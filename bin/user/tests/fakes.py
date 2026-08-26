#
# Copyright 2024 - Distributed under the terms of the GNU Public License (GPLv3)
#
"""Broker-free stand-ins for a paho MQTT client, shared by the test modules.

FakeClient replaces an already-connected client (assign it to MQTTThread.mc).
FakePahoClient replaces the paho Client *class*, so the connection setup in
MQTTThread._new_client can be exercised without a broker.
"""
import os
import sys

# Make 'import mqtt' work whether or not the extension is installed as user.mqtt.
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import mqtt  # noqa: E402


class FakeMessageInfo:
    """Stand-in for paho's MQTTMessageInfo.

    Mirrors the real class where MQTTThread depends on it: wait_for_publish
    raises RuntimeError rather than returning when the message can no longer be
    delivered, and returns normally (leaving is_published() False) on timeout.
    """

    def __init__(self, rc=None, published=True):
        self.rc = mqtt.mqtt.MQTT_ERR_SUCCESS if rc is None else rc
        self.mid = 1
        self._published = published
        self.waited = False

    def wait_for_publish(self, timeout=None):
        self.waited = True
        if self.rc > 0:
            raise RuntimeError('Message publish failed: %s'
                               % mqtt.mqtt.error_string(self.rc))

    def is_published(self):
        return self._published


class FakeClient:
    """A connected client that records what was published.

    publish_rc -- error code publish() should return instead of success
    confirmed  -- whether the broker acknowledges published messages
    connected  -- what is_connected() reports
    """

    def __init__(self, publish_rc=None, confirmed=True, connected=True):
        self.published = []
        self.infos = []
        self.publish_rc = publish_rc
        self.confirmed = confirmed
        self.connected = connected
        self.disconnected = False
        self.loop_stopped = False

    def is_connected(self):
        return self.connected

    def publish(self, topic, payload=None, retain=False, qos=0):
        self.published.append((topic, payload, retain, qos))
        info = FakeMessageInfo(self.publish_rc, self.confirmed)
        self.infos.append(info)
        return info

    def disconnect(self):
        self.disconnected = True
        self.connected = False

    def loop_stop(self):
        self.loop_stopped = True


class FakePahoClient(FakeClient):
    """Stand-in for the paho Client class itself.

    Patch it over mqtt.mqtt.Client so MQTTThread._new_client runs its real
    callback wiring. Class attributes control what the "broker" does:

    connack_rc -- reason code delivered to on_connect; None means no CONNACK
                  ever arrives, which is how a connect timeout is simulated
    """
    connack_rc = 0
    instances = []

    def __init__(self, *args, **kwargs):
        super(FakePahoClient, self).__init__(connected=False)
        self.client_id = kwargs.get('client_id')
        self.connect_args = None
        self.reconnect_delays = None
        self.credentials = None
        self.tls_args = None
        self.on_connect = None
        self.on_disconnect = None
        FakePahoClient.instances.append(self)

    @classmethod
    def reset(cls, connack_rc=0):
        cls.connack_rc = connack_rc
        cls.instances = []

    def reconnect_delay_set(self, min_delay=None, max_delay=None):
        self.reconnect_delays = (min_delay, max_delay)

    def username_pw_set(self, username, password=None):
        self.credentials = (username, password)

    def tls_set(self, **kwargs):
        self.tls_args = kwargs

    def connect(self, host, port, keepalive=60):
        self.connect_args = (host, port, keepalive)

    def loop_start(self):
        # A real client receives the CONNACK on its network thread, after
        # connect() has returned; fire the callback from here to match.
        if self.connack_rc is None:
            return
        self.connected = (self.connack_rc == 0)
        self.fire_connect(self.connack_rc)

    def fire_connect(self, rc):
        if mqtt.PAHO2:
            self.on_connect(self, None, {}, rc, None)
        else:
            self.on_connect(self, None, {}, rc)

    def fire_disconnect(self, rc):
        self.connected = False
        if mqtt.PAHO2:
            self.on_disconnect(self, None, {}, rc, None)
        else:
            self.on_disconnect(self, None, rc)
