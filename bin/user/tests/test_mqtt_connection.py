#
# Copyright 2024 - Distributed under the terms of the GNU Public License (GPLv3)
#
"""Broker-free tests for connection handling in user.mqtt.

These cover the failure that motivated the work: once the connection to the
broker was lost, the cached client was never checked or rebuilt, so every
subsequent publish failed with "The client is not currently connected" until
weewx was restarted -- and the failure was only logged, never reported, so weewx
logged a successful post on top of the lost record.

Run from the extension root:
    PYTHONPATH=/path/to/weewx/src python bin/user/tests/test_mqtt_connection.py
"""
import os
import sys
import time
import unittest

try:
    import queue as Queue
except ImportError:
    import Queue

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import mqtt  # noqa: E402
from fakes import FakeClient, FakePahoClient  # noqa: E402

import weewx  # noqa: E402
import weewx.restx  # noqa: E402


RECORD = {
    'dateTime': 1781339400, 'usUnits': weewx.US, 'interval': 30,
    'outTemp': 61.0, 'outHumidity': 78.0, 'barometer': 29.95,
}

NO_CONN = mqtt.mqtt.MQTT_ERR_NO_CONN


def make_thread(**kwargs):
    opts = dict(server_url='mqtt://localhost:1883/', topic='weather',
                aggregation='individual,aggregate',
                # Keep the tests fast: no waiting between retries, and short
                # timeouts on the paths that are supposed to time out.
                retry_wait=0, connect_timeout=0.05, publish_timeout=0.05)
    opts.update(kwargs)
    return mqtt.MQTTThread(Queue.Queue(), **opts)


class EnsureClientTest(unittest.TestCase):
    """_ensure_client must hand back a *connected* client, or build a new one.

    The original code only checked that the client object existed, which is why
    a dead connection was never noticed.
    """

    def setUp(self):
        # A live client recovers well inside this; shrink it so the tests that
        # exercise a dead client do not sit through the real grace period.
        self.grace = mqtt.RECONNECT_GRACE
        mqtt.RECONNECT_GRACE = 0.01

    def tearDown(self):
        mqtt.RECONNECT_GRACE = self.grace

    def test_connected_client_is_reused(self):
        t = make_thread()
        t.mc = FakeClient(connected=True)
        t._new_client = lambda: self.fail('should not have rebuilt the client')
        self.assertIs(t._ensure_client(), t.mc)

    def test_disconnected_client_is_torn_down_and_rebuilt(self):
        t = make_thread()
        dead = FakeClient(connected=False)
        t.mc = dead
        fresh = FakeClient(connected=True)

        def rebuild():
            t.mc = fresh
            return fresh
        t._new_client = rebuild

        self.assertIs(t._ensure_client(), fresh)
        self.assertTrue(dead.disconnected, 'dead client should be disconnected')
        self.assertTrue(dead.loop_stopped, 'dead client loop should be stopped')

    def test_client_reconnecting_on_its_own_is_kept(self):
        # If the client's own reconnect lands within the grace period there is
        # no reason to throw it away.
        t = make_thread()

        class Reconnecting(FakeClient):
            """Not connected when first asked, connected a moment later."""
            def is_connected(inner):
                was = inner.connected
                inner.connected = True
                t._connected.set()
                return was

        t.mc = Reconnecting(connected=False)
        t._new_client = lambda: self.fail('should not have rebuilt the client')
        self.assertIs(t._ensure_client(), t.mc)

    def test_client_that_only_claims_to_be_connected_is_rebuilt(self):
        # A stale connected flag must not get a dead client past the check;
        # the client's own view of the socket is what counts.
        t = make_thread()
        dead = FakeClient(connected=False)
        t.mc = dead
        t._connected.set()
        fresh = FakeClient()
        t._new_client = lambda: fresh
        self.assertIs(t._ensure_client(), fresh)
        self.assertTrue(dead.disconnected)

    def test_missing_client_is_built(self):
        t = make_thread()
        fresh = FakeClient()
        t._new_client = lambda: fresh
        self.assertIs(t._ensure_client(), fresh)


class ConnectTest(unittest.TestCase):
    """_new_client must wait for the broker to accept the connection.

    connect() only opens the socket and sends CONNECT; the original code cached
    the client without waiting for the CONNACK, so a refused connection looked
    identical to a good one until a publish failed much later.
    """

    def setUp(self):
        self.real_client = mqtt.mqtt.Client
        mqtt.mqtt.Client = FakePahoClient
        FakePahoClient.reset()

    def tearDown(self):
        mqtt.mqtt.Client = self.real_client
        FakePahoClient.reset()

    def test_successful_connect(self):
        t = make_thread()
        mc = t._new_client()
        self.assertIs(t.mc, mc)
        self.assertTrue(t._connected.is_set())
        self.assertEqual(mc.connect_args, ('localhost', 1883, t.keepalive))
        self.assertEqual(mc.reconnect_delays,
                         (t.reconnect_min_delay, t.reconnect_max_delay))

    def test_no_connack_times_out_and_tears_down(self):
        FakePahoClient.reset(connack_rc=None)   # broker never answers
        t = make_thread()
        with self.assertRaises(weewx.restx.FailedPost) as cm:
            t._new_client()
        self.assertIn('timed out', str(cm.exception))
        self.assertIsNone(t.mc, 'a client that never connected must not be kept')
        self.assertTrue(FakePahoClient.instances[0].loop_stopped)

    def test_refused_connection_reports_the_reason(self):
        FakePahoClient.reset(connack_rc=5)      # not authorised
        t = make_thread()
        with self.assertRaises(weewx.restx.FailedPost) as cm:
            t._new_client()
        self.assertIn('refused', str(cm.exception))
        self.assertIn('authoris', str(cm.exception).lower())
        self.assertIsNone(t.mc)

    def test_refused_connection_fails_immediately(self):
        # A refusal is final, so there is nothing to wait for. Waiting out
        # connect_timeout would only let the client retry in the background and
        # fill the log with the same refusal several times over.
        FakePahoClient.reset(connack_rc=5)
        t = make_thread(connect_timeout=30)
        started = time.time()
        with self.assertRaises(weewx.restx.FailedPost):
            t._new_client()
        self.assertLess(time.time() - started, 1.0)

    def test_credentials_and_port_from_url(self):
        t = make_thread(server_url='mqtt://bob:secret@broker.example:1884/')
        mc = t._new_client()
        self.assertEqual(mc.credentials, ('bob', 'secret'))
        self.assertEqual(mc.connect_args[:2], ('broker.example', 1884))

    def test_tls_defaults_to_port_8883(self):
        t = make_thread(server_url='mqtts://broker.example/',
                        tls={'tls_version': 'tlsv12'})
        mc = t._new_client()
        self.assertEqual(mc.connect_args[1], 8883)
        self.assertIsNotNone(mc.tls_args)

    def test_password_is_not_leaked_in_errors(self):
        FakePahoClient.reset(connack_rc=None)
        t = make_thread(server_url='mqtt://bob:hunter2@broker.example:1883/')
        with self.assertRaises(weewx.restx.FailedPost) as cm:
            t._new_client()
        self.assertNotIn('hunter2', str(cm.exception))


class CallbackTest(unittest.TestCase):
    """The connect/disconnect callbacks are the only warning of an outage."""

    def test_disconnect_clears_connected_state(self):
        t = make_thread()
        t._connected.set()
        t._on_disconnect(NO_CONN)
        self.assertFalse(t._connected.is_set())

    def test_connect_sets_state_and_reannounces_discovery(self):
        # A broker that came back without its retained store would leave Home
        # Assistant with no discovery configs, so a reconnect re-announces them.
        t = make_thread()
        t._discovery_sent = True
        t._on_connect(0)
        self.assertTrue(t._connected.is_set())
        self.assertFalse(t._discovery_sent)

    def test_refused_connect_leaves_state_clear(self):
        t = make_thread()
        t._on_connect(5)
        self.assertFalse(t._connected.is_set())
        self.assertEqual(t._connect_rc, 5)


class ReasonCodeTest(unittest.TestCase):
    """paho 1.x reports result codes as ints, paho 2.x as ReasonCode objects.
    error_string() renders the latter as "Unknown error.", which would hide the
    one detail that matters most in a disconnect log."""

    def test_int_result_code(self):
        self.assertEqual(mqtt._rc_string(mqtt.mqtt.MQTT_ERR_NO_CONN),
                         'The client is not currently connected.')

    @unittest.skipUnless(mqtt.PAHO2, 'requires paho-mqtt 2.x')
    def test_reason_code_object(self):
        from paho.mqtt.reasoncodes import ReasonCode
        # 142 is "Session taken over": what a broker sends when a second client
        # connects with the same client_id and evicts this one.
        rc = ReasonCode(mqtt.mqtt.DISCONNECT >> 4, identifier=142)
        self.assertEqual(mqtt._rc_string(rc), 'Session taken over')


class RetryTest(unittest.TestCase):
    """A failed publish must be retried on a fresh connection, and a record that
    cannot be delivered at all must be reported to weewx as a failure."""

    def _clients(self, t, *clients):
        """Hand out the given clients in order as the thread rebuilds."""
        queue = list(clients)

        def build():
            t.mc = queue.pop(0)
            return t.mc
        t._new_client = build
        return queue

    def test_retry_succeeds_on_a_fresh_connection(self):
        t = make_thread()
        dead = FakeClient(publish_rc=NO_CONN)
        good = FakeClient()
        remaining = self._clients(t, dead, good)

        t.process_record(dict(RECORD), None)    # must not raise

        self.assertEqual(remaining, [], 'both clients should have been used')
        self.assertTrue(good.published, 'the record should land on the new client')
        self.assertTrue(dead.disconnected,
                        'the failed client must be discarded, not reused')

    def test_failure_is_reported_after_max_tries(self):
        # The original code only logged this, so weewx went on to log a
        # successful post for a record that was never delivered.
        t = make_thread(max_tries=3)
        clients = [FakeClient(publish_rc=NO_CONN) for _ in range(3)]
        remaining = self._clients(t, *clients)

        with self.assertRaises(weewx.restx.FailedPost) as cm:
            t.process_record(dict(RECORD), None)
        self.assertIn('3 attempts', str(cm.exception))
        self.assertEqual(remaining, [], 'should have tried max_tries times')
        self.assertIsNone(t.mc)

    def test_failed_attempt_publishes_no_duplicates(self):
        # At qos >= 1 a client holds a message published while disconnected and
        # redelivers it on reconnect. Discarding the client is what keeps the
        # retry from delivering every observation twice.
        t = make_thread()
        dead = FakeClient(publish_rc=NO_CONN)
        good = FakeClient()
        self._clients(t, dead, good)

        t.process_record(dict(RECORD), None)

        self.assertTrue(dead.disconnected)
        topics = [p[0] for p in good.published]
        self.assertEqual(len(topics), len(set(topics)), 'no topic twice')

    def test_unconfirmed_delivery_is_a_failure(self):
        # publish() succeeding only means the message was handed to the client;
        # at qos >= 1 the broker's acknowledgement is the real evidence.
        t = make_thread(max_tries=1)
        self._clients(t, FakeClient(confirmed=False))
        with self.assertRaises(weewx.restx.FailedPost) as cm:
            t.process_record(dict(RECORD), None)
        self.assertIn('confirm', str(cm.exception))

    def test_connection_failure_is_retried(self):
        t = make_thread(max_tries=2)
        attempts = []

        def build():
            attempts.append(1)
            if len(attempts) < 2:
                raise weewx.restx.FailedPost('broker down')
            t.mc = FakeClient()
            return t.mc
        t._new_client = build

        t.process_record(dict(RECORD), None)    # must not raise
        self.assertEqual(len(attempts), 2)


class QosTest(unittest.TestCase):
    """The configured qos applies to every message. Individual observations
    previously ignored it and were always published at qos 0."""

    def test_individual_publishes_honor_configured_qos(self):
        t = make_thread(qos=2)
        c = FakeClient()
        t._new_client = lambda: c
        t.mc = c
        t.process_record(dict(RECORD), None)

        individual = [p for p in c.published if not p[0].endswith('/loop')]
        self.assertTrue(individual)
        for topic, _payload, _retain, qos in c.published:
            self.assertEqual(qos, 2, 'wrong qos on %s' % topic)

    def test_default_qos_is_at_least_once(self):
        # At qos 0 anything published while the connection is down is silently
        # dropped, which loses the record outright.
        self.assertEqual(make_thread().qos, 1)

    def test_qos_zero_skips_delivery_confirmation(self):
        # Nothing is acknowledged at qos 0, so there is nothing to wait for.
        t = make_thread(qos=0)
        c = FakeClient(confirmed=False)
        t._new_client = lambda: c
        t.mc = c
        t.process_record(dict(RECORD), None)    # must not raise
        self.assertTrue(c.infos)
        self.assertFalse(any(i.waited for i in c.infos))


class SkipUploadTest(unittest.TestCase):

    def test_skip_upload_touches_no_connection(self):
        t = make_thread(skip_upload=True)
        t._new_client = lambda: self.fail('should not have connected')
        t.process_record(dict(RECORD), None)


if __name__ == '__main__':
    unittest.main()
