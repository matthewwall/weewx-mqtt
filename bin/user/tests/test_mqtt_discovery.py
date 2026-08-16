#
# Copyright 2024 - Distributed under the terms of the GNU Public License (GPLv3)
#
"""Broker-free tests for the Home Assistant discovery support in user.mqtt.

These tests exercise discovery payload generation, the reuse of the 'unit_system'
option to drive unit conversion, the configurable 'skip_fields' option, and the
once-before-first-packet trigger -- all without needing a live MQTT broker.

Run from the extension root:
    PYTHONPATH=/path/to/weewx/src python bin/user/tests/test_mqtt_discovery.py
"""
import copy
import os
import sys
import unittest

try:
    import queue as Queue
except ImportError:
    import Queue

# Make 'import mqtt' work whether or not the extension is installed as user.mqtt.
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import mqtt  # noqa: E402
from fakes import FakeClient  # noqa: E402

import weewx  # noqa: E402


# A record exactly as a US-units station reports it (weewx keys, usUnits == US).
US_RECORD = {
    'dateTime': 1781339400, 'usUnits': weewx.US, 'interval': 30,
    'outTemp': 61.0, 'inTemp': 78.7, 'outHumidity': 78.0, 'inHumidity': 39.0,
    'barometer': 29.95, 'rain': 0.0, 'rainRate': 0.0,
    'windSpeed': 2.0, 'windGust': 9.0, 'windDir': 90.0,
}

# The same conditions reported by a METRICWX station, to prove discovery reflects
# whatever units are actually in the record (not an assumed US source).
METRICWX_RECORD = {
    'dateTime': 1781339400, 'usUnits': weewx.METRICWX, 'interval': 30,
    'outTemp': 16.1111, 'outHumidity': 78.0, 'barometer': 1014.2,
    'windSpeed': 0.8941, 'windDir': 90.0,
}

HA_DISCOVERY = {
    'enable': True,
    'discovery_prefix': 'homeassistant',
    'node_id': 'weewx',
    'unique_id_prefix': 'weewx',
    'skip_fields': [],
    'device': {'name': 'Test Station', 'manufacturer': 'WeeWX',
               'model': 'Simulator', 'identifiers': 'weewx', 'sw_version': '5.x'},
}


def make_thread(ha=None, **kwargs):
    # Mirror the MQTT service's default aggregation ('individual,aggregate'); the
    # MQTTThread class default on its own is just 'individual'.
    opts = dict(server_url='mqtt://localhost:1883/', topic='weather',
                aggregation='individual,aggregate',
                ha_discovery=copy.deepcopy(HA_DISCOVERY if ha is None else ha))
    opts.update(kwargs)
    return mqtt.MQTTThread(Queue.Queue(), **opts)


class DiscoveryUnitsTest(unittest.TestCase):
    """Discovery must describe the data in whatever units the record carries."""

    def test_native_us_units(self):
        configs = dict(make_thread()._discovery_configs(US_RECORD))
        t = configs['homeassistant/sensor/weewx/outTemp/config']
        self.assertEqual(t['device_class'], 'temperature')
        self.assertEqual(t['unit_of_measurement'], '°F')
        self.assertEqual(t['value_template'], '{{ value_json.outTemp_F }}')
        self.assertEqual(t['state_topic'], 'weather/loop')
        p = configs['homeassistant/sensor/weewx/barometer/config']
        self.assertEqual(p['device_class'], 'atmospheric_pressure')
        self.assertEqual(p['unit_of_measurement'], 'inHg')

    def test_metricwx_units(self):
        configs = dict(make_thread()._discovery_configs(METRICWX_RECORD))
        t = configs['homeassistant/sensor/weewx/outTemp/config']
        self.assertEqual(t['unit_of_measurement'], '°C')
        self.assertEqual(t['value_template'], '{{ value_json.outTemp_C }}')
        p = configs['homeassistant/sensor/weewx/barometer/config']
        self.assertEqual(p['unit_of_measurement'], 'hPa')
        w = configs['homeassistant/sensor/weewx/windSpeed/config']
        self.assertEqual(w['unit_of_measurement'], 'm/s')
        self.assertEqual(w['device_class'], 'wind_speed')

    def test_humidity_device_class(self):
        configs = dict(make_thread()._discovery_configs(US_RECORD))
        p = configs['homeassistant/sensor/weewx/outHumidity/config']
        self.assertEqual(p['device_class'], 'humidity')
        self.assertEqual(p['unit_of_measurement'], '%')

    def test_individual_mode_uses_published_subtopic(self):
        configs = dict(make_thread(aggregation='individual')._discovery_configs(US_RECORD))
        p = configs['homeassistant/sensor/weewx/windSpeed/config']
        self.assertEqual(p['state_topic'], 'weather/windSpeed_mph')
        self.assertNotIn('value_template', p)

    def test_device_block(self):
        configs = dict(make_thread()._discovery_configs(US_RECORD))
        dev = configs['homeassistant/sensor/weewx/outTemp/config']['device']
        self.assertEqual(dev['name'], 'Test Station')
        self.assertEqual(dev['identifiers'], ['weewx'])

    def test_list_valued_device_fields_coerced_to_strings(self):
        # ConfigObj turns "Bronx, New York" into a list; HA needs a string.
        ha = dict(HA_DISCOVERY,
                  device={'name': ['Bronx', 'New York'],
                          'manufacturer': 'WeeWX',
                          'model': ['Vantage', 'Pro2'],
                          'identifiers': 'weewx'})
        configs = dict(make_thread(ha=ha)._discovery_configs(US_RECORD))
        dev = configs['homeassistant/sensor/weewx/outTemp/config']['device']
        self.assertEqual(dev['name'], 'Bronx, New York')
        self.assertEqual(dev['model'], 'Vantage, Pro2')
        self.assertEqual(dev['identifiers'], ['weewx'])
        for key in ('name', 'manufacturer', 'model'):
            self.assertIsInstance(dev[key], str)


class DeviceClassUnitValidationTest(unittest.TestCase):
    """HA rejects an entity whose unit is invalid for its device_class. We keep
    the (correct) unit and drop the device_class instead."""

    def test_us_rainrate_keeps_device_class(self):
        c = dict(make_thread()._discovery_configs(US_RECORD))
        p = c['homeassistant/sensor/weewx/rainRate/config']
        self.assertEqual(p['unit_of_measurement'], 'in/h')
        self.assertEqual(p['device_class'], 'precipitation_intensity')

    def test_metric_rainrate_drops_invalid_device_class(self):
        # weewx METRIC reports rainRate in cm/h, which HA does NOT accept for
        # precipitation_intensity.
        rec = {'dateTime': 1781339400, 'usUnits': weewx.METRIC, 'interval': 5,
               'rainRate': 0.5, 'rain': 0.1}
        c = dict(make_thread()._discovery_configs(rec))
        p = c['homeassistant/sensor/weewx/rainRate/config']
        self.assertEqual(p['unit_of_measurement'], 'cm/h')   # unit kept (the truth)
        self.assertNotIn('device_class', p)                  # invalid pairing dropped
        self.assertEqual(p['state_class'], 'measurement')    # still a measurement
        # rain (cm) is valid for the 'precipitation' device_class, so it stays.
        self.assertEqual(c['homeassistant/sensor/weewx/rain/config']['device_class'],
                         'precipitation')


class NoUnitsLabelTest(unittest.TestCase):
    """With append_units_label=False the topic/key carries no unit suffix, so
    unit_of_measurement is the ONLY way HA learns the unit -- it must still be set,
    and must match the published (suffix-less) field name."""

    def test_uom_set_and_name_has_no_suffix_us(self):
        configs = dict(make_thread(append_units_label=False)._discovery_configs(US_RECORD))
        t = configs['homeassistant/sensor/weewx/outTemp/config']
        self.assertEqual(t['unit_of_measurement'], '°F')   # unit still reported
        self.assertEqual(t['value_template'], '{{ value_json.outTemp }}')  # no _F

    def test_uom_follows_metric(self):
        configs = dict(make_thread(append_units_label=False)._discovery_configs(METRICWX_RECORD))
        t = configs['homeassistant/sensor/weewx/outTemp/config']
        self.assertEqual(t['unit_of_measurement'], '°C')
        self.assertEqual(t['value_template'], '{{ value_json.outTemp }}')

    def test_per_input_unit_override(self):
        # US station, but outTemp forced to degree_C via inputs, label off.
        ha = dict(HA_DISCOVERY)
        t = make_thread(ha=ha, append_units_label=False,
                        inputs={'outTemp': {'unit': 'degree_C'}})
        configs = dict(t._discovery_configs(US_RECORD))
        c = configs['homeassistant/sensor/weewx/outTemp/config']
        self.assertEqual(c['unit_of_measurement'], '°C')
        self.assertEqual(c['value_template'], '{{ value_json.outTemp }}')

    def test_each_observation_is_its_own_entity(self):
        # outTemp and inTemp are distinct entities/topics -- no F/C collision even
        # though neither name carries a unit suffix.
        configs = dict(make_thread(append_units_label=False)._discovery_configs(US_RECORD))
        out = configs['homeassistant/sensor/weewx/outTemp/config']
        inn = configs['homeassistant/sensor/weewx/inTemp/config']
        self.assertNotEqual(out['unique_id'], inn['unique_id'])
        self.assertNotEqual(out['value_template'], inn['value_template'])


class TimestampTest(unittest.TestCase):
    """dateTime becomes a proper, descriptively-named HA timestamp entity."""

    def test_timestamp_semantics_aggregate(self):
        c = dict(make_thread()._discovery_configs(US_RECORD))
        p = c['homeassistant/sensor/weewx/dateTime/config']
        self.assertEqual(p['device_class'], 'timestamp')
        self.assertEqual(p['name'], 'Observation Time')      # not a generic "Date Time"
        self.assertEqual(p['unique_id'], 'weewx_observation_time')
        self.assertEqual(p['object_id'], 'weewx_observation_time')
        # HA forbids state_class / unit on a timestamp entity.
        self.assertNotIn('state_class', p)
        self.assertNotIn('unit_of_measurement', p)
        # Epoch is converted to a datetime for HA.
        self.assertEqual(p['value_template'], '{{ as_datetime(value_json.dateTime | float) }}')

    def test_timestamp_individual_mode(self):
        c = dict(make_thread(aggregation='individual')._discovery_configs(US_RECORD))
        p = c['homeassistant/sensor/weewx/dateTime/config']
        self.assertEqual(p['state_topic'], 'weather/dateTime')
        self.assertEqual(p['value_template'], '{{ as_datetime(value | float) }}')


class SkipFieldsTest(unittest.TestCase):

    def test_mandatory_fields_always_skipped(self):
        configs = dict(make_thread()._discovery_configs(US_RECORD))
        for skip in ('interval', 'usUnits'):
            self.assertNotIn('homeassistant/sensor/weewx/%s/config' % skip, configs)
        # dateTime is NOT skipped: it is published as a timestamp entity.
        self.assertIn('homeassistant/sensor/weewx/dateTime/config', configs)

    def test_admin_skip_fields_excluded(self):
        ha = dict(HA_DISCOVERY, skip_fields=['inTemp', 'inHumidity'])
        configs = dict(make_thread(ha=ha)._discovery_configs(US_RECORD))
        self.assertNotIn('homeassistant/sensor/weewx/inTemp/config', configs)
        self.assertNotIn('homeassistant/sensor/weewx/inHumidity/config', configs)
        # Other observations are still announced.
        self.assertIn('homeassistant/sensor/weewx/outTemp/config', configs)

    def test_skip_fields_union_with_mandatory(self):
        # Even when the admin sets skip_fields, the mandatory ones stay excluded.
        ha = dict(HA_DISCOVERY, skip_fields=['inTemp'])
        t = make_thread(ha=ha)
        self.assertTrue({'usUnits', 'interval', 'inTemp'} <= t.skip_fields)


class UnitSystemConversionTest(unittest.TestCase):
    """Setting unit_system converts the published data; discovery should follow."""

    def _run(self, unit_system):
        t = make_thread(unit_system=unit_system)
        t.mc = FakeClient()
        t.process_record(dict(US_RECORD), None)
        published = dict((tp[0], tp[1]) for tp in t.mc.published)
        return t, published

    def test_no_unit_system_keeps_us(self):
        _t, pub = self._run(None)
        self.assertIn('outTemp_F', pub['weather/loop'])
        cfg = mqtt.json.loads(pub['homeassistant/sensor/weewx/outTemp/config'])
        self.assertEqual(cfg['unit_of_measurement'], '°F')

    def test_metricwx_converts(self):
        _t, pub = self._run(weewx.METRICWX)
        # filter_data renamed/relabeled the field to metric, and discovery agrees.
        self.assertIn('outTemp_C', pub['weather/loop'])
        cfg = mqtt.json.loads(pub['homeassistant/sensor/weewx/outTemp/config'])
        self.assertEqual(cfg['unit_of_measurement'], '°C')
        self.assertEqual(cfg['value_template'], '{{ value_json.outTemp_C }}')


class MissingUsUnitsTest(unittest.TestCase):
    """A record may or may not carry usUnits; without it we cannot describe the
    data, and discovery must skip gracefully and retry later (never get stuck)."""

    NO_UNITS = {'dateTime': 1781339400, 'interval': 30,
                'outTemp': 61.0, 'barometer': 29.95}

    def test_configs_empty_without_usunits(self):
        # No crash, just nothing to announce.
        self.assertEqual(make_thread()._discovery_configs(self.NO_UNITS), [])

    def test_publish_skips_and_does_not_latch(self):
        t = make_thread()
        t.mc = FakeClient()
        t.publish_ha_discovery(self.NO_UNITS)
        self.assertEqual(t.mc.published, [], "nothing should be published")
        self.assertFalse(t._discovery_sent,
                         "must not latch: a later record should still trigger it")

    def test_retries_on_later_valid_record(self):
        t = make_thread()
        t.mc = FakeClient()
        # First attempt: no usUnits -> skipped, not latched.
        t.publish_ha_discovery(self.NO_UNITS)
        self.assertFalse(t._discovery_sent)
        # Later record carries usUnits -> discovery is published and latched.
        t.publish_ha_discovery(dict(US_RECORD))
        disc = [tp for tp in t.mc.published if tp[0].startswith('homeassistant/')]
        self.assertTrue(disc)
        self.assertTrue(t._discovery_sent)

    def test_process_record_skips_without_crashing(self):
        # The whole data path (augment/convert/label) needs usUnits; a record
        # without it must be skipped, not raise (which would kill the thread).
        t = make_thread()
        t.mc = FakeClient()
        t.process_record(dict(self.NO_UNITS), None)  # must not raise
        self.assertEqual(t.mc.published, [])
        self.assertFalse(t._discovery_sent)
        # A subsequent valid record is still processed normally.
        t.process_record(dict(US_RECORD), None)
        self.assertTrue(any(tp[0].startswith('weather/') for tp in t.mc.published))
        self.assertTrue(t._discovery_sent)


class TriggerTest(unittest.TestCase):

    def test_discovery_published_once_before_data(self):
        t = make_thread()
        t.mc = FakeClient()
        t.process_record(dict(US_RECORD), None)

        disc = [tp for tp in t.mc.published if tp[0].startswith('homeassistant/')]
        data = [tp for tp in t.mc.published if tp[0].startswith('weather/')]
        self.assertTrue(disc, "expected discovery configs on the first record")
        self.assertTrue(all(tp[2] for tp in disc), "discovery must be retained")
        self.assertTrue(data, "expected data to be published too")
        self.assertTrue(t._discovery_sent)

        # Second record in the same run must NOT re-publish discovery.
        t.mc.published = []
        t.process_record(dict(US_RECORD), None)
        disc2 = [tp for tp in t.mc.published if tp[0].startswith('homeassistant/')]
        self.assertEqual(disc2, [], "discovery should only be sent once")

    def test_disabled_discovery_publishes_no_configs(self):
        t = make_thread(ha={'enable': False})
        t.mc = FakeClient()
        t.process_record(dict(US_RECORD), None)
        disc = [tp for tp in t.mc.published if tp[0].startswith('homeassistant/')]
        self.assertEqual(disc, [])


if __name__ == '__main__':
    unittest.main()
