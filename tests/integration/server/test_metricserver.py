from predictionserver.servermixins.metricserver import MetricServer, MetricType, MetricGranularity
from predictionserver.set_config import MICRO_TEST_CONFIG
from predictionserver.futureconventions.activityconventions import Activity, ActivityContext
from predictionserver.futureconventions.memoconventions import Memo, MemoCategory, MemoGranularity
import pytest
import time

BABLOH_CATTLE = MICRO_TEST_CONFIG['BABLOH_CATTLE']

METRICS = {MetricType.latest: time.time(),
           MetricType.earliest: time.time(),
           MetricType.count: 18,
           MetricType.rating: 75,
           MetricType.budget: 54.1,
           MetricType.volume: 10000
           }


def test_init():
    server = MetricServer()
    with pytest.raises(RuntimeError):
        server.obscurity()
    server.connect(**MICRO_TEST_CONFIG)
    obs = server.obscurity()


def test_write_key_private_owner_metric():
    server = MetricServer()
    server.connect(**MICRO_TEST_CONFIG)

    for metric, value in METRICS.items():
        assert server.set_metric(metric=metric, granularity=MetricGranularity.write_key,
                                 value=value, write_key=BABLOH_CATTLE, verbose=False)
        value_back = server.get_metric(metric=metric, granularity=MetricGranularity.write_key,
                                       write_key=BABLOH_CATTLE)
        assert abs(value_back-value) < 1e-6
        value_implicit = server.get_metric(metric=metric, write_key=BABLOH_CATTLE)
        assert abs(value_implicit-value) < 1e-6

        # SPECIAL CASES
        owner_value = server.get_private_owner_metric(
            metric=metric, write_key=BABLOH_CATTLE)
        assert abs(owner_value - value) < 1e-6

        # Get them all
        mtrs = server.get_metrics(metric=metric, granularity=MetricGranularity.write_key)
        assert mtrs[BABLOH_CATTLE] == value

        # INCREMENT
        server.incr_metric(metric=metric, granularity=MetricGranularity.write_key,
                           amount=1.0, write_key=BABLOH_CATTLE)
        value_updated = server.get_metric(metric=metric, write_key=BABLOH_CATTLE)
        assert abs(value_updated-(value+1.0)) < 1e-6


def test_write_key_public_owner_metric():
    server = MetricServer()
    server.connect(**MICRO_TEST_CONFIG)

    code = server.shash(BABLOH_CATTLE)
    for metric, value in METRICS.items():
        assert server.set_metric(metric=metric, granularity=MetricGranularity.code,
                                 value=value, code=code, verbose=False)
        value_back = server.get_metric(metric=metric, granularity=MetricGranularity.code,
                                       code=code)
        assert abs(value_back-value) < 1e-6
        value_implicit = server.get_metric(metric=metric, code=code)
        assert abs(value_implicit-value) < 1e-6

        # SPECIAL CASES
        owner_value = server.get_public_owner_metric(metric=metric, code=code)
        assert abs(owner_value - value) < 1e-6

        # Get them all
        mtrs = server.get_metrics(metric=metric, granularity=MetricGranularity.code)
        assert mtrs[code] == value

        # INCREMENT
        server.incr_metric(
            metric=metric, granularity=MetricGranularity.code, amount=1.0, code=code)
        value_updated = server.get_metric(metric=metric, code=code)
        assert abs(value_updated-(value+1.0)) < 1e-6


def test_stream():
    server = MetricServer()
    server.connect(**MICRO_TEST_CONFIG)
    name = 'not_real_stream'
    for metric, value in METRICS.items():
        assert server.set_metric(metric=metric, granularity=MetricGranularity.name,
                                 value=value, name=name)
        value_back = server.get_metric(metric=metric, granularity=MetricGranularity.name,
                                       name=name)
        assert abs(value_back - value) < 1e-6
        value_implicit = server.get_metric(
            metric=metric, name=name, granularity=MetricGranularity.name)
        assert value_implicit == value
        special_value = server.get_stream_metric(name=name, metric=metric)
        assert abs(special_value - value) < 1e-6

        # SPECIAL CASES
        if metric == MetricType.budget:
            special_value = server.get_stream_budget(name=name)
            assert abs(special_value - value) < 1e-6
            budgets = server.get_stream_budgets()
            assert budgets[name] == value
            assert server.get_stream_budget(name=name) == value

        elif metric == MetricType.volume:
            special_value = server.get_stream_volume(name=name)
            assert abs(special_value - value) < 1e-6

            volumes = server.get_stream_volumes()
            assert volumes[name] == value

        # INCREMENT
        server.incr_metric(
            metric=metric, granularity=MetricGranularity.name, amount=1.0, name=name)
        value_updated = server.get_metric(metric=metric, write_key=BABLOH_CATTLE)
        assert abs(value_updated - (value + 1.0)) < 1e-6


def test_horizon():
    server = MetricServer()
    server.connect(**MICRO_TEST_CONFIG)
    name = 'not_real_stream_either'
    delay = 310
    for metric, value in METRICS.items():
        assert server.set_metric(metric=metric, granularity=MetricGranularity.name_and_delay,
                                 value=value, name=name, delay=delay)
        value_back = server.get_metric(metric=metric, granularity=MetricGranularity.name_and_delay,
                                       name=name, delay=delay)
        assert abs(value_back - value) < 1e-6
        value_implicit = server.get_metric(metric=metric, name=name, delay=delay,
                                           granularity=MetricGranularity.name_and_delay)
        assert value_implicit == value
        special_value = server.get_horizon_metric(name=name, delay=delay, metric=metric)
        assert abs(special_value - value) < 1e-6

        # SPECIAL CASES
        if metric == MetricType.budget:
            special_value = server.get_horizon_budget(name=name, delay=delay)
            assert abs(special_value - value) < 1e-6
            budgets = server.get_horizon_budgets()
            hrz = MetricGranularity.name_and_delay.instance_name(name=name, delay=delay)
            assert budgets[hrz] == value
            assert server.get_horizon_budget(name=name, delay=delay) == value

        # INCREMENT
        server.incr_metric(metric=metric, granularity=MetricGranularity.name_and_delay, amount=1.0,
                           name=name, delay=delay)
        value_updated = server.get_metric(metric=metric, name=name, delay=delay)
        assert abs(value_updated - (value + 1.0)) < 1e-6
