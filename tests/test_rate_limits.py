import pytest
from datetime import datetime, timedelta

import yf_parqed.primary_class as primary_module
from yf_parqed.primary_class import YFParqed


def install_fake_datetime(monkeypatch, values):
    real_datetime = primary_module.datetime

    class FakeDatetime(real_datetime):
        queue = list(values)

        @classmethod
        def now(cls):
            if not cls.queue:
                raise RuntimeError("No queued times left for FakeDatetime.now().")
            return cls.queue.pop(0)

        @classmethod
        def set_queue(cls, new_values):
            cls.queue = list(new_values)

    FakeDatetime.strptime = real_datetime.strptime
    monkeypatch.setattr(primary_module, "datetime", FakeDatetime)
    return FakeDatetime


@pytest.fixture
def instance(tmp_path):
    return YFParqed(my_path=tmp_path, my_intervals=["1d"])


def test_set_limiter_configures_internal_state(instance):
    instance.set_limiter(max_requests=5, duration=10)
    assert instance.max_requests == 5
    assert instance.duration == 10

    instance.set_limiter(max_requests=1, duration=4)
    assert instance.max_requests == 1
    assert instance.duration == 4


def test_enforce_limits_initial_call_adds_timestamp(instance, monkeypatch):
    first_time = datetime(2024, 1, 1, 12, 0, 0)
    install_fake_datetime(monkeypatch, [first_time])

    sleep_calls: list[float] = []
    monkeypatch.setattr(
        primary_module.time, "sleep", lambda seconds: sleep_calls.append(seconds)
    )

    instance.call_list = []
    instance.set_limiter(max_requests=3, duration=6)

    instance.enforce_limits()

    assert instance.call_list == [first_time]
    assert sleep_calls == []


def test_enforce_limits_waits_before_retry(instance, monkeypatch):
    base_time = datetime(2024, 1, 1, 12, 0, 0)
    fake_datetime = install_fake_datetime(
        monkeypatch,
        [
            base_time + timedelta(seconds=0.2),
            base_time + timedelta(seconds=1.5),
        ],
    )

    sleep_calls: list[float] = []

    def track_sleep(seconds: float):
        sleep_calls.append(seconds)

    monkeypatch.setattr(primary_module.time, "sleep", track_sleep)

    instance.set_limiter(max_requests=2, duration=2)  # sleepytime = 1 second
    instance.call_list = [base_time]

    instance.enforce_limits()

    assert pytest.approx(sleep_calls[0], rel=1e-6) == 0.8
    assert len(instance.call_list) == 2
    assert instance.call_list[-1] == base_time + timedelta(seconds=1.5)
    assert fake_datetime.queue == []


def test_enforce_limits_trims_old_entries(instance, monkeypatch):
    base_time = datetime(2024, 1, 1, 12, 0, 0)
    install_fake_datetime(monkeypatch, [base_time + timedelta(seconds=4)])

    sleep_calls: list[float] = []
    monkeypatch.setattr(
        primary_module.time, "sleep", lambda seconds: sleep_calls.append(seconds)
    )

    instance.set_limiter(max_requests=2, duration=2)
    instance.call_list = [
        base_time,
        base_time + timedelta(seconds=2),
    ]

    instance.enforce_limits()

    expected = [
        base_time + timedelta(seconds=2),
        base_time + timedelta(seconds=4),
    ]
    assert instance.call_list == expected
    assert sleep_calls == []


def test_enforce_limits_handles_bursty_sequence(instance, monkeypatch):
    base_time = datetime(2024, 1, 1, 12, 0, 0)
    fake_datetime = install_fake_datetime(monkeypatch, [base_time])

    sleep_calls: list[float] = []

    def record_sleep(seconds: float):
        sleep_calls.append(seconds)

    monkeypatch.setattr(primary_module.time, "sleep", record_sleep)

    instance.set_limiter(max_requests=3, duration=2)
    sleepytime = instance.duration / instance.max_requests

    instance.call_list = []
    instance.enforce_limits()

    total_calls = 6
    for _ in range(1, total_calls):
        last_added = instance.call_list[-1]
        arrival = last_added + timedelta(seconds=0.01)
        post_sleep = last_added + timedelta(seconds=sleepytime)
        fake_datetime.set_queue([arrival, post_sleep])
        instance.enforce_limits()
        assert fake_datetime.queue == []

    assert len(instance.call_list) == instance.max_requests

    gaps = [
        (instance.call_list[i + 1] - instance.call_list[i]).total_seconds()
        for i in range(len(instance.call_list) - 1)
    ]
    for gap in gaps:
        assert pytest.approx(gap, rel=1e-6) == sleepytime

    assert len(sleep_calls) == total_calls - 1
    for sleep_value in sleep_calls:
        assert sleep_value == pytest.approx(sleepytime - 0.01, rel=1e-6)
