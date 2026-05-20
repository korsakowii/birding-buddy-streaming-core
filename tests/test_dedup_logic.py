from __future__ import annotations

from stream_processor.streaming_logic import InMemoryDeduplicator


def test_dedup_first_observation_passes() -> None:
    d = InMemoryDeduplicator()
    assert d.is_duplicate("evt_1") is False


def test_dedup_second_observation_is_duplicate() -> None:
    d = InMemoryDeduplicator()
    assert d.is_duplicate("evt_1") is False
    assert d.is_duplicate("evt_1") is True


def test_distinct_keys_independent() -> None:
    d = InMemoryDeduplicator()
    assert d.is_duplicate("a") is False
    assert d.is_duplicate("b") is False
    assert d.is_duplicate("a") is True
