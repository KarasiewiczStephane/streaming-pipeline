"""Tests for the CDC event generator."""

import pytest

from src.producer.cdc_generator import CDCEventGenerator
from src.producer.schemas import CDC_SCHEMA, validate_event


class TestCDCEventGenerator:
    """Tests for CDCEventGenerator."""

    @pytest.fixture()
    def generator(self) -> CDCEventGenerator:
        return CDCEventGenerator(num_users=10)

    def test_initializes_users(self, generator: CDCEventGenerator) -> None:
        assert len(generator.user_states) == 10

    def test_generate_returns_valid_structure(
        self, generator: CDCEventGenerator
    ) -> None:
        event = generator.generate_cdc_event()
        assert "op" in event
        assert "ts_ms" in event
        assert "source" in event
        assert "before" in event
        assert "after" in event

    def test_validates_against_schema(self, generator: CDCEventGenerator) -> None:
        for _ in range(20):
            event = generator.generate_cdc_event()
            is_valid, error = validate_event(event, CDC_SCHEMA)
            assert is_valid, f"Invalid CDC event: {error}"

    def test_create_has_null_before(self, generator: CDCEventGenerator) -> None:
        for _ in range(200):
            event = generator.generate_cdc_event()
            if event["op"] == "c":
                assert event["before"] is None
                assert event["after"] is not None
                assert "user_id" in event["after"]
                return
        pytest.skip("No create event generated in 200 iterations")

    def test_update_has_both_states(self, generator: CDCEventGenerator) -> None:
        for _ in range(200):
            event = generator.generate_cdc_event()
            if event["op"] == "u":
                assert event["before"] is not None
                assert event["after"] is not None
                return
        pytest.skip("No update event generated in 200 iterations")

    def test_delete_has_null_after(self, generator: CDCEventGenerator) -> None:
        for _ in range(200):
            event = generator.generate_cdc_event()
            if event["op"] == "d":
                assert event["before"] is not None
                assert event["after"] is None
                return
        pytest.skip("No delete event generated in 200 iterations")

    def test_source_is_correct(self, generator: CDCEventGenerator) -> None:
        event = generator.generate_cdc_event()
        assert event["source"]["table"] == "users"
        assert event["source"]["db"] == "ecommerce"

    def test_ts_ms_is_positive(self, generator: CDCEventGenerator) -> None:
        event = generator.generate_cdc_event()
        assert event["ts_ms"] > 0

    def test_user_states_updated_on_create(self, generator: CDCEventGenerator) -> None:
        for _ in range(200):
            event = generator.generate_cdc_event()
            if event["op"] == "c":
                assert event["after"]["user_id"] in generator.user_states
                return
        pytest.skip("No create event in 200 iterations")

    def test_user_states_updated_on_delete(self, generator: CDCEventGenerator) -> None:
        for _ in range(200):
            event = generator.generate_cdc_event()
            if event["op"] == "d":
                # After a delete, the deleted user should be gone
                assert event["before"]["user_id"] not in generator.user_states
                return
        pytest.skip("No delete event in 200 iterations")

    def test_empty_state_generates_create(self) -> None:
        gen = CDCEventGenerator(num_users=0)
        event = gen.generate_cdc_event()
        assert event["op"] == "c"
