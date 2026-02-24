"""Tests for the clickstream event generator."""

from collections import Counter

import pytest

from src.producer.event_generator import EventGenerator, UserSession
from src.producer.schemas import CLICKSTREAM_SCHEMA, validate_event


class TestUserSession:
    """Tests for the UserSession dataclass."""

    def test_default_empty_lists(self) -> None:
        from datetime import datetime, timezone

        session = UserSession(
            user_id="u1",
            session_id="s1",
            start_time=datetime.now(tz=timezone.utc),
        )
        assert session.events == []
        assert session.cart_items == []


class TestEventGenerator:
    """Tests for the EventGenerator class."""

    @pytest.fixture()
    def generator(self) -> EventGenerator:
        return EventGenerator(events_per_second=10, num_users=10)

    def test_init_creates_user_pool(self, generator: EventGenerator) -> None:
        assert len(generator.users) == 10
        assert generator.users[0] == "USER-000000"

    def test_generate_event_returns_dict(self, generator: EventGenerator) -> None:
        event = generator.generate_event()
        assert isinstance(event, dict)

    def test_generate_event_has_required_fields(
        self, generator: EventGenerator
    ) -> None:
        event = generator.generate_event()
        for field in ["event_id", "user_id", "timestamp", "event_type", "session_id"]:
            assert field in event, f"Missing field: {field}"

    def test_generate_event_validates_against_schema(
        self, generator: EventGenerator
    ) -> None:
        for _ in range(20):
            event = generator.generate_event()
            is_valid, error = validate_event(event, CLICKSTREAM_SCHEMA)
            assert is_valid, f"Invalid event: {error}"

    def test_event_type_is_valid(self, generator: EventGenerator) -> None:
        for _ in range(100):
            event = generator.generate_event()
            assert event["event_type"] in EventGenerator.EVENT_TYPES

    def test_product_id_set_for_relevant_types(self, generator: EventGenerator) -> None:
        events = [generator.generate_event() for _ in range(200)]
        for event in events:
            if event["event_type"] in ("page_view", "add_to_cart", "purchase"):
                assert event["product_id"] is not None
            elif event["event_type"] in ("search", "user_login"):
                assert event["product_id"] is None

    def test_metadata_has_base_fields(self, generator: EventGenerator) -> None:
        event = generator.generate_event()
        metadata = event["metadata"]
        assert "page_url" in metadata
        assert "device_type" in metadata

    def test_search_events_have_query(self, generator: EventGenerator) -> None:
        search_events = []
        for _ in range(500):
            event = generator.generate_event()
            if event["event_type"] == "search":
                search_events.append(event)
        assert len(search_events) > 0
        for event in search_events:
            assert "search_query" in event["metadata"]

    def test_cart_events_have_value(self, generator: EventGenerator) -> None:
        cart_events = []
        for _ in range(500):
            event = generator.generate_event()
            if event["event_type"] in ("add_to_cart", "purchase"):
                cart_events.append(event)
        assert len(cart_events) > 0
        for event in cart_events:
            assert "cart_value" in event["metadata"]
            assert "quantity" in event["metadata"]

    def test_session_continuity(self, generator: EventGenerator) -> None:
        """Same user should get same session within a session window."""
        events = [generator.generate_event() for _ in range(100)]
        user_sessions: dict[str, set[str]] = {}
        for event in events:
            uid = event["user_id"]
            sid = event["session_id"]
            user_sessions.setdefault(uid, set()).add(sid)

        # At least some users should have maintained the same session
        maintained = sum(1 for sessions in user_sessions.values() if len(sessions) == 1)
        assert maintained > 0

    def test_transition_distribution(self, generator: EventGenerator) -> None:
        """Event type distribution should roughly match transition probs."""
        events = [generator.generate_event() for _ in range(1000)]
        counts = Counter(e["event_type"] for e in events)
        # page_view should be the most common
        assert counts["page_view"] > counts.get("user_login", 0)

    def test_unique_event_ids(self, generator: EventGenerator) -> None:
        events = [generator.generate_event() for _ in range(100)]
        ids = [e["event_id"] for e in events]
        assert len(set(ids)) == 100

    def test_stream_events_yields(self, generator: EventGenerator) -> None:
        stream = generator.stream_events()
        # Take just 3 events without blocking too long
        events = []
        for i, event in enumerate(stream):
            events.append(event)
            if i >= 2:
                break
        assert len(events) == 3
        for event in events:
            assert "event_id" in event
