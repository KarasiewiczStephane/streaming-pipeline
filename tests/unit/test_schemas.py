"""Tests for event schema definitions and validation."""

from src.producer.schemas import (
    CDC_SCHEMA,
    CLICKSTREAM_SCHEMA,
    USER_PROFILE_SCHEMA,
    SchemaRegistry,
    validate_event,
)


class TestValidateClickstreamEvent:
    """Tests for clickstream event validation."""

    def test_valid_event(self) -> None:
        event = {
            "event_id": "550e8400-e29b-41d4-a716-446655440000",
            "user_id": "USER-001",
            "timestamp": "2024-01-01T12:00:00Z",
            "event_type": "page_view",
            "product_id": "PROD-001",
            "session_id": "session-123",
            "metadata": {"page_url": "/", "device_type": "desktop"},
        }
        is_valid, error = validate_event(event, CLICKSTREAM_SCHEMA)
        assert is_valid is True
        assert error is None

    def test_missing_required_field(self) -> None:
        event = {"event_id": "123", "user_id": "u1"}
        is_valid, error = validate_event(event, CLICKSTREAM_SCHEMA)
        assert is_valid is False
        assert error is not None

    def test_invalid_event_type(self) -> None:
        event = {
            "event_id": "123",
            "user_id": "u1",
            "timestamp": "2024-01-01T12:00:00Z",
            "event_type": "invalid_type",
            "session_id": "s1",
        }
        is_valid, error = validate_event(event, CLICKSTREAM_SCHEMA)
        assert is_valid is False

    def test_null_product_id_is_valid(self) -> None:
        event = {
            "event_id": "123",
            "user_id": "u1",
            "timestamp": "2024-01-01T12:00:00Z",
            "event_type": "search",
            "product_id": None,
            "session_id": "s1",
        }
        is_valid, error = validate_event(event, CLICKSTREAM_SCHEMA)
        assert is_valid is True

    def test_extra_fields_pass(self) -> None:
        event = {
            "event_id": "123",
            "user_id": "u1",
            "timestamp": "2024-01-01T12:00:00Z",
            "event_type": "page_view",
            "session_id": "s1",
            "extra_field": "value",
        }
        is_valid, error = validate_event(event, CLICKSTREAM_SCHEMA)
        assert is_valid is True

    def test_empty_event_fails(self) -> None:
        is_valid, error = validate_event({}, CLICKSTREAM_SCHEMA)
        assert is_valid is False

    def test_all_event_types_valid(self) -> None:
        for event_type in [
            "page_view",
            "add_to_cart",
            "purchase",
            "search",
            "user_login",
        ]:
            event = {
                "event_id": "123",
                "user_id": "u1",
                "timestamp": "2024-01-01T12:00:00Z",
                "event_type": event_type,
                "session_id": "s1",
            }
            is_valid, _ = validate_event(event, CLICKSTREAM_SCHEMA)
            assert is_valid is True, f"Event type {event_type} should be valid"


class TestValidateCDCEvent:
    """Tests for CDC event validation."""

    def test_valid_create(self) -> None:
        event = {
            "op": "c",
            "ts_ms": 1704067200000,
            "source": {"table": "users", "db": "ecommerce"},
            "before": None,
            "after": {"user_id": "u1", "name": "Alice"},
        }
        is_valid, error = validate_event(event, CDC_SCHEMA)
        assert is_valid is True

    def test_valid_update(self) -> None:
        event = {
            "op": "u",
            "ts_ms": 1704067200000,
            "source": {"table": "users", "db": "ecommerce"},
            "before": {"user_id": "u1", "name": "Alice"},
            "after": {"user_id": "u1", "name": "Bob"},
        }
        is_valid, error = validate_event(event, CDC_SCHEMA)
        assert is_valid is True

    def test_valid_delete(self) -> None:
        event = {
            "op": "d",
            "ts_ms": 1704067200000,
            "source": {"table": "users", "db": "ecommerce"},
            "before": {"user_id": "u1", "name": "Alice"},
            "after": None,
        }
        is_valid, error = validate_event(event, CDC_SCHEMA)
        assert is_valid is True

    def test_invalid_operation(self) -> None:
        event = {
            "op": "x",
            "ts_ms": 1704067200000,
            "source": {"table": "users", "db": "ecommerce"},
            "before": None,
            "after": None,
        }
        is_valid, error = validate_event(event, CDC_SCHEMA)
        assert is_valid is False

    def test_all_operations_valid(self) -> None:
        for op in ["c", "u", "d", "r"]:
            event = {
                "op": op,
                "ts_ms": 1704067200000,
                "source": {"table": "users", "db": "ecommerce"},
                "before": None,
                "after": None,
            }
            is_valid, _ = validate_event(event, CDC_SCHEMA)
            assert is_valid is True, f"Operation '{op}' should be valid"


class TestValidateUserProfile:
    """Tests for user profile schema validation."""

    def test_valid_profile(self) -> None:
        profile = {
            "user_id": "u1",
            "name": "Alice Smith",
            "email": "alice@example.com",
            "preferences": {"category": "electronics"},
        }
        is_valid, error = validate_event(profile, USER_PROFILE_SCHEMA)
        assert is_valid is True

    def test_minimal_profile(self) -> None:
        profile = {"user_id": "u1"}
        is_valid, error = validate_event(profile, USER_PROFILE_SCHEMA)
        assert is_valid is True

    def test_missing_user_id(self) -> None:
        profile = {"name": "Alice"}
        is_valid, error = validate_event(profile, USER_PROFILE_SCHEMA)
        assert is_valid is False


class TestSchemaRegistry:
    """Tests for the SchemaRegistry class."""

    def test_get_existing_schema(self) -> None:
        registry = SchemaRegistry()
        schema = registry.get_schema("clickstream")
        assert schema == CLICKSTREAM_SCHEMA

    def test_get_cdc_schema(self) -> None:
        registry = SchemaRegistry()
        schema = registry.get_schema("cdc")
        assert schema == CDC_SCHEMA

    def test_get_user_profile_schema(self) -> None:
        registry = SchemaRegistry()
        schema = registry.get_schema("user_profile")
        assert schema == USER_PROFILE_SCHEMA

    def test_get_nonexistent_schema(self) -> None:
        registry = SchemaRegistry()
        assert registry.get_schema("nonexistent") is None

    def test_register_new_schema(self) -> None:
        registry = SchemaRegistry()
        new_schema = {"type": "object", "properties": {"id": {"type": "string"}}}
        registry.register_schema("custom", new_schema)
        assert registry.get_schema("custom") == new_schema

    def test_list_schemas(self) -> None:
        registry = SchemaRegistry()
        names = registry.list_schemas()
        assert "clickstream" in names
        assert "cdc" in names
        assert "user_profile" in names

    def test_overwrite_schema(self) -> None:
        registry = SchemaRegistry()
        new_schema = {"type": "object"}
        registry.register_schema("clickstream", new_schema)
        assert registry.get_schema("clickstream") == new_schema
