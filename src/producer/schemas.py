"""JSON schema definitions and validation for pipeline events.

Defines schemas for clickstream events, CDC events, and user profiles.
Provides validation utilities and a simple in-memory schema registry.
"""

from typing import Any

from jsonschema import ValidationError, validate

CLICKSTREAM_SCHEMA: dict[str, Any] = {
    "type": "object",
    "required": ["event_id", "user_id", "timestamp", "event_type", "session_id"],
    "properties": {
        "event_id": {"type": "string"},
        "user_id": {"type": "string"},
        "timestamp": {"type": "string"},
        "event_type": {
            "type": "string",
            "enum": ["page_view", "add_to_cart", "purchase", "search", "user_login"],
        },
        "product_id": {"type": ["string", "null"]},
        "session_id": {"type": "string"},
        "metadata": {
            "type": "object",
            "properties": {
                "page_url": {"type": "string"},
                "referrer": {"type": "string"},
                "device_type": {"type": "string"},
                "search_query": {"type": "string"},
                "cart_value": {"type": "number"},
                "quantity": {"type": "integer"},
            },
        },
    },
}

CDC_SCHEMA: dict[str, Any] = {
    "type": "object",
    "required": ["op", "ts_ms", "source", "before", "after"],
    "properties": {
        "op": {"type": "string", "enum": ["c", "u", "d", "r"]},
        "ts_ms": {"type": "integer"},
        "source": {
            "type": "object",
            "properties": {
                "table": {"type": "string"},
                "db": {"type": "string"},
            },
        },
        "before": {"type": ["object", "null"]},
        "after": {"type": ["object", "null"]},
    },
}

USER_PROFILE_SCHEMA: dict[str, Any] = {
    "type": "object",
    "required": ["user_id"],
    "properties": {
        "user_id": {"type": "string"},
        "name": {"type": "string"},
        "email": {"type": "string"},
        "preferences": {"type": "object"},
    },
}


def validate_event(
    event: dict[str, Any], schema: dict[str, Any]
) -> tuple[bool, str | None]:
    """Validate an event against a JSON schema.

    Args:
        event: The event dictionary to validate.
        schema: JSON Schema to validate against.

    Returns:
        Tuple of (is_valid, error_message). error_message is None if valid.
    """
    try:
        validate(instance=event, schema=schema)
        return True, None
    except ValidationError as e:
        return False, str(e.message)


class SchemaRegistry:
    """Simple in-memory schema registry for event validation.

    Stores and retrieves JSON schemas by name, supporting runtime
    registration of custom schemas.
    """

    def __init__(self) -> None:
        self._schemas: dict[str, dict[str, Any]] = {
            "clickstream": CLICKSTREAM_SCHEMA,
            "cdc": CDC_SCHEMA,
            "user_profile": USER_PROFILE_SCHEMA,
        }

    def get_schema(self, name: str) -> dict[str, Any] | None:
        """Retrieve a schema by name.

        Args:
            name: Schema identifier.

        Returns:
            The JSON schema dictionary, or None if not found.
        """
        return self._schemas.get(name)

    def register_schema(self, name: str, schema: dict[str, Any]) -> None:
        """Register a new schema or overwrite an existing one.

        Args:
            name: Schema identifier.
            schema: JSON Schema dictionary to store.
        """
        self._schemas[name] = schema

    def list_schemas(self) -> list[str]:
        """List all registered schema names.

        Returns:
            List of schema name strings.
        """
        return list(self._schemas.keys())
