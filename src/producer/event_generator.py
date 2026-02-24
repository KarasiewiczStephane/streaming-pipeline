"""Realistic e-commerce clickstream event generator.

Generates events with configurable user sessions, browsing patterns,
cart abandonment, and Markov chain-based state transitions.
"""

import random
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Iterator


@dataclass
class UserSession:
    """Represents an active user browsing session.

    Attributes:
        user_id: Unique identifier for the user.
        session_id: Unique identifier for this session.
        start_time: When the session started.
        events: List of event types generated in this session.
        cart_items: Products currently in the cart.
    """

    user_id: str
    session_id: str
    start_time: datetime
    events: list[str] = field(default_factory=list)
    cart_items: list[str] = field(default_factory=list)


class EventGenerator:
    """Generates realistic e-commerce clickstream events.

    Uses Markov chain transition probabilities to simulate realistic
    user browsing patterns including sessions, cart operations, and
    abandonment behaviors.

    Args:
        events_per_second: Target event generation rate.
        num_users: Size of the simulated user pool.
    """

    EVENT_TYPES = ["page_view", "add_to_cart", "purchase", "search", "user_login"]
    PRODUCTS = [f"PROD-{i:04d}" for i in range(1, 101)]
    PAGES = ["/", "/products", "/cart", "/checkout", "/profile", "/search"]
    DEVICE_TYPES = ["desktop", "mobile", "tablet"]

    TRANSITIONS: dict[str, dict[str, float]] = {
        "user_login": {"page_view": 0.9, "search": 0.1},
        "page_view": {
            "page_view": 0.4,
            "search": 0.2,
            "add_to_cart": 0.3,
            "purchase": 0.1,
        },
        "search": {"page_view": 0.7, "search": 0.2, "add_to_cart": 0.1},
        "add_to_cart": {
            "page_view": 0.3,
            "add_to_cart": 0.2,
            "purchase": 0.4,
            "search": 0.1,
        },
        "purchase": {"page_view": 0.8, "search": 0.2},
    }

    def __init__(self, events_per_second: int = 100, num_users: int = 1000) -> None:
        self.events_per_second = events_per_second
        self.num_users = num_users
        self.users = [f"USER-{i:06d}" for i in range(num_users)]
        self.active_sessions: dict[str, UserSession] = {}

    def _create_session(self, user_id: str) -> UserSession:
        """Create a new browsing session for a user.

        Args:
            user_id: The user to create a session for.

        Returns:
            New UserSession instance.
        """
        return UserSession(
            user_id=user_id,
            session_id=str(uuid.uuid4()),
            start_time=datetime.now(tz=timezone.utc),
        )

    def _get_next_event_type(self, last_event: str | None) -> str:
        """Determine the next event type using transition probabilities.

        Args:
            last_event: The previous event type, or None for session start.

        Returns:
            Next event type string.
        """
        if last_event is None:
            return random.choices(["user_login", "page_view"], weights=[0.3, 0.7])[0]
        probs = self.TRANSITIONS.get(last_event, {"page_view": 1.0})
        return random.choices(list(probs.keys()), weights=list(probs.values()))[0]

    def _generate_metadata(
        self, event_type: str, product_id: str | None
    ) -> dict[str, str | int | float]:
        """Generate event metadata based on event type.

        Args:
            event_type: Type of the event.
            product_id: Associated product, if any.

        Returns:
            Metadata dictionary with page, device, and type-specific fields.
        """
        metadata: dict[str, str | int | float] = {
            "page_url": random.choice(self.PAGES),
            "device_type": random.choice(self.DEVICE_TYPES),
            "referrer": random.choice(["google", "direct", "facebook", "email", ""]),
        }
        if event_type == "search":
            metadata["search_query"] = random.choice(
                ["laptop", "phone", "headphones", "camera"]
            )
        if event_type in ("add_to_cart", "purchase"):
            metadata["quantity"] = random.randint(1, 3)
            metadata["cart_value"] = round(random.uniform(10, 500), 2)
        return metadata

    def generate_event(self) -> dict:
        """Generate a single clickstream event.

        Returns:
            Event dictionary conforming to the clickstream schema.
        """
        user_id = random.choice(self.users)

        if user_id not in self.active_sessions or random.random() < 0.05:
            self.active_sessions[user_id] = self._create_session(user_id)

        session = self.active_sessions[user_id]
        last_event = session.events[-1] if session.events else None
        event_type = self._get_next_event_type(last_event)

        if last_event == "add_to_cart" and random.random() < 0.3:
            del self.active_sessions[user_id]

        product_id = (
            random.choice(self.PRODUCTS)
            if event_type in ("page_view", "add_to_cart", "purchase")
            else None
        )

        event = {
            "event_id": str(uuid.uuid4()),
            "user_id": user_id,
            "timestamp": datetime.now(tz=timezone.utc).isoformat(),
            "event_type": event_type,
            "product_id": product_id,
            "session_id": session.session_id,
            "metadata": self._generate_metadata(event_type, product_id),
        }

        session.events.append(event_type)
        return event

    def stream_events(self) -> Iterator[dict]:
        """Infinite generator yielding events at the configured rate.

        Yields:
            Event dictionaries at approximately ``events_per_second`` rate.
        """
        interval = 1.0 / self.events_per_second
        while True:
            yield self.generate_event()
            time.sleep(interval)
