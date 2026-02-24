"""Simulated Debezium-style CDC event generator.

Generates create, update, and delete operations for user profile
data, maintaining internal state for realistic change tracking.
"""

import random
import uuid
from datetime import datetime, timezone
from typing import Iterator

from src.utils.logger import get_logger

logger = get_logger(__name__)


class CDCEventGenerator:
    """Generates Debezium-style CDC events for user profiles.

    Maintains an internal state of user profiles and generates
    realistic create, update, and delete operations.

    Args:
        num_users: Initial number of users to seed.
    """

    FIRST_NAMES = ["Alice", "Bob", "Charlie", "Diana", "Eve", "Frank"]
    LAST_NAMES = ["Smith", "Johnson", "Williams", "Brown", "Jones"]
    DOMAINS = ["gmail.com", "yahoo.com", "outlook.com"]
    PREFERENCES = ["electronics", "clothing", "home", "sports", "books"]

    def __init__(self, num_users: int = 100) -> None:
        self.num_users = num_users
        self.user_states: dict[str, dict] = {}
        self._initialize_users()

    def _initialize_users(self) -> None:
        """Seed the initial user population."""
        for i in range(self.num_users):
            user_id = f"USER-{i:06d}"
            self.user_states[user_id] = self._generate_profile(user_id)

    def _generate_profile(self, user_id: str) -> dict:
        """Generate a random user profile.

        Args:
            user_id: Unique user identifier.

        Returns:
            User profile dictionary.
        """
        first = random.choice(self.FIRST_NAMES)
        last = random.choice(self.LAST_NAMES)
        return {
            "user_id": user_id,
            "name": f"{first} {last}",
            "email": f"{first.lower()}.{last.lower()}@{random.choice(self.DOMAINS)}",
            "preferences": {
                "category": random.choice(self.PREFERENCES),
                "notifications": random.choice([True, False]),
            },
        }

    def generate_cdc_event(self) -> dict:
        """Generate a single CDC event.

        Operations are weighted: 10% create, 80% update, 10% delete.

        Returns:
            Debezium-format CDC event dictionary.
        """
        if not self.user_states:
            user_id = f"USER-{0:06d}"
            new_state = self._generate_profile(user_id)
            self.user_states[user_id] = new_state
            return self._build_event("c", None, new_state)

        user_id = random.choice(list(self.user_states.keys()))
        current_state = self.user_states[user_id]

        op = random.choices(["c", "u", "d"], weights=[0.1, 0.8, 0.1])[0]

        if op == "c":
            user_id = f"USER-{len(self.user_states):06d}"
            new_state = self._generate_profile(user_id)
            self.user_states[user_id] = new_state
            return self._build_event("c", None, new_state)

        if op == "u":
            before = current_state.copy()
            after = current_state.copy()
            field = random.choice(["name", "email", "preferences"])
            if field == "name":
                after["name"] = (
                    f"{random.choice(self.FIRST_NAMES)} "
                    f"{random.choice(self.LAST_NAMES)}"
                )
            elif field == "email":
                after["email"] = f"{uuid.uuid4().hex[:8]}@{random.choice(self.DOMAINS)}"
            else:
                after["preferences"] = {
                    "category": random.choice(self.PREFERENCES),
                    "notifications": random.choice([True, False]),
                }
            self.user_states[user_id] = after
            return self._build_event("u", before, after)

        # Delete
        before = current_state
        del self.user_states[user_id]
        return self._build_event("d", before, None)

    def _build_event(
        self,
        op: str,
        before: dict | None,
        after: dict | None,
    ) -> dict:
        """Build a Debezium-format CDC event.

        Args:
            op: Operation type (c, u, d, r).
            before: State before the change.
            after: State after the change.

        Returns:
            CDC event dictionary.
        """
        return {
            "op": op,
            "ts_ms": int(datetime.now(tz=timezone.utc).timestamp() * 1000),
            "source": {"table": "users", "db": "ecommerce"},
            "before": before,
            "after": after,
        }

    def stream_events(self, interval: float = 1.0) -> Iterator[dict]:
        """Infinite generator yielding CDC events.

        Args:
            interval: Seconds between events.

        Yields:
            CDC event dictionaries.
        """
        import time

        while True:
            yield self.generate_cdc_event()
            time.sleep(interval)
