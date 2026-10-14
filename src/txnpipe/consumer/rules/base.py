"""Rule protocol and the per-account state snapshot rules read from."""

from dataclasses import dataclass, field
from typing import Any, Protocol

from txnpipe.common.schemas import TransactionEvent


@dataclass(frozen=True)
class AccountState:
    """Read-only view of what we know about an account before the current event."""

    velocity_ts: tuple[float, ...] = field(default_factory=tuple)  # event times inside the window
    count: int = 0  # prior transactions seen
    ewma: float = 0.0  # exponentially weighted mean amount
    last_country: str | None = None
    last_ts: float | None = None


@dataclass(frozen=True)
class RuleResult:
    rule: str
    severity: str
    details: dict[str, Any]


class Rule(Protocol):
    name: str

    def evaluate(self, event: TransactionEvent, state: AccountState) -> RuleResult | None:
        """Pure and read-only: never mutates state."""
        ...
