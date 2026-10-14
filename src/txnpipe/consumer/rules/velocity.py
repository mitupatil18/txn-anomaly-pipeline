from txnpipe.common.schemas import TransactionEvent
from txnpipe.consumer.rules.base import AccountState, RuleResult


class VelocityRule:
    name = "VELOCITY"

    def __init__(self, window_seconds: int = 60, limit: int = 5) -> None:
        self.window_seconds = window_seconds
        self.limit = limit

    def evaluate(self, event: TransactionEvent, state: AccountState) -> RuleResult | None:
        ts = event.event_epoch
        # Events older than the window (relative to the newest one seen) are ignored.
        if state.velocity_ts and ts < max(state.velocity_ts) - self.window_seconds:
            return None
        count = 1 + sum(1 for t in state.velocity_ts if ts - self.window_seconds <= t <= ts)
        if count <= self.limit:
            return None
        severity = "HIGH" if count > 2 * self.limit else "MEDIUM"
        return RuleResult(
            self.name,
            severity,
            {"count": count, "limit": self.limit, "window_seconds": self.window_seconds},
        )
