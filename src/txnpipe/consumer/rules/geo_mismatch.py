from txnpipe.common.schemas import TransactionEvent
from txnpipe.consumer.rules.base import AccountState, RuleResult


class GeoMismatchRule:
    """Impossible travel: a different country within a short gap of the previous transaction."""

    name = "GEO_MISMATCH"

    def __init__(self, min_gap_minutes: int = 10) -> None:
        self.min_gap_minutes = min_gap_minutes

    def evaluate(self, event: TransactionEvent, state: AccountState) -> RuleResult | None:
        if state.last_country is None or state.last_ts is None:
            return None
        if state.last_country == event.country:
            return None
        gap_minutes = (event.event_epoch - state.last_ts) / 60
        if gap_minutes < 0 or gap_minutes >= self.min_gap_minutes:
            return None
        return RuleResult(
            self.name,
            "HIGH",
            {
                "previous_country": state.last_country,
                "country": event.country,
                "gap_minutes": round(gap_minutes, 2),
                "min_gap_minutes": self.min_gap_minutes,
            },
        )
