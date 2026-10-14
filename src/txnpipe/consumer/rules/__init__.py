from txnpipe.common.config import Settings
from txnpipe.consumer.rules.amount_spike import AmountSpikeRule
from txnpipe.consumer.rules.base import AccountState, Rule, RuleResult
from txnpipe.consumer.rules.geo_mismatch import GeoMismatchRule
from txnpipe.consumer.rules.velocity import VelocityRule

__all__ = ["AccountState", "Rule", "RuleResult", "build_rules"]


def build_rules(s: Settings) -> list[Rule]:
    """Registry of active rules. Add a new rule here; the handler needs no change."""
    return [
        VelocityRule(s.velocity_window_seconds, s.velocity_limit),
        AmountSpikeRule(s.spike_multiplier, s.spike_min_amount, s.spike_min_history),
        GeoMismatchRule(s.geo_min_gap_minutes),
    ]
