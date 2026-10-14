from txnpipe.common.schemas import TransactionEvent
from txnpipe.consumer.rules.base import AccountState, RuleResult


class AmountSpikeRule:
    name = "AMOUNT_SPIKE"

    def __init__(
        self, multiplier: float = 5.0, min_amount: float = 1000.0, min_history: int = 5
    ) -> None:
        self.multiplier = multiplier
        self.min_amount = min_amount
        self.min_history = min_history

    def evaluate(self, event: TransactionEvent, state: AccountState) -> RuleResult | None:
        amount = float(event.amount)
        if state.count < self.min_history or state.ewma <= 0 or amount < self.min_amount:
            return None
        ratio = amount / state.ewma
        if ratio <= self.multiplier:
            return None
        return RuleResult(
            self.name,
            "HIGH" if ratio > 10 else "MEDIUM",
            {
                "amount": amount,
                "ewma": round(state.ewma, 2),
                "ratio": round(ratio, 2),
                "multiplier": self.multiplier,
                "min_amount": self.min_amount,
                "history": state.count,
            },
        )
