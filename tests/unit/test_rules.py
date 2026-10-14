from tests.unit.helpers import BASE_TS, make_event
from txnpipe.consumer.rules import AccountState
from txnpipe.consumer.rules.amount_spike import AmountSpikeRule
from txnpipe.consumer.rules.geo_mismatch import GeoMismatchRule
from txnpipe.consumer.rules.velocity import VelocityRule


def _vel(n_prior: int, *, age: float = 1.0) -> AccountState:
    return AccountState(velocity_ts=tuple(BASE_TS - age for _ in range(n_prior)))


class TestVelocity:
    rule = VelocityRule(window_seconds=60, limit=5)

    def test_below_and_at_limit(self) -> None:
        assert self.rule.evaluate(make_event(), _vel(3)) is None
        assert self.rule.evaluate(make_event(), _vel(4)) is None  # count 5 == limit

    def test_above_limit_is_medium(self) -> None:
        r = self.rule.evaluate(make_event(), _vel(5))
        assert r and r.severity == "MEDIUM" and r.details["count"] == 6

    def test_more_than_double_limit_is_high(self) -> None:
        high = self.rule.evaluate(make_event(), _vel(10))  # count 11 > 10
        medium = self.rule.evaluate(make_event(), _vel(9))  # count 10 == 2x limit
        assert high and high.severity == "HIGH"
        assert medium and medium.severity == "MEDIUM"

    def test_no_history(self) -> None:
        assert self.rule.evaluate(make_event(), AccountState()) is None

    def test_window_boundary(self) -> None:
        inside = AccountState(velocity_ts=tuple(BASE_TS - 60 for _ in range(5)))
        outside = AccountState(velocity_ts=tuple(BASE_TS - 61 for _ in range(5)))
        assert self.rule.evaluate(make_event(), inside) is not None
        assert self.rule.evaluate(make_event(), outside) is None

    def test_event_older_than_window_is_ignored(self) -> None:
        state = AccountState(velocity_ts=tuple(BASE_TS + 100 for _ in range(10)))
        assert self.rule.evaluate(make_event(), state) is None


class TestAmountSpike:
    rule = AmountSpikeRule(multiplier=5, min_amount=1000, min_history=5)

    def _state(self, count: int = 10, ewma: float = 100.0) -> AccountState:
        return AccountState(count=count, ewma=ewma)

    def test_spike_detected(self) -> None:
        r = self.rule.evaluate(make_event(amount="1200.00"), self._state())
        assert r and r.severity == "HIGH" and r.details["ratio"] == 12.0

    def test_medium_when_ratio_up_to_ten(self) -> None:
        r = self.rule.evaluate(make_event(amount="1000.00"), self._state(ewma=150))
        assert r and r.severity == "MEDIUM"

    def test_at_multiplier_boundary_no_alert(self) -> None:
        assert self.rule.evaluate(make_event(amount="1000.00"), self._state(ewma=200)) is None

    def test_below_min_amount(self) -> None:
        assert self.rule.evaluate(make_event(amount="999.99"), self._state(ewma=10)) is None

    def test_insufficient_history(self) -> None:
        assert self.rule.evaluate(make_event(amount="5000"), self._state(count=4)) is None
        assert self.rule.evaluate(make_event(amount="5000"), self._state(count=5)) is not None

    def test_missing_history(self) -> None:
        assert self.rule.evaluate(make_event(amount="5000"), AccountState()) is None


class TestGeo:
    rule = GeoMismatchRule(min_gap_minutes=10)

    def _state(self, gap_s: float, country: str = "IN") -> AccountState:
        return AccountState(last_country=country, last_ts=BASE_TS - gap_s)

    def test_impossible_travel(self) -> None:
        r = self.rule.evaluate(make_event(country="US"), self._state(30))
        assert r and r.severity == "HIGH" and r.details["previous_country"] == "IN"

    def test_same_country(self) -> None:
        assert self.rule.evaluate(make_event(country="IN"), self._state(30)) is None

    def test_gap_boundary(self) -> None:
        assert self.rule.evaluate(make_event(country="US"), self._state(599)) is not None
        assert self.rule.evaluate(make_event(country="US"), self._state(600)) is None

    def test_no_history_or_out_of_order(self) -> None:
        assert self.rule.evaluate(make_event(), AccountState()) is None
        future = AccountState(last_country="IN", last_ts=BASE_TS + 5)
        assert self.rule.evaluate(make_event(country="US"), future) is None
