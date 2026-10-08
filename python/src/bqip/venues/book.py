"""Finite offline synchronization proof for one original capture session."""

from enum import Enum
from functools import cmp_to_key

from bqip.contracts.values import ContractError
from bqip.v1 import market_pb2 as m
from bqip.venues.binance import compare
from bqip.venues.validation import validate_depth, validate_snapshot


class BookSyncState(Enum):
    EMPTY = "EMPTY"
    BUFFERING = "BUFFERING"
    SNAPSHOT_REQUIRED = "SNAPSHOT_REQUIRED"
    BRIDGING = "BRIDGING"
    HEALTHY = "HEALTHY"
    DEGRADED = "DEGRADED"
    RESYNC_REQUIRED = "RESYNC_REQUIRED"


def _key(level: m.PriceLevel) -> str:
    return f"{level.price.coefficient}:{level.price.scale}"


def _semantic(v: m.BinanceDepthUpdate) -> bytes:
    # Construct known fields only; transport observations and unknown wire fields
    # cannot manufacture a content conflict. Last listed replacement wins.
    def side(items: list[m.PriceLevel]) -> list[m.PriceLevel]:
        by_price = {_key(level): m.PriceLevel(price=level.price, quantity=level.quantity) for level in items}
        return [by_price[k] for k in sorted(by_price)]
    return m.BinanceDepthUpdate(
        first_update_id=v.first_update_id, final_update_id=v.final_update_id,
        previous_final_update_id=v.previous_final_update_id,
        native_event_time_ms=v.native_event_time_ms, native_transaction_time_ms=v.native_transaction_time_ms,
        event_timestamp_ns=v.event_timestamp_ns, transaction_timestamp_ns=v.transaction_timestamp_ns,
        bids=side(list(v.bids)), asks=side(list(v.asks))).SerializeToString(deterministic=True)


class BookSync:
    def __init__(self, original_session_id: bytes) -> None:
        if type(original_session_id) is not bytes or len(original_session_id) != 16:
            raise ContractError("INVALID_ORIGINAL_SESSION")
        self._session = original_session_id
        self._state = BookSyncState.EMPTY
        self._transitions = [self._state]
        self._cause: str | None = None
        self._buffer: list[m.BinanceDepthUpdate] = []
        self._seen: dict[bytes, bytes] = {}
        self._snapshot: m.BinanceDepthSnapshot | None = None
        self._previous: int | None = None
        self._bids: dict[str, m.PriceLevel] = {}
        self._asks: dict[str, m.PriceLevel] = {}

    @property
    def state(self) -> BookSyncState:
        return self._state

    @property
    def transitions(self) -> tuple[BookSyncState, ...]:
        return tuple(self._transitions)

    @property
    def cause(self) -> str | None:
        return self._cause

    def _transition(self, state: BookSyncState) -> None:
        self._state = state
        self._transitions.append(state)

    def _fail(self, cause: str) -> None:
        self._cause = cause
        self._transition(BookSyncState.DEGRADED)
        self._transition(BookSyncState.RESYNC_REQUIRED)
        raise ContractError(cause)

    def begin(self) -> None:
        if self.state != BookSyncState.EMPTY:
            raise ContractError("INVALID_SYNC_STATE")
        self._transition(BookSyncState.BUFFERING)

    def request_snapshot(self) -> None:
        if self.state != BookSyncState.BUFFERING:
            raise ContractError("INVALID_SYNC_STATE")
        self._transition(BookSyncState.SNAPSHOT_REQUIRED)

    def ingest(self, event: m.BinanceDepthUpdate) -> str:
        if self.state not in (BookSyncState.BUFFERING, BookSyncState.SNAPSHOT_REQUIRED,
                              BookSyncState.BRIDGING, BookSyncState.HEALTHY):
            raise ContractError("INVALID_SYNC_STATE")
        try:
            validate_depth(event)
        except ContractError as error:
            self._fail(str(error))
        if event.source.original_session_id != self._session:
            self._fail("ORIGINAL_SESSION_MISMATCH")
        content = _semantic(event)
        previous = self._seen.get(event.logical_event_id)
        if previous is not None:
            if previous != content:
                self._fail("VENUE_IDENTITY_CONFLICT")
            return "DUPLICATE"
        self._seen[event.logical_event_id] = content
        owned = m.BinanceDepthUpdate.FromString(event.SerializeToString(deterministic=True))
        if self.state == BookSyncState.HEALTHY:
            self._apply(owned, continuation=True)
            return "APPLIED"
        self._buffer.append(owned)
        self._bridge()
        return "APPLIED" if self._state == BookSyncState.HEALTHY else "BUFFERED"

    def snapshot(self, value: m.BinanceDepthSnapshot) -> None:
        if self.state not in (BookSyncState.SNAPSHOT_REQUIRED, BookSyncState.BRIDGING):
            raise ContractError("INVALID_SYNC_STATE")
        validate_snapshot(value)
        self._snapshot = m.BinanceDepthSnapshot.FromString(value.SerializeToString(deterministic=True))
        self._transition(BookSyncState.BRIDGING)
        self._bridge()

    def _bridge(self) -> None:
        if self.state != BookSyncState.BRIDGING or self._snapshot is None:
            return
        snapshot = self._snapshot
        self._buffer = [v for v in self._buffer if v.final_update_id >= snapshot.last_update_id]
        if not self._buffer or self._buffer[0].first_update_id > snapshot.last_update_id:
            return
        self._bids.clear()
        self._asks.clear()
        self._replace(list(snapshot.bids), self._bids)
        self._replace(list(snapshot.asks), self._asks)
        events, self._buffer = self._buffer, []
        self._apply(events[0], continuation=False)
        self._transition(BookSyncState.HEALTHY)
        for event in events[1:]:
            self._apply(event, continuation=True)

    @staticmethod
    def _replace(levels: list[m.PriceLevel], target: dict[str, m.PriceLevel]) -> None:
        for level in levels:
            if level.quantity.coefficient == "0":
                target.pop(_key(level), None)
            else:
                target[_key(level)] = m.PriceLevel(price=level.price, quantity=level.quantity)

    def _apply(self, event: m.BinanceDepthUpdate, *, continuation: bool) -> None:
        if continuation and event.previous_final_update_id != self._previous:
            self._fail("VENUE_GAP")
        self._replace(list(event.bids), self._bids)
        self._replace(list(event.asks), self._asks)
        self._previous = event.final_update_id

    def trusted_book(self) -> tuple[list[m.PriceLevel], list[m.PriceLevel]]:
        if self.state != BookSyncState.HEALTHY:
            raise ContractError("BOOK_NOT_HEALTHY")
        def level_compare(a: m.PriceLevel, b: m.PriceLevel) -> int:
            return compare(a.price, b.price)
        def ordered(values: dict[str, m.PriceLevel], reverse: bool) -> list[m.PriceLevel]:
            return [m.PriceLevel(price=v.price, quantity=v.quantity) for v in sorted(
                values.values(), key=cmp_to_key(level_compare), reverse=reverse)]
        return ordered(self._bids, True), ordered(self._asks, False)
