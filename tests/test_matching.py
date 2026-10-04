from collections.abc import Callable
from datetime import datetime

import pytest

from vnpy.event import Event
from vnpy.trader.constant import Direction, Exchange, Offset, OrderType, Product, Status
from vnpy.trader.event import EVENT_TICK, EVENT_TRADE
from vnpy.trader.object import (
    BarData,
    CancelRequest,
    ContractData,
    HistoryRequest,
    OrderData,
    OrderRequest,
    PositionData,
    QuoteRequest,
    SubscribeRequest,
    TickData,
    TradeData,
)
from vnpy.trader.utility import TEMP_DIR
from vnpy_paperaccount.engine import PaperEngine


class FakeEventEngine:
    def __init__(self) -> None:
        self._handlers: dict[str, list[Callable[[Event], None]]] = {}
        self.events: list[Event] = []

    def register(self, event_type: str, handler: Callable[[Event], None]) -> None:
        handlers: list[Callable[[Event], None]] = self._handlers.setdefault(event_type, [])
        if handler not in handlers:
            handlers.append(handler)

    def put(self, event: Event) -> None:
        self.events.append(event)
        handler: Callable[[Event], None]
        for handler in list(self._handlers.get(event.type, [])):
            handler(event)


class FakeMainEngine:
    def __init__(self) -> None:
        self.contracts: dict[str, ContractData] = {}

    def get_contract(self, vt_symbol: str) -> ContractData | None:
        return self.contracts.get(vt_symbol)

    def get_all_gateway_names(self) -> list[str]:
        return []

    def subscribe(self, req: SubscribeRequest, gateway_name: str) -> None:
        return None

    def query_history(self, req: HistoryRequest, gateway_name: str) -> list[BarData]:
        return []

    def send_order(self, req: OrderRequest, gateway_name: str) -> str:
        return ""

    def cancel_order(self, req: CancelRequest, gateway_name: str) -> None:
        return None

    def send_quote(self, req: QuoteRequest, gateway_name: str) -> str:
        return ""

    def cancel_quote(self, req: CancelRequest, gateway_name: str) -> None:
        return None


class MatchingRig:
    def __init__(
        self,
        engine: PaperEngine,
        event_engine: FakeEventEngine,
        contract: ContractData,
    ) -> None:
        self.engine: PaperEngine = engine
        self.event_engine: FakeEventEngine = event_engine
        self.contract: ContractData = contract

    def send_limit(self, price: float, volume: float) -> OrderData:
        req: OrderRequest = OrderRequest(
            symbol=self.contract.symbol,
            exchange=self.contract.exchange,
            direction=Direction.LONG,
            type=OrderType.LIMIT,
            volume=volume,
            price=price,
            offset=Offset.OPEN,
        )
        vt_orderid: str = self.engine.send_order(req, "CTP")
        orderid: str = vt_orderid.split(".", 1)[1]
        return self.engine.active_orders[self.contract.vt_symbol][orderid]

    def push_tick(self, bid_price: float, ask_price: float) -> None:
        tick: TickData = TickData(
            symbol=self.contract.symbol,
            exchange=self.contract.exchange,
            datetime=datetime(2026, 10, 5, 9, 30),
            last_price=ask_price,
            bid_price_1=bid_price,
            ask_price_1=ask_price,
            gateway_name="CTP",
        )
        self.event_engine.put(Event(EVENT_TICK, tick))

    def trades(self) -> list[TradeData]:
        trades: list[TradeData] = []
        event: Event
        for event in self.event_engine.events:
            if event.type == EVENT_TRADE:
                trade: TradeData = event.data
                trades.append(trade)
        return trades


def _clear_saved_state() -> None:
    filename: str
    for filename in ("paper_account_data.json", "paper_account_setting.json"):
        path = TEMP_DIR.joinpath(filename)
        if path.exists():
            path.unlink()


def _build_rig() -> MatchingRig:
    _clear_saved_state()
    contract: ContractData = ContractData(
        symbol="rb2501",
        exchange=Exchange.SHFE,
        name="rebar",
        product=Product.FUTURES,
        size=10,
        pricetick=1,
        net_position=True,
        gateway_name="CTP",
    )
    main_engine: FakeMainEngine = FakeMainEngine()
    main_engine.contracts[contract.vt_symbol] = contract
    events: FakeEventEngine = FakeEventEngine()
    engine: PaperEngine = PaperEngine(main_engine, events)  # type: ignore[arg-type]
    engine.instant_trade = False
    return MatchingRig(engine, events, contract)


@pytest.fixture
def rig() -> MatchingRig:
    return _build_rig()


class TestPaperMatching:
    def test_resting_limit_stays_open_when_tick_does_not_cross(self, rig: MatchingRig) -> None:
        order: OrderData = rig.send_limit(3500, 3)
        assert order.status == Status.NOTTRADED
        assert order.traded == 0
        assert order.is_active()

        rig.push_tick(bid_price=3498, ask_price=3502)

        assert order.status == Status.NOTTRADED
        assert order.traded == 0
        assert order.is_active()
        assert order.orderid in rig.engine.active_orders[rig.contract.vt_symbol]
        assert rig.trades() == []
        assert (rig.contract.vt_symbol, Direction.NET) not in rig.engine.positions

    def test_resting_limit_trades_when_tick_crosses(self, rig: MatchingRig) -> None:
        order: OrderData = rig.send_limit(3500, 3)
        assert order.status == Status.NOTTRADED
        assert order.is_active()

        rig.push_tick(bid_price=3498, ask_price=3499)

        assert order.status == Status.ALLTRADED
        assert order.traded == 3
        assert not order.is_active()
        assert order.orderid not in rig.engine.active_orders[rig.contract.vt_symbol]

        trades: list[TradeData] = rig.trades()
        assert len(trades) == 1
        assert trades[0].volume == 3
        assert trades[0].price == 3499
        assert trades[0].direction == Direction.LONG

        position: PositionData = rig.engine.get_position(rig.contract.vt_symbol, Direction.NET)
        assert position.volume == 3
        assert position.price == 3499
