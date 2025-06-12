from copy import copy
from datetime import datetime
from tzlocal import get_localzone_name

from vnpy.event import Event, EventEngine
from vnpy.trader.utility import extract_vt_symbol, save_json, load_json, ZoneInfo
from vnpy.trader.engine import BaseEngine, MainEngine
from vnpy.trader.gateway import BaseGateway
from vnpy.trader.object import (
    OrderRequest, CancelRequest, QuoteData, QuoteRequest, SubscribeRequest,
    ContractData, OrderData, TradeData, TickData,
    LogData, PositionData, HistoryRequest, BarData
)
from vnpy.trader.event import (
    EVENT_ORDER,
    EVENT_QUOTE,
    EVENT_TRADE,
    EVENT_TICK,
    EVENT_POSITION,
    EVENT_CONTRACT,
    EVENT_LOG,
    EVENT_TIMER
)
from vnpy.trader.constant import (
    Status,
    OrderType,
    Direction,
    Offset
)


LOCAL_TZ = ZoneInfo(get_localzone_name())
APP_NAME = "PaperAccount"
GATEWAY_NAME = "PAPER"

EVENT_PAPER_NEW_ORDER = "ePaperNewOrder"
EVENT_PAPER_CANCEL_ORDER = "ePaperCancelOrder"
EVENT_PAPER_NEW_QUOTE = "ePaperNewQuote"
EVENT_PAPER_CANCEL_QUOTE = "ePaperCancelQuote"


class PaperEngine(BaseEngine):
    """"""
    setting_filename: str = "paper_account_setting.json"
    data_filename: str = "paper_account_data.json"

    def __init__(self, main_engine: MainEngine, event_engine: EventEngine) -> None:
        """"""
        super().__init__(main_engine, event_engine, APP_NAME)

        self.trade_slippage: int = 0
        self.timer_interval: int = 3
        self.instant_trade: bool = False

        self.order_count: int = 100000
        self.quote_count: int = 100000
        self.trade_count: int = 0
        self.timer_count: int = 0

        self.active_orders: dict[str, dict[str, OrderData]] = {}
        self.active_quotes: dict[str, QuoteData] = {}
        self.gateway_map: dict[str, str] = {}
        self.ticks: dict[str, TickData] = {}
        self.positions: dict[tuple[str, Direction], PositionData] = {}

        # Patch main engine functions
        self._subscribe = main_engine.subscribe
        self._query_history = main_engine.query_history

        main_engine.subscribe = self.subscribe
        main_engine.query_history = self.query_history
        main_engine.send_order = self.send_order
        main_engine.cancel_order = self.cancel_order
        main_engine.send_quote = self.send_quote
        main_engine.cancel_quote = self.cancel_quote

        self.load_setting()
        self.load_data()
        self.register_event()

        if "IB" in main_engine.get_all_gateway_names():
            self.ib_gateway: BaseGateway = main_engine.get_gateway("IB")
        else:
            self.ib_gateway = None

    def register_event(self) -> None:
        """"""
        self.event_engine.register(EVENT_CONTRACT, self.process_contract_event)
        self.event_engine.register(EVENT_TICK, self.process_tick_event)
        self.event_engine.register(EVENT_TIMER, self.process_timer_event)

        self.event_engine.register(EVENT_PAPER_NEW_ORDER, self.process_new_order_event)
        self.event_engine.register(EVENT_PAPER_CANCEL_ORDER, self.process_cancel_order_event)
        self.event_engine.register(EVENT_PAPER_NEW_QUOTE, self.process_new_quote_event)
        self.event_engine.register(EVENT_PAPER_CANCEL_QUOTE, self.process_cancel_quote_event)

    def process_contract_event(self, event: Event) -> None:
        """"""
        contract: ContractData = event.data
        self.gateway_map[contract.vt_symbol] = contract.gateway_name
        contract.gateway_name = GATEWAY_NAME

        for direciton in Direction:
            key: tuple = (contract.vt_symbol, direciton)
            if key in self.positions:
                position: PositionData = self.positions[key]
                self.put_event(EVENT_POSITION, position)

    def process_tick_event(self, event: Event) -> None:
        """"""
        tick: TickData = event.data
        tick.gateway_name = GATEWAY_NAME

        self.ticks[tick.vt_symbol] = tick

        active_orders: dict | None = self.active_orders.get(tick.vt_symbol, None)
        if active_orders:
            for orderid, order in list(active_orders.items()):
                self.cross_order(order, tick)

                if not order.is_active():
                    active_orders.pop(orderid)

        quote: QuoteData | None = self.active_quotes.get(tick.vt_symbol, None)
        if quote:
            self.cross_quote(quote, tick)

            if not quote.is_active():
                self.active_quotes.pop(tick.vt_symbol)

    def process_timer_event(self, event: Event) -> None:
        """"""
        self.timer_count += 1
        if self.timer_count < self.timer_interval:
            return
        self.timer_count = 0

        for position in self.positions.values():
            contract: ContractData | None = self.main_engine.get_contract(position.vt_symbol)
            if contract:
                self.calculate_pnl(position)
                self.put_event(EVENT_POSITION, copy(position))

    def calculate_pnl(self, position: PositionData) -> None:
        """"""
        tick: TickData | None = self.ticks.get(position.vt_symbol, None)
        contract: ContractData | None = self.main_engine.get_contract(position.vt_symbol)

        if tick and contract:

            if position.direction == Direction.SHORT:
                multiplier: float = -position.volume * contract.size
            else:
                multiplier = position.volume * contract.size

            position.pnl = (tick.last_price - position.price) * multiplier
            position.pnl = round(position.pnl, 2)

    def subscribe(self, req: SubscribeRequest, gateway_name: str) -> None:
        """"""
        original_gateway_name: str = self.gateway_map.get(req.vt_symbol, "")
        if original_gateway_name:
            self._subscribe(req, original_gateway_name)
        elif self.ib_gateway and req.exchange in self.ib_gateway.exchanges:
            self._subscribe(req, "IB")
        else:
            self.write_log(f"订阅行情失败，找不到该合约{req.vt_symbol}")

    def query_history(self, req: HistoryRequest, gateway_name: str) -> list[BarData] | None:
        """"""
        original_gateway_name: str = self.gateway_map.get(req.vt_symbol, "")
        if original_gateway_name:
            data: list[BarData] = self._query_history(req, original_gateway_name)
            return data
        elif self.ib_gateway and req.exchange in self.ib_gateway.exchanges:
            data = self._query_history(req, "IB")
            return data
        else:
            return None

    def send_order(self, req: OrderRequest, gateway_name: str) -> str:
        """"""
        if not req.volume:
            self.write_log("委托数量非法，请检查")
            return ""

        contract: ContractData | None = self.main_engine.get_contract(req.vt_symbol)
        if not contract:
            self.write_log(f"委托失败，找不到该合约{req.vt_symbol}")
            return ""

        self.order_count += 1
        now: str = datetime.now().strftime("%y%m%d%H%M%S")
        orderid: str = now + str(self.order_count)
        vt_orderid: str = f"{GATEWAY_NAME}.{orderid}"

        # Put simulated order update event from gateway
        order: OrderData = req.create_order_data(orderid, GATEWAY_NAME)
        self.put_event(EVENT_ORDER, copy(order))
        self.put_event(EVENT_PAPER_NEW_ORDER, order)

        return vt_orderid

    def process_new_order_event(self, event: Event) -> None:
        """"""
        # Check if order is valid
        order: OrderData = event.data
        contract = self.main_engine.get_contract(order.vt_symbol)

        updated_position: PositionData = self.check_order_valid(order, contract)

        # Put simulated order update event from exchange
        if order.status != Status.REJECTED:
            order.datetime = datetime.now(LOCAL_TZ)
            order.status = Status.NOTTRADED
            active_orders: dict = self.active_orders.setdefault(order.vt_symbol, {})
            active_orders[order.orderid] = order

        self.put_event(EVENT_ORDER, copy(order))

        # Update position frozen for close order
        if updated_position:
            self.put_event(EVENT_POSITION, copy(updated_position))

        # Cross order immediately with last tick data
        if self.instant_trade and order.status != Status.REJECTED:
            tick: TickData | None = self.ticks.get(order.vt_symbol, None)
            if tick:
                self.cross_order(order, tick)

                if not order.is_active():
                    active_orders = self.active_orders[order.vt_symbol]
                    active_orders.pop(order.orderid)

    def cancel_order(self, req: CancelRequest, gateway_name: str) -> None:
        """"""
        self.put_event(EVENT_PAPER_CANCEL_ORDER, req)

    def process_cancel_order_event(self, event: Event) -> None:
        """"""
        req: CancelRequest = event.data

        active_orders: dict[str, OrderData] = self.active_orders[req.vt_symbol]

        if req.orderid in active_orders:
            order: OrderData = active_orders.pop(req.orderid)
            order.status = Status.CANCELLED
            self.put_event(EVENT_ORDER, copy(order))

            # Free frozen position volume
            contract: ContractData = self.main_engine.get_contract(order.vt_symbol)
            if contract.net_position:
                return

            if order.offset == Offset.OPEN:
                return

            if order.direction == Direction.LONG:
                position: PositionData = self.get_position(order.vt_symbol, Direction.SHORT)
            else:
                position = self.get_position(order.vt_symbol, Direction.LONG)
            position.frozen -= order.volume

            self.put_event(EVENT_POSITION, copy(position))

    def send_quote(self, req: QuoteRequest, gateway_name: str) -> str:
        """"""
        contract: ContractData | None = self.main_engine.get_contract(req.vt_symbol)
        if not contract:
            self.write_log(f"报价失败，找不到该合约{req.vt_symbol}")
            return ""

        self.quote_count += 1
        now: str = datetime.now().strftime("%y%m%d%H%M%S")
        quoteid: str = now + str(self.quote_count)
        vt_quoteid: str = f"{GATEWAY_NAME}.{quoteid}"

        # Put simulated quote update event from gateway
        quote: QuoteData = req.create_quote_data(quoteid, GATEWAY_NAME)
        self.put_event(EVENT_QUOTE, copy(quote))
        self.put_event(EVENT_PAPER_NEW_QUOTE, quote)

        return vt_quoteid

    def process_new_quote_event(self, event: Event) -> None:
        """"""
        quote: QuoteData = event.data
        # Put old quote cancel event
        if quote.vt_symbol in self.active_quotes:
            old_quote: QuoteData = self.active_quotes.pop(quote.vt_symbol)
            old_quote.status = Status.CANCELLED
            self.put_event(EVENT_QUOTE, old_quote)

        # Put simulated quote update event from exchange
        quote.datetime = datetime.now(LOCAL_TZ)
        quote.status = Status.NOTTRADED
        self.active_quotes[quote.vt_symbol] = quote

        self.put_event(EVENT_QUOTE, copy(quote))

    def cancel_quote(self, req: CancelRequest, gateway_name: str) -> None:
        """"""
        self.put_event(EVENT_PAPER_CANCEL_QUOTE, req)

    def process_cancel_quote_event(self, event: Event) -> None:
        """"""
        req: CancelRequest = event.data

        quote: QuoteData | None = self.active_quotes.get(req.vt_symbol, None)
        if not quote:
            return

        if req.orderid != quote.quoteid:
            return

        self.active_quotes.pop(req.vt_symbol)
        quote.status = Status.CANCELLED
        self.put_event(EVENT_QUOTE, copy(quote))

    def put_event(self, event_type: str, data: object) -> None:
        """"""
        event: Event = Event(event_type, data)
        self.event_engine.put(event)

    def check_order_valid(self, order: OrderData, contract: ContractData) -> PositionData | None:
        """"""
        # Reject unsupported order type
        if order.type in {OrderType.FAK, OrderType.FOK, OrderType.RFQ}:
            order.status = Status.REJECTED
        elif order.type == OrderType.STOP and not contract.stop_supported:
            order.status = Status.REJECTED

        if order.status == Status.REJECTED:
            self.write_log(f"委托被拒单，不支持的委托类型{order.type.value}")

        # Reject close order if no more available position
        if contract.net_position or order.offset == Offset.OPEN:
            return None

        if order.direction == Direction.LONG:
            short_position: PositionData = self.get_position(order.vt_symbol, Direction.SHORT)
            available: float = short_position.volume - short_position.frozen

            if order.volume > available:
                order.status = Status.REJECTED
                self.write_log("委托被拒单，可平仓位不足")
            else:
                short_position.frozen += order.volume
                return short_position
        else:
            long_position: PositionData = self.get_position(order.vt_symbol, Direction.LONG)
            available = long_position.volume - long_position.frozen

            if order.volume > available:
                order.status = Status.REJECTED
                self.write_log("委托被拒单，可平仓位不足")
            else:
                long_position.frozen += order.volume
                return long_position

        return None

    def cross_order(self, order: OrderData, tick: TickData) -> None:
        """"""
        contract: ContractData = self.main_engine.get_contract(order.vt_symbol)

        trade_price = 0

        # Cross market order immediately after received
        if order.type == OrderType.MARKET:
            if order.direction == Direction.LONG:
                trade_price = tick.ask_price_1 + self.trade_slippage * contract.pricetick
            else:
                trade_price = tick.bid_price_1 - self.trade_slippage * contract.pricetick
        # Cross limit order only if price touched
        elif order.type == OrderType.LIMIT:
            if order.direction == Direction.LONG:
                if order.price >= tick.ask_price_1:
                    trade_price = tick.ask_price_1
            else:
                if order.price <= tick.bid_price_1:
                    trade_price = tick.bid_price_1
        # Cross limit order only if price broken
        elif order.type == OrderType.STOP:
            if order.direction == Direction.LONG:
                if tick.ask_price_1 >= order.price:
                    trade_price = tick.ask_price_1 + self.trade_slippage * contract.pricetick
            else:
                if tick.bid_price_1 <= order.price:
                    trade_price = tick.bid_price_1 - self.trade_slippage * contract.pricetick

        if trade_price:
            order.status = Status.ALLTRADED
            order.traded = order.volume
            self.put_event(EVENT_ORDER, order)

            trade: TradeData = TradeData(
                symbol=order.symbol,
                exchange=order.exchange,
                orderid=order.orderid,
                tradeid=order.orderid,
                direction=order.direction,
                offset=order.offset,
                price=trade_price,
                volume=order.volume,
                datetime=datetime.now(LOCAL_TZ),
                gateway_name=order.gateway_name
            )
            self.put_event(EVENT_TRADE, trade)

            self.update_position(trade, contract)

    def cross_quote(self, quote: QuoteData, tick: TickData) -> None:
        """"""
        contract: ContractData | None = self.main_engine.get_contract(quote.vt_symbol)

        trade_price = 0

        if tick.last_price >= quote.ask_price and quote.ask_volume:
            trade_price = quote.ask_price

            direction: Direction = Direction.SHORT
            offset: Offset = Offset.CLOSE
            volume = quote.ask_volume

            quote.ask_volume = 0
        elif tick.last_price <= quote.bid_price and quote.bid_volume:
            trade_price = quote.bid_price

            direction = Direction.LONG
            offset = Offset.OPEN
            volume = quote.bid_volume

            quote.bid_volume = 0

        if trade_price:
            if not quote.bid_volume and not quote.ask_volume:
                quote.status = Status.ALLTRADED
            else:
                quote.status = Status.PARTTRADED
            self.put_event(EVENT_QUOTE, quote)

            self.trade_count += 1
            trade: TradeData = TradeData(
                symbol=quote.symbol,
                exchange=quote.exchange,
                orderid=str(self.trade_count),
                tradeid=str(self.trade_count),
                direction=direction,
                offset=offset,
                price=trade_price,
                volume=volume,
                datetime=datetime.now(LOCAL_TZ),
                gateway_name=quote.gateway_name
            )
            self.put_event(EVENT_TRADE, trade)

            self.update_position(trade, contract)

    def update_position(self, trade: TradeData, contract: ContractData) -> None:
        """"""
        vt_symbol: str = trade.vt_symbol

        # Net position mode
        if contract.net_position:
            position: PositionData = self.get_position(vt_symbol, Direction.NET)

            old_volume: float = position.volume
            old_cost: float = position.volume * position.price

            if trade.direction == Direction.LONG:
                pos_change = trade.volume
            else:
                pos_change = -trade.volume

            new_volume: float = position.volume + pos_change

            # No position holding, clear price
            if not new_volume:
                position.price = 0
            # Position direction changed, set to open price
            elif (
                (new_volume > 0 and old_volume < 0)
                or (new_volume < 0 and old_volume > 0)
            ):
                position.price = trade.price
            # Position is add on the same direction
            elif (
                (old_volume >= 0 and pos_change > 0)
                or (old_volume <= 0 and pos_change < 0)
            ):
                new_cost = old_cost + pos_change * trade.price
                position.price = new_cost / new_volume

            position.volume = new_volume
            self.calculate_pnl(position)
            self.put_event(EVENT_POSITION, copy(position))
        # Long/Short position mode
        else:
            long_position: PositionData = self.get_position(vt_symbol, Direction.LONG)
            short_position: PositionData = self.get_position(vt_symbol, Direction.SHORT)

            if trade.direction == Direction.LONG:
                if trade.offset == Offset.OPEN:
                    old_cost = long_position.volume * long_position.price
                    new_cost = old_cost + trade.volume * trade.price

                    long_position.volume += trade.volume
                    long_position.price = new_cost / long_position.volume
                else:
                    short_position.volume -= trade.volume
                    short_position.frozen -= trade.volume

                    if not short_position.volume:
                        short_position.price = 0
            else:
                if trade.offset == Offset.OPEN:
                    old_cost = short_position.volume * short_position.price
                    new_cost = old_cost + trade.volume * trade.price

                    short_position.volume += trade.volume
                    short_position.price = new_cost / short_position.volume
                else:
                    long_position.volume -= trade.volume
                    long_position.frozen -= trade.volume

                    if not long_position.volume:
                        long_position.price = 0

            self.calculate_pnl(long_position)
            self.calculate_pnl(short_position)

            self.put_event(EVENT_POSITION, copy(long_position))
            self.put_event(EVENT_POSITION, copy(short_position))

        self.save_data()

    def get_position(self, vt_symbol: str, direction: Direction) -> PositionData:
        """"""
        key: tuple = (vt_symbol, direction)

        if key in self.positions:
            return self.positions[key]
        else:
            symbol, exchange = extract_vt_symbol(vt_symbol)
            position: PositionData = PositionData(
                symbol=symbol,
                exchange=exchange,
                direction=direction,
                gateway_name=GATEWAY_NAME
            )

            self.positions[key] = position
            return position

    def write_log(self, msg: str) -> None:
        """"""
        log: LogData = LogData(msg=msg, gateway_name=GATEWAY_NAME)
        self.put_event(EVENT_LOG, log)

    def save_data(self) -> None:
        """"""
        position_data: list = []

        for position in self.positions.values():
            if not position.volume:
                continue

            d: dict = {
                "vt_symbol": position.vt_symbol,
                "volume": position.volume,
                "price": position.price,
                "direction": position.direction.value
            }
            position_data.append(d)

        save_json(self.data_filename, position_data)

    def load_data(self) -> None:
        """"""
        position_data: dict = load_json(self.data_filename)

        for d in position_data:
            vt_symbol: str = d["vt_symbol"]
            direction: Direction = Direction(d["direction"])

            position: PositionData = self.get_position(vt_symbol, direction)
            position.volume = d["volume"]
            position.price = d["price"]

    def load_setting(self) -> None:
        """"""
        setting: dict = load_json(self.setting_filename)

        if setting:
            self.trade_slippage = setting["trade_slippage"]
            self.timer_interval = setting["timer_interval"]
            self.instant_trade = setting["instant_trade"]

    def save_setting(self) -> None:
        """"""
        setting: dict = {
            "trade_slippage": self.trade_slippage,
            "timer_interval": self.timer_interval,
            "instant_trade": self.instant_trade
        }
        save_json(self.setting_filename, setting)

    def clear_position(self) -> None:
        """"""
        for position in self.positions.values():
            position.volume = 0
            position.frozen = 0
            position.price = 0
            self.put_event(EVENT_POSITION, position)

        self.save_data()

    def set_trade_slippage(self, trade_slippage: int) -> None:
        """"""
        self.trade_slippage = trade_slippage
        self.save_setting()

    def set_timer_interval(self, timer_interval: int) -> None:
        """"""
        self.timer_interval = timer_interval
        self.save_setting()

    def set_instant_trade(self, instant_trade: bool) -> None:
        """"""
        self.instant_trade = bool(instant_trade)
        self.save_setting()

    def get_trade_slippage(self) -> int:
        """"""
        return self.trade_slippage

    def get_timer_interval(self) -> int:
        """"""
        return self.timer_interval

    def get_instant_trade(self) -> bool:
        """"""
        return self.instant_trade
