from datetime import datetime
from typing import Any, Dict, List, Optional

import numpy
import pandas as pd
from sqlalchemy import JSON, VARCHAR, BigInteger, Column, ForeignKey, Index, Integer, Numeric, Text
from sqlalchemy.orm import Session, relationship

from hummingbot.core.event.events import PositionAction
from hummingbot.model import HummingbotBase


class TradeFill(HummingbotBase):
    __tablename__ = "TradeFill"
    __table_args__ = (Index("tf_config_timestamp_index",
                            "config_file_path", "timestamp"),
                      Index("tf_market_trading_pair_timestamp_index",
                            "market", "symbol", "timestamp"),
                      Index("tf_market_base_asset_timestamp_index",
                            "market", "base_asset", "timestamp"),
                      Index("tf_market_quote_asset_timestamp_index",
                            "market", "quote_asset", "timestamp")
                      )

    config_file_path = Column(VARCHAR(255), nullable=False)
    strategy = Column(Text, nullable=False)
    market = Column(VARCHAR(255), primary_key=True, nullable=False)
    symbol = Column(VARCHAR(255), nullable=False)
    base_asset = Column(VARCHAR(255), nullable=False)
    quote_asset = Column(VARCHAR(255), nullable=False)
    timestamp = Column(BigInteger, nullable=False)
    order_id = Column(VARCHAR(255), ForeignKey("Order.id"), primary_key=True, nullable=False)
    trade_type = Column(Text, nullable=False)
    order_type = Column(Text, nullable=False)
    price = Column(Numeric(48, 18), nullable=False)
    amount = Column(Numeric(48, 18), nullable=False)
    leverage = Column(Integer, nullable=False, default=1)
    trade_fee = Column(JSON, nullable=False)
    exchange_trade_id = Column(VARCHAR(255), primary_key=True, nullable=False)
    position = Column(Text, nullable=True, default=PositionAction.NIL.value)
    order = relationship("Order", back_populates="trade_fills")
    base_rate = Column(Numeric(48, 18), nullable=False)
    quote_rate = Column(Numeric(48, 18), nullable=False)

    def __repr__(self) -> str:
        return f"TradeFill(config_file_path='{self.config_file_path}', strategy='{self.strategy}', " \
               f"market='{self.market}', symbol='{self.symbol}', base_asset='{self.base_asset}', " \
               f"quote_asset='{self.quote_asset}', timestamp={self.timestamp}, order_id='{self.order_id}', " \
               f"trade_type='{self.trade_type}', order_type='{self.order_type}', price={self.price}, " \
               f"amount={self.amount}, leverage={self.leverage}, trade_fee={self.trade_fee}, " \
               f"exchange_trade_id={self.exchange_trade_id}, position={self.position}), " \
               f"base_rate='{self.base_asset}', quote_rate={self.quote_rate}"

    @staticmethod
    def get_trades(sql_session: Session,
                   strategy: str = None,
                   market: str = None,
                   trading_pair: str = None,
                   base_asset: str = None,
                   quote_asset: str = None,
                   trade_type: str = None,
                   order_type: str = None,
                   start_time: int = None,
                   end_time: int = None,
                   ) -> Optional[List["TradeFill"]]:
        filters = []
        if strategy is not None:
            filters.append(TradeFill.strategy == strategy)
        if market is not None:
            filters.append(TradeFill.market == market)
        if trading_pair is not None:
            filters.append(TradeFill.symbol == trading_pair)
        if base_asset is not None:
            filters.append(TradeFill.base_asset == base_asset)
        if quote_asset is not None:
            filters.append(TradeFill.quote_asset == quote_asset)
        if trade_type is not None:
            filters.append(TradeFill.trade_type == trade_type)
        if order_type is not None:
            filters.append(TradeFill.order_type == order_type)
        if start_time is not None:
            filters.append(TradeFill.timestamp >= start_time)
        if end_time is not None:
            filters.append(TradeFill.timestamp <= end_time)

        trades: Optional[List[TradeFill]] = (sql_session
                                             .query(TradeFill)
                                             .filter(*filters)
                                             .order_by(TradeFill.timestamp.asc())
                                             .all())
        return trades

    @classmethod
    def to_pandas(cls, trades: List):
        columns: List[str] = ["Id",
                              "Timestamp",
                              "Exchange",
                              "Market",
                              "Order_type",
                              "Side",
                              "Price",
                              "Amount",
                              "Leverage",
                              "Position",
                              "Age",
                              "Base_rate",
                              "Quote_rate"
                              ]
        data = []
        for trade in trades:

            # // indicates order is a paper order so 'n/a'. For real orders, calculate age.
            age = pd.Timestamp(int(trade.timestamp / 1e3 - trade.order.creation_timestamp / 1e3),
                               unit='s').strftime('%H:%M:%S')
            data.append([
                trade.exchange_trade_id,
                datetime.fromtimestamp(int(trade.timestamp / 1e3)).strftime("%Y-%m-%d %H:%M:%S"),
                trade.market,
                trade.symbol,
                trade.order_type.lower(),
                trade.trade_type.lower(),
                trade.price,
                trade.amount,
                trade.leverage,
                trade.position,
                age,
                trade.base_rate,
                trade.quote_rate
            ])
        df = pd.DataFrame(data=data, columns=columns)
        df.set_index('Id', inplace=True)

        return df

    @staticmethod
    def to_bounty_api_json(trade_fill: "TradeFill") -> Dict[str, Any]:
        return {
            "market": trade_fill.market,
            "trade_id": trade_fill.exchange_trade_id,
            "price": numpy.format_float_positional(trade_fill.price),
            "quantity": numpy.format_float_positional(trade_fill.amount),
            "symbol": trade_fill.symbol,
            "trade_timestamp": trade_fill.timestamp,
            "trade_type": trade_fill.trade_type,
            "base_asset": trade_fill.base_asset,
            "quote_asset": trade_fill.quote_asset,
            "base_rate": numpy.format_float_positional(trade_fill.base_rate),
            "quote_rate": numpy.format_float_positional(trade_fill.quote_rate),
            "raw_json": {
                "trade_fee": trade_fill.trade_fee,
            }
        }

    @staticmethod
    def attribute_names_for_file_export():

        return [
            "exchange_trade_id",  # Keep the key attribute first in the list
            "config_file_path",
            "strategy",
            "market",
            "symbol",
            "base_asset",
            "quote_asset",
            "timestamp",
            "order_id",
            "trade_type",
            "order_type",
            "price",
            "amount",
            "leverage",
            "trade_fee",
            "position",
            "base_rate",
            "quote_rate", ]
