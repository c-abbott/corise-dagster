import csv
from datetime import datetime
from heapq import nlargest
from random import randint
from typing import Iterator, List

from dagster import (
    Any,
    DynamicOut,
    DynamicOutput,
    In,
    Nothing,
    Out,
    Output,
    String,
    job,
    op,
    usable_as_dagster_type,
)
from pydantic import BaseModel


@usable_as_dagster_type(description="Stock data")
class Stock(BaseModel):
    date: datetime
    close: float
    volume: int
    open: float
    high: float
    low: float

    @classmethod
    def from_list(cls, input_list: List[List]):
        """Do not worry about this class method for now"""
        return cls(
            date=datetime.strptime(input_list[0], "%Y/%m/%d"),
            close=float(input_list[1]),
            volume=int(float(input_list[2])),
            open=float(input_list[3]),
            high=float(input_list[4]),
            low=float(input_list[5]),
        )


@usable_as_dagster_type(description="Aggregation of stock data")
class Aggregation(BaseModel):
    date: datetime
    high: float


def csv_helper(file_name: str) -> Iterator[Stock]:
    with open(file_name) as csvfile:
        reader = csv.reader(csvfile)
        for row in reader:
            yield Stock.from_list(row)


@op(
    config_schema={"s3_key": String},
    out={
        "stocks": Out(is_required=False, dagster_type=List[Stock]),
        "empty_stocks": Out(is_required=False, dagster_type=Any)
    }
)
def get_s3_data(context) -> List[Stock]:
    s3_key = context.op_config["s3_key"]
    data = csv_helper(s3_key)
    stocks = [stock for stock in data]
    if stocks:
        yield Output([stock for stock in data], "stocks")
    else:
        yield Output(None, "empty_stocks")


@op(
    config_schema={"nlargest": int},
    out=DynamicOut(is_required=False, dagster_type=Aggregation)
)
def process_data(context, stocks: List[Stock]) -> Aggregation:
    prices = [stock.high for stock in stocks]
    indices = [i for i in range(len(prices))]
    sorted_prices_and_indices = sorted (zip(prices, indices))
    nlargest_indices = [i for _, i in sorted_prices_and_indices][:context.op_config["nlargest"]-1]
    
    for index in nlargest_indices:
        yield DynamicOutput(Aggregation(date=stocks[index].date, high=stocks[index].high), mapping_key="aggregated_stocks") 


@op(
    out=Out(is_required=False, dagster_type=Nothing)
)
def put_redis_data(context, data: Aggregation):
    pass


@op(
    ins={"empty_stocks": In(dagster_type=Any)},
    description="Notifiy if stock list is empty",
)
def empty_stock_notify(context, empty_stocks) -> Nothing:
    context.log.info("No stocks returned")


@job
def week_1_challenge():
    stocks, empty_stocks = get_s3_data()

    empty_stock_notify(empty_stocks)

    aggregated_stock = process_data(stocks)
    aggregated_stock.map(put_redis_data).collect()
    