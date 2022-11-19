import csv
from datetime import datetime
from typing import Iterator, List

from dagster import In, Nothing, Out, String, job, op, usable_as_dagster_type
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


@op(config_schema={"s3_key": String})
def get_s3_data(context) -> List[Stock]:
    s3_key = context.op_config["s3_key"]
    data = csv_helper(s3_key)
    return [stock for stock in data]


@op
def process_data(context, stocks: List[Stock]) -> Aggregation:
    highest_price = 0.0
    highest_price_index = 0
    counter = 0
    for stock in stocks:
        if stock.high >= highest_price:
            highest_price = stock.high
            highest_price_index = counter
        counter += 1
    return Aggregation(date=stocks[highest_price_index].date, high=stocks[highest_price_index].high)


@op
def put_redis_data(context, aggregated_stock: Aggregation):
    pass


@job
def week_1_pipeline():
    data = get_s3_data()
    put_redis_data(process_data(data))
