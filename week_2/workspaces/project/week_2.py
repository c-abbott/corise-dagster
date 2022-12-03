from typing import List

from dagster import In, Nothing, Out, ResourceDefinition, String, graph, op
from workspaces.resources import mock_s3_resource, redis_resource, s3_resource
from workspaces.types import Aggregation, Stock


@op(
    config_schema={"s3_key": String},
    required_resource_keys={"S3"},
    tags={"kind": "S3"},
    description="Fetches stock data from S3 bucket."
)
def get_s3_data(context):
    s3_key = context.op_config["s3_key"]
    return [Stock.from_list(item) for item in context.resources.S3.get_data(s3_key)]
   

@op(
    ins={"stocks": In(dagster_type=List[Stock])},
    out={"agg": Out(dagster_type=Aggregation)}
    description: "Returns the stock with the highest price and when that price was attained."
)
def process_data(context, stocks: List[Stock]) -> Aggregation:
    highest_stock = max(stocks, key= lambda stock: stock.high)
    return Aggregation(date=highest_stock.date,high=highest_stock.high)


@op(
    required_resource_keys={"Redis"},
    tags={"kind": "Redis"}
    description="Take highest Stock date and value and upload this data to Redis cache."
)
def put_redis_data(context, agg: Aggregation) -> Nothing:
    context.resources.Redis.put_data(name=str(agg.date), value=str(agg.high))

@op(
    required_resource_keys={"S3"},
    tags={"kind": "S3"}
    description="Take highest Stock date and value and upload this data to S3 bucket."
)
def put_s3_data(context, agg: Aggregation) -> Nothing:
    context.resources.S3.put_data(key_name=str(agg.date), data=agg)


@graph
def week_2_pipeline():
    stocks = get_s3_data()
    processed_stocks = process_data(stocks)
    put_redis_data(processed_stocks)
    put_s3_data(processed_stocks)


local = {
    "ops": {"get_s3_data": {"config": {"s3_key": "prefix/stock.csv"}}},
}

docker = {
    "resources": {
        "s3": {
            "config": {
                "bucket": "dagster",
                "access_key": "test",
                "secret_key": "test",
                "endpoint_url": "http://localstack:4566",
            }
        },
        "redis": {
            "config": {
                "host": "redis",
                "port": 6379,
            }
        },
    },
    "ops": {"get_s3_data": {"config": {"s3_key": "prefix/stock.csv"}}},
}

week_2_pipeline_local = week_2_pipeline.to_job(
    name="week_2_pipeline_local",
)

week_2_pipeline_docker = week_2_pipeline.to_job(
    name="week_2_pipeline_docker",
)
