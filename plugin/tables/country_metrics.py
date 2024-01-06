from typing import Any, Generator

import pyarrow as pa
from cloudquery.sdk.scheduler import TableResolver
from cloudquery.sdk.schema import Column
from cloudquery.sdk.schema import Table
from cloudquery.sdk.schema.resource import Resource

from plugin.client import Client


class CountryMetrics(Table):
    def __init__(self) -> None:
        super().__init__(
            name="yt_country_metrics",
            title="Country Metrics",
            is_incremental=True,
            columns=[
                Column("uniqueKey", pa.string(), unique=True, primary_key=True),
                Column("country", pa.string()),
                Column("sampleStartTS", pa.timestamp(unit="s")),
                Column("sampleEndTS", pa.timestamp(unit="s")),
                Column("views", pa.int64()),
            ],
        )

    @property
    def resolver(self):
        return CountryMetricsResolver(table=self)


class CountryMetricsResolver(TableResolver):
    def __init__(self, table) -> None:
        super().__init__(table=table)

    def resolve(
        self, client: Client, parent_resource: Resource
    ) -> Generator[Any, None, None]:
        for video_response in client.client.yt_analytics_iterator(
            dimensions=["country"],
            metrics=[
                "views",
            ],
            sort="views",
            filters=[],
            aggregate_unit=7,
        ):
            video_response[
                "uniqueKey"
            ] = f"{video_response['country']};{video_response['sampleStartTS']}"
            yield video_response
