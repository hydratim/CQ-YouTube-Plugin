from typing import Any, Generator

import pyarrow as pa
from cloudquery.sdk.scheduler import TableResolver
from cloudquery.sdk.schema import Column
from cloudquery.sdk.schema import Table
from cloudquery.sdk.schema.resource import Resource

from plugin.client import Client


class ChannelMetrics(Table):
    def __init__(self) -> None:
        super().__init__(
            name="yt_channel_metrics",
            title="Channel Metrics",
            is_incremental=True,
            columns=[
                Column("sampleStartTS", pa.timestamp(unit="s"), primary_key=True),
                Column("sampleEndTS", pa.timestamp(unit="s")),
                Column("comments", pa.int64()),
                Column("dislikes", pa.int64()),
                Column("likes", pa.int64()),
                Column("shares", pa.int64()),
                Column("views", pa.int64()),
            ],
        )

    @property
    def resolver(self):
        return ChannelMetricsResolver(table=self)


class ChannelMetricsResolver(TableResolver):
    def __init__(self, table) -> None:
        super().__init__(table=table)

    def resolve(
        self, client: Client, parent_resource: Resource
    ) -> Generator[Any, None, None]:
        for video_response in client.client.yt_analytics_iterator(
            dimensions=["channel"],
            metrics=[
                "comments",
                "dislikes",
                "likes",
                "shares",
                "views",
            ],
            sort="-views",
            filters=[],
            aggregate_unit=7,
        ):
            yield video_response
