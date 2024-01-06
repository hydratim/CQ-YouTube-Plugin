from datetime import datetime, time
from typing import Any, Generator

import pyarrow as pa
from cloudquery.sdk.scheduler import TableResolver
from cloudquery.sdk.schema import Column
from cloudquery.sdk.schema import Table
from cloudquery.sdk.schema.resource import Resource

from plugin.client import Client
from ..youtube.client import midnight_from_datetime


class VideoMetrics(Table):
    def __init__(self) -> None:
        super().__init__(
            name="yt_video_metrics",
            title="Video Metrics",
            is_incremental=True,
            columns=[
                Column("video", pa.string()),
                Column("uniqueKey", pa.string(), unique=True, primary_key=True),
                Column("sampleStartTS", pa.timestamp(unit="s")),
                Column("sampleEndTS", pa.timestamp(unit="s")),
                Column("averageViewPercentage", pa.float64()),
                Column("comments", pa.int64()),
                Column("dislikes", pa.int64()),
                Column("likes", pa.int64()),
                Column("shares", pa.int64()),
                Column("subscribersGained", pa.int64()),
                Column("subscribersLost", pa.int64()),
                Column("views", pa.int64()),
            ],
            relations=[],
        )

    @property
    def resolver(self):
        return VideoMetricsResolver(table=self)


class VideoMetricsResolver(TableResolver):
    def __init__(self, table) -> None:
        super().__init__(table=table)

    def resolve(
        self, client: Client, parent_resource: Resource
    ) -> Generator[Any, None, None]:
        for video_response in client.client.yt_analytics_iterator(
            dimensions=["video"],
            metrics=[
                "averageViewPercentage",
                "comments",
                "dislikes",
                "likes",
                "shares",
                "subscribersGained",
                "subscribersLost",
                "views",
            ],
            sort="-views",
            filters=[f"video=={parent_resource.item['video']}"],
            start_date=midnight_from_datetime(parent_resource.item["publishedAt"]),
        ):
            video_response[
                "uniqueKey"
            ] = f"{parent_resource.item['video']};{video_response['sampleStartTS']}"
            video_response["video"] = parent_resource.item["video"]
            yield video_response
