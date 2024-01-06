from typing import Any, Generator

import pyarrow as pa
from cloudquery.sdk.scheduler import TableResolver
from cloudquery.sdk.schema import Column
from cloudquery.sdk.schema import Table
from cloudquery.sdk.schema.resource import Resource

from plugin.client import Client
from .view_source_detail import ViewSourceDetail
from ..youtube.client import midnight_from_datetime


class ViewSourceType(Table):
    def __init__(self) -> None:
        super().__init__(
            name="yt_video_view_source_types",
            title="Video View Source Type",
            is_incremental=True,
            columns=[
                Column("video", pa.string()),
                Column("uniqueKey", pa.string(), unique=True, primary_key=True),
                Column("sampleStartTS", pa.timestamp(unit="s")),
                Column("sampleEndTS", pa.timestamp(unit="s")),
                Column("insightTrafficSourceType", pa.string()),
                Column("views", pa.int64()),
            ],
        )

    @property
    def resolver(self):
        return ViewSourceTypesResolver(table=self)


class ViewSourceTypesResolver(TableResolver):
    def __init__(self, table) -> None:
        super().__init__(table=table)

    def resolve(
        self, client: Client, parent_resource: Resource
    ) -> Generator[Any, None, None]:
        for video_response in client.client.yt_analytics_iterator(
            dimensions=["insightTrafficSourceType"],
            metrics=["views"],
            sort="-views",
            filters=[f"video=={parent_resource.item['video']}"],
            start_date=midnight_from_datetime(parent_resource.item["publishedAt"]),
        ):
            video_response[
                "uniqueKey"
            ] = f"{parent_resource.item['video']};{video_response['sampleStartTS']};{video_response['insightTrafficSourceType']}"
            video_response["video"] = parent_resource.item["video"]
            yield video_response
