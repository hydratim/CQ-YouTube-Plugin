from typing import Any, Generator

import pyarrow as pa
from cloudquery.sdk.scheduler import TableResolver
from cloudquery.sdk.schema import Column
from cloudquery.sdk.schema import Table
from cloudquery.sdk.schema.resource import Resource

from plugin.client import Client
from plugin.youtube.client import midnight_from_datetime


class ViewSourceDetail(Table):
    def __init__(self) -> None:
        super().__init__(
            name="yt_video_view_source_details",
            title="Video View Source Detail",
            is_incremental=True,
            columns=[
                Column("video", pa.string()),
                Column("uniqueKey", pa.string(), unique=True, primary_key=True),
                Column("sampleStartTS", pa.timestamp(unit="s")),
                Column("sampleEndTS", pa.timestamp(unit="s")),
                Column("insightTrafficSourceType", pa.string()),
                Column("insightTrafficSourceDetail", pa.string()),
                Column("views", pa.int64()),
            ],
        )

    @property
    def resolver(self):
        return ViewSourceDetailResolver(table=self)


class ViewSourceDetailResolver(TableResolver):
    def __init__(self, table) -> None:
        super().__init__(table=table)

    def resolve(
        self, client: Client, parent_resource: Resource
    ) -> Generator[Any, None, None]:
        for traffic_type in [
            "YT_SEARCH",
            "EXT_URL",
        ]:
            for video_response in client.client.yt_analytics_iterator(
                dimensions=["insightTrafficSourceDetail"],
                metrics=["views"],
                sort="-views",
                filters=[
                    f"video=={parent_resource.item['video']}",
                    f"insightTrafficSourceType=={traffic_type}",
                ],
                start_date=midnight_from_datetime(parent_resource.item["publishedAt"]),
            ):
                video_response[
                    "uniqueKey"
                ] = f"{parent_resource.item['video']};{video_response['sampleStartTS']};{traffic_type};{video_response['insightTrafficSourceDetail']}"
                video_response["insightTrafficSourceType"] = traffic_type
                video_response["video"] = parent_resource.item["video"]
                yield video_response
