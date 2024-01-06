from typing import Any, Generator

import pyarrow as pa
from cloudquery.sdk.scheduler import TableResolver
from cloudquery.sdk.schema import Column
from cloudquery.sdk.schema import Table
from cloudquery.sdk.schema.resource import Resource

from plugin.client import Client
from .video_meta import VideoMeta
from .video_metrics import VideoMetrics
from .view_source_type import ViewSourceType
from .view_source_detail import ViewSourceDetail
from ..youtube.client import midnight_from_datetime


class Videos(Table):
    def __init__(self) -> None:
        super().__init__(
            name="yt_video",
            title="Video",
            columns=[
                Column("video", pa.string(), primary_key=True),
                Column("publishedAt", pa.timestamp("s")),
                Column("publishedDate", pa.date64()),
            ],
            relations=[
                VideoMeta(),
                VideoMetrics(),
                ViewSourceType(),
                ViewSourceDetail(),
            ],
        )

    @property
    def resolver(self):
        return VideosResolver(table=self)


class VideosResolver(TableResolver):
    def __init__(self, table) -> None:
        super().__init__(table=table)

    def resolve(
        self, client: Client, parent_resource: Resource
    ) -> Generator[Any, None, None]:
        for video_response in client.client.yt_video_iterator():
            video_response["publishedDate"] = midnight_from_datetime(
                video_response["publishedAt"]
            )
            yield video_response

    @property
    def child_resolvers(self):
        return [table.resolver for table in self._table.relations]
