import hashlib
from typing import Any, Generator

import pandas as pd
import pyarrow as pa
from cloudquery.sdk.scheduler import TableResolver
from cloudquery.sdk.schema import Column
from cloudquery.sdk.schema import Table
from cloudquery.sdk.schema.resource import Resource

from plugin.client import Client
from ..youtube.client import get_thumnail_hash


class VideoMeta(Table):
    def __init__(self) -> None:
        super().__init__(
            name="yt_video_meta",
            title="Video Meta",
            is_incremental=True,
            columns=[
                Column("video", pa.string()),
                Column("uniqueKey", pa.string(), unique=True, primary_key=True),
                Column("sampleTS", pa.timestamp(unit="s")),
                Column("title", pa.string()),
                Column("description", pa.string()),
                Column("thumbnailHash", pa.string()),
                Column("durationIso", pa.string()),
                Column("durationHours", pa.int64()),
                Column("durationMins", pa.int64()),
                Column("durationSecs", pa.int64()),
            ],
            relations=[],
        )

    @property
    def resolver(self):
        return VideoMetaResolver(table=self)


class VideoMetaResolver(TableResolver):
    def __init__(self, table) -> None:
        super().__init__(table=table)

    def resolve(
        self, client: Client, parent_resource: Resource
    ) -> Generator[Any, None, None]:
        for video_response in client.client.yt_video_fetch(
            parent_resource.item["video"]
        ):
            video_response["thumbnailHash"] = get_thumnail_hash(
                parent_resource.item["video"]
            )
            duration = pd.Timedelta(video_response["durationIso"])
            video_response["durationHours"] = duration.seconds // 3600
            video_response["durationMins"] = duration.seconds // 60
            video_response["durationSecs"] = (
                duration.seconds
                - (video_response["durationMins"] * 60)
                - (video_response["durationHours"] * 3600)
            )
            title_hash = hashlib.sha256(video_response["title"].encode()).hexdigest()
            description_hash = hashlib.sha256(
                video_response["description"].encode()
            ).hexdigest()
            video_response[
                "uniqueKey"
            ] = f"{parent_resource.item['video']};{title_hash};{video_response['thumbnailHash']};{description_hash};"
            yield video_response
