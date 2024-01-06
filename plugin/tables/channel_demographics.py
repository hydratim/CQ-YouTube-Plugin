from typing import Any, Generator

import pyarrow as pa
from cloudquery.sdk.scheduler import TableResolver
from cloudquery.sdk.schema import Column
from cloudquery.sdk.schema import Table
from cloudquery.sdk.schema.resource import Resource

from plugin.client import Client


class ChannelDemographics(Table):
    def __init__(self) -> None:
        super().__init__(
            name="yt_channel_demographics",
            title="Channel Demographics",
            is_incremental=True,
            columns=[
                Column("uniqueKey", pa.string(), unique=True, primary_key=True),
                Column("sampleStartTS", pa.timestamp(unit="s")),
                Column("sampleEndTS", pa.timestamp(unit="s")),
                Column("ageGroup", pa.string()),
                Column("gender", pa.string()),
                Column("viewerPercentage", pa.float64()),
            ],
        )

    @property
    def resolver(self):
        return ChannelDemographicsResolver(table=self)


class ChannelDemographicsResolver(TableResolver):
    def __init__(self, table) -> None:
        super().__init__(table=table)

    def resolve(
        self, client: Client, parent_resource: Resource
    ) -> Generator[Any, None, None]:
        for video_response in client.client.yt_analytics_iterator(
            dimensions=["ageGroup", "gender"],
            metrics=["viewerPercentage"],
            sort="-ageGroup",
            filters=[],
            aggregate_unit=7,
        ):
            video_response[
                "uniqueKey"
            ] = f"{video_response['sampleStartTS']};{video_response['ageGroup']};{video_response['gender']}"
            yield video_response
