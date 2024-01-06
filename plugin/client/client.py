from dataclasses import field
from datetime import datetime
from zoneinfo import ZoneInfo

from pydantic.dataclasses import dataclass
from cloudquery.sdk.scheduler import Client as ClientABC

from plugin.youtube.client import YouTubeClient

DEFAULT_CONCURRENCY = 100
DEFAULT_QUEUE_SIZE = 10000


@dataclass
class Spec:
    access_token: str
    channel_id: str = field(default="MINE")
    start_datetime: datetime = field(default=datetime(year=2022, month=1, day=1))
    aggregate_unit: int = field(default=1)
    results_per_query: int = field(default=200)
    concurrency: int = field(default=DEFAULT_CONCURRENCY)
    queue_size: int = field(default=DEFAULT_QUEUE_SIZE)

    def validate(self):
        if self.access_token is None:
            raise Exception("access_token must be provided")


class Client(ClientABC):
    def __init__(self, spec: Spec) -> None:
        self._spec = spec
        self._client = YouTubeClient(
            self._spec.access_token,
            self._spec.channel_id,
            self._spec.results_per_query,
            self._spec.start_datetime.astimezone(tz=ZoneInfo("UTC")),
            self._spec.aggregate_unit,
        )

    def id(self):
        return "youtube"

    @property
    def client(self) -> YouTubeClient:
        return self._client
