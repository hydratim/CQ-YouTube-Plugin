from datetime import datetime, timedelta, time
from typing import Generator, Dict, Any, Optional, List, Union
from zoneinfo import ZoneInfo

import requests
import imagehash
from PIL import Image
from google.oauth2.credentials import Credentials as OAuth2Credentials
import googleapiclient.discovery
import googleapiclient.errors


class Credentials(OAuth2Credentials):
    def refresh(self, request):
        pass


class YouTubeClient:
    def __init__(
        self,
        access_token: str,
        channel_id: str,
        results_per_query: int,
        start_date: datetime,
        aggregate_unit: int,
    ):
        self.__scopes = [
            "https://www.googleapis.com/auth/youtube.readonly",
            "https://www.googleapis.com/auth/yt-analytics.readonly",
        ]
        self.__access_token = access_token
        self.__credentials = Credentials(
            token=self.__access_token,
            expiry=datetime.now() + timedelta(days=1),
        )
        self.__youtube_analytics = googleapiclient.discovery.build(
            "youtubeAnalytics", "v2", credentials=self.__credentials
        )
        self.__youtube_search = googleapiclient.discovery.build(
            "youtube", "v3", credentials=self.__credentials
        )
        self._channel_id = channel_id
        self._results_per_query = results_per_query
        self._start_date = midnight_from_datetime(start_date)
        self._aggregate_unit = aggregate_unit
        self._end_date = calculate_end_date(self._start_date, self._aggregate_unit)

    def yt_analytics_iterator(
        self,
        dimensions: List[str],
        metrics: List[str],
        sort: str,
        filters: List[str],
        start_date: Optional[datetime] = None,
        stop_date: Optional[datetime] = None,
        index: Optional[int] = None,
        aggregate_unit: Optional[int] = None,
    ) -> Generator[Dict[str, Any], None, None]:
        if index is None:
            index = 1
        if start_date is None:
            start_date = self._start_date
        if stop_date is None:
            stop_date = datetime.now(tz=ZoneInfo("UTC"))
        if aggregate_unit is None:
            aggregate_unit = self._aggregate_unit
        end_date = calculate_end_date(start_date, aggregate_unit)
        while end_date <= stop_date:
            request = self.__youtube_analytics.reports().query(
                dimensions=",".join(dimensions),
                endDate=end_date.date().isoformat(),
                ids=f"channel=={self._channel_id}",
                filters=";".join(filters),
                maxResults=self._results_per_query,
                metrics=",".join(metrics),
                sort=sort,
                startDate=start_date.date().isoformat(),
                startIndex=index,
            )
            try:
                response = request.execute()
            except googleapiclient.errors.HttpError as e:
                if e.status_code == 401:
                    raise ValueError(f"authentication error: {e.reason}") from e
                else:
                    print(e)
                    start_date = end_date + timedelta(seconds=1)
                    end_date = calculate_end_date(start_date, self._aggregate_unit)
                    continue
            except Exception as e:
                print(e)
                start_date = end_date + timedelta(seconds=1)
                end_date = calculate_end_date(start_date, self._aggregate_unit)
                continue
            if response.get("rows") is not None:
                field_names = [
                    field.get("name") for field in response.get("columnHeaders")
                ]
                rows = response.get("rows")
                for row in rows:
                    _row = dict(zip(field_names, row))
                    _row["sampleStartTS"] = start_date
                    _row["sampleEndTS"] = end_date
                    yield _row
                if len(rows) == self._results_per_query:
                    for row in self.yt_analytics_iterator(
                        dimensions,
                        metrics,
                        sort,
                        filters,
                        start_date,
                        stop_date,
                        index + self._results_per_query,
                        aggregate_unit,
                    ):
                        yield row
            start_date = end_date + timedelta(seconds=1)
            end_date = calculate_end_date(start_date, self._aggregate_unit)

    def yt_video_iterator(
        self, page_token: Optional[str] = None
    ) -> Generator[Dict[str, Any], None, None]:
        request = self.__youtube_search.search().list(
            part="snippet",
            forMine=True,
            type="video",
            maxResults=self._results_per_query,
            order="date",
            pageToken=page_token,
        )
        try:
            response = request.execute()
        except googleapiclient.errors.HttpError as e:
            if e.status_code == 401:
                raise ValueError(f"authentication error: {e.reason}") from e
            else:
                raise ValueError(f"unexpected response: {e.status_code} - {e.reason}")
        if not response.get("items", []):
            raise ValueError(f"unexpected response: {response.get('error')}")
        page_token = response.get("nextPageToken")
        rows = response.get("items")
        for row in rows:
            yield {
                "video": row["id"]["videoId"],
                "publishedAt": datetime.fromisoformat(row["snippet"]["publishedAt"]),
            }
        if page_token is not None:
            for row in self.yt_video_iterator(page_token):
                yield row

    def yt_video_fetch(self, video) -> Generator[Dict[str, Any], None, None]:
        request = self.__youtube_search.videos().list(
            part="snippet,contentDetails",
            maxResults=self._results_per_query,
            id=video,
        )
        try:
            response = request.execute()
        except googleapiclient.errors.HttpError as e:
            if e.status_code == 401:
                raise ValueError(f"authentication error: {e.reason}") from e
            else:
                raise ValueError(f"unexpected response: {e.status_code} - {e.reason}")
        if not response.get("items", []):
            raise ValueError(f"unexpected response: {response.get('error')}")
        rows = response.get("items")
        for row in rows:
            yield {
                "video": row["id"],
                "title": row["snippet"]["title"],
                "sampleTS": datetime.now(tz=ZoneInfo("UTC")),
                "thumbnailUrl": row["snippet"]["thumbnails"]["default"]["url"],
                "description": row["snippet"]["description"],
                "durationIso": row["contentDetails"]["duration"],
            }


def get_thumnail_hash(video):
    headers = {
        "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/53.0.2785.143 Safari/537.36"
    }
    response = requests.get(
        f"https://i.ytimg.com/vi/{video}/maxresdefault.jpg",
        headers=headers,
        timeout=10,
        stream=True,
    )
    if response.status_code == 404:
        response = requests.get(
            f"https://i.ytimg.com/vi/{video}/default.jpg",
            headers=headers,
            timeout=10,
            stream=True,
        )
    elif response.status_code == 429:
        return "Unauthorized"

    if response.status_code == 200:
        img = Image.open(response.raw)
        image_hash = str(imagehash.average_hash(img))
        return image_hash
    elif response.status_code == 404:
        return "NotFound"
    else:
        response.raise_for_status()


def calculate_end_date(previous_date: datetime, aggregation_unit: int) -> datetime:
    return (
        datetime.combine(previous_date.date(), time(0, 0), tzinfo=ZoneInfo("UTC"))
        + timedelta(days=aggregation_unit)
        - timedelta(seconds=1)
    )


def midnight_from_datetime(dt: Union[datetime, str]) -> datetime:
    if not isinstance(dt, datetime):
        dt = datetime.fromisoformat(dt)
    dt = datetime.combine(dt.date(), time(0, 0), tzinfo=ZoneInfo("UTC"))
    return dt
