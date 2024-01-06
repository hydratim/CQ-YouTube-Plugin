from datetime import datetime, date
from typing import Type

from pony import orm

source_db = orm.Database()


class SourceTables:
    __tables__ = {}

    @classmethod
    def register(cls, child_cls: Type) -> Type:
        cls.__tables__[child_cls.__name__] = child_cls
        return child_cls

    @classmethod
    def get_tables(cls):
        return cls.__tables__


@SourceTables.register
class SourceChannelDemographics(source_db.Entity):
    _table_ = "yt_channel_demographics"
    uniqueKey = orm.PrimaryKey(str)
    sampleStartTS = orm.Required(datetime)
    sampleEndTS = orm.Required(datetime)
    ageGroup = orm.Required(str)
    gender = orm.Required(str)
    viewerPercentage = orm.Required(float)


@SourceTables.register
class SourceChannelMetrics(source_db.Entity):
    _table_ = "yt_channel_metrics"
    sampleStartTS = orm.PrimaryKey(datetime)
    sampleEndTS = orm.Required(datetime)
    comments = orm.Required(int)
    dislikes = orm.Required(int)
    likes = orm.Required(int)
    shares = orm.Required(int)
    views = orm.Required(int)


@SourceTables.register
class SourceChannelCountryMetrics(source_db.Entity):
    _table_ = "yt_country_metrics"
    uniqueKey = orm.PrimaryKey(str)
    country = orm.Required(str)
    sampleStartTS = orm.Required(datetime)
    sampleEndTS = orm.Required(datetime)
    views = orm.Required(int)


@SourceTables.register
class SourceVideo(source_db.Entity):
    _table_ = "yt_video"
    video = orm.PrimaryKey(str)
    publishedAt = orm.Required(datetime)
    publishedDate = orm.Required(date)


@SourceTables.register
class SourceVideoMeta(source_db.Entity):
    _table_ = "yt_video_meta"
    video = orm.Required(str)
    uniqueKey = orm.PrimaryKey(str)
    sampleTS = orm.Required(datetime)
    title = orm.Required(str)
    description = orm.Required(str)
    thumbnailHash = orm.Required(str)
    durationIso = orm.Required(str)
    durationHours = orm.Required(int)
    durationMins = orm.Required(int)
    durationSecs = orm.Required(int)


@SourceTables.register
class SourceVideoMetrics(source_db.Entity):
    _table_ = "yt_video_metrics"
    video = orm.Required(str)
    uniqueKey = orm.PrimaryKey(str)
    sampleStartTS = orm.Required(datetime)
    sampleEndTS = orm.Required(datetime)
    averageViewPercentage = orm.Required(float)
    comments = orm.Required(int)
    dislikes = orm.Required(int)
    likes = orm.Required(int)
    shares = orm.Required(int)
    subscribersGained = orm.Required(int)
    subscribersLost = orm.Required(int)
    views = orm.Required(int)


@SourceTables.register
class SourceVideoViewSourceType(source_db.Entity):
    _table_ = "yt_video_view_source_types"
    video = orm.Required(str)
    uniqueKey = orm.PrimaryKey(str)
    sampleStartTS = orm.Required(datetime)
    sampleEndTS = orm.Required(datetime)
    insightTrafficSourceType = orm.Required(str)
    views = orm.Required(int)


@SourceTables.register
class SourceVideoViewSourceDetail(source_db.Entity):
    _table_ = "yt_video_view_source_details"
    video = orm.Required(str)
    uniqueKey = orm.PrimaryKey(str)
    sampleStartTS = orm.Required(datetime)
    sampleEndTS = orm.Required(datetime)
    insightTrafficSourceType = orm.Required(str)
    insightTrafficSourceDetail = orm.Required(str)
    views = orm.Required(int)
