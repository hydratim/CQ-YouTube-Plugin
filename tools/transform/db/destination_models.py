import hashlib
from datetime import datetime, date, timedelta

from pony import orm

destination_db = orm.Database()


class DestinationTables:
    __tables__ = []

    @classmethod
    def register(cls, child_cls):
        cls.__tables__.append(child_cls)
        return child_cls

    @classmethod
    def get_tables(cls):
        return cls.__tables__


@DestinationTables.register
class DimensionDateKey(destination_db.Entity):
    _table_ = "d_date"
    datetime = orm.PrimaryKey(datetime)  # Always midnight
    date = orm.Required(date, unique=True)
    year = orm.Required(int)
    month = orm.Required(int)
    day = orm.Required(int)
    week_day = orm.Required(int)
    quarter_number = orm.Required(int)
    week_number = orm.Required(int)
    videos = orm.Set("DimensionYoutubeVideo")
    videos_titles = orm.Set("FactYoutubeVideoTitle")
    videos_descriptions = orm.Set("FactYoutubeVideoDescription")
    videos_thumbnails = orm.Set("FactYoutubeVideoThumbnail")
    videos_views = orm.Set("FactYoutubeVideoViews")
    videos_views_source_types = orm.Set("FactYoutubeVideoViewSourceType")
    videos_views_source_details = orm.Set("FactYoutubeVideoViewSourceDetails")
    videos_engagements = orm.Set("FactYoutubeVideoEngagement")
    channel_views = orm.Set("FactYoutubeChannelViewSourceCountry")
    channel_engagement = orm.Set("FactYoutubeChannelEngagement")
    channel_demographics = orm.Set("FactYoutubeChannelDemographics")

    @classmethod
    def sync(cls, source_models):
        dt = datetime(2020, 1, 1)
        while dt.date() <= datetime.now().date():
            if cls.get(date=dt.date()) is not None:
                dt += timedelta(days=1)
                continue
            cls(
                datetime=dt,
                date=dt.date(),
                year=dt.year,
                month=dt.month,
                day=dt.day,
                week_day=dt.isoweekday(),
                quarter_number=(dt.month - 1) // 3 + 1,
                week_number=dt.isocalendar().week,
            )
            dt += timedelta(days=1)
        destination_db.commit()


@DestinationTables.register
class DimensionCountry(destination_db.Entity):
    _table_ = "d_country"
    iso_country_code = orm.PrimaryKey(str)
    english_name = orm.Required(str, unique=True)
    channel_views = orm.Set("FactYoutubeChannelViewSourceCountry")

    @classmethod
    def sync(cls, source_models):
        iso_country_codes = [
            ("AD", "Andorra"),
            ("AE", "United Arab Emirates"),
            ("AF", "Afghanistan"),
            ("AG", "Antigua and Barbuda"),
            ("AI", "Anguilla"),
            ("AL", "Albania"),
            ("AM", "Armenia"),
            ("AO", "Angola"),
            ("AQ", "Antarctica"),
            ("AR", "Argentina"),
            ("AS", "American Samoa"),
            ("AT", "Austria"),
            ("AU", "Australia"),
            ("AW", "Aruba"),
            ("AX", "Åland Islands"),
            ("AZ", "Azerbaijan"),
            ("BA", "Bosnia and Herzegovina"),
            ("BB", "Barbados"),
            ("BD", "Bangladesh"),
            ("BE", "Belgium"),
            ("BF", "Burkina Faso"),
            ("BG", "Bulgaria"),
            ("BH", "Bahrain"),
            ("BI", "Burundi"),
            ("BJ", "Benin"),
            ("BL", "Saint Barthélemy"),
            ("BM", "Bermuda"),
            ("BN", "Brunei Darussalam"),
            ("BO", "Bolivia"),
            ("BQ", "Bonaire, Sint Eustatius and Saba"),
            ("BR", "Brazil"),
            ("BS", "Bahamas"),
            ("BT", "Bhutan"),
            ("BV", "Bouvet Island"),
            ("BW", "Botswana"),
            ("BY", "Belarus"),
            ("BZ", "Belize"),
            ("CA", "Canada"),
            ("CC", "Cocos(Keeling) Islands"),
            ("CD", "Democratic Republic of the Congo"),
            ("CF", "Central African Republic"),
            ("CG", "Congo"),
            ("CH", "Switzerland"),
            ("CI", "Côte d'Ivoire"),
            ("CK", "Cook Islands"),
            ("CL", "Chile"),
            ("CM", "Cameroon"),
            ("CN", "China"),
            ("CO", "Colombia"),
            ("CR", "Costa Rica"),
            ("CU", "Cuba"),
            ("CV", "Cabo Verde"),
            ("CW", "Curaçao"),
            ("CX", "Christmas Island"),
            ("CY", "Cyprus"),
            ("CZ", "Czechia"),
            ("DE", "Germany"),
            ("DJ", "Djibouti"),
            ("DK", "Denmark"),
            ("DM", "Dominica"),
            ("DO", "Dominican Republic"),
            ("DZ", "Algeria"),
            ("EC", "Ecuador"),
            ("EE", "Estonia"),
            ("EG", "Egypt"),
            ("EH", "Western Sahara"),
            ("ER", "Eritrea"),
            ("ES", "Spain"),
            ("ET", "Ethiopia"),
            ("FI", "Finland"),
            ("FJ", "Fiji"),
            ("FK", "Falkland Islands"),
            ("FM", "Federated States of Micronesia"),
            ("FO", "Faroe Islands"),
            ("FR", "France"),
            ("GA", "Gabon"),
            ("GB", "United Kingdom"),
            ("GD", "Grenada"),
            ("GE", "Georgia"),
            ("GF", "French Guiana"),
            ("GG", "Guernsey"),
            ("GH", "Ghana"),
            ("GI", "Gibraltar"),
            ("GL", "Greenland"),
            ("GM", "Gambia"),
            ("GN", "Guinea"),
            ("GP", "Guadeloupe"),
            ("GQ", "Equatorial Guinea"),
            ("GR", "Greece"),
            ("GS", "South Georgia and the South Sandwich Islands"),
            ("GT", "Guatemala"),
            ("GU", "Guam"),
            ("GW", "Guinea - Bissau"),
            ("GY", "Guyana"),
            ("HK", "Hong Kong"),
            ("HM", "Heard Island and McDonald Islands"),
            ("HN", "Honduras"),
            ("HR", "Croatia"),
            ("HT", "Haiti"),
            ("HU", "Hungary"),
            ("ID", "Indonesia"),
            ("IE", "Ireland"),
            ("IL", "Israel"),
            ("IM", "Isle of Man"),
            ("IN", "India"),
            ("IO", "British Indian Ocean Territory"),
            ("IQ", "Iraq"),
            ("IR", "Iran"),
            ("IS", "Iceland"),
            ("IT", "Italy"),
            ("JE", "Jersey"),
            ("JM", "Jamaica"),
            ("JO", "Jordan"),
            ("JP", "Japan"),
            ("KE", "Kenya"),
            ("KG", "Kyrgyzstan"),
            ("KH", "Cambodia"),
            ("KI", "Kiribati"),
            ("KM", "Comoros"),
            ("KN", "Saint Kitts and Nevis"),
            ("KP", "North Korea"),
            ("KR", "South Korea"),
            ("KW", "Kuwait"),
            ("KY", "Cayman Islands"),
            ("KZ", "Kazakhstan"),
            ("LA", "Laos"),
            ("LB", "Lebanon"),
            ("LC", "Saint Lucia"),
            ("LI", "Liechtenstein"),
            ("LK", "Sri Lanka"),
            ("LR", "Liberia"),
            ("LS", "Lesotho"),
            ("LT", "Lithuania"),
            ("LU", "Luxembourg"),
            ("LV", "Latvia"),
            ("LY", "Libya"),
            ("MA", "Morocco"),
            ("MC", "Monaco"),
            ("MD", "Moldova"),
            ("ME", "Montenegro"),
            ("MF", "Saint Martin"),
            ("MG", "Madagascar"),
            ("MH", "Marshall Islands"),
            ("MK", "North Macedonia"),
            ("ML", "Mali"),
            ("MM", "Myanmar"),
            ("MN", "Mongolia"),
            ("MO", "Macao"),
            ("MP", "Northern Mariana Islands"),
            ("MQ", "Martinique"),
            ("MR", "Mauritania"),
            ("MS", "Montserrat"),
            ("MT", "Malta"),
            ("MU", "Mauritius"),
            ("MV", "Maldives"),
            ("MW", "Malawi"),
            ("MX", "Mexico"),
            ("MY", "Malaysia"),
            ("MZ", "Mozambique"),
            ("NA", "Namibia"),
            ("NC", "New Caledonia"),
            ("NE", "Niger"),
            ("NF", "Norfolk Island"),
            ("NG", "Nigeria"),
            ("NI", "Nicaragua"),
            ("NL", "Netherlands"),
            ("NO", "Norway"),
            ("NP", "Nepal"),
            ("NR", "Nauru"),
            ("NU", "Niue"),
            ("NZ", "New Zealand"),
            ("OM", "Oman"),
            ("PA", "Panama"),
            ("PE", "Peru"),
            ("PF", "French Polynesia"),
            ("PG", "Papua New Guinea"),
            ("PH", "Philippines"),
            ("PK", "Pakistan"),
            ("PL", "Poland"),
            ("PM", "Saint Pierre and Miquelon"),
            ("PN", "Pitcairn Islands"),
            ("PR", "Puerto Rico"),
            ("PS", "Palestine"),
            ("PT", "Portugal"),
            ("PW", "Palau"),
            ("PY", "Paraguay"),
            ("QA", "Qatar"),
            ("RE", "Réunion"),
            ("RO", "Romania"),
            ("RS", "Serbia"),
            ("RU", "Russia"),
            ("RW", "Rwanda"),
            ("SA", "Saudi Arabia"),
            ("SB", "Solomon Islands"),
            ("SC", "Seychelles"),
            ("SD", "Sudan"),
            ("SE", "Sweden"),
            ("SG", "Singapore"),
            ("SH", "Saint Helena"),
            ("SI", "Slovenia"),
            ("SJ", "Svalbard and Jan Mayen"),
            ("SK", "Slovakia"),
            ("SL", "Sierra Leone"),
            ("SM", "San Marino"),
            ("SN", "Senegal"),
            ("SO", "Somalia"),
            ("SR", "Suriname"),
            ("SS", "South Sudan"),
            ("ST", "Sao Tome and Principe"),
            ("SV", "El Salvador"),
            ("SX", "Sint Maarten"),
            ("SY", "Syria"),
            ("SZ", "Eswatini"),
            ("TC", "Turks and Caicos Islands"),
            ("TD", "Chad"),
            ("TF", "French Southern and Antarctic Lands"),
            ("TG", "Togo"),
            ("TH", "Thailand"),
            ("TJ", "Tajikistan"),
            ("TK", "Tokelau"),
            ("TL", "East Timor"),
            ("TM", "Turkmenistan"),
            ("TN", "Tunisia"),
            ("TO", "Tonga"),
            ("TR", "Türkiye"),
            ("TT", "Trinidad and Tobago"),
            ("TV", "Tuvalu"),
            ("TW", "Taiwan"),
            ("TZ", "Tanzania"),
            ("UA", "Ukraine"),
            ("UG", "Uganda"),
            ("UM", "United States Minor Outlying Islands"),
            ("US", "United States"),
            ("UY", "Uruguay"),
            ("UZ", "Uzbekistan"),
            ("VA", "Vatican"),
            ("VC", "Saint Vincent and the Grenadines"),
            ("VE", "Venezuela"),
            ("VG", "Virgin Islands(British)"),
            ("VI", "Virgin Islands(U.S.)"),
            ("VN", "Vietnam"),
            ("VU", "Vanuatu"),
            ("WF", "Wallis and Futuna"),
            ("WS", "Samoa"),
            ("YE", "Yemen"),
            ("YT", "Mayotte"),
            ("ZA", "South Africa"),
            ("ZM", "Zambia"),
            ("ZW", "Zimbabwe"),
        ]
        for cc, name in iso_country_codes:
            if cls.get(iso_country_code=cc) is not None:
                continue
            cls(iso_country_code=cc, english_name=name)
        destination_db.commit()


@DestinationTables.register
class DimensionImageHashAlgo(destination_db.Entity):
    _table_ = "d_image_hash_algo"
    id = orm.PrimaryKey(int)
    name = orm.Required(str)
    type = orm.Required(str)
    language = orm.Required(str)
    videos_thumbnails = orm.Set("FactYoutubeVideoThumbnail")

    @classmethod
    def sync(cls, source_models):
        if cls.get(id=1) is not None:
            return
        cls(id=1, name="imagehash.average_hash", type="perceptual", language="python")
        destination_db.commit()


@DestinationTables.register
class DimensionYoutubeVideo(destination_db.Entity):
    _table_ = "d_youtube_video"
    video = orm.PrimaryKey(str)
    publishedAt = orm.Required(datetime)
    publishedDate = orm.Required(DimensionDateKey)
    titles = orm.Set("FactYoutubeVideoTitle")
    descriptions = orm.Set("FactYoutubeVideoDescription")
    thumbnails = orm.Set("FactYoutubeVideoThumbnail")
    duration = orm.Optional("FactYoutubeVideoDuration")
    views = orm.Set("FactYoutubeVideoViews")
    views_source_types = orm.Set("FactYoutubeVideoViewSourceType")
    views_source_details = orm.Set("FactYoutubeVideoViewSourceDetails")
    engagements = orm.Set("FactYoutubeVideoEngagement")

    @classmethod
    def sync(cls, source_models):
        for video in source_models.get_tables()["SourceVideo"].select():
            if cls.get(video=video.video) is not None:
                continue
            cls(
                video=video.video,
                publishedAt=video.publishedAt,
                publishedDate=DimensionDateKey.get(date=video.publishedDate),
            )
        destination_db.commit()


@DestinationTables.register
class FactYoutubeVideoTitle(destination_db.Entity):
    _table_ = "f_youtube_video_title"
    video = orm.Required(DimensionYoutubeVideo)
    hash = orm.PrimaryKey(str)
    title = orm.Required(str)
    modifiedOn = orm.Required(DimensionDateKey)

    @classmethod
    def sync(cls, source_models):
        for meta in source_models.get_tables()["SourceVideoMeta"].select():
            hash = hashlib.sha256(meta.title.encode()).hexdigest()
            if cls.get(hash=hash) is not None:
                continue
            cls(
                video=DimensionYoutubeVideo.get(video=meta.video),
                title=meta.title,
                hash=hashlib.sha256(meta.title.encode()).hexdigest(),
                modifiedOn=DimensionDateKey.get(date=meta.sampleTS.date()),
            )
        destination_db.commit()


@DestinationTables.register
class FactYoutubeVideoDescription(destination_db.Entity):
    _table_ = "f_youtube_video_description"
    video = orm.Required(DimensionYoutubeVideo)
    hash = orm.PrimaryKey(str)
    description = orm.Required(str)
    modifiedOn = orm.Required(DimensionDateKey)

    @classmethod
    def sync(cls, source_models):
        for meta in source_models.get_tables()["SourceVideoMeta"].select():
            hash = hashlib.sha256(meta.description.encode()).hexdigest()
            if cls.get(hash=hash) is not None:
                continue
            cls(
                video=DimensionYoutubeVideo.get(video=meta.video),
                description=meta.description,
                hash=hashlib.sha256(meta.description.encode()).hexdigest(),
                modifiedOn=DimensionDateKey.get(date=meta.sampleTS.date()),
            )
        destination_db.commit()


@DestinationTables.register
class FactYoutubeVideoThumbnail(destination_db.Entity):
    _table_ = "f_youtube_video_thumbnail"
    video = orm.Required(DimensionYoutubeVideo)
    imageHash = orm.PrimaryKey(str)
    hashAlgoVersion = orm.Required(DimensionImageHashAlgo)
    modifiedOn = orm.Required(DimensionDateKey)

    @classmethod
    def sync(cls, source_models):
        for meta in source_models.get_tables()["SourceVideoMeta"].select():
            if cls.get(imageHash=meta.thumbnailHash) is not None:
                continue
            cls(
                video=DimensionYoutubeVideo.get(video=meta.video),
                imageHash=meta.thumbnailHash,
                hashAlgoVersion=1,
                modifiedOn=DimensionDateKey.get(date=meta.sampleTS.date()),
            )
        destination_db.commit()


@DestinationTables.register
class FactYoutubeVideoDuration(destination_db.Entity):
    _table_ = "f_youtube_video_duration"
    video = orm.PrimaryKey(DimensionYoutubeVideo)
    isoFormatDuration = orm.Required(str)
    hours = orm.Required(int)
    minutes = orm.Required(int)
    seconds = orm.Required(int)
    totalSeconds = orm.Required(int)
    totalMinutes = orm.Required(float)

    @classmethod
    def sync(cls, source_models):
        for meta in source_models.get_tables()["SourceVideoMeta"].select():
            if cls.get(video=DimensionYoutubeVideo.get(video=meta.video)) is not None:
                continue
            cls(
                video=DimensionYoutubeVideo.get(video=meta.video),
                isoFormatDuration=meta.durationIso,
                hours=meta.durationHours,
                minutes=meta.durationMins,
                seconds=meta.durationSecs,
                totalSeconds=meta.durationHours * 3600
                + meta.durationMins * 60
                + meta.durationSecs,
                totalMinutes=(
                    meta.durationHours * 3600
                    + meta.durationMins * 60
                    + meta.durationSecs
                )
                / 60,
            )
        destination_db.commit()


@DestinationTables.register
class FactYoutubeVideoViews(destination_db.Entity):
    _table_ = "f_youtube_video_views"
    video = orm.Required(DimensionYoutubeVideo)
    views = orm.Required(int)
    averageViewPercentage = orm.Required(float)
    sampleDate = orm.Required(DimensionDateKey)
    sampleStartTS = orm.Required(datetime)
    sampleEndTS = orm.Required(datetime)
    uniqueKey = orm.PrimaryKey(str)

    @classmethod
    def sync(cls, source_models):
        for metrics in source_models.get_tables()["SourceVideoMetrics"].select():
            if cls.get(uniqueKey=metrics.uniqueKey) is not None:
                continue
            cls(
                video=DimensionYoutubeVideo.get(video=metrics.video),
                views=metrics.views,
                averageViewPercentage=metrics.averageViewPercentage,
                sampleDate=DimensionDateKey.get(date=metrics.sampleStartTS.date()),
                sampleStartTS=metrics.sampleStartTS,
                sampleEndTS=metrics.sampleEndTS,
                uniqueKey=metrics.uniqueKey,
            )
        destination_db.commit()


@DestinationTables.register
class FactYoutubeVideoEngagement(destination_db.Entity):
    _table_ = "f_youtube_video_engagement"
    video = orm.Required(DimensionYoutubeVideo)
    comments = orm.Required(int)
    likes = orm.Required(int)
    dislikes = orm.Required(int)
    shares = orm.Required(int)
    subscribersGained = orm.Required(int)
    subscribersLost = orm.Required(int)
    sampleDate = orm.Required(DimensionDateKey)
    sampleStartTS = orm.Required(datetime)
    sampleEndTS = orm.Required(datetime)
    uniqueKey = orm.PrimaryKey(str)

    @classmethod
    def sync(cls, source_models):
        for metrics in source_models.get_tables()["SourceVideoMetrics"].select():
            if cls.get(uniqueKey=metrics.uniqueKey) is not None:
                continue
            cls(
                video=DimensionYoutubeVideo.get(video=metrics.video),
                comments=metrics.comments,
                likes=metrics.likes,
                dislikes=metrics.dislikes,
                shares=metrics.shares,
                subscribersGained=metrics.subscribersGained,
                subscribersLost=metrics.subscribersLost,
                sampleDate=DimensionDateKey.get(date=metrics.sampleStartTS.date()),
                sampleStartTS=metrics.sampleStartTS,
                sampleEndTS=metrics.sampleEndTS,
                uniqueKey=metrics.uniqueKey,
            )
        destination_db.commit()


@DestinationTables.register
class DimensionYoutubeViewSourceType(destination_db.Entity):
    _table_ = "d_youtube_view_source_type"
    sourceType = orm.PrimaryKey(str)
    details = orm.Set("DimensionYoutubeViewSourceDetail")
    facts = orm.Set("FactYoutubeVideoViewSourceType")

    @classmethod
    def sync(cls, source_models):
        for source_type in source_models.get_tables()[
            "SourceVideoViewSourceType"
        ].select():
            if cls.get(sourceType=source_type.insightTrafficSourceType) is not None:
                continue
            cls(sourceType=source_type.insightTrafficSourceType)
        destination_db.commit()


@DestinationTables.register
class DimensionYoutubeViewSourceDetail(destination_db.Entity):
    _table_ = "d_youtube_view_source_detail"
    sourceType = orm.Required(DimensionYoutubeViewSourceType)
    sourceDetail = orm.Required(str)
    uniqueKey = orm.PrimaryKey(str)
    facts = orm.Set("FactYoutubeVideoViewSourceDetails")

    @classmethod
    def sync(cls, source_models):
        for source_detail in source_models.get_tables()[
            "SourceVideoViewSourceDetail"
        ].select():
            if (
                cls.get(
                    uniqueKey=source_detail.insightTrafficSourceType
                    + source_detail.insightTrafficSourceDetail
                )
                is not None
            ):
                continue
            cls(
                sourceType=DimensionYoutubeViewSourceType.get(
                    sourceType=source_detail.insightTrafficSourceType
                ),
                sourceDetail=source_detail.insightTrafficSourceDetail,
                uniqueKey=source_detail.insightTrafficSourceType
                + source_detail.insightTrafficSourceDetail,
            )
        destination_db.commit()


@DestinationTables.register
class FactYoutubeVideoViewSourceType(destination_db.Entity):
    _table_ = "f_youtube_video_view_source_type"
    uniqueKey = orm.PrimaryKey(str)
    video = orm.Required(DimensionYoutubeVideo)
    sourceType = orm.Required(DimensionYoutubeViewSourceType)
    views = orm.Required(int)
    sampleDate = orm.Required(DimensionDateKey)
    sampleStartTS = orm.Required(datetime)
    sampleEndTS = orm.Required(datetime)

    @classmethod
    def sync(cls, source_models):
        for source_type in source_models.get_tables()[
            "SourceVideoViewSourceType"
        ].select():
            if cls.get(uniqueKey=source_type.uniqueKey) is not None:
                continue
            cls(
                uniqueKey=source_type.uniqueKey,
                video=DimensionYoutubeVideo.get(video=source_type.video),
                sourceType=DimensionYoutubeViewSourceType.get(
                    sourceType=source_type.insightTrafficSourceType
                ),
                views=source_type.views,
                sampleDate=DimensionDateKey.get(date=source_type.sampleStartTS.date()),
                sampleStartTS=source_type.sampleStartTS,
                sampleEndTS=source_type.sampleEndTS,
            )
        destination_db.commit()


@DestinationTables.register
class FactYoutubeVideoViewSourceDetails(destination_db.Entity):
    _table_ = "f_youtube_video_view_source_details"
    uniqueKey = orm.PrimaryKey(str)
    video = orm.Required(DimensionYoutubeVideo)
    sourceDetail = orm.Required(DimensionYoutubeViewSourceDetail)
    views = orm.Required(int)
    sampleDate = orm.Required(DimensionDateKey)
    sampleStartTS = orm.Required(datetime)
    sampleEndTS = orm.Required(datetime)

    @classmethod
    def sync(cls, source_models):
        for source_details in source_models.get_tables()[
            "SourceVideoViewSourceDetail"
        ].select():
            if cls.get(uniqueKey=source_details.uniqueKey) is not None:
                continue
            cls(
                uniqueKey=source_details.uniqueKey,
                video=DimensionYoutubeVideo.get(video=source_details.video),
                sourceDetail=DimensionYoutubeViewSourceDetail.get(
                    sourceDetail=source_details.insightTrafficSourceDetail,
                    sourceType=DimensionYoutubeViewSourceType.get(
                        sourceType=source_details.insightTrafficSourceType
                    ),
                ),
                views=source_details.views,
                sampleDate=DimensionDateKey.get(
                    date=source_details.sampleStartTS.date()
                ),
                sampleStartTS=source_details.sampleStartTS,
                sampleEndTS=source_details.sampleEndTS,
            )
        destination_db.commit()


@DestinationTables.register
class FactYoutubeChannelViewSourceCountry(destination_db.Entity):
    _table_ = "f_youtube_channel_view_source_country"
    uniqueKey = orm.PrimaryKey(str)
    country = orm.Required(DimensionCountry)
    views = orm.Required(int)
    sampleDate = orm.Required(DimensionDateKey)
    sampleStartTS = orm.Required(datetime)
    sampleEndTS = orm.Required(datetime)

    @classmethod
    def sync(cls, source_models):
        for metrics in source_models.get_tables()[
            "SourceChannelCountryMetrics"
        ].select():
            if cls.get(uniqueKey=metrics.uniqueKey) is not None:
                continue
            cls(
                uniqueKey=metrics.uniqueKey,
                country=DimensionCountry.get(iso_country_code=metrics.country),
                views=metrics.views,
                sampleDate=DimensionDateKey.get(date=metrics.sampleStartTS.date()),
                sampleStartTS=metrics.sampleStartTS,
                sampleEndTS=metrics.sampleEndTS,
            )
        destination_db.commit()


@DestinationTables.register
class FactYoutubeChannelEngagement(destination_db.Entity):
    _table_ = "f_youtube_channel_engagement"
    views = orm.Required(int)
    comments = orm.Required(int)
    likes = orm.Required(int)
    dislikes = orm.Required(int)
    shares = orm.Required(int)
    sampleDate = orm.PrimaryKey(DimensionDateKey)
    sampleStartTS = orm.Required(datetime)
    sampleEndTS = orm.Required(datetime)

    @classmethod
    def sync(cls, source_models):
        for metrics in source_models.get_tables()["SourceChannelMetrics"].select():
            if cls.get(sampleDate=DimensionDateKey.get(date=metrics.sampleStartTS.date())) is not None:
                continue
            cls(
                views=metrics.views,
                comments=metrics.comments,
                likes=metrics.likes,
                dislikes=metrics.dislikes,
                shares=metrics.shares,
                sampleDate=DimensionDateKey.get(date=metrics.sampleStartTS.date()),
                sampleStartTS=metrics.sampleStartTS,
                sampleEndTS=metrics.sampleEndTS,
            )
        destination_db.commit()


@DestinationTables.register
class FactYoutubeChannelDemographics(destination_db.Entity):
    _table_ = "f_youtube_channel_demographics"
    uniqueKey = orm.PrimaryKey(str)
    ageGroup = orm.Required(str)
    gender = orm.Required(str)
    viewerPercentage = orm.Required(float)
    sampleDate = orm.Required(DimensionDateKey)
    sampleStartTS = orm.Required(datetime)
    sampleEndTS = orm.Required(datetime)

    @classmethod
    def sync(cls, source_models):
        for metrics in source_models.get_tables()["SourceChannelDemographics"].select():
            if cls.get(uniqueKey=metrics.uniqueKey) is not None:
                continue
            cls(
                uniqueKey=metrics.uniqueKey,
                ageGroup=metrics.ageGroup,
                gender=metrics.gender,
                viewerPercentage=metrics.viewerPercentage,
                sampleDate=DimensionDateKey.get(date=metrics.sampleStartTS.date()),
                sampleStartTS=metrics.sampleStartTS,
                sampleEndTS=metrics.sampleEndTS,
            )
        destination_db.commit()
