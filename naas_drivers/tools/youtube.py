import requests
import pydash as _
import pandas as pd
import datetime

YOUTUBE_API_URL = "https://www.googleapis.com/youtube/v3"


class Youtube:
    def connect(self, api_key: str):
        # Init lk attribute
        self.api_key = api_key

        # Init cookies
        self.base_params = {"key": self.api_key}

        # Init end point
        self.channel = Channel(self.base_params)
        self.video = Video(self.base_params)

        # Set connexion to active
        self.connected = True
        return self


class Channel(Youtube):
    def __init__(self, base_params):
        Youtube.__init__(self)
        self.base_params = base_params

    def __get_channel_id_from_url(self, channel_url):
        channel_id = channel_url.split("channel/")[-1].split("/")[0]
        return channel_id

    def __get_uploads_id_from_channel(self, channel_url):
        channel_id = self.__get_channel_id_from_url(channel_url)
        params = {"part": "contentDetails", "id": channel_id}
        params.update(self.base_params)
        res = requests.get(f"{YOUTUBE_API_URL}/channels", params=params)
        try:
            res.raise_for_status()
        except requests.HTTPError as e:
            return e
        playlist_id = _.get(
            res.json(), "items[0].contentDetails.relatedPlaylists.uploads"
        )
        return playlist_id

    def get_statistics(self, channel_url):
        if "www.youtube.com/channel" not in channel_url:
            return (
                "❌ Channel url not valid. "
                "Please get url with channel id. "
                "It must start with 'www.youtube.com/channel'"
            )
        channel_id = self.__get_channel_id_from_url(channel_url)
        params = {"part": "statistics,snippet", "id": channel_id}
        params.update(self.base_params)
        res = requests.get(f"{YOUTUBE_API_URL}/channels", params=params)
        try:
            res.raise_for_status()
        except requests.HTTPError as e:
            return e
        stats = _.get(res.json(), "items[0]")
        data = {
            "ID": _.get(stats, "id"),
            "URL": channel_url,
            "TITLE": _.get(stats, "snippet.title"),
            "COUNTRY": _.get(stats, "snippet.country"),
            "DESCRIPTION": _.get(stats, "snippet.description"),
            "VIEWCOUNT": _.get(stats, "statistics.viewCount"),
            "SUBSCRIBERCOUNT": _.get(stats, "statistics.subscriberCount"),
            "VIDEOCOUNT": _.get(stats, "statistics.videoCount"),
            "THUMBNAILS": _.get(stats, "snippet.thumbnails.high.url"),
            "PUBLISHEDAT": _.get(stats, "snippet.publishedAt"),
        }
        df = pd.DataFrame([data])
        return df

    def get_uploads(self, channel_url, number=100):
        if "www.youtube.com/channel" not in channel_url:
            return (
                "❌ Channel url not valid. "
                "Please get url with channel id. "
                "It must start with 'www.youtube.com/channel'"
            )
        playlist_id = self.__get_uploads_id_from_channel(channel_url)
        videos = []
        data = []
        nextPageToken = None
        while len(videos) < number:
            maxResults = 50 if (number - len(videos)) > 50 else (number - len(videos))
            params = {
                "part": "snippet,contentDetails",
                "maxResults": maxResults,
                "playlistId": playlist_id,
            }
            if nextPageToken:
                params["PageToken"] = nextPageToken
            params.update(self.base_params)
            res = requests.get(f"{YOUTUBE_API_URL}/playlistItems", params=params)
            try:
                res.raise_for_status()
            except requests.HTTPError as e:
                return e
            videos += _.get(res.json(), "items")
            nextPageToken = _.get(res.json(), "nextPageToken")

            for video in videos:
                meta = {
                    "CHANNEL_ID": _.get(video, "snippet.channelId"),
                    "PLAYLIST_ID": _.get(video, "snippet.playlistId"),
                    "VIDEO_ID": _.get(video, "snippet.resourceId.videoId"),
                    "VIDEO_TITLE": _.get(video, "snippet.title", ""),
                    "VIDEO_DESCRIPTION": _.get(video, "snippet.description", ""),
                    "VIDEO_PUBLISHEDAT": _.get(
                        video, "contentDetails.videoPublishedAt"
                    ),
                    "VIDEO_THUMBNAILS": _.get(
                        video, "snippet.thumbnails.maxres.url", ""
                    ),
                }
                data.append(meta)
            if nextPageToken is None:
                break
        df = pd.DataFrame(data)
        df["PUBLISHEDAT"] = pd.to_datetime(df["VIDEO_PUBLISHEDAT"])
        return df


class Video(Youtube):
    def __init__(self, base_params):
        Youtube.__init__(self)
        self.base_params = base_params

    def __get_video_id_from_url(self, video_url):
        video_id = video_url.split("watch?v=")[-1].split("&")[0]
        return video_id

    def get_statistics(self, video_url):
        video_id = self.__get_video_id_from_url(video_url)
        params = {"part": "statistics,snippet,contentDetails", "id": video_id}
        params.update(self.base_params)
        res = requests.get(f"{YOUTUBE_API_URL}/videos", params=params)
        try:
            res.raise_for_status()
        except requests.HTTPError as e:
            return e
        stats = _.get(res.json(), "items[0]")

        # Calcul duration
        duration = _.get(stats, "contentDetails.duration")

        def duration_to_seconds(duration):
            total_seconds = 0
            duration = duration.replace("P", "").replace("T", "")

            def calcul_duration(string, variable):
                result = 0
                if variable in duration:
                    result = string.split(variable)[0]
                    try:
                        result = int(result[-2:])
                    except ValueError:
                        result = int(result[-1:])
                return result

            # Get duration split
            seconds = calcul_duration(duration, "S")
            minutes = calcul_duration(duration, "M")
            hours = calcul_duration(duration, "H")
            days = calcul_duration(duration, "D")

            total_seconds = (
                seconds + (minutes * 60) + (hours * 3600) + (days * 3600 * 24)
            )
            return total_seconds

        duration_seconds = duration_to_seconds(duration)
        duration_final = str(datetime.timedelta(seconds=duration_seconds))

        data = {
            "CHANNEL_ID": _.get(stats, "snippet.channelId"),
            "CHANNEL_TITLE": _.get(stats, "snippet.channelTitle"),
            "ID": _.get(stats, "id"),
            "URL": video_url,
            "TITLE": _.get(stats, "snippet.title"),
            "DESCRIPTION": _.get(stats, "snippet.description"),
            "VIEWCOUNT": _.get(stats, "statistics.viewCount"),
            "LIKECOUNT": _.get(stats, "statistics.likeCount"),
            "DISLIKECOUNT": _.get(stats, "statistics.dislikeCount"),
            "FAVORITECOUNT": _.get(stats, "statistics.favoriteCount"),
            "COMMENTCOUNT": _.get(stats, "statistics.commentCount"),
            "DURATION": duration_final,
            "DURATION_SECONDS": duration_seconds,
            "THUMBNAILS": _.get(stats, "snippet.thumbnails.high.url"),
            "PUBLISHEDAT": _.get(stats, "snippet.publishedAt"),
        }
        df = pd.DataFrame([data])
        df["PUBLISHEDAT"] = pd.to_datetime(df["PUBLISHEDAT"])
        return df
