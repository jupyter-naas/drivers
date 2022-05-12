from naas_drivers.driver import InDriver
import pandas as pd
import os
from newsapi.newsapi_client import NewsApiClient


class Newsapi(InDriver):
    __key = None

    def connect(self, key=None):
        self.__key = key if key else os.environ.get("APINEW_KEY", None)
        self.connected = True
        return self

    def __transformDate(self, data, fields, limit):
        news = []
        for element in data:
            new_formated = {}
            for field in fields:
                if field == "title":
                    new_formated["title"] = element["title"]
                elif field == "link":
                    new_formated["link"] = element["url"]
                elif field == "description":
                    new_formated["description"] = element["description"]
                elif field == "source":
                    new_formated["source"] = element["source"]["name"]
                elif field == "image":
                    new_formated["image"] = element["urlToImage"]
                elif field == "author":
                    new_formated["author"] = element["author"]
                elif field == "date":
                    new_formated["date"] = element["publishedAt"]
                else:
                    error_text = f"Unknow parameter {field}"
                    self.print_error(error_text)
                    return None
            news.append(new_formated)
        if limit and isinstance(limit, int) and limit > 0:
            news = news[:limit]
        return pd.DataFrame.from_records(news)

    def get_sources(
        self, q, fields=["image", "title", "source", "link"], limit=20, **kargs
    ):
        kargs["q"] = q
        self.check_connect()
        newsapi = NewsApiClient(api_key=self.__key)
        sources = newsapi.get_sources(**kargs)
        return self.__transformDate(sources.get("sources"), fields, limit)

    def get_top(
        self, q, fields=["image", "title", "source", "link"], limit=20, **kargs
    ):
        kargs["q"] = q
        self.check_connect()
        newsapi = NewsApiClient(api_key=self.__key)
        tops = newsapi.get_top_headlines(**kargs)
        return self.__transformDate(tops.get("articles"), fields, limit)

    def get(self, q, fields=["image", "title", "source", "link"], limit=20, **kargs):
        kargs["q"] = q
        self.check_connect()
        newsapi = NewsApiClient(api_key=self.__key)
        all_news = newsapi.get_everything(**kargs)
        return self.__transformDate(all_news.get("articles"), fields, limit)
