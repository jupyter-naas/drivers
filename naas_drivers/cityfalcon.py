import requests
import pandas as pd
import os


class Cityfalcon:
    __key = os.environ.get("CITYFALCON_KEY", None)
    _url_base = os.environ.get(
        "CITYFALCON_API", "https://api.cityfalcon.com/v0.2/stories"
    )

    def connect(self, key):
        self.__key = key

    def get(
        self,
        action,
        fields=[
            "image",
            "title",
            "source_logo",
            "link",
        ],
        country="US",
        limit=None,
        min_score=20,
        paywall=False,
        identifier_type="full_tickers",
        time_filter="d1",
        languages="en",
    ):
        url = f"{self._url_base}?access_token={self.__key}"
        url = f"{url}&identifier_type={identifier_type}&paywall={paywall}&identifiers={action}_{country}&categories=mp%2Cop"
        url = f"{url}&min_cityfalcon_score={min_score}&order_by=latest&time_filter={time_filter}&all_languages=false&languages={languages}"
        req = requests.get(url)
        dict_news = req.json()
        news = []
        for element in dict_news["stories"]:
            new_formated = {}
            for field in fields:
                if field == "title":
                    new_formated["title"] = element["title"]
                elif field == "link":
                    new_formated["link"] = element["url"]
                elif field == "description":
                    new_formated["description"] = element["description"]
                elif field == "score":
                    new_formated["score"] = element["cityfalconScore"]
                elif field == "sentiment":
                    new_formated["sentiment"] = element["sentiment"]
                elif field == "source":
                    new_formated["source"] = element["source"]["brandName"]
                elif field == "image":
                    new_formated["image"] = (
                        element["imageUrls"][0] if element["imageUrls"] else ""
                    )
                elif field == "source_logo":
                    new_formated["logo"] = (
                        element["source"]["imageUrls"]["large"]
                        if element["source"] and element["source"]["imageUrls"]
                        else ""
                    )
                else:
                    raise ValueError("Unknow parameter")
            news.append(new_formated)
        if limit and isinstance(limit, int) and limit > 0:
            news = news[:limit]
        return pd.DataFrame.from_records(news)
