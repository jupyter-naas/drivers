from naas_drivers.driver import InDriver
import requests
import pandas as pd
import os


class Cityfalcon(InDriver):
    __key = None
    _url_base = os.environ.get(
        "CITYFALCON_API", "https://api.cityfalcon.com/v0.2/stories"
    )

    def connect(self, key):
        self.__key = key if key else os.environ.get("CITYFALCON_KEY", None)
        self.connected = True
        return self

    def convert_data_to_df(self, data, fields, limit) -> pd.DataFrame:
        news = []
        for element in data["stories"]:
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
                elif field == "logo":
                    new_formated["logo"] = (
                        element["source"]["imageUrls"]["large"]
                        if element["source"] and element["source"]["imageUrls"]
                        else ""
                    )
                elif field == "date":
                    new_formated["date"] = element["publishTime"]
                else:
                    raise ValueError(f"Unknow parameter {field}")
            news.append(new_formated)
        if limit and isinstance(limit, int) and limit > 0:
            news = news[:limit]
        return pd.DataFrame.from_records(news)

    def get(
        self,
        action,
        fields=[
            "image",
            "title",
            "logo",
            "link",
        ],
        country="US",
        limit=None,
        min_score=20,
        paywall=False,
        identifier_type="full_tickers",
        time_filter="d1",
        languages="en",
    ) -> pd.DataFrame:
        self.check_connect()
        url = f"{self._url_base}?access_token={self.__key}"
        url = f"{url}&identifier_type={identifier_type}&paywall={paywall}&identifiers={action}_{country}&categories=mp%2Cop"
        url = f"{url}&min_cityfalcon_score={min_score}&order_by=latest&time_filter={time_filter}&all_languages=false&languages={languages}"
        req = requests.get(url)
        dict_news = req.json()
        return self.convert_data_to_df(dict_news, fields, limit)
