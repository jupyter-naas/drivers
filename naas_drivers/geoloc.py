import pandas as pd
from geopy.extra.rate_limiter import RateLimiter


class GeoLocator:
    """GeoLocator allows users to fetch latitude and longitude for
    a list of address in dataframe column
    Example usage:
        geo = GeoLocator(limit=50)
        geo.use_google(api_key="your_api_key") #to set the service to google maps
         OR
        geo.use_osm(user_agent="your_app_name") #to set the service to open street maps
        df = geo.geoloc(df, column="address")
    """

    def __init__(self, limit=50):
        self.client = None
        self.limit = limit

    def use_google(
        self,
        api_key: str,
        domain: str = "maps.googleapis.com",
        min_delay_seconds: int = 1,
        **kwargs,
    ):
        from geopy.geocoders import GoogleV3

        client = GoogleV3(api_key=api_key, domain=domain, **kwargs)
        self.client = RateLimiter(client.geocode, min_delay_seconds=min_delay_seconds)

    def use_osm(self, user_agent: str, min_delay_seconds: int = 1, **kwargs):
        if min_delay_seconds < 1:
            raise ValueError("Minimum 1 second delay is required for osm")
        from geopy.geocoders import Nominatim

        client = Nominatim(user_agent=user_agent, **kwargs)
        self.client = RateLimiter(client.geocode, min_delay_seconds=min_delay_seconds)

    def geoloc(self, df: pd.DataFrame, column: str) -> pd.DataFrame:
        if not self.client:
            raise ValueError(
                "Please set the map service using either `use_google` or `use_osm` method"
            )
        if df.shape[0] > self.limit:
            raise ValueError(
                f"Dataset number of rows is more than the set limit of {self.limit}"
            )
        df["_location"] = df[column].apply(self.client)
        df["LATITUDE"] = df["_location"].apply(
            lambda loc: loc.latitude if loc else None
        )
        df["LONGITUDE"] = df["_location"].apply(
            lambda loc: loc.longitude if loc else None
        )
        df = df.drop(columns=["_location"])
        return df

    def help(self):
        print(f"=== {type(self).__name__} === \n")
        print(
            '.use_google(api_key, domain: str = "maps.googleapis.com", min_delay_seconds: int = 1, **kwargs)\
                => get file from ftp path\n'
        )
        print(".geoloc(df, column) => get file from ftp path\n")
        print(
            ".use_osm(user_agent, min_delay_seconds, kwargs) => do ls in ftp in path\n"
        )
