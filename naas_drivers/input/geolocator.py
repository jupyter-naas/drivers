from naas_drivers.driver import InDriver
import pandas as pd
from geopy.extra.rate_limiter import RateLimiter


class Geolocator(InDriver):
    _key = None
    """GeoLocator allows users to fetch latitude and longitude for
    a list of address in dataframe column
    Example usage:
        naas_drivers.geolocator.connect(api_key="your_api_key", mode="google") #to set the service to google maps
         OR
        naas_drivers.geolocator.connect(api_key="your_app_name", mode = "osm") #to set the service to open street maps
        df = naas_drivers.geolocator.connect.get(df, column="address")
    """

    def __init__(self):
        self.client = None

    def connect(
        self,
        api_key,
        mode="google",
        domain: str = "maps.googleapis.com",
        min_delay_seconds: int = 1,
        **kwargs,
    ):
        if min_delay_seconds < 1:
            error_text = f"Minimum 1 second delay is required not {min_delay_seconds}"
            self.print_error(error_text)
            return None
        if mode == "google":
            from geopy.geocoders import GoogleV3

            client = GoogleV3(api_key=api_key, domain=domain, **kwargs)
            self.client = RateLimiter(
                client.geocode, min_delay_seconds=min_delay_seconds
            )
        elif mode == "som":
            from geopy.geocoders import Nominatim

            client = Nominatim(user_agent=api_key, **kwargs)
            self.client = RateLimiter(
                client.geocode, min_delay_seconds=min_delay_seconds
            )
        else:
            error_text = "mode should be osm or google"
            self.print_error(error_text)
        self.connected = True
        return self

    def check_connect(self):
        if not self.client:
            error_text = "Please set the map service using connect method"
            self.print_error(error_text)

    def get(self, df: pd.DataFrame, column: str, limit=50) -> pd.DataFrame:
        self.check_connect()
        if df.shape[0] > limit:
            error_text = f"Dataset number of rows is more than the set limit of {limit}"
            self.print_error(error_text)
        df["_location"] = df[column].apply(self.client)
        df["LATITUDE"] = df["_location"].apply(
            lambda loc: loc.latitude if loc else None
        )
        df["LONGITUDE"] = df["_location"].apply(
            lambda loc: loc.longitude if loc else None
        )
        df = df.drop(columns=["_location"])
        return df
