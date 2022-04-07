"""Google Analytics Driver."""
import re
from datetime import datetime, timedelta
import numpy as np
import pandas as pd
from google.oauth2 import service_account
from apiclient.discovery import build
from naas_drivers.driver import InDriver, OutDriver


# Helper function
def ga_naming_to_title(ga_nanimg: str):
    name = ga_nanimg.split(":")[-1]
    splited_name = re.findall(r"[A-Z]?[a-z]+|[A-Z]+(?=[A-Z]|$)", name)
    return " ".join([name.title() for name in splited_name])


ga_metrics = [
    "ga:users",
    "ga:newUsers",
    "ga:percentNewSessions",
    "ga:sessions",
    "ga:bounces",
    "ga:bounceRate",
    "ga:sessionDuration",
    "ga:avgSessionDuration",
    "ga:organicSearches",
    "ga:entrances",
    "ga:entranceRate",
    "ga:pageviews",
    "ga:pageviewsPerSession",
    "ga:uniquePageviews",
    "ga:timeOnPage",
    "ga:avgTimeOnPage",
    "ga:exits",
    "ga:exitRate",
    "ga:pageLoadTime",
    "ga:pageLoadSample",
    "ga:avgPageLoadTime",
    "ga:domainLookupTime",
    "ga:avgDomainLookupTime",
    "ga:pageDownloadTime",
    "ga:avgPageDownloadTime",
    "ga:redirectionTime",
    "ga:avgRedirectionTime",
    "ga:serverConnectionTime",
    "ga:avgServerConnectionTime",
    "ga:serverResponseTime",
    "ga:avgServerResponseTime",
    "ga:speedMetricsSample",
    "ga:domInteractiveTime",
    "ga:avgDomInteractiveTime",
    "ga:domContentLoadedTime",
    "ga:avgDomContentLoadedTime",
    "ga:domLatencyMetricsSample",
    "ga:uniqueDimensionCombinations",
    "ga:hits",
    "ga:sessionsPerUser",
]


class GoogleAnalytics(InDriver, OutDriver):
    """
    Google Analytics driver.
    """

    def __init__(self) -> None:
        self.views = Views(self)
        self.available_metrics = []
        self.__generate_methods()

    def connect(self, json_path: str, view_id: str):
        credentials = service_account.Credentials.from_service_account_file(
            json_path, scopes=["https://www.googleapis.com/auth/analytics.readonly"]
        )
        self.view_id = view_id
        self.service = build("analyticsreporting", "v4", credentials=credentials)
        return self

    # This method is used to automatically generate methods based on metrics available in GA.
    def __generate_methods(self):
        for metric_id in ga_metrics:

            computed_name = metric_id
            computed_name = re.sub(r"(?<!^)(?=[A-Z])", "_", computed_name).lower()[3:]

            try:
                getattr(self, computed_name)
            except Exception as e:  # noqa: F841
                self.available_metrics.append(computed_name)
                setattr(self, computed_name, Metric())

            def custom_get_trend(metric_id):
                return (
                    lambda dimensions, start_date=None, end_date=None: self.get_trend(
                        metric_id, dimensions, start_date, end_date
                    )
                )

            setattr(
                getattr(self, computed_name), "get_trend", custom_get_trend(metric_id)
            )

    def get_trend(self, metrics, dimensions, start_date, end_date):
        """
        Return an dataframe object with 6 columns:
        - DATE         GA dimensions
        - METRIC       GA metrics
        - VALUE        Metrics value
        - VALUE_COMP   Metrics last value comparison
        - VARV         Variation in value between VALUE and VALUE_COMP
        - VARP         Variation in % between VALUE and VALUE_COMP

        Parameters
        ----------
        metrics: str:
            - New visitors = "ga:newUsers"
            - User = "ga:users"
        dimensions: str:
            List of google analytics dimensions
            - Hourly = "hourly"
            - Day = "daily"
            - Week = "weekly"
            - Month = "monthly"
        start_date: str: default="30daysAgo"
            "NdaysAgo" with n equal to int
        end_date: str: default="today"
            "today", "yesterday"
        """

        allowed_aliases = ["hourly", "daily", "weekly", "monthly"]
        allowed_dimensions = [
            "ga:date,ga:hour",
            "ga:date",
            "ga:year,ga:week",
            "ga:year,ga:month",
        ]

        if dimensions in allowed_aliases:
            dimensions = allowed_dimensions[allowed_aliases.index(dimensions)]

        if dimensions not in allowed_dimensions:
            raise Exception(f"'dimensions' should be one of {allowed_dimensions}")

        # Get data
        df = self.views.get_data(
            self.view_id,
            metrics=metrics,
            dimensions=dimensions,
            start_date=start_date,
            end_date=end_date,
            format_type="summary",
            pivots_dimensions="ga:country",  # not used
        )

        # Format trend dataset
        df["DATE_ISO"] = df.index
        df = df.reset_index(drop=True)
        if dimensions == "ga:date,ga:hour":
            df["DATE_ISO"] = pd.to_datetime(
                df.apply(lambda row: f"{row.DATE_ISO[0]} {row.DATE_ISO[1]}:00", axis=1)
            )
            df["DATE"] = df["DATE_ISO"].dt.strftime("%Y-%m-%d %H:00:00")
        elif dimensions == "ga:date":
            df["DATE_ISO"] = pd.to_datetime(
                df.apply(lambda row: row.DATE_ISO[0], axis=1)
            )
            df["DATE"] = df["DATE_ISO"].dt.strftime("%Y-%m-%d")
        elif dimensions == "ga:year,ga:week":
            df["DATE_ISO"] = pd.to_datetime(
                df.apply(
                    lambda row: datetime.strptime(
                        f"{row.DATE_ISO[0]}-W{row.DATE_ISO[1]}" + "-1", "%Y-W%W-%w"
                    ),
                    axis=1,
                )
            )
            df["DATE"] = df["DATE_ISO"].dt.strftime("%Y W%W")
        elif dimensions == "ga:year,ga:month":
            df["DATE_ISO"] = pd.to_datetime(
                df.apply(
                    lambda row: datetime.strptime(
                        f"{row.DATE_ISO[0]}-M{row.DATE_ISO[1]}", "%Y-m%m"
                    ),
                    axis=1,
                )
            )
            df["DATE"] = df["DATE_ISO"].dt.strftime("%Y %b")
        df["METRIC"] = metrics.replace("ga:", "")
        df["VALUE"] = df[metrics]
        df = df.drop(metrics, axis=1)
        df.columns = df.columns.str.upper()

        # Calc variation
        for idx, row in df.iterrows():
            if idx == 0:
                value_n1 = 0
            else:
                value_n1 = df.loc[df.index[idx - 1], "VALUE"]
            df.loc[df.index[idx], "VALUE_COMP"] = value_n1
        df["VARV"] = df["VALUE"] - df["VALUE_COMP"]
        df["VARP"] = df["VARV"] / abs(df["VALUE_COMP"])
        return df


class Metric:
    pass


class Views:
    def __init__(self, parent) -> None:
        self.parent = parent

    @staticmethod
    def _get_body(
        view_id: str,
        start_date: str,
        end_date: str,
        metrics: str,
        pivots_dimensions: str,
        dimensions: str = "ga:yearMonth",
        max_group_count: int = 1000,
    ) -> dict:
        """
        Create the body of the request to Google Analytics Reporting API V4.

        Args:
            view_id: your access point for reports; a defined view of data from a property.
            date_ranges: e.g. [{"startDates": "2020-01-01", "endDates": "2020-12-31"}]
            metrics: e.g. [{'expression': 'ga:users'}, {"expression": "ga:bounceRate"}]
            pivot_dimension: e.g. [{"name": "ga:channelGrouping"}]
            dimensions: e.g. [{'name': 'ga:yearMonth'}]

        Returns response in JSON format.
        """
        return {
            "reportRequests": [
                {
                    "viewId": view_id,
                    "dateRanges": {"startDate": start_date, "endDate": end_date},
                    "metrics": [
                        {"expression": metric} for metric in metrics.split(",")
                    ],
                    "dimensions": [
                        {"name": dimension} for dimension in dimensions.split(",")
                    ],
                    "pivots": [
                        {
                            "dimensions": {"name": pivots_dimensions},
                            "metrics": [{"expression": metrics}],
                            "maxGroupCount": max_group_count,
                        }
                    ],
                }
            ]
        }

    def get_data(
        self,
        view_id: str,
        metrics: str,
        pivots_dimensions: str,
        dimensions: str = "ga:yearMonth",
        start_date: str = None,
        end_date: str = None,
        format_type: str = "summary",
        max_group_count: int = 1000,
    ) -> pd.DataFrame:
        """
        Get data from Google Analytics Reporting API V4.
        """
        if format_type not in ("summary", "pivot"):
            raise ValueError(
                f"format_type must be either <summary> or <pivot> but is: {format_type}"
            )
        # Default date values
        start_date = (
            start_date
            if start_date
            else (datetime.today() - timedelta(days=365)).strftime("%Y-%m-%d")
        )
        end_date = end_date if end_date else datetime.today().strftime("%Y-%m-%d")
        # Create body
        body = self._get_body(
            view_id,
            start_date,
            end_date,
            metrics,
            pivots_dimensions,
            dimensions,
            max_group_count=max_group_count,
        )
        # Fetch Data
        try:
            response = self.parent.service.reports().batchGet(body=body).execute()
        except Exception as error:
            raise error()
        # JSON to Pandas DataFrame
        if format_type == "summary":
            return self.format_summary(response)
        return self.format_pivot(response)

    def get_unique_visitors(
        self, view_id: str, start_date: str = None, end_date: str = None
    ) -> pd.DataFrame:
        """
        Get the number of unique visitors.
        """
        unique_visitors = self.get_data(
            view_id,
            metrics="ga:users",
            pivots_dimensions="ga:channelGrouping",
            dimensions="ga:yearMonth",
            start_date=start_date,
            end_date=end_date,
            format_type="summary",
        )
        unique_visitors.reset_index(inplace=True)
        unique_visitors.columns = [
            ga_naming_to_title(col) for col in unique_visitors.columns
        ]
        return unique_visitors

    def get_bounce_rate(
        self, view_id=str, start_date: str = None, end_date: str = None
    ) -> pd.DataFrame:
        """
        Get the number of unique visitors.
        """
        bounce_rate = self.get_data(
            view_id,
            metrics="ga:bounceRate",
            pivots_dimensions="ga:channelGrouping",
            dimensions="ga:yearMonth",
            start_date=start_date,
            end_date=end_date,
            format_type="summary",
        )
        bounce_rate.reset_index(inplace=True)
        bounce_rate.columns = [ga_naming_to_title(col) for col in bounce_rate.columns]
        bounce_rate["Bounce Rate"] /= 100
        return bounce_rate

    def get_time_landing(
        self,
        view_id: str,
        landing_path: str = "/",
        start_date: str = None,
        end_date: str = None,
    ) -> pd.DataFrame:
        """
        Get the average time on landing page.
        """
        avg_time_landing = self.get_data(
            view_id,
            metrics="ga:avgTimeOnPage",
            pivots_dimensions="ga:landingPagePath",
            dimensions="ga:yearMonth",
            start_date=start_date,
            end_date=end_date,
            format_type="pivot",
        )
        if landing_path in avg_time_landing.columns:
            avg_time_landing = avg_time_landing.loc[:, landing_path]
        else:
            raise KeyError(
                f"Landing Path ({landing_path}) is not an available url pattern."
            )
        avg_time_landing.index.rename("Year Month", inplace=True)
        avg_time_landing.rename(
            columns={"ga:avgTimeOnPage": "avg_time_landing"}, inplace=True
        )
        avg_time_landing.reset_index(inplace=True)
        return avg_time_landing

    def get_pageview(
        self, view_id: str, start_date: str = None, end_date: str = None
    ) -> pd.DataFrame:
        """
        Get the views of pages.
        """
        pageview = self.get_data(
            view_id,
            metrics="ga:pageviews",
            pivots_dimensions="ga:pagePath",
            dimensions="ga:year",
            start_date=start_date,
            end_date=end_date,
            format_type="pivot",
        )
        pageview.columns = [page[0] for page in pageview.columns]
        pageview = pageview.head(1).T
        pageview.reset_index(inplace=True)
        pageview.columns = ["Pages", "Pageview"]
        return pageview

    def get_country(
        self,
        view_id: str,
        metrics: str = "ga:sessions",
        start_date: str = None,
        end_date: str = None,
    ):
        """
        Get sessions per country.
        """
        country = self.get_data(
            view_id,
            metrics=metrics,
            pivots_dimensions="ga:country",
            dimensions="ga:year",
            start_date=start_date,
            end_date=end_date,
            format_type="pivot",
        )
        country.columns = [ga_naming_to_title(c[0]) for c in country.columns]
        country = country.T
        country.reset_index(inplace=True)
        country.columns = ["Country", ga_naming_to_title(metrics)]
        return country

    @staticmethod
    def format_summary(response):
        """
        Format summary table.
        """
        row_index_names = response["reports"][0]["columnHeader"]["dimensions"]
        row_index = [
            element["dimensions"] for element in response["reports"][0]["data"]["rows"]
        ]
        row_index_named = pd.MultiIndex.from_arrays(
            np.transpose(np.array(row_index)), names=np.array(row_index_names)
        )
        # extract column names
        summary_column_names = [
            item["name"]
            for item in response["reports"][0]["columnHeader"]["metricHeader"][
                "metricHeaderEntries"
            ]
        ]
        # extract table values
        summary_values = [
            element["metrics"][0]["values"]
            for element in response["reports"][0]["data"]["rows"]
        ]
        return pd.DataFrame(
            data=np.array(summary_values),
            index=row_index_named,
            columns=summary_column_names,
        ).astype("float")

    @staticmethod
    def format_pivot(response):
        """
        Creates the final dataframe.
        """
        # extract table values
        pivot_values = [
            item["metrics"][0]["pivotValueRegions"][0]["values"]
            for item in response["reports"][0]["data"]["rows"]
        ]
        # create column index
        top_header = [
            item["dimensionValues"]
            for item in response["reports"][0]["columnHeader"]["metricHeader"][
                "pivotHeaders"
            ][0]["pivotHeaderEntries"]
        ]
        column_metrics = [
            item["metric"]["name"]
            for item in response["reports"][0]["columnHeader"]["metricHeader"][
                "pivotHeaders"
            ][0]["pivotHeaderEntries"]
        ]
        array = np.concatenate(
            (
                np.array(top_header),
                np.array(column_metrics).reshape((len(column_metrics), 1)),
            ),
            axis=1,
        )
        column_index = pd.MultiIndex.from_arrays(np.transpose(array))
        # create row index
        row_index_names = response["reports"][0]["columnHeader"]["dimensions"]
        row_index = [
            element["dimensions"] for element in response["reports"][0]["data"]["rows"]
        ]
        row_index_named = pd.MultiIndex.from_arrays(
            np.transpose(np.array(row_index)), names=np.array(row_index_names)
        )
        # combine into a dataframe
        return pd.DataFrame(
            data=np.array(pivot_values), index=row_index_named, columns=column_index
        ).astype("float")
