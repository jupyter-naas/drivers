from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
import pandas as pd
from pandas.io.json import json_normalize
from typing import Union


class SentimentAnalysis:
    def __sanitize_dataset(
        self, dataset: Union[pd.DataFrame, pd.Series], column_name: str
    ) -> pd.DataFrame:
        if isinstance(dataset, pd.Series):
            df = dataset.to_frame().reset_index()
        elif isinstance(dataset, pd.DataFrame):
            df = dataset
        else:
            raise ValueError("The dataset should either be a Dataframe or a Series")

        if column_name not in df.columns:
            raise ValueError(f"The {column_name} is not in dataset columns.")
        return df

    @staticmethod
    def __calculate_sentiment(
        df: pd.DataFrame, column_name: str, details: bool
    ) -> pd.DataFrame:
        analyzer = SentimentIntensityAnalyzer()
        df["senti_score"] = df[column_name].map(analyzer.polarity_scores)
        senti_df = json_normalize(df["senti_score"])

        def categorize_sentiment(comp_score):
            if float(comp_score) >= 0.05:
                return "Positive"
            elif float(comp_score) <= -0.05:
                return "Negative"
            else:
                return "Neutral"

        senti_df["Sentiment"] = senti_df["compound"].map(categorize_sentiment)
        senti_df = senti_df.rename(
            columns={
                "compound": "Score",
                "pos": "Positive",
                "neg": "Negative",
                "neu": "Neutral",
            }
        )
        df = pd.concat([df, senti_df], axis=1)
        cols = [column_name, "Sentiment", "Score"]
        if details:
            cols.extend(["Positive", "Negative", "Neutral"])
        df = df[cols]

        return df

    def calculate(
        self,
        dataset: Union[pd.DataFrame, pd.Series],
        column_name: str,
        details: bool = False,
    ) -> pd.DataFrame:
        """
        Calculates the sentiment of the text in each row in the dataset &
        returns it along with the input as a single dataframe in the output.
        :param dataset: either a pandas Dataframe or a Series
        :param column_name: String
                            The exact column name on which the sentiment is to be calculated .
                            This should be present in the dataset.
        :param details: Boolean, Default: False
                         Tells whether the details of sentiment analysis are to be outputted
        :return: pandas dataframe

        """
        df = self.__sanitize_dataset(dataset, column_name)
        df = self.__calculate_sentiment(df, column_name, details)

        return df
