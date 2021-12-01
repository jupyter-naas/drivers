import pandas as pd
import datetime as dt


class Yahoofinance:
    def get(
        self,
        tickers,
        date_from=-36,
        date_to="today",
        interval="1d",
        moving_averages: list = [],
        moving_average_col="Close",
    ):
        """Generate financial data"""
        # Init dataframe
        df_stocks = pd.DataFrame()

        # If tickers is string => change to list
        if isinstance(tickers, str):
            tickers = [tickers]
        # Set date from
        if isinstance(date_from, int):
            if date_from < 0:
                date_from = dt.datetime.today() + dt.timedelta(days=date_from)
            else:
                error_text = f"❌ date_from ({date_from}) cannot be positive."
                print(error_text)
        # Set date to
        if isinstance(date_to, int):
            if date_to < 0:
                date_to = dt.datetime.today() + dt.timedelta(days=date_to)
            else:
                error_text = f"❌ date_from ({date_to}) cannot be positive."
                print(error_text)
        if isinstance(date_to, str):
            if date_to == "today":
                date_to = dt.datetime.today()
            else:
                error_text = f"❌ date_to cannot be ({date_to}). Try with 'today' or use an integer."
                print(error_text)
        period1 = date_from.strftime("%s")
        period2 = date_to.strftime("%s")
        for ticker in tickers:
            url = (
                f"https://query1.finance.yahoo.com/v7/finance/download/"
                f"{ticker}?period1={period1}&period2={period2}&interval={interval}&events=history"
            )
            df = pd.read_csv(url)
            df["Ticker"] = ticker
            if len(moving_averages) > 0:
                if moving_average_col in df.columns:
                    for moving_average in moving_averages:
                        if isinstance(moving_average, str):
                            moving_average = int(moving_average)
                        if isinstance(moving_average, int):
                            df[f"MA{moving_average}"] = (
                                df[moving_average_col].rolling(moving_average).mean()
                            )
                        else:
                            error_text = f"❌ Moving average '{moving_average_col}' not recognize."
                            print(error_text)
                else:
                    error_text = f"❌ We can not calculate moving averages. Columns '{moving_average_col}' does not exist in dataframe."
                    print(error_text)
            df_stocks = pd.concat([df_stocks, df])
        df_stocks["Date"] = pd.to_datetime(df_stocks["Date"], format="%Y-%m-%d")
        return df_stocks
