import pandas as pd
import numpy as np


class Prediction:

    param_model_map = {"arima": "ARIMA", "linear": "LINEAR", "svr": "SVR"}
    # either all to predict using all the models else one of arima, svr or linear
    prediction_type = None
    # input data
    input_df = None
    # the column name to be used as the date column
    date_column = None
    # The value to be predicted
    label = None
    # the number of data points in the future to be predicted, max 1000
    data_points = 20
    model_params = dict(
        arima=dict(
            start_p=1,
            start_q=1,
            test="adf",  # use adftest to find optimal 'd'
            max_p=3,
            max_q=3,  # maximum p and q
            m=1,  # frequency of series
            d=None,  # let model determine 'd'
            seasonal=False,  # No Seasonality / True for seasoanal i.e. SARIMA
            start_P=0,
            D=0,
            trace=True,
            error_action="ignore",
            suppress_warnings=True,
            stepwise=True,
        ),
        linear=dict(fit_intercept=True, normalize=False, n_jobs=None),
        svr=dict(kernel="rbf", C=1e3, gamma=0.1),
    )

    def __init_class_vars(
        self,
        prediction_type: str,
        dataset: pd.DataFrame,
        label: str,
        date_column: str,
        data_points: int,
    ):

        # either all to predict using all the models else one of arima, svr or linear
        self.prediction_type = prediction_type

        # input data
        if isinstance(dataset, pd.Series):
            self.input_df = dataset.to_frame()
        elif isinstance(dataset, pd.DataFrame):
            self.input_df = dataset
        else:
            error_text = f"The dataset should either be a Dataframe or Series, not {dataset.__name__}"
            self.print_error(error_text)
            return None

        # the column name to be used as the date column
        self.date_column = date_column

        # The value to be predicted
        if label in self.input_df.columns:
            self.label = label
        else:
            error_text = "The label should a column name of the dataset."
            self.print_error(error_text)
        # the number of data points in the future to be predicted, max 1000
        self.data_points = data_points

    def __createmodel(self, model_type, df):

        if model_type == "ARIMA":
            import pmdarima as pm

            model = pm.auto_arima(df[self.label], **self.model_params["arima"])
            predicted_values = model.predict(n_periods=self.data_points)
        elif model_type in ("LINEAR", "SVR"):
            prediction = (
                df[self.label].shift(-self.data_points).to_numpy().reshape(-1, 1)
            )
            X = df[self.label].to_numpy().reshape(-1, 1)[: -self.data_points]
            y = prediction[: -self.data_points].ravel()
            if model_type == "SVR":
                from sklearn.svm import SVR

                model = SVR(**self.model_params["svr"])
            else:
                from sklearn.linear_model import LinearRegression

                model = LinearRegression(**self.model_params["linear"])
            model.fit(X, y)
            x_predict = (
                df[self.label]
                .to_numpy()
                .reshape(-1, 1)[-self.data_points :]  # noqa: E203
            )
            predicted_values = model.predict(x_predict)
        else:
            error_text = (
                "Please specify an prediction_type as arima OR linear OR svr or all"
            )
            self.print_error(error_text)
            return None
        return model, [df[self.label].iloc[-1], *predicted_values]

    def __transform_output(self, data_df, predicted_values, predicted_col):
        predicted_date = pd.date_range(
            start=data_df[self.date_column].max(), periods=self.data_points + 1
        )
        predict_df = pd.DataFrame(data=predicted_values, columns=[predicted_col])
        predict_df[self.date_column] = predicted_date
        return predict_df

    def __modelling_prediction(self, df):
        if self.prediction_type == "all" or self.prediction_type == "COMPOUND":
            output_dfs = [
                df.copy(),
            ]
            models_dict = {}
            predicted_cols = []
            for param in self.model_params:
                model_type = self.param_model_map[param]

                predicted_col = model_type
                predicted_cols.append(predicted_col)
                model, predicted_values = self.__createmodel(model_type, df)
                models_dict[model_type] = model
                predicted_df = self.__transform_output(
                    df.copy(), predicted_values, predicted_col
                )
                output_dfs.append(predicted_df)
        else:
            output_dfs = [
                df.copy(),
            ]
            models_dict = {}
            predicted_cols = []
            model_type = self.param_model_map[self.prediction_type]
            predicted_col = model_type
            predicted_cols.append(predicted_col)
            model, predicted_values = self.__createmodel(model_type, df)
            models_dict[model_type] = model
            predicted_df = self.__transform_output(
                df.copy(), predicted_values, predicted_col
            )
            output_dfs.append(predicted_df)
        return predicted_cols, output_dfs

    def __multi_company(self):
        companies = self.input_df.Company.unique()
        output_dfs = []
        predicted_cols = []
        for company in companies:
            filtered = self.input_df.loc[self.input_df["Company"] == company]
            predicted_cols, outputs = self.__modelling_prediction(filtered)
            for out in outputs:
                out["Company"] = company
                out = out.reset_index(drop=True)
            output_dfs = [*output_dfs, *outputs]

        return predicted_cols, output_dfs

    def get(
        self,
        dataset: pd.DataFrame,
        prediction_type: str = "COMPOUND",
        label: str = "Close",
        date_column: str = "Date",
        data_points: int = 20,
        concat_label=None,
    ):
        # initializes the class variables
        self.__init_class_vars(
            prediction_type=prediction_type,
            dataset=dataset,
            label=label,
            date_column=date_column,
            data_points=data_points,
        )

        # modelling and making the predictions
        output_dfs = None
        predicted_cols = None
        group = None
        if "Company" in dataset.columns:
            group = ["Date", "Company"]
            predicted_cols, output_dfs = self.__multi_company()
        else:
            group = ["Date"]
            predicted_cols, output_dfs = self.__modelling_prediction(dataset)
        res = pd.concat(output_dfs)
        res = res.reset_index(drop=True)
        agg = {i: ("sum" if i in predicted_cols else "first") for i in res.columns}
        res = res.groupby(group, as_index=False, dropna=False).agg(agg)
        if len(predicted_cols) > 1:
            res["COMPOUND"] = res[predicted_cols].mean(axis=1)
            res["COMPOUND"] = res["COMPOUND"].replace(0.000000, np.nan)
        for col in predicted_cols:
            res[col] = res[col].replace(0.000000, np.nan)
        if prediction_type == "COMPOUND":
            res = res.drop(columns=predicted_cols)
        if concat_label is not None:
            if len(predicted_cols) > 1:
                res[concat_label] = res["COMPOUND"].replace(np.nan, 0.000000) + res[
                    label
                ].replace(np.nan, 0.000000)
            else:
                res[concat_label] = res[predicted_cols[0]].replace(
                    np.nan, 0.000000
                ) + res[label].replace(np.nan, 0.000000)
        return res
