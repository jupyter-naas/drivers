import pandas as pd
from pandas.tseries.offsets import Day


class Prediction:
    def __init__(self):

        self.param_model_map = {"arima": "ARIMA", "linear": "LINEAR", "svr": "SVR"}

        # either all to predict using all the models else one of arima, svr or linear
        self.prediction_type = None

        # input data
        self.input_df = None

        # the column name to be used as the date column
        self.date_column = None

        # The value to be predicted
        self.label = None

        # the number of data points in the future to be predicted, max 1000
        self.data_points = 20

        # True if the final output needs to be stored as graph, False otherwise
        self.plot = True

        self.model_params = dict(
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
        self.final_predicted_df = None
        self.plot_df = None

    def __init_class_vars(
        self,
        prediction_type: str,
        dataset: pd.DataFrame,
        label: str,
        date_column: str,
        data_points: int,
        plot: bool,
    ):

        # either all to predict using all the models else one of arima, svr or linear
        self.prediction_type = prediction_type

        # input data
        if isinstance(dataset, pd.Series):
            self.input_df = dataset.to_frame()
        elif isinstance(dataset, pd.DataFrame):
            self.input_df = dataset
        else:
            raise ValueError("The dataset should either be a Dataframe or Series.")

        # the column name to be used as the date column
        self.date_column = date_column

        # The value to be predicted
        if label in self.input_df.columns:
            self.label = label
        else:
            raise ValueError("The label should a column name of the dataset.")

        # the number of data points in the future to be predicted, max 1000
        self.data_points = data_points

        # True if the final output needs to be stored as graph, False otherwise
        self.plot = plot

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
            X_predict = (
                df[self.label]
                .to_numpy()
                .reshape(-1, 1)[-self.data_points :]  # noqa: E203
            )
            predicted_values = model.predict(X_predict)
        else:
            raise ValueError(
                "Please specify an prediction_type as arima OR linear OR svr or all"
            )
        return model, predicted_values

    def __transform_output(self, data_df, predicted_values, predicted_col):
        predicted_date = pd.date_range(
            start=data_df[self.date_column].max() + Day(1), periods=self.data_points
        )
        predict_df = pd.DataFrame(data=predicted_values, columns=[predicted_col])
        predict_df[self.date_column] = predicted_date
        # predict_df.index.name = self.date_column
        return predict_df

    def __modelling_prediction(self, df):
        if self.prediction_type == "all":
            output_dfs = [
                df,
            ]
            models_dict = {}
            for param in self.model_params:
                model_type = self.param_model_map[param]

                predicted_col = model_type
                model, predicted_values = self.__createmodel(model_type, df)
                models_dict[model_type] = model
                predicted_df = self.__transform_output(
                    df.copy(), predicted_values, predicted_col
                )
                output_dfs.append(predicted_df)
        else:
            output_dfs = [
                df,
            ]
            models_dict = {}
            model_type = self.param_model_map[self.prediction_type]
            predicted_col = model_type
            model, predicted_values = self.__createmodel(model_type)
            models_dict[model_type] = model
            predicted_df = self.__transform_output(
                df.copy(), predicted_values, predicted_col
            )
            output_dfs.append(predicted_df)
        return output_dfs

    def __multi_company(self):
        companies = self.input_df.Company.unique()
        output_dfs = []
        for company in companies:
            filtered = self.input_df.loc[self.input_df["Company"] == company]
            outputs = self.__modelling_prediction(filtered)
            for out in outputs:
                out["Company"] = company
            output_dfs = [*output_dfs, *outputs]
        return output_dfs

    def get(
        self,
        dataset: pd.DataFrame,
        prediction_type: str = "all",
        label: str = "Close",
        date_column: str = "Date",
        data_points: int = 20,
        plot: bool = False,
    ):
        # initializes the class variables
        self.__init_class_vars(
            prediction_type=prediction_type,
            dataset=dataset,
            label=label,
            date_column=date_column,
            data_points=data_points,
            plot=plot,
        )

        # modelling and making the predictions
        output_dfs = None
        if "Company" in dataset.columns:
            output_dfs = self.__multi_company()
        else:
            output_dfs = self.__modelling_prediction(dataset)
        res = pd.concat(output_dfs)
        res = res.reset_index(drop=True)
        return res
