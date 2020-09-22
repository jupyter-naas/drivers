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

    def plot_output(
        self, data_points: int = 0, plot_width: int = 15, plot_height: int = 6
    ):
        return self.__plot_output(data_points, plot_width, plot_height)

    def init_class_vars(
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

    def __createmodel(self, model_type):

        if model_type == "ARIMA":
            import pmdarima as pm

            model = pm.auto_arima(
                self.input_df[self.label], **self.model_params["arima"]
            )
            predicted_values = model.predict(n_periods=self.data_points)
        elif model_type in ("LINEAR", "SVR"):
            prediction = (
                self.input_df[self.label]
                .shift(-self.data_points)
                .to_numpy()
                .reshape(-1, 1)
            )
            X = self.input_df[self.label].to_numpy().reshape(-1, 1)[: -self.data_points]
            y = prediction[: -self.data_points].ravel()
            if model_type == "SVR":
                from sklearn.svm import SVR

                model = SVR(**self.model_params["svr"])
            else:
                from sklearn.linear_model import LinearRegression

                model = LinearRegression(**self.model_params["linear"])
            model.fit(X, y)
            X_predict = (
                self.input_df[self.label]
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
        predicted_index = pd.date_range(
            start=data_df.index.max() + Day(1), periods=self.data_points
        )
        predict_df = pd.DataFrame(
            data=predicted_values, index=predicted_index, columns=[predicted_col]
        )
        predict_df.index.name = self.date_column
        return predict_df

    def __modelling_prediction(self):
        if self.prediction_type == "all":
            output_dfs = [
                self.input_df,
            ]
            models_dict = {}
            predicted_cols = []
            for param in self.model_params:
                model_type = self.param_model_map[param]

                predicted_col = model_type
                predicted_cols.append(predicted_col)
                model, predicted_values = self.__createmodel(model_type)
                models_dict[model_type] = model
                predicted_df = self.__transform_output(
                    self.input_df.copy(), predicted_values, predicted_col
                )
                output_dfs.append(predicted_df)
        else:
            output_dfs = [
                self.input_df,
            ]
            models_dict = {}
            predicted_cols = []
            model_type = self.param_model_map[self.prediction_type]
            predicted_col = model_type
            predicted_cols.append(predicted_col)
            model, predicted_values = self.__createmodel(model_type)
            models_dict[model_type] = model
            predicted_df = self.__transform_output(
                self.input_df.copy(), predicted_values, predicted_col
            )
            output_dfs.append(predicted_df)
        return predicted_cols, output_dfs

    def __plot_output(
        self, data_points: int, plot_width: int = 15, plot_height: int = 6
    ):
        if data_points == 0:
            data_points = self.data_points + 7
        if self.plot:
            if self.prediction_type == "all":
                # lines no from the df = no of prediction_types * the datapoints required
                line_nos = data_points * 5  # lines from the end to plot
            else:
                line_nos = data_points * 3  # lines from the end to plot

            return self.plot_df.tail(line_nos).plot.line(
                figsize=(plot_width, plot_height)
            )
        else:
            raise ValueError("Rerun the prediction with plot set to True")

    def __melt_output(self, predicted_cols, output_dfs):
        final_df = pd.concat(
            output_dfs,
            ignore_index=False,
            axis=1,
        )
        for col in predicted_cols:
            final_df[col] = final_df[col].fillna(value=self.input_df[self.label])
        final_df["COMPOUND"] = final_df[predicted_cols].mean(axis=1)

        final_df.index.name = self.date_column
        final_df = final_df.rename(columns={self.label: "ACTUAL"})
        plot_df = final_df.copy()
        final_df = final_df.reset_index().melt(
            id_vars=[self.date_column], var_name="LABEL", value_name="VALUE"
        )
        final_df = final_df.sort_values(by=[self.date_column, "LABEL"])

        return final_df, plot_df

    def predict(
        self,
        prediction_type: str,
        dataset: pd.DataFrame,
        label: str,
        date_column: str,
        data_points: int = 20,
        plot: bool = True,
    ):
        # initializes the class variables
        self.init_class_vars(
            prediction_type=prediction_type,
            dataset=dataset,
            label=label,
            date_column=date_column,
            data_points=data_points,
            plot=plot,
        )

        # modelling and making the predictions
        predicted_cols, output_dfs = self.__modelling_prediction()

        # making the output more user friendly and also plotting the graph
        self.final_predicted_df, self.plot_df = self.__melt_output(
            predicted_cols, output_dfs
        )
        return self.final_predicted_df
