import pandas as pd
from preprocessing import Preprocessor


class TrainingDataGenerator:
    def __init__(
        self,
        ohlc_data: pd.DataFrame,
        binning_thresholds: tuple[int],
        ohlc_window: int,
        prediction_window: int
    ) -> None:
        """
        Args:
            ohlc_window: the number of seconds after which we generate new candles when handling missing data
            binning_thresholds: a tuple consisting of the lower and upper thresholds.
            prediction_window: the number of seconds in the future we would like to make predictions with respect to.
        """
        self.ohlc_data = ohlc_data
        self.ohlc_window = ohlc_window
        self.prediction_window = prediction_window
        self.classification_thresholds = binning_thresholds

    def _process_features(self) -> pd.DataFrame:
        """
        Apply our preprocessing tasks to the raw features.

        Returns:
            pd.DataFrame: the preprocessed features
        """
        processor = Preprocessor(ohlc_window=self.ohlc_window)
        return processor.process(ohlc_data=self.ohlc_data)

    def _make_bins(self, close_pct_change: float) -> int | None:
        if close_pct_change < self.classification_thresholds[0]:
            return 0  # indicates "the same" price
        elif close_pct_change < self.classification_thresholds[1]:
            return 1  # indicates a lower price
        elif close_pct_change > self.classification_thresholds[1]:
            return 2  # indicates a higher price
        else:
            return None  # applies when the close_pct_change is a NoN

    @staticmethod
    def _insert_pct_changes(ohlc_data: pd.DataFrame, number_candles_in_future: int) -> pd.DataFrame:

        percentage_changes = ohlc_data["close"].pct_change(periods=number_candles_in_future)
        ohlc_data.insert(
            loc=ohlc_data.shape[1],
            column="close_pct_change",
            value=percentage_changes,
            allow_duplicates=False
        )

        return ohlc_data

    def _make_and_insert_target(self, ohlc_data: pd.DataFrame, number_candles_in_future: int) -> pd.DataFrame:
        """
        Create the target data using the binning procedure above and insert them into the feature dataframe.
        Doing this results in some NaNs that are managed through shifting and eventual deletion of some final rows.

        Args:
            ohlc_data: preprocessed OHLC data
            number_candles_in_future: the

        Returns:
            pd.DataFrame:
        """
        ohlc_data.insert(
            loc=ohlc_data.shape[1],
            column="target",
            value=ohlc_data["close_pct_change"].apply(self._make_bins)
        )

        # This shifting is required because the pct_changes for the first {number_candles_in_future} rows will be NaNs.
        ohlc_data["target"] = ohlc_data["target"].shift(periods=-number_candles_in_future)
        ohlc_data.dropna(subset=["target"], axis=0)  # Because there will be NaNs at the end following the shift
        ohlc_data = ohlc_data.drop(columns=["close_pct_change"])  # No longer required
        return ohlc_data

    def make_training_data(self) -> pd.DataFrame:
        """
        Creates target data from the features by categorizing the close price in the next {prediction_window} seconds
        using pre-defined classification thresholds. Then we add this data to the features, and return the resulting
        dataframe after some cleanup.

        Returns:
            pd.DataFrame: the full training data
        """
        assert self.prediction_window % self.ohlc_window == 0, "Prediction window must be a multiple of OHLC window"
        number_candles_in_future = self.prediction_window // self.ohlc_window

        processed_features = self._process_features()
        processed_features_with_pct_changes = self._insert_pct_changes(
            ohlc_data=processed_features,
            number_candles_in_future=number_candles_in_future
        )

        training_data = self._make_and_insert_target(
            ohlc_data=processed_features_with_pct_changes, number_candles_in_future=number_candles_in_future
        )
        return training_data
