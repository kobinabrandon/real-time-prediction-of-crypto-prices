import pandas as pd
from tqdm import tqdm


class Preprocessor:
    def __init__(self, ohlc_window: int) -> None:
        self.ohlc_window = ohlc_window

    def _index_features(self, ohlc_data: pd.DataFrame):
        ohlc_data.set_index("timestamp", inplace=True)
        from_ms, to_ms = ohlc_data.index.min(), ohlc_data.index.max()
        minutes = range(from_ms, to_ms, self.ohlc_window * 1000)

        # Reindex the data using the full range of timestamps in minutes, and make a corresponding column with datetime
        ohlc_data = ohlc_data.reindex(minutes)
        ohlc_data.insert(
            loc=ohlc_data.shape[1],
            column="datetime",
            value=pd.to_datetime(ohlc_data["timestamp"], unit="ms")
        )

        return ohlc_data

    @staticmethod
    def _manual_forward_fill(ohlc_data: pd.DataFrame):
        """
        Searches all rows for missing values and forward fills them with the last "close" value.
        In the event that any entry in the first row is missing, an exception will be raised so that you can determine
        how best to fill the entry.

        The function is said to be "manual" because I am aware that pd.Series has the fillna method, but I
        chose to write this as a challenge.

        Returns:
            ohlc_data: the OHLC feature data with no missing values.

        """
        rows = tqdm(
            iterable=range(ohlc_data.shape[0]),
            desc="Searching rows for missing values..."
        )

        for row in rows:
            row_index = ohlc_data.index[row]
            for column_name in ["open", "high", "low", "close", "product_id"]:
                data_value = ohlc_data.iloc[row, ohlc_data.columns.get_loc(key=column_name)]
                if pd.isnull(data_value) and row != 0:
                    ohlc_data.loc[row_index, column_name] = \
                        ohlc_data.iloc[row - 1, ohlc_data.columns.get_loc(key="close")]
                elif pd.isnull(data_value) and row == 0:
                    raise Exception(f"The first value from the {column_name} column is missing. Investigate further")
        return ohlc_data

    def process(self, ohlc_data: pd.DataFrame) -> pd.DataFrame:
        """
        Perform indexing, and forward filling

        Returns:
            pd.DataFrame: processed OHLC data.

        """
        indexed_features = self._index_features(ohlc_data=ohlc_data)

        # Forward fill missing values
        processed_features = self._manual_forward_fill(ohlc_data=indexed_features)
        return processed_features
