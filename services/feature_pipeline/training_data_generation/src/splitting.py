import pandas as pd
from datetime import timedelta


def train_test_split(training_data: pd.DataFrame, last_n_days_for_testing: int) -> tuple[pd.DataFrame, pd.DataFrame]:
    cutoff_date = training_data["datetime"].max() - timedelta(days=last_n_days_for_testing)
    training_set = training_data[training_data["datetime"] < cutoff_date]
    test_set = training_data[training_data["datetime"] >= cutoff_date]
    return training_set, test_set
