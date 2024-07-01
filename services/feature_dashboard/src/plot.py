import pandas as pd 
from bokeh.plotting import figure


def plot_candles(data: pd.DataFrame) -> figure:
    """
    Generates and returns a candle stick chart.

    Args:
        data (pd.DataFrame): The OHLC data to plot, containing the columns:
                             "open", "high", "low", "close".

    Returns:
        figure: Bokeh figure containing the chart.
    """
    