import pandas as pd 

from datetime import timedelta
from bokeh.plotting import figure


def plot_candles(data: pd.DataFrame, title: str | None = "", window_seconds: int | None = 60) -> figure:
    """
    Generates and returns a candle stick chart.

    Args:
        data (pd.DataFrame): The OHLC data to plot, containing the columns: "open", "high", "low", "close".
        title (str | None, optional): _description_. Defaults to "".
        window_seconds (int | None, optional): _description_. Defaults to 60.

    Returns:
        figure: Bokeh figure containing the chart.
    """
    data.insert(
        loc=data.shape[1],
        column="date",
        value=pd.to_datetime(data["timestamp"], unit="ms"),
        allow_duplicates=False
    )

    price_increase: bool = data["close"] > data["open"]
    price_decrease: bool = data["close"] < data["open"]

    bandwidth = 1000*window_seconds / 2

    x_max = data["date"].max() - timedelta(minutes=5)
    x_min = data["date"].min() - timedelta(minutes=5)

    tools = "pan, wheel_zoom, box_zoom, reset, save"
    plot_object = figure(x_axis_type="datetime", x_range=(x_min, x_max), tools=tools, width=800, height=800, title=title)
    plot_object.grid.grid_line_alpha = 0.3

    plot_object.segment(data["date"], data["high"], data["date"], data["low"], color="black")
    
    plot_object.vbar(
        data["date"][price_increase],
        bandwidth, 
        data["open"][price_increase],
        data["close"][price_increase],
        fill_color="#70bd40",
        line_color="black"
    )

    plot_object.vbar(
        data["date"][price_decrease],
        bandwidth, 
        data["open"][price_decrease],
        data["close"][price_decrease],
        fill_color="#70bd40",
        line_color="black"
    )

    return plot_object