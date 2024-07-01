import streamlit as st
import pandas as pd

from loguru import logger 

from src.backend import get_features_from_store
from src.dashboard_config import config
from src.plot import plot_candles


st.write(
    """
    # OHLC Feature Dashboard
    """
)

data = get_features_from_store(
    feature_group_name=config.feature_group_name,
    feature_group_version=config.feature_group_version
)

st.bokeh_chart(
    figure=plot_candles(data=data.tail(1440))
)

#st.table(data)

