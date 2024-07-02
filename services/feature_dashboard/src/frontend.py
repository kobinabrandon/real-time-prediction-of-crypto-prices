import streamlit as st
import pandas as pd

from loguru import logger 

from src.backend import get_features_from_store, get_feature_view
from src.dashboard_config import config
from src.plot import plot_candles


def make_title_page(title: str):

    st.write(
        f"""
        # {title}
        """
    )

    online_or_offline = st.sidebar.selectbox(
        label="Select the feature store that you want to fetch features from",
        options=("Online", "Offline")
    )

    feature_view = get_feature_view(
        project=config.hopsworks_project_name,
        api_key=config.hopsworks_api_key,
        feature_group_name=config.feature_group_name,
        feature_group_version=config.feature_group_version
    )

    features = get_features_from_store(live_or_historical=config.live, feature_view_object=feature_view)
    logger.info(f"Received {len(features)} rows of data from the feature store")

    st.bokeh_chart(
        figure=plot_candles(data=features.tail(1440))
    )


make_title_page(title="OHLC Feature Dashboard")
