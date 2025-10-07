import asyncio
import datetime as dt
from pathlib import Path

import polars as pl
import streamlit as st


async def main() -> None:
    st.set_page_config(
        page_title="Sensor Data Dashboard",
        layout="wide",
    )
    st.title("Sensor Data Dashboard")

    st.sidebar.title("Settings")

    with st.sidebar:
        col1, _ = st.columns(2)
        with col1:
            granularity = st.selectbox(
                "Data Granularity",
                options=[
                    "7d",
                    "3d",
                    "1d",
                    "12h",
                    "6h",
                    "3h",
                    "1h",
                    "30m",
                    "15m",
                    "5m",
                    "1m",
                ],
                index=0,
                help="Select the granularity for data aggregation.",
            )

        col1, col2 = st.columns(2)
        with col1:
            start_date = st.date_input(
                "Start Date",
                value=dt.date.today() - dt.timedelta(days=365 * 2),
                help="Select the start date for data filtering.",
            )
        with col2:
            end_date = st.date_input(
                "End Date",
                value=dt.date.today(),
                help="Select the end date for data filtering.",
            )

    root_dir = Path(__file__).parent.parent
    resources_dir = root_dir / "resources"

    tab_main, tab_sub = st.tabs(["Sensor Hub 2", "Sensor Sub"])

    # "Timestamp","Temperature_Celsius(°C)","Relative_Humidity(%)","Absolute_Humidity(g/m³)","DPT_Celsius(°C)","VPD(kPa)"
    with tab_main:
        sensor_main_path = resources_dir / "sensor_hub2.csv"
        df_main = (
            pl.read_csv(sensor_main_path, try_parse_dates=True).rename(
                {
                    "Temperature_Celsius(°C)": "temperature",
                    "Relative_Humidity(%)": "humidity_relative",
                    "Absolute_Humidity(g/m³)": "humidity_absolute",
                    "DPT_Celsius(°C)": "dpt",
                    "VPD(kPa)": "vpd",
                }
            )
        ).filter(
            (pl.col("Timestamp") >= dt.datetime.combine(start_date, dt.time.min))
            & (pl.col("Timestamp") <= dt.datetime.combine(end_date, dt.time.max))
        )

        render_summary_stats(df_main)

        df_main = df_main.group_by_dynamic("Timestamp", every=granularity).agg(
            [
                pl.col("temperature").mean().alias("temperature"),
                pl.col("humidity_relative").mean().alias("humidity_relative"),
                pl.col("humidity_absolute").mean().alias("humidity_absolute"),
                pl.col("dpt").mean().alias("dpt"),
                pl.col("vpd").mean().alias("vpd"),
            ]
        )

        col1, col2 = st.columns(2)
        col1.subheader("Temperature (°C)")
        col1.line_chart(
            df_main.select(pl.col("Timestamp"), pl.col("temperature"))
            .to_pandas()
            .set_index("Timestamp")
        )

        col2.subheader("Humidity")
        col2.line_chart(
            df_main.select(
                pl.col("Timestamp"),
                pl.col("humidity_relative"),
                pl.col("humidity_absolute"),
            )
            .to_pandas()
            .set_index("Timestamp")
        )

        st.subheader("DPT and VPD")
        st.write("DPT: Dew Point Temperature / VPD: Vapor Pressure Deficit")
        st.line_chart(
            df_main.select(pl.col("Timestamp"), pl.col("dpt"), pl.col("vpd"))
            .to_pandas()
            .set_index("Timestamp")
        )

    with tab_sub:
        # "Timestamp","Temperature_Celsius(°C)","Relative_Humidity(%)","CO2(ppm)","Absolute_Humidity(g/m³)","DPT_Celsius(°C)","VPD(kPa)"
        sensor_sub_path = resources_dir / "sensor_sub.csv"
        df_sub = (
            (
                pl.read_csv(sensor_sub_path, try_parse_dates=True).rename(
                    {
                        "Temperature_Celsius(°C)": "temperature",
                        "CO2(ppm)": "co2",
                        "Relative_Humidity(%)": "humidity_relative",
                        "Absolute_Humidity(g/m³)": "humidity_absolute",
                        "DPT_Celsius(°C)": "dpt",
                        "VPD(kPa)": "vpd",
                    }
                )
            )
            .group_by_dynamic("Timestamp", every=granularity)
            .agg(
                [
                    pl.col("temperature").mean().alias("temperature"),
                    pl.col("co2").mean().alias("co2"),
                    pl.col("humidity_relative").mean().alias("humidity_relative"),
                    pl.col("humidity_absolute").mean().alias("humidity_absolute"),
                    pl.col("dpt").mean().alias("dpt"),
                    pl.col("vpd").mean().alias("vpd"),
                ]
            )
        ).filter(
            (pl.col("Timestamp") >= dt.datetime.combine(start_date, dt.time.min))
            & (pl.col("Timestamp") <= dt.datetime.combine(end_date, dt.time.max))
        )

        render_summary_stats(df_sub)

        col1, col2 = st.columns(2)

        col1.subheader("Temperature (°C)")
        col1.line_chart(
            df_sub.select(pl.col("Timestamp"), pl.col("temperature"))
            .to_pandas()
            .set_index("Timestamp")
        )

        col2.subheader("Humidity")
        col2.line_chart(
            df_sub.select(
                pl.col("Timestamp"),
                pl.col("humidity_relative"),
                pl.col("humidity_absolute"),
            )
            .to_pandas()
            .set_index("Timestamp")
        )

        col1.subheader("CO2")
        col1.line_chart(
            df_sub.select(pl.col("Timestamp"), pl.col("co2"))
            .to_pandas()
            .set_index("Timestamp")
        )

        col2.subheader("DPT and VPD")
        col2.line_chart(
            df_sub.select(pl.col("Timestamp"), pl.col("dpt"), pl.col("vpd"))
            .to_pandas()
            .set_index("Timestamp")
        )


def render_summary_stats(df: pl.DataFrame) -> None:
    st.subheader("Summary Statistics")
    col1, col2, col3, col4, col5, col6 = st.columns(6)
    max_temp = df.select(pl.col("temperature").max()).item()
    min_temp = df.select(pl.col("temperature").min()).item()
    avg_temp = df.select(pl.col("temperature").mean()).item()

    col1.metric("Avg Temperature (°C)", f"{avg_temp:.2f}", border=True)
    col2.metric("Max Temperature (°C)", f"{max_temp:.2f}", border=True)
    col3.metric("Min Temperature (°C)", f"{min_temp:.2f}", border=True)

    max_humidity = df.select(pl.col("humidity_relative").max()).item()
    min_humidity = df.select(pl.col("humidity_relative").min()).item()
    avg_humidity = df.select(pl.col("humidity_relative").mean()).item()

    col4.metric("Avg Humidity (%)", f"{avg_humidity:.2f}", border=True)
    col5.metric("Max Humidity (%)", f"{max_humidity:.2f}", border=True)
    col6.metric("Min Humidity (%)", f"{min_humidity:.2f}", border=True)


if __name__ == "__main__":
    asyncio.run(main())
