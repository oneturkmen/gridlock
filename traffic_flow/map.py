import polars as pl
import folium
from folium.plugins import TimestampedGeoJson
import json
import datetime


import numpy as np

def get_color(intensity, min_traffic, max_traffic):
    """Return a hex color code ranging from green (0) to red (1) based on intensity."""
    intensity = (intensity - min_traffic) / (max_traffic - min_traffic)

    if intensity <= 0.5:
        # Green to Yellow (0 to 0.5)
        r = int(255 * (intensity / 0.5))      # 0 to 255
        g = 255                               # always fully green
        b = 0                                 # no blue
    else:
        # Yellow to Red (0.5 to 1)
        r = 255                               # fully red
        g = int(255 * (1 - intensity) / 0.5)  # 255 to 0
        b = 0                                 # no blue
    
    # Format RGB values into a hex color string
    return f'#{r:02x}{g:02x}{b:02x}'


def get_map_interactive(daily_traffic: pl.DataFrame):
    """
    input dataframe cols: location, traffic_total, time
    """
    # Function to map intensity to color
    def get_color(intensity):
        if intensity <= 0.5:
            r = int(255 * (intensity / 0.5))
            g = 255
            b = 0
        else:
            r = 255
            g = int(255 * ((1 - intensity) / 0.5))
            b = 0
        return f'#{r:02x}{g:02x}{b:02x}'

    # FIXME: Find better design for this
    daily_traffic = daily_traffic.with_columns(
        pl.datetime(
            year=pl.lit(2024),
            month=pl.lit(11),
            day=pl.lit(10),
            hour=pl.col("time_start_hour"),
            minute=pl.col("time_start_min"),
            second=pl.lit(0)
        ).cast(pl.String).alias("time")
    )

    # Normalize traffic count across different timestamps per location
    # Use log because sometimes they values can be vastly different (e.g. 25 vs 2K)
    locations_ids = daily_traffic.select("location_id").unique()["location_id"].to_list()
    for loc_id in locations_ids:
        min_val = np.log(1 + daily_traffic.filter(pl.col("location_id") == loc_id)["traffic_total"].min())
        max_val = np.log(1 + daily_traffic.filter(pl.col("location_id") == loc_id)["traffic_total"].max())

        daily_traffic = daily_traffic.with_columns(
            pl.when(pl.col("location_id") == loc_id)
            .then(
                (pl.col("traffic_total").log1p() - min_val) / (max_val - min_val)
            )
            .otherwise(pl.col("traffic_total"))
        )

    # Convert traffic data to GeoJSON format
    features = []
    for data in daily_traffic.iter_rows(named=True):
        color = get_color(data["traffic_total"])

        features.append({
            "type": "Feature",
            "geometry": {
                "type": "Point",
                "coordinates": [data["lng"], data["lat"]],  # GeoJSON uses [lon, lat]
            },
            "properties": {
                "time": data["time"],
                "style": {"color": "black", "fillColor": color, "radius": 10},
                "icon": "circle",
            },
        })

    geojson_data = {
        "type": "FeatureCollection",
        "features": features,
    }

    # Map center
    m = folium.Map(location=[43.66, -79.38], zoom_start=14, prefer_canvas=True)

    # Add Timestamped GeoJson
    TimestampedGeoJson(
        geojson_data,
        period="PT15M", # 15-minute intervals, weird notation but ok
        add_last_point=False,
        auto_play=False,
    ).add_to(m)

    # Display the map
    m.save("daily_traffic_map_interactive.html")
