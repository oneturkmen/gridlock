import polars as pl
import polars.selectors as cs
import seaborn as sns
import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import numpy as np
from itertools import product

import traffic_flow.map as map


RAW_DATA_PATH="./data/raw-data-2020-2029.csv"
FROM_YEAR=2023

def read_data(filename):
    return pl.read_csv(filename)


class FeatureProcessor:

    def get_columns(self, dataset: pl.DataFrame):
        dataset: pl.DataFrame = dataset.clone()
        directions_in = [
            # southbound, northbound, westbound, eastbound
            "sb", "nb", "wb", "eb"
        ]
        directions_out = [
            # right, through, left
            "r", "t", "l"
        ]
        vehicle_types = [
            "cars", "truck", "bus" 
        ]
        vehicles = [
            f"{din}_{vh}_{dout}" for din, vh, dout in product(directions_in, vehicle_types, directions_out)
        ]

        columns = [
            "count_date",
            "location_id",
            "location",
            "lng","lat",
            "time_start",
            "time_end",
            *vehicles
        ]

        return dataset.select(columns)

    def cast_columns(self, dataset: pl.DataFrame):
        dataset: pl.DataFrame = dataset.clone()

        # Convert "count_date" into proper date
        dataset = dataset.with_columns(
            pl.col("count_date").str.to_date("%Y-%m-%d"),
            pl.col("time_start").str.to_datetime(),
        )

        return dataset

    def aggregate_counts(self, dataset: pl.DataFrame):
        dataset: pl.DataFrame = dataset.clone()

        # Group over all vehicle count columns
        dataset_grouped_counts = (
            dataset.group_by(
                ["count_date", "location_id", "lng", "lat", "time_start"]
            )
            .agg(
                (cs.contains("cars") | cs.contains("truck") | cs.contains("bus")).sum()
            )
            .sort(by=["count_date", "time_start"])
        )

        return dataset_grouped_counts

    def combine_traffic(self, dataset: pl.DataFrame):
        dataset: pl.DataFrame = dataset.clone()

        dataset_combined = (
            dataset.with_columns(
                (
                    # North and south
                    (
                    # Cars
                    pl.col("nb_cars_l") + pl.col("nb_cars_r") + pl.col("nb_cars_t")
                    + pl.col("sb_cars_l") + pl.col("sb_cars_r") + pl.col("sb_cars_t")

                    # Trucks
                    + pl.col("nb_truck_l") + pl.col("nb_truck_r") + pl.col("nb_truck_t")
                    + pl.col("sb_truck_l") + pl.col("sb_truck_r") + pl.col("sb_truck_t")

                    # Buses
                    + pl.col("nb_bus_l") + pl.col("nb_bus_r") + pl.col("nb_bus_t")
                    + pl.col("sb_bus_l") + pl.col("sb_bus_r") + pl.col("sb_bus_t")
                    ).alias("ns_total"),

                    # East and west
                    (
                    # Cars
                    pl.col("eb_cars_l") + pl.col("eb_cars_r") + pl.col("eb_cars_t")
                    + pl.col("wb_cars_l") + pl.col("wb_cars_r") + pl.col("wb_cars_t")

                    # Trucks
                    + pl.col("eb_truck_l") + pl.col("eb_truck_r") + pl.col("eb_truck_t")
                    + pl.col("wb_truck_l") + pl.col("wb_truck_r") + pl.col("wb_truck_t")

                    # Buses
                    + pl.col("eb_bus_l") + pl.col("eb_bus_r") + pl.col("eb_bus_t")
                    + pl.col("wb_bus_l") + pl.col("wb_bus_r") + pl.col("wb_bus_t")
                    ).alias("ew_total"),
                )
            )
        )

        # Ugly. Make it more
        # Needs some re-design, ideally.
        dataset_combined = (
            dataset_combined
            .filter(pl.col("time_start").dt.year() >= FROM_YEAR, pl.col("time_start").dt.hour() < 18)
            .select(["time_start", "location_id", "lng", "lat", "ns_total", "ew_total"])
            .with_columns(
                pl.col("time_start").dt.hour().alias("time_start_hour"),
                pl.col("time_start").dt.minute().alias("time_start_min"),
                (pl.col("ns_total") + pl.col("ew_total")).alias("traffic_total"),
            )
            .group_by(["location_id", "lng", "lat", "time_start_hour", "time_start_min"])
            .agg(pl.col("traffic_total").sum())
            .sort(by=["time_start_hour", "time_start_min"])
            .with_columns(
                (pl.col("time_start_hour").cast(pl.String).alias("time") + ":" + (pl.col("time_start_min").cast(pl.String))).alias("time")
            )
        )

        return dataset_combined


    def transform(self, dataset: pl.DataFrame):
        dataset = self.get_columns(dataset)
        dataset = self.cast_columns(dataset)
        dataset = self.aggregate_counts(dataset)
        dataset = self.combine_traffic(dataset)

        return dataset


def main():
    dataset = read_data(RAW_DATA_PATH)
    feature_processor = FeatureProcessor()
    processed_dataset = feature_processor.transform(dataset)

    print(processed_dataset)
    print(processed_dataset.filter(pl.col("time") == "8:30"))

    # Show traffic count over time 
    ax = sns.lineplot(processed_dataset, x="time", y="traffic_total")
    ticks = [x for x in processed_dataset["time"][::4]]
    plt.xticks(ticks)
    ax.get_figure().savefig("traffic_lineplot.png")

    # Generate an interactive map (HTML canvas)
    map.get_map_interactive(processed_dataset)

main()