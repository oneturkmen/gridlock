# Toronto Traffic Analysis

Everyone has been talking about how bad Toronto traffic can get.

This is an attempt to analyze city traffic, look at traffic patterns, find hotspots, etc.

If you notice a bug or mistake, please open an issue.

![Toronto Traffic](./gif/toronto.gif)

## Run Locally

Tooling prerequisites:

- Poetry
- Python 3.10 or above.

Data prerequisites:

- Data from [City of Toronto Open Data](https://open.toronto.ca/dataset/traffic-volumes-at-intersections-for-all-modes/) platform. I downloaded `raw-data-2020-2029.csv`, but any of the other years work fine.


1. Install the project dependencies:

```bash
poetry install
```

2. Activate and enter the environment:

```bash
poetry shell
```

3. Run `main.py`:

```bash
python traffic_flow/main.py
```

4. Open `daily_traffic_map_interactive.html` and feel free to play around :)