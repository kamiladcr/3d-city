import pandas as pd
import geopandas as gpd
from cjio import cityjson
from shapely.geometry import Polygon, Point
from datetime import datetime

TARGET_CRS = 3857


def load_uprn(path) -> gpd.GeoDataFrame:
    # Load UPRN data with the default mercator (4326) and then
    # reprojecting it to 3856 (same CRS as CityJSON file). Note that
    # Longitude goes for X while Latitude for Y.
    df = pd.read_csv(path)
    geometry = [Point(xy) for xy in zip(df["LONGITUDE"], df["LATITUDE"])]
    gdf = gpd.GeoDataFrame(df, geometry=geometry)
    gdf.crs = 4326
    return gdf.to_crs(TARGET_CRS)


def load_buildings(path) -> cityjson.CityJSON:
    # Load source for buildings geometry.
    # CityJson file is in version 1.0 and the cjio library now expects
    # different format for EPSG, setting according to documentation.
    # Setting it manually helps to avoid deserialization errors.
    cm = cityjson.load(path)
    cm.set_epsg(TARGET_CRS)
    return cm


def get_exterior_ring(vertices, boundaries) -> Polygon:
    # This is a recursive function to get a Shapely polygon based on
    # object boundaries in CityJSON.
    #
    # For MultiSurface boundary types we can get exterior ring of
    # the building by simply fetching the first surface from the
    # provided boundaries.
    #
    # https://www.cityjson.org/dev/geom-arrays/#multisurface
    is_nested = type(boundaries[0]) == list
    if is_nested:
        return get_exterior_ring(vertices, boundaries[0])
    else:
        coordinates = [vertices[index] for index in boundaries]
        return Polygon(coordinates)


def extract_geometry(cm: cityjson.CityJSON) -> gpd.GeoDataFrame:
    data = []
    for co_id, co in cm.j["CityObjects"].items():

        # This ensures that if script succeeds without errors only if
        # we have MultiSurface data for each building.
        geometry_type = co["geometry"][0]["type"]
        if not geometry_type == "MultiSurface":
            raise ValueError(f"Unexpected surface type: {geometry_type}")

        boundaries = co["geometry"][0]["boundaries"]
        polygon = get_exterior_ring(cm.j["vertices"], boundaries)
        data.append({"id": co_id, "geometry": polygon})

    gdf = gpd.GeoDataFrame(data, columns=["id", "geometry"], geometry="geometry")
    gdf.crs = TARGET_CRS
    return gdf


def main():
    log = lambda msg: print(datetime.now(), msg)

    log("Loading UPRN")
    uprn = load_uprn("./data/UPRN.csv")

    log("Loading CityJSON")
    cm = load_buildings("./data/Buildings.json")

    log("Extracting geometry")
    gdf = extract_geometry(cm)

    log("Executing spatial json")
    joined = uprn.sjoin(gdf, predicate="within").groupby("id")["UPRN"].apply(list)

    log("Updating CityObjects")
    for co_id, co in cm.j["CityObjects"].items():
        co["attributes"]["uprn"] = joined.get(co_id) or []

    log("Saving output")
    cityjson.save(cm, path="./answer.json")

    log("Done")


main()
