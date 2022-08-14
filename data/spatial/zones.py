import pandas as pd
import geopandas as gpd
import os

def configure(context):
    context.config("data_path")
    context.config("shapefile_municipalities_name")
    context.config("shapefile_zsj_city_name")
    context.config("shapefile_cadastral_city_name")
    context.config("shapefile_gates")

def validate(context):

    data_path = context.config("data_path")
    shapefile_municipalities_name = "%s/Spatial/%s" % (data_path, context.config("shapefile_municipalities_name"))
    shapefile_zsj_city_name = "%s/Spatial/%s" % (data_path, context.config("shapefile_zsj_city_name"))
    shapefile_cadastral_city_name = "%s/Spatial/%s" % (data_path, context.config("shapefile_cadastral_city_name"))
    shapefile_gates = "%s/Spatial/%s" % (data_path, context.config("shapefile_gates"))

    if not os.path.isdir(data_path):
        raise RuntimeError("Input directory must exist: %s" % data_path)

    if not os.path.exists(shapefile_municipalities_name):
        raise RuntimeError("Input file must exist: %s" % shapefile_municipalities_name)

    if not os.path.exists(shapefile_zsj_city_name):
        raise RuntimeError("Input file must exist: %s" % shapefile_zsj_city_name)

    if not os.path.exists(shapefile_cadastral_city_name):
        raise RuntimeError("Input file must exist: %s" % shapefile_cadastral_city_name)

    if not os.path.exists(shapefile_gates):
        raise RuntimeError("Input file must exist: %s" % shapefile_gates)

def execute(context):

    print("Reading zoning files")

    # Get files with the zones
    df_zones_municipalities = gpd.read_file("%s/Spatial/%s" % (context.config("data_path"),
                                                               context.config("shapefile_municipalities_name")))
    df_zones_zsj_city = gpd.read_file("%s/Spatial/%s" % (context.config("data_path"),
                                                         context.config("shapefile_zsj_city_name")))
    df_zones_cadastral_city = gpd.read_file("%s/Spatial/%s" % (context.config("data_path"),
                                                           context.config("shapefile_cadastral_city_name")))
    df_zones_gates = gpd.read_file("%s/Spatial/%s" % (context.config("data_path"),
                                                      context.config("shapefile_gates")))

    # Define Coordinate reference system (CRS)
    df_zones_municipalities.crs = "epsg:5514"
    df_zones_zsj_city.crs = "epsg:5514"
    df_zones_cadastral_city.crs = "epsg:5514"
    df_zones_gates.crs = "epsg:5514"

    # Ignore warning when working on slices of dataframes
    pd.options.mode.chained_assignment = None

    # Select only desired data:
    # Coordinates and town code
    df_zones_municipalities_dissolved = df_zones_municipalities[['geometry', 'KOD_LAU2']]
    # Coordinates and basic settlement unit code
    df_zones_zsj_city_dissolved = df_zones_zsj_city[['geometry', 'KOD_ZSJ']]
    # Coordinates and cadastral area code
    df_zones_cadastral_city_dissolved = df_zones_cadastral_city[['geometry', 'KOD_KU']]
    # Coordinates and synthetic city gates code
    df_zones_gates_dissolved = df_zones_gates[['geometry', 'osm_id']]

    # Rename columns
    df_zones_municipalities_dissolved.columns = ["geometry", "ZoneID"]
    df_zones_cadastral_city_dissolved.columns = ["geometry", "ZoneID"]
    df_zones_gates_dissolved.columns = ["geometry", "ZoneID"]
    df_zones_zsj_city_dissolved.columns = ["geometry", "ZoneID"]

    # Fix inconsistencies
    df_zones_municipalities_dissolved["ZoneID"] = df_zones_municipalities_dissolved["ZoneID"].astype(str)
    df_zones_cadastral_city_dissolved["ZoneID"] = df_zones_cadastral_city_dissolved["ZoneID"].astype(str)
    df_zones_gates_dissolved["ZoneID"] = df_zones_gates_dissolved["ZoneID"].astype(str)
    df_zones_zsj_city_dissolved["ZoneID"] = df_zones_zsj_city_dissolved["ZoneID"].astype(str) + '0'

    return df_zones_municipalities_dissolved, df_zones_cadastral_city_dissolved, \
           df_zones_zsj_city_dissolved, df_zones_gates_dissolved
