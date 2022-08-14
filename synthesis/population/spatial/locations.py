import pandas as pd
import geopandas as gpd
import shapely.geometry as geo
import os

def configure(context):
    context.stage("synthesis.population.spatial.by_person.primary_locations")
    context.stage("synthesis.population.spatial.by_person.secondary.locations")

    context.stage("synthesis.population.activities")
    context.stage("synthesis.population.trips")
    context.stage("synthesis.population.sampled")

def validate(context):
    output_path = context.config("output_path")

    if not os.path.isdir(output_path):
        raise RuntimeError("Output directory must exist: %s" % output_path)

def execute(context):
    output_path = context.config("output_path")

    print("Preparing spatial activities and spatial trips")

    # Get population and their assigned locations, activities, and trips
    df_home, df_work, df_education = context.stage("synthesis.population.spatial.by_person.primary_locations")
    df_secondary = context.stage("synthesis.population.spatial.by_person.secondary.locations")[0]
    df_persons = context.stage("synthesis.population.sampled")
    df_persons = pd.concat(df_persons)[["PersonID",
                                        # "HouseholdID" # not at the moment
                                        ]]
    df_activities = context.stage("synthesis.population.activities")[["PersonID", "ActivityID",
                                                                      "TripOrderNum", "Purpose"]]
    df_trips = pd.DataFrame(context.stage("synthesis.population.trips"), copy=True)

    # Define home activities
    df_home_activities = df_activities[df_activities["Purpose"] == "1"] # home
    df_home_activities = pd.merge(df_home_activities, df_persons, on = "PersonID", how = 'left')
    df_home_activities = pd.merge(df_home_activities, df_home[[
                                                             # "HouseholdID", # not at the moment, using PersonID instead
                                                             "PersonID",
                                                             "geometry"]].drop_duplicates(),
                                 # on = "HouseholdID", how = 'left') # not at the moment, using PersonID instead
                                 on = "PersonID", how = 'left')
    df_home_activities["DestinationID"] = -1
    df_home_activities = df_home_activities[["PersonID", "ActivityID", "TripOrderNum",
                                             "Purpose", "DestinationID", "geometry"]]

    # Define work activities
    df_work_activities = df_activities[df_activities["Purpose"] == "4"] # work
    df_work_activities = pd.merge(df_work_activities, df_work[["PersonID", "LocationID", "geometry"]],
                                  on = "PersonID", how = 'left')
    df_work_activities = df_work_activities[["PersonID", "ActivityID", "TripOrderNum",
                                             "Purpose", "LocationID", "geometry"]]
    df_work_activities = df_work_activities.rename(columns={"LocationID": "DestinationID"})

    # Define education activities
    df_education_activities = df_activities[df_activities["Purpose"] == "5"] # education
    df_education_activities = pd.merge(df_education_activities, df_education[["PersonID", "LocationID", "geometry"]],
                                      on = "PersonID", how = 'left')
    df_education_activities = df_education_activities[["PersonID", "ActivityID", "TripOrderNum",
                                                       "Purpose", "LocationID", "geometry"]]
    df_education_activities = df_education_activities.rename(columns={"LocationID": "DestinationID"})

    # Define secondary activities
    df_secondary_activities = df_activities[~df_activities["Purpose"].isin(("1", "4", "5"))].copy()
    df_secondary["ActivityID"] = df_secondary["TripID"].copy()
    df_secondary["TripOrderNum"] = df_secondary["TripOrderNum"].astype(int)
    df_secondary_activities = pd.merge(df_secondary_activities, df_secondary[[
        "PersonID", "ActivityID", "TripOrderNum", "LocationID", "geometry"
    ]], on = ["PersonID", "TripOrderNum", "ActivityID"])
    df_secondary_activities = df_secondary_activities[
        ["PersonID", "ActivityID", "TripOrderNum", "Purpose", "LocationID", "geometry"]]
    df_secondary_activities = df_secondary_activities.rename(columns={"LocationID": "DestinationID"})

    # Checking if all activities have assigned location
    initial_count = len(df_activities)
    df_location_activities = pd.concat([df_home_activities, df_work_activities,
                                        df_education_activities, df_secondary_activities])

    # Finishing up
    df_location_activities = df_location_activities.sort_values(by = ["PersonID", "TripOrderNum"])
    final_count = len(df_location_activities)

    assert initial_count == final_count

    df_location_activities = gpd.GeoDataFrame(df_location_activities, crs="epsg:5514")

    # Prepare spatial data sets
    df_location_activities = df_location_activities[[
        "PersonID", "ActivityID", "TripOrderNum", "DestinationID", "Purpose", "geometry"
    ]]

    # Write spatial activities
    print("Saving spatial activities")

    df_location_activities.to_file("%s/Trips/activities.gpkg" % output_path, driver="GPKG")

    print("Saved spatial activities")

    # Write spatial trips
    df_trips["TripOrderNum"] = df_trips["TripOrderNum"].astype(int)
    df_trips = df_trips.sort_values(by=["PersonID", "TripOrderNum"])
    df_trips["OriginTripOrderNum"] = df_trips["TripOrderNum"] - 1
    df_trips["DestTripOrderNum"] = df_trips["TripOrderNum"]

    df_trips = df_trips[[
        "PersonID",
        "ActivitySector", "AgeGroup", "DeclaredTripTime",
        "TripID", "TripOrderNum",
        "OriginTripOrderNum", "DestTripOrderNum",
        "OriginStart", "DestEnd", "TripMainMode",
        "OriginPurpose", "DestPurpose",
        # "is_first", "is_last"
    ]]
    
    df_location_trips = pd.merge(df_trips, df_location_activities[[
        "PersonID", "TripOrderNum", "geometry"
    ]].rename(columns={
        "TripOrderNum": "OriginTripOrderNum",
        "geometry": "OriginGeometry"
    }), how="left", on=["PersonID", "OriginTripOrderNum"])

    df_location_trips = pd.merge(df_location_trips, df_location_activities[[
        "PersonID", "TripOrderNum", "geometry"
    ]].rename(columns={
        "TripOrderNum": "DestTripOrderNum",
        "geometry": "DestGeometry"
    }), how="left", on=["PersonID", "DestTripOrderNum"])

    df_location_trips["geometry"] = [
        geo.LineString(od)
        for od in zip(df_location_trips["OriginGeometry"], df_location_trips["DestGeometry"])
    ]

    df_location_trips = df_location_trips.drop(columns=["OriginGeometry", "DestGeometry",
                                                        "OriginTripOrderNum", "DestTripOrderNum"])
    df_location_trips = gpd.GeoDataFrame(df_location_trips, crs="epsg:5514")

    print("Saving spatial trips")

    df_location_trips.to_file("%s/Trips/trips.gpkg" % output_path, driver="GPKG")

    print("Saving spatial trips")

    return df_location_activities
