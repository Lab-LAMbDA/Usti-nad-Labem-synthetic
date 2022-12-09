import io, gzip
from tqdm import tqdm
import pandas as pd
import os
import matsim.writers as writers


# Define globals
FACILITY_FIELDS = [
    "LocationID", "geometry",
    "offers_home", "offers_freetime", "offers_shopping", "offers_work", "offers_education", "offers_errands"
]

HOME_FIELDS = [
    "HouseholdID", "geometry"
]

ACTIVITY_FIELDS = ["PersonID", "geometry"]

def configure(context):
    context.config("output_path")
    context.stage("synthesis.destinations")
    context.stage("synthesis.population.spatial.by_person.primary_locations")

def validate(context):
    output_path = context.config("output_path")

    if not os.path.isdir(output_path):
        raise RuntimeError("Output directory must exist: %s" % output_path)

def execute(context):
    output_path = "%s/facilities.xml.gz" % context.config("output_path")

    df_destinations = context.stage("synthesis.destinations")
    df_destinations = df_destinations[FACILITY_FIELDS]

    # As (at the moment) we are not assigning people to households, we use one household for every person.
    # So, get the home locations of every person
    df_activities = context.stage("synthesis.population.activities")[
        ["PersonID", "TripOrderNum"]
    ].sort_values(by=["PersonID", "TripOrderNum"])
    df_activities = df_activities.loc[df_activities["TripOrderNum"] == 0, "PersonID"]
    df_location_activities = context.stage("synthesis.population.spatial.locations")[
        ["PersonID", "TripOrderNum", "geometry"]
    ].sort_values(by=["PersonID", "TripOrderNum"])
    df_location_activities = df_location_activities.loc[df_location_activities["TripOrderNum"] == 0,
                                                        ["PersonID", "geometry"]]
    df_activities = pd.merge(df_activities, df_location_activities, how="left", on=["PersonID"])

    # MATSIM don't support Czech's Krovak coordinate system (epsg:5514)
    df_destinations = df_destinations.to_crs(epsg=4326)
    df_activities.geometry = df_activities.geometry.values.to_crs(epsg=4326)

    # not at the moment
    # df_homes = context.stage("synthesis.population.spatial.by_person.primary_locations")[0]
    # df_homes = df_homes[HOME_FIELDS].drop_duplicates(subset='HouseholdID',keep='first')

    # Write MATSim input file
    with gzip.open(output_path, 'wb+') as writer:
        with io.BufferedWriter(writer, buffer_size = 2 * 1024**3) as writer:
            writer = writers.FacilitiesWriter(writer)
            writer.start_facilities()

            for item in tqdm(df_destinations.itertuples(index=False),
                               desc="Writing facilities ...", ascii=True,
                               position=0, leave=False):
                geometry = item[FACILITY_FIELDS.index("geometry")]

                try:
                    writer.start_facility(
                        item[FACILITY_FIELDS.index("LocationID")],
                        geometry.x, geometry.y
                    )
                except AttributeError:
                    # If no point geometry but rather an area, use the facility centroid
                    writer.start_facility(
                        item[FACILITY_FIELDS.index("LocationID")],
                        geometry.centroid.x, geometry.centroid.y
                    )

                for purpose in ("freetime", "shopping", "work", "education", "errands"):
                    if item[FACILITY_FIELDS.index("offers_%s" % purpose)]:
                        writer.add_activity(purpose)

                writer.end_facility()

            # As (at the moment) we are not assigning people to households, we use one household for every person
            for activity in tqdm(df_activities.itertuples(index=False),
                                 desc="Writing homes ...", ascii=True,
                                 position=0, leave=False):
                geometry = activity[ACTIVITY_FIELDS.index("geometry")]

                try:
                    writer.start_facility(
                        "home_%s" % activity[ACTIVITY_FIELDS.index("PersonID")],
                        geometry.x, geometry.y
                    )
                except AttributeError:
                    writer.start_facility(
                        "home_%s" % activity[ACTIVITY_FIELDS.index("PersonID")],
                        geometry.centroid.x, geometry.centroid.y
                    )
                writer.add_activity("home")
                writer.end_facility()

            # Not at the moment
            # for item in tqdm(df_homes.itertuples(index=False),
            #                  desc="Writing homes ...", ascii=True,
            #                  position=0, leave=False):
            #     geometry = item[HOME_FIELDS.index("geometry")]
            #
            #     writer.start_facility(
            #         "home_%s" % item[HOME_FIELDS.index("HouseholdID")],
            #         geometry.x, geometry.y
            #     )
            #
            #     writer.add_activity("home")
            #     writer.end_facility()

            writer.end_facilities()

    return "facilities.xml.gz"
