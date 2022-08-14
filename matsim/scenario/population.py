import io, gzip
import itertools
import numpy as np
import pandas as pd
import os
from tqdm import tqdm
import matsim.writers as writers
from matsim.writers import backlog_iterator


# Define globals
PERSON_FIELDS = [
    'PersonID',
    # 'HouseholdID', # not at the moment
    # 'ActivitySector', # potential useful info
    'Activity',
    'Gender',
    "Age",
    "hts_PersonID",
    "AvailCar", "AvailBike", "DrivingLicense", "PtSubscription", "IsPassenger",
    # 'Homeoffice', # potential useful info
    # 'FlexibleBegEndTime', # potential useful info
    # 'FlexibleHours', # potential useful info
    # 'AvailCarSharing', # potential useful info
]

ACTIVITY_FIELDS = [
    "PersonID", "StartTime", "EndTime", "Purpose", "geometry", "DestinationID"
]

TRIP_FIELDS = [
    "PersonID", "TripMainMode", "OriginStart", "DeclaredTripTime"
]

TRIP_MODES = ["walk", "bike", "pt", "bus", "train", "car", "car_passenger",
              "taxi", # classified as other but used as taxi in MATsim
              ]

def configure(context):
    context.config("output_path")
    context.stage("synthesis.population.sociodemographics")
    context.stage("synthesis.population.activities")
    context.stage("synthesis.population.spatial.locations")
    context.stage("synthesis.population.trips")

def validate(context):
    output_path = context.config("output_path")

    if not os.path.isdir(output_path):
        raise RuntimeError("Output directory must exist: %s" % output_path)

def add_person(writer, person, activities, trips):
    writer.start_person(person[PERSON_FIELDS.index("PersonID")])

    writer.start_attributes()
    # writer.add_attribute("householdId", "java.lang.Integer", person[PERSON_FIELDS.index("HouseholdID")]) # not at the moment

    writer.add_attribute("carAvailability", "java.lang.String", "always" if person[PERSON_FIELDS.index("AvailCar")] else "never")
    writer.add_attribute("bikeAvailability", "java.lang.String", "always" if person[PERSON_FIELDS.index("AvailBike")] else "never")

    writer.add_attribute("htsPersonId", "java.lang.Long", person[PERSON_FIELDS.index("hts_PersonID")])

    writer.add_attribute("hasPtSubscription", "java.lang.Boolean", person[PERSON_FIELDS.index("PtSubscription")])
    writer.add_attribute("hasLicense", "java.lang.String", writer.yes_no(person[PERSON_FIELDS.index("DrivingLicense")]))

    writer.add_attribute("isPassenger", "java.lang.Boolean", person[PERSON_FIELDS.index("IsPassenger")])

    writer.add_attribute("age", "java.lang.Integer", person[PERSON_FIELDS.index("Age")])
    writer.add_attribute("employment", "java.lang.String", "yes" if person[PERSON_FIELDS.index("Activity") in ("1",
                                                                                                               "2",
                                                                                                               "3")]
                                                                 else "no")
    writer.add_attribute("sex", "java.lang.String", "man" if person[PERSON_FIELDS.index("Gender")] == "1" else "woman")

    writer.end_attributes()

    writer.start_plan(selected = True)

    for activity, trip in itertools.zip_longest(activities, trips):
        start_time = activity[ACTIVITY_FIELDS.index("StartTime")]
        end_time = activity[ACTIVITY_FIELDS.index("EndTime")]
        destination_id = activity[ACTIVITY_FIELDS.index("DestinationID")]
        geometry = activity[ACTIVITY_FIELDS.index("geometry")]

        if activity[ACTIVITY_FIELDS.index("Purpose")] == "1":
            # destination_id = "home_%s" % person[PERSON_FIELDS.index("HouseholdID")] # not at the moment
            destination_id = "home_%s" % person[PERSON_FIELDS.index("PersonID")]

        # If home destination, write None as the geometry
        location = writer.location(
            geometry.x, geometry.y,
            None if destination_id == -1 else destination_id
        )

        writer.add_activity(
            type = activity[ACTIVITY_FIELDS.index("Purpose")],
            location = location,
            start_time = None if np.isnan(start_time) else start_time,
            end_time = None if np.isnan(end_time) else end_time
        )

        if not trip is None:
            writer.add_leg(
                mode = TRIP_MODES[int(trip[TRIP_FIELDS.index("TripMainMode")]) - 1],
                departure_time = trip[TRIP_FIELDS.index("OriginStart")],
                travel_time = trip[TRIP_FIELDS.index("DeclaredTripTime")]
            )

    writer.end_plan()
    writer.end_person()

def execute(context):
    output_path = "%s/population.xml.gz" % context.config("output_path")

    df_persons = context.stage("synthesis.population.sociodemographics")
    # df_persons = df_persons.sort_values(by = ["HouseholdID", "PersonID"]) # not at the moment
    df_persons = df_persons.sort_values(by = ["PersonID"])
    df_persons = df_persons[PERSON_FIELDS]

    df_activities = context.stage("synthesis.population.activities").sort_values(by = ["PersonID", "TripOrderNum"])
    df_location_activities = context.stage("synthesis.population.spatial.locations")[[
        "PersonID", "ActivityID", "TripOrderNum", "geometry", "DestinationID"
    ]].sort_values(by=["PersonID", "TripOrderNum"])
    df_activities = pd.merge(df_activities, df_location_activities, how = "left", on = ["PersonID", "TripOrderNum"])
    df_activities["DestinationID"] = df_activities["DestinationID"].fillna(-1).astype(str)

    df_trips = context.stage("synthesis.population.trips").sort_values(by = ["PersonID", "TripOrderNum"])
    df_trips["TravelTime"] = df_trips["DestEnd"] - df_trips["OriginStart"]

    # Write MATSim input file
    with gzip.open(output_path, 'wb+') as writer:
        with io.BufferedWriter(writer, buffer_size = 2 * 1024**3) as writer:
            writer = writers.PopulationWriter(writer)
            writer.start_population()

            activity_iterator = backlog_iterator(iter(df_activities[ACTIVITY_FIELDS].itertuples(index = False)))
            trip_iterator = backlog_iterator(iter(df_trips[TRIP_FIELDS].itertuples(index = False)))

            for person in tqdm(df_persons.itertuples(index = False),
                      desc="Writing population ...",
                      position=0, leave=False):
                person_id = person[PERSON_FIELDS.index("PersonID")]

                activities = []
                trips = []

                # Track all activities for person
                while activity_iterator.has_next():
                    activity = activity_iterator.next()

                    if not activity[ACTIVITY_FIELDS.index("PersonID")] == person_id:
                        activity_iterator.previous()
                        break
                    else:
                        activities.append(activity)

                assert len(activities) > 0

                # Track all trips for person
                while trip_iterator.has_next():
                    trip = trip_iterator.next()

                    if not trip[TRIP_FIELDS.index("PersonID")] == person_id:
                        trip_iterator.previous()
                        break
                    else:
                        trips.append(trip)

                assert len(trips) == len(activities) - 1

                add_person(writer, person, activities, trips)

            writer.end_population()

    return "population.xml.gz"
