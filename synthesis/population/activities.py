import pandas as pd
import numpy as np
import os

def configure(context):
    context.stage("synthesis.population.sociodemographics")
    context.stage("synthesis.population.trips")
    context.config("output_path")

def validate(context):
    output_path = context.config("output_path")

    if not os.path.isdir(output_path):
        raise RuntimeError("Output directory must exist: %s" % output_path)

def execute(context):

    print("Preparing activities")

    output_path = context.config("output_path")
    df_trips = pd.DataFrame(context.stage("synthesis.population.trips"), copy = True)
    df_trips["TripOrderNum"] = df_trips["TripOrderNum"].astype(int)
    df_trips = df_trips.sort_values(by = ["PersonID", "TripOrderNum"])
    df_trips["PreviousTripOrderNum"] = df_trips["TripOrderNum"] - 1

    # Set up activities per trips origin and destination
    df_activities = pd.merge(
        df_trips, df_trips, left_on = ["PersonID", "PreviousTripOrderNum"], right_on = ["PersonID", "TripOrderNum"],
        suffixes = ["_DestTrip", "_OriginTrip"], how = "left"
    )

    df_activities.loc[:, "StartTime"] = df_activities.loc[:, "DestEnd_OriginTrip"]
    df_activities.loc[:, "EndTime"] = df_activities.loc[:, "OriginStart_DestTrip"]

    df_activities.loc[:, "Purpose"] = df_activities.loc[:, "DestPurpose_OriginTrip"]
    df_activities.loc[:, "ActivityID"] = df_activities.loc[:, "TripID_OriginTrip"]
    df_activities.loc[:, "TripOrderNum"] = df_activities.loc[:, "TripOrderNum_OriginTrip"]
    df_activities.loc[:, "IsLast"] = False

    # Assume that the plans start at home
    df_activities.loc[:, "TripOrderNum"] = df_activities.loc[:, "TripOrderNum"].fillna(0)
    df_activities.loc[:, "ActivityID"] = df_activities.loc[:, "ActivityID"].fillna("FIRST_" + df_activities["PersonID"].astype(str))
    df_activities.loc[:, "Purpose"] = df_activities.loc[:, "Purpose"].fillna("1")

    # Add the last activity of each trip the chain
    df_last = df_activities.sort_values(by = ["PersonID", "TripOrderNum"])
    df_last = df_last.drop_duplicates("PersonID", keep = "last")
    df_last.loc[:, "Purpose"] = df_last.loc[:, "DestPurpose_DestTrip"]
    df_last.loc[:, "StartTime"] = df_last.loc[:, "DestEnd_DestTrip"]
    df_last.loc[:, "EndTime"] = np.nan
    df_last.loc[:, "ActivityID"] = ["LAST_" + str(PersonID) for PersonID in df_last["PersonID"]]
    df_last.loc[:, "TripOrderNum"] = df_last.loc[:, "TripOrderNum_DestTrip"]
    df_last.loc[:, "IsLast"] = True
    df_activities = pd.concat([df_activities, df_last])

    # Organize dataframe
    df_activities = df_activities.sort_values(by = ["PersonID", "TripOrderNum"])

    # In case there are people without trips, add only the last activity
    df_persons = context.stage("synthesis.population.sociodemographics")[["PersonID"]]
    missing_ids = set(np.unique(df_persons["PersonID"])) - set(np.unique(df_activities["PersonID"]))
    print("Found %d persons without activities" % len(missing_ids))
    df_missing = pd.DataFrame.from_records([
        (person_id, "LAST_" + str(person_id), "1", True) for person_id in missing_ids
    ], columns = ["PersonID", "ActivityID", "Purpose", "IsLast"])

    # Get household id in the people without trips
    #df_missing = pd.merge(df_missing, df_persons[["PersonID"]]) # not at the moment

    # Merge persons with and without trips
    df_activities = pd.concat([df_activities, df_missing], sort = True)
    assert(len(np.unique(df_persons["PersonID"])) == len(np.unique(df_activities["PersonID"])))

    # Clean up
    df_activities = df_activities.sort_values(by = ["PersonID", "TripOrderNum"])
    df_activities.loc[:, "Duration"] = df_activities.loc[:, "EndTime"] - df_activities.loc[:, "StartTime"]

    df_activities = df_activities[[
        "PersonID", "ActivityID", "TripOrderNum", "StartTime", "EndTime", "Duration", "Purpose", "IsLast"
    ]]

    print("Saving activities")
    df_activities.to_csv("%s/Trips/activities.csv" % output_path)
    df_activities.to_xml("%s/Trips/activities.xml" % output_path)

    df_activities = df_activities[[
        "PersonID", "ActivityID", "TripOrderNum",
        "Purpose", "StartTime", "EndTime",
        "IsLast"
    ]]
    print("Saved activities")

    return df_activities
