import pandas as pd
import numpy as np
import os
from data import commonFunctions

def configure(context):

    context.config("output_path")
    context.stage("synthesis.population.sociodemographics")
    context.stage("data.hts.cleaned")
    # context.config("routes_file") # (included in hts.filtered and also for later when defined zones)

def validate(context):
    output_path = context.config("output_path")

    if not os.path.isdir(output_path):
        raise RuntimeError("Output directory must exist: %s" % output_path)

def execute(context):

    print("Preparing trips of matched population")

    # Get population (sociodemographics)
    df_persons = context.stage("synthesis.population.sociodemographics")[[
        "PersonID", "hts_PersonID", "AgeGroup", "ActivitySector",
        "PrimaryLocDistrictCode", "PrimaryLocTownCode",
        # 'DistrictCode', 'TownCode',
    ]]

    df_trips_CzechiaHTS = pd.DataFrame(context.stage("data.hts.cleaned")[2], copy=True)
    df_trips_CityHTS = pd.DataFrame(context.stage("data.hts.cleaned")[3], copy=True)

    # Define trip attributes for every person in the population
    for df_ind,df_trips in enumerate([df_trips_CzechiaHTS, df_trips_CityHTS]):
        df_trips = df_trips[[
            "PersonID", "TripID", "TripOrderNum", "OriginStart", "DestEnd", "TripMainMode", "OriginPurpose", "DestPurpose",
            "DeclaredTripTime",
            # "CrowFliesTripDist", "OriginTownCode", "DestTownCode",
            # "OriginDistrictCode", "DestDistrictCode",
            # "OriginState", "DestState"
        ]]

        df_trips = df_trips.rename(columns={'PersonID': 'hts_PersonID'}).copy()

        # Merge trips to persons
        df_trips = pd.merge(df_persons, df_trips, on='hts_PersonID')

        # Diversify departure times
        df_trips.loc[df_trips["DestEnd"] < df_trips["OriginStart"], "DestEnd"] += 24.0 * 3600.0
        # df_trips.loc[:, "travel_time"] = df_trips.loc[:, "arrival_time"] - df_trips.loc[:, "departure_time"]
        df_trips = df_trips.sort_values(by = ["PersonID", "TripOrderNum"])

        # Define half-hour intervals for offsets
        counts = df_trips[["PersonID", "TripID"]].groupby("PersonID").size().reset_index(name = "count")["count"].values
        interval = df_trips[["PersonID", "OriginStart"]].groupby("PersonID").min().reset_index()["OriginStart"].values
        interval = np.minimum(1800.0, interval)

        offset = np.random.random(size = (len(counts), )) * interval * 2.0 - interval
        offset = np.repeat(offset, counts)

        df_trips["OriginStart"] += offset
        df_trips["DestEnd"] += offset
        df_trips["OriginStart"] = np.round(df_trips["OriginStart"])
        df_trips["DestEnd"] = np.round(df_trips["DestEnd"])

        # Clean up
        df_trips = df_trips[[
            "PersonID", "TripID", "TripOrderNum", "OriginStart", "DestEnd", "DeclaredTripTime", "TripMainMode", "OriginPurpose",
            "DestPurpose", "AgeGroup", "hts_PersonID", "PrimaryLocDistrictCode", "PrimaryLocTownCode", "ActivitySector",
            # "OriginTownCode", "DestTownCode", "OriginCadastralAreaCode", "DestCadastralAreaCode",
        ]]

        # Create a final dataframe for trips by merging city and municipalities records
        try:
            final_df_trips = pd.concat([final_df_trips, df_trips], sort=True)
        except NameError:
            final_df_trips = df_trips

    print("\nSaving population trips")

    final_df_trips.to_csv("%s/Trips/trips.csv" % context.config("output_path"))
    commonFunctions.toXML(final_df_trips, "%s/Trips/trips.xml" % context.config("output_path"))

    print("\nSaved population trips")

    return final_df_trips


