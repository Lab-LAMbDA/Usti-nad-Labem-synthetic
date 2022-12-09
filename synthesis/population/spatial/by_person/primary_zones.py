from tqdm import tqdm
import pandas as pd
import numpy as np

def configure(context):
    context.stage("data.od.cleaned")
    context.stage("synthesis.population.sociodemographics")
    context.stage("synthesis.population.trips")

def validate(context):

    pass

def execute(context):

    print("Prepare the primary zones of the population")

    # Get population sociodemographics
    df_persons = pd.DataFrame(context.stage("synthesis.population.sociodemographics"), copy=True)

    # Get OD proportions
    df_work_od, df_education_od = context.stage("data.od.cleaned")

    df_persons = df_persons[["PersonID",
                             "ZoneID",
                             "AgeGroup",
                             "PrimaryLocDistrictCode",
                             "PrimaryLocTownCode",
                             "ActivitySector",
                             "EducationPlace",
                             "HasWorkTrip",
                             "HasEducationTrip",
                             # "HouseholdID", # not at the moment
                             ]]

    # Define the households' home zone.
    # As at the moment not knowing which person is in each household, it is assumed 1 household = 1 person
    df_home = df_persons[["PersonID",
                          "ZoneID",
                          # "HouseholdID", # not at the moment
                          ]]

    # Define the persons/agents' work zone given zones weight (from OD proportion as origin point of the trip)
    df_work = []

    for origin_id in tqdm(np.unique(df_persons["ZoneID"]), desc = "Sampling work zones", ascii=True):
        f = (df_persons["ZoneID"] == origin_id) & df_persons["HasWorkTrip"]
        df_origin = pd.DataFrame(df_persons[f][["PersonID", "ActivitySector"]], copy = True)
        df_destination = df_work_od[df_work_od["OriginID"] == origin_id]

        if len(df_origin) > 0:
            counts = np.random.multinomial(len(df_origin), df_destination["Weight"].values)
            indices = np.repeat(np.arange(len(df_destination)), counts)
            df_origin["ZoneID"] = df_destination.iloc[indices]["DestID"].values
            df_work.append(df_origin[["PersonID", "ZoneID", "ActivitySector",
                                      ]])

    # Merge each zone dataframe into one dataframe
    try:
        df_work = pd.concat(df_work)
    except ValueError:
        df_work = pd.DataFrame(columns=["PersonID", "ZoneID", "ActivitySector"])

    # Define the persons/agents' work zone given zones weight (from OD proportion as origin point of the trip)
    df_education = []

    for origin_id in tqdm(np.unique(df_persons["ZoneID"]), desc = "Sampling education zones", ascii=True):
        f = (df_persons["ZoneID"] == origin_id) & df_persons["HasEducationTrip"]
        df_origin = pd.DataFrame(df_persons[f][["PersonID", "AgeGroup"]], copy = True)
        df_destination = df_education_od[df_education_od["OriginID"] == origin_id]

        if len(df_origin) > 0:
            counts = np.random.multinomial(len(df_origin), df_destination["Weight"].values)
            indices = np.repeat(np.arange(len(df_destination)), counts)
            df_origin.loc[:, "ZoneID"] = df_destination.iloc[indices]["DestID"].values
            df_education.append(df_origin[["PersonID", "ZoneID", "AgeGroup"]])

    # Merge each zone dataframe into one dataframe
    try:
        df_education = pd.concat(df_education)
    except ValueError:
        df_education = pd.DataFrame(columns=["PersonID", "ZoneID", "AgeGroup"])

    len_home = len(df_home)
    len_work = len(df_persons.loc[df_persons["HasWorkTrip"]])
    len_edu = len(df_persons.loc[df_persons["HasEducationTrip"]])

    df_home.drop_duplicates()
    df_education.drop_duplicates()
    df_work = df_work[~df_work.astype(str).duplicated()]

    assert len_home == len(df_home)
    assert len_work == len(df_work)
    assert len_edu == len(df_education)

    return df_home, df_work, df_education
