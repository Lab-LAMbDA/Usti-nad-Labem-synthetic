import pandas as pd
import numpy as np
from tqdm import tqdm
import os
from data import commonFunctions
import warnings

def configure(context):
    context.config("data_path")
    context.stage("synthesis.population.matched")
    context.stage("synthesis.population.sampled")
    context.stage("data.hts.cleaned")
    context.config("routes_file")
    context.config("output_path")

def validate(context):
    data_path = context.config("data_path")
    output_path = context.config("output_path")
    routes_file = "%s/%s" % (context.config("data_path"), context.config("routes_file"))

    if not os.path.isdir(data_path):
        raise RuntimeError("Input directory must exist: %s" % data_path)

    if not os.path.isdir(output_path):
        raise RuntimeError("Output directory must exist: %s" % output_path)

    if not os.path.exists(routes_file):
        raise RuntimeError("Input file must exist: %s" % routes_file)

def execute(context):

    # Ignore warning when working on slices of dataframes
    pd.options.mode.chained_assignment = None

    # Ignore header warning when reading excel files
    warnings.filterwarnings('ignore', category=UserWarning, module='openpyxl')

    # Define the code of the cities within Ustí nad Labem district
    cities_usti_district = ('530620',
                            '546186',
                            '546925',
                            '553697',
                            '554804',
                            '555223',
                            '567931',
                            '567957',
                            '567973',
                            '568007',
                            '568015',
                            '568023',
                            '568058',
                            '568091',
                            '568104',
                            '568147',
                            '568155',
                            '568201',
                            '568287',
                            '568295',
                            '568309',
                            '568350',
                            '568384')

    print("Preparing sociodemographics of matched population")

    all_df_matching, _ = context.stage("synthesis.population.matched")
    all_df_persons = context.stage("synthesis.population.sampled")
    all_df_hts = list(context.stage("data.hts.cleaned"))
    df_routes_gate = pd.read_excel("%s/%s" % (context.config("data_path"), context.config("routes_file")),
                                   header=0,
                                   # encoding="cp1250",
                                   dtype=str)[["State", "KOD_LAU2", "Shape_Length", "GATEosm_id"]]

    # Get distance of Czech towns to Ustí city
    df_routes_gate["Shape_Length"] = df_routes_gate["Shape_Length"].astype(np.float).astype(np.int)

    # Get both HTS data
    all_df_hts[0] = all_df_hts[0].rename(columns={'ActivityCzechiaHTS': 'Activity'}).copy()
    all_df_hts[1] = all_df_hts[1].rename(columns={'ActivityCityHTS': 'Activity'}).copy()
    all_df_persons[0] = all_df_persons[0].rename(columns={'ActivityCzechiaHTS': 'Activity'}).copy()
    all_df_persons[0] = all_df_persons[0].drop(columns=['ActivityCityHTS'])
    all_df_persons[1] = all_df_persons[1].rename(columns={'ActivityCzechiaHTS': 'Activity'}).copy()
    all_df_persons[1] = all_df_persons[1].drop(columns=['ActivityCityHTS'])

    for df_ind, df_matching in enumerate(all_df_matching):
        df_persons = all_df_persons[df_ind]
        df_hts = pd.DataFrame(all_df_hts[df_ind], copy = True)

        df_hts["hts_PersonID"] = df_hts["PersonID"]
        del df_hts["PersonID"]

        # Select desirable data
        df_persons = df_persons[[
            'PersonID',
            # 'HouseholdID', # not at the moment
            'BasicSettlementCode', 'CadastralAreaCode', 'TownCode', "DistrictCode",
            'ActivitySector',
            'EducationPlace',
            'DeclaredJourneyTime',
            'Activity',
            # 'Education', # potential useful info
            'Gender',
            "Age",
            'AgeGroup',
            'PrimaryLocTownCode',
            'PrimaryLocDistrictCode',
            'PrimaryLocRegionCode',
            'PrimaryLocStateCode',
            'PrimaryLocRelationHome',
            # 'TownSize' # potential useful info
            # 'ApartmentType', 'BuildingType', # not at the moment
        ]]

        df_hts = df_hts[[
            "hts_PersonID",
            'PrimaryLocStateName',
            "JourneyMainMode",
            "PrimaryLocCrowFliesTripDist",
            "HasWorkTrip",
            "HasEducationTrip",
            # 'Homeoffice',# potential useful info
            # 'FlexibleBegEndTime', # potential useful info
            'FlexibleHours',
            # 'AvailCarSharing', # potential useful info
            "AvailCar", "AvailBike", "DrivingLicense", "PtSubscription", "IsPassenger",
        ]]

        # Merge attributes from HTS (keep Census data if overlapping attributes)
        df_persons = pd.merge(df_persons, df_matching, on="PersonID", how="inner")
        df_persons = df_persons[~df_persons.astype(str).duplicated()]
        df_persons = pd.merge(df_persons, df_hts, on="hts_PersonID", how="left")
        df_persons = df_persons[~df_persons.astype(str).duplicated()]

        # Change the following if starting/leaving the district and going/coming to/from an outer area:
        # a) PrimaryLocCrowFliesTripDist for each person that lives and travels (primarily) to outside the area

        all_outside_towns = set(df_persons["TownCode"].dropna())
        all_outside_towns.update(set(df_persons["PrimaryLocTownCode"].dropna()))
        all_outside_towns.difference_update(set(cities_usti_district))
        all_outside_towns.difference_update({"0"})

        for town_code in all_outside_towns:
            town_dist = df_routes_gate.loc[df_routes_gate["KOD_LAU2"] == town_code, "Shape_Length"]
            town_dist = town_dist.values[0]
            persons_inds = (df_persons["TownCode"] == town_code) \
                            | (df_persons["PrimaryLocTownCode"] == town_code)
            df_persons.loc[persons_inds, "PrimaryLocCrowFliesTripDist"] = \
                df_persons.loc[persons_inds, "PrimaryLocCrowFliesTripDist"] - town_dist
            negative_dist_persons = ((df_persons["TownCode"] == town_code)
                                     | (df_persons["PrimaryLocTownCode"] == town_code)) \
                                     & (df_persons["PrimaryLocCrowFliesTripDist"] < town_dist)
            df_persons.loc[negative_dist_persons, "PrimaryLocCrowFliesTripDist"] = town_dist

        all_outside_states = set(df_persons["PrimaryLocStateName"].dropna()) - set(['Česko'])

        for state_name in all_outside_states:
            state_dist = df_routes_gate.loc[df_routes_gate["State"] == state_name, "Shape_Length"]
            state_dist = state_dist.values[0]

            persons_inds = (df_persons["PrimaryLocStateName"] == state_name)
            df_persons.loc[persons_inds, "PrimaryLocCrowFliesTripDist"] = \
                df_persons.loc[persons_inds, "PrimaryLocCrowFliesTripDist"] - state_dist

            negative_dist_persons = (df_persons["PrimaryLocStateName"] == state_name) \
                                    & (df_persons["PrimaryLocCrowFliesTripDist"] < state_dist)
            df_persons.loc[negative_dist_persons, "PrimaryLocCrowFliesTripDist"] = state_dist

        # Change the real town ID to synthetic gate ID if zone is not within the district
        df_groups = df_routes_gate.groupby("GATEosm_id")
        desc = "Change the real town ID to synthetic gate ID if zone is not within the district for df_ind: " + str(df_ind)
        for GATEosm_id, places_via_gate in tqdm(df_groups, total=len(df_groups),
                                                desc=desc):
            cities_gate_ids = places_via_gate["KOD_LAU2"].dropna().values.tolist()

            home_out_inds = (df_persons["DistrictCode"] != '4214') \
                            & (df_persons["TownCode"].isin(cities_gate_ids))
            df_persons.loc[home_out_inds, "BasicSettlementCode"] = GATEosm_id

        if df_ind == 0:
            # CzechiaHTS data
            df_persons['ZoneID'] = df_persons.loc[:, 'TownCode'].copy()
        else:
            # CityHTS data
            df_persons['ZoneID'] = df_persons.loc[:, 'CadastralAreaCode'].copy()

        # Create a final dataframe for persons by merging city and municipalities records
        try:
            final_df_persons = pd.concat([final_df_persons, df_persons], sort=True)
        except NameError:
            final_df_persons = df_persons

    print("\nSaving population sociodemographics")

    final_df_persons.to_csv("%s/Population/persons.csv" % context.config("output_path"))
    commonFunctions.toXML(final_df_persons, "%s/Population/persons.xml" % context.config("output_path"))

    print("\nSaved population sociodemographics")

    return final_df_persons
