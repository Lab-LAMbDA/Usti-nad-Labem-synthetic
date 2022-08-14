from tqdm import tqdm
import pandas as pd
import numpy as np
import os
from data import commonFunctions
import warnings

def configure(context):

    context.config("random_seed")
    context.config("data_path")
    context.config("output_path")
    context.stage("data.hts.cleaned")
    context.config("routes_file")

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
    
    # Ignore header warning when reading excel files
    warnings.filterwarnings('ignore', category=UserWarning, module='openpyxl')

    print("Retrieving cleaned Household Travel Survey (HTS) data and supporting files")

    # Get random seed for defining the most likely synthetic gate for each town outside the study area
    random = np.random.RandomState(context.config("random_seed"))

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

    # Ignore warning when working on slices of dataframes
    pd.options.mode.chained_assignment = None

    # Get cleaned HTS
    df_persons_CzechiaHTS, df_persons_CityHTS = context.stage("data.hts.cleaned")[:2]
    df_persons_CzechiaHTS = df_persons_CzechiaHTS.copy()
    df_persons_CityHTS = df_persons_CityHTS.copy()
    df_trips_CzechiaHTS, df_trips_CityHTS = context.stage("data.hts.cleaned")[2:]
    df_trips_CzechiaHTS = df_trips_CzechiaHTS.copy()
    df_trips_CityHTS = df_trips_CityHTS.copy()

    # Get the most suitable synthetic gates, distance and travel times to every town in Czechia
    df_routes_gate = pd.read_excel("%s/%s" % (context.config("data_path"), context.config("routes_file")),
                                   header=0,
                                   # encoding="cp1250",
                                   dtype=str)[["State", "KOD_LAU2", "KOD_ORP", "GATEosm_id",
                                               "Total_HourCostDrive", "Shape_Length", 'POPULATION_LAU2']]
    df_routes_gate["Shape_Length"] = df_routes_gate["Shape_Length"].astype(np.float).astype(np.int)
    df_routes_gate["Total_HourCostDrive"] = df_routes_gate["Total_HourCostDrive"].astype(np.float)
    df_routes_gate['POPULATION_LAU2'] = df_routes_gate['POPULATION_LAU2'].fillna("0")
    df_routes_gate['POPULATION_LAU2'] = df_routes_gate['POPULATION_LAU2'].astype('int')

    print("Filtering Household Travel Survey (HTS) data")

    # Filter each HTS
    all_df_persons = [df_persons_CzechiaHTS, df_persons_CityHTS]
    all_df_trips = [df_trips_CzechiaHTS, df_trips_CityHTS]
    for df_ind,df_trips in enumerate(all_df_trips):

        df_persons = all_df_persons[df_ind].copy()

        # # Use only persons and their trips from and to Ustí district (removed because it causes the samples to be too small)
        # df_trips = df_trips.loc[(df_trips["OriginDistrictCode"] == '4214') | (df_trips["DestDistrictCode"] == '4214')]
        # person_remain = df_persons["PersonID"].isin(df_trips["PersonID"])
        # df_persons = df_persons[person_remain]

        ### Code below only if considering peope living in the near areas out of the district
        # Change the following if trip starting/leaving the district and going/coming to/from the outer area:
            # a) DeclaredTripTime from df_CzechiaHTS_t and CityHTS_t
            # b) OriginStart from df_CzechiaHTS_t and CityHTS_t
            # c) DestEnd from df_CzechiaHTS_t and CityHTS_t
            # d) CrowFliesTripDist from df_CzechiaHTS_t and CityHTS_t
            # e) DestDuration from df_CzechiaHTS_t and CityHTS_t
        # df_trips = df_trips.sort_values(by = ["PersonID", "TripID"])
        # df_trips = df_trips.rename(columns={'PersonID': 'hts_PersonID'}).copy()
        # all_outside_towns = set(df_trips["OriginTownCode"].dropna())
        # all_outside_towns.update(set(df_trips["DestTownCode"].dropna()))
        # all_outside_towns.difference_update(set(cities_usti_district))
        # all_outside_towns.difference_update({"0"})
        #
        # for town_code in all_outside_towns:
        #     town_dist = df_routes_gate.loc[df_routes_gate["KOD_LAU2"] == town_code, "Shape_Length"]
        #     town_time = df_routes_gate.loc[df_routes_gate["KOD_LAU2"] == town_code, "Total_HourCostDrive"] * 60
        #
        #     persons_inds = df_trips["OriginTownCode"] == town_code
        #     df_trips[persons_inds, "OriginStart"] += town_time
        #     df_trips[persons_inds, "CrowFliesTripDist"] -= town_dist
        #     df_trips[persons_inds, "DeclaredTripTime"] -= town_time
        #
        #     negative_dist_persons = (df_trips["OriginTownCode"] == town_code) \
        #                             & (df_trips["CrowFliesTripDist"] < town_dist)
        #     df_trips.loc[negative_dist_persons, "CrowFliesTripDist"] = town_dist
        #     negative_dist_persons = (df_trips["OriginTownCode"] == town_code) \
        #                             & (df_trips["DeclaredTripTime"] < town_time)
        #     df_trips.loc[negative_dist_persons, "DeclaredTripTime"] = town_time
        #
        #     persons_inds = df_trips["DestTownCode"] == town_code
        #     df_trips[persons_inds, "DestEnd"] -= town_time
        #     df_trips[persons_inds, "CrowFliesTripDist"] -= town_dist
        #     df_trips[persons_inds, "DeclaredTripTime"] -= town_time
        #
        #     negative_dist_persons = (df_trips["DestTownCode"] == town_code) \
        #                             & (df_trips["DestEnd"] < 0)
        #     df_trips.loc[negative_dist_persons, "DestEnd"] = 0
        #     negative_dist_persons = (df_trips["DestTownCode"] == town_code) \
        #                             & (df_trips["CrowFliesTripDist"] < town_dist)
        #     df_trips.loc[negative_dist_persons, "CrowFliesTripDist"] = town_dist
        #     negative_dist_persons = (df_trips["DestTownCode"] == town_code) \
        #                             & (df_trips["DeclaredTripTime"] < town_time)
        #     df_trips.loc[negative_dist_persons, "DeclaredTripTime"] = town_time
        #
        # all_outside_states = set(df_trips["DestState"].dropna()).union(df_trips["OriginState"].dropna()).difference(set(['Česko']))
        #
        # for state_name in all_outside_states:
        #     state_dist = df_routes_gate.loc[df_routes_gate["State"] == state_name, "Shape_Length"]
        #     state_time = df_routes_gate.loc[df_routes_gate["State"] == state_name, "Total_HourCostDrive"] * 60
        #
        #     persons_inds = df_trips["OriginState"] == state_name
        #     df_trips[persons_inds, "OriginStart"] += state_time
        #     df_trips[persons_inds, "CrowFliesTripDist"] -= state_dist
        #     df_trips[persons_inds, "DeclaredTripTime"] -= state_time
        #
        #     negative_dist_persons = (df_trips["DestState"] == state_name) \
        #                             & (df_trips["CrowFliesTripDist"] < state_dist)
        #     df_trips.loc[negative_dist_persons, "CrowFliesTripDist"] = state_dist
        #     negative_dist_persons = (df_trips["DestState"] == state_name) \
        #                             & (df_trips["DeclaredTripTime"] < state_time)
        #     df_trips.loc[negative_dist_persons, "DeclaredTripTime"] = state_time
        #
        #     persons_inds = df_trips["DestState"] == state_name
        #     df_trips[persons_inds, "DestEnd"] -= state_time
        #     df_trips[persons_inds, "CrowFliesTripDist"] -= state_dist
        #     df_trips[persons_inds, "DeclaredTripTime"] -= state_time
        #
        #     negative_dist_persons = (df_trips["DestState"] == state_name) \
        #                             & (df_trips["DestEnd"] < 0)
        #     df_trips.loc[negative_dist_persons, "DestEnd"] = 0
        #     negative_dist_persons = (df_trips["DestState"] == state_name) \
        #                             & (df_trips["CrowFliesTripDist"] < state_dist)
        #     df_trips.loc[negative_dist_persons, "CrowFliesTripDist"] = state_dist
        #     negative_dist_persons = (df_trips["DestState"] == state_name) \
        #                             & (df_trips["DeclaredTripTime"] < state_time)
        #     df_trips.loc[negative_dist_persons, "DeclaredTripTime"] = state_time
        ### Code above only if considering peope living in the near areas out of the district

        if len(df_persons) > 0 or len(df_trips) > 0:
            if df_ind == 0:
                # IF CzechiaHTS data
                # Change the real town ID to synthetic gate ID if real town is not within Ustí nad Labem district
                # Not necessary country/state because CzechiaHTS trips does not account country
                df_groups = df_routes_gate.groupby("GATEosm_id")
                for GATEosm_id, places_via_gate in tqdm(df_groups, total=len(df_groups),
                                                        desc="Changing real town ID to synthetic gate ID, "
                                                             "if zone is not within the district (for CzechiaHTS)"):
                    cities_gate_ids = places_via_gate["KOD_LAU2"].dropna().values.tolist()

                    df_persons.loc[((df_persons["DistrictCode"] != '4214')
                                   & (df_persons["TownCode"].isin(cities_gate_ids))), "TownCode"] = GATEosm_id

                    df_trips.loc[((df_trips["OriginDistrictCode"] != '4214')
                                  & (df_trips["OriginTownCode"].isin(cities_gate_ids))), "OriginTownCode"] = GATEosm_id

                    df_trips.loc[((df_trips["DestDistrictCode"] != '4214')
                                  & (df_trips["DestTownCode"].isin(cities_gate_ids))), "DestTownCode"] = GATEosm_id

                # Change the real town ID to synthetic gate ID if town code is unknown (but known district code)
                # Select (probabilistically) the most suitable gate
                df_groups = df_routes_gate.groupby("KOD_ORP")
                for district_code, df_region in tqdm(df_groups, total=len(df_groups),
                                                     desc="Changing real town ID to synthetic gate ID,"
                                                          " if the town code is unknown (for CzechiaHTS)"):

                    gate_groups = df_region.groupby('GATEosm_id')[['GATEosm_id', 'POPULATION_LAU2']]
                    gate_groups = gate_groups.agg('sum')
                    try:
                        prob_gates = gate_groups.get('POPULATION_LAU2')['POPULATION_LAU2']
                    except KeyError:
                        prob_gates = gate_groups.get('POPULATION_LAU2')
                    prob_gates = prob_gates.map(lambda x: x / sum(prob_gates))
                    gates = list(prob_gates.keys())
                    probs = list(prob_gates)

                    df_persons.loc[((pd.isnull(df_persons["TownCode"]))
                                   & (df_persons["DistrictCode"] == district_code)),
                                  "TownCode"] = \
                        df_persons.loc[(pd.isnull(df_persons["TownCode"])
                                       & (df_persons["DistrictCode"] == district_code)),
                                      "TownCode"].map(lambda x: random.choice(gates, p=probs)).copy()

                    df_trips.loc[(pd.isnull(df_trips["OriginTownCode"])
                                  & (df_trips["OriginDistrictCode"] == district_code)),
                                 "OriginTownCode"] = \
                        df_trips.loc[(pd.isnull(df_trips["OriginTownCode"])
                                      & (df_trips["OriginDistrictCode"] == district_code)),
                                     "OriginTownCode"].map(lambda x: random.choice(gates, p=probs)).copy()

                    df_trips.loc[(pd.isnull(df_trips["DestTownCode"])
                                  & (df_trips["DestDistrictCode"] == district_code)),
                                 "DestTownCode"] = \
                        df_trips.loc[(pd.isnull(df_trips["DestTownCode"])
                                      & (df_trips["DestDistrictCode"] == district_code)),
                                     "DestTownCode"].map(lambda x: random.choice(gates, p=probs)).copy()
            else:
                # If CityHTS data
                # Change the real cadastral area ID to the town ID if cadastral area is unknown but town code is known
                # Not necessary for home location because CityHTS only have people living within Ústí town
                df_trips.loc[((df_trips["OriginCadastralAreaCode"] == "0")
                                & (df_trips["OriginTownCode"].isin(cities_usti_district))), "OriginCadastralAreaCode"] = \
                    df_trips.loc[((df_trips["OriginCadastralAreaCode"] == "0")
                                    & (df_trips["OriginTownCode"].isin(cities_usti_district))), "OriginTownCode"].copy()

                df_trips.loc[((df_trips["DestCadastralAreaCode"] == "0")
                                & (df_trips["DestTownCode"].isin(cities_usti_district))), "DestCadastralAreaCode"] = \
                    df_trips.loc[((df_trips["DestCadastralAreaCode"] == "0")
                                    & (df_trips["DestTownCode"].isin(cities_usti_district))), "DestTownCode"].copy()

                # Change the real cadastral area IDs to synthetic gate ID if the town code is unknown
                df_groups = df_routes_gate.groupby("GATEosm_id")
                for GATEosm_id, places_via_gate in tqdm(df_groups, total=len(df_groups),
                                                        desc="Changing real cadastral area ID to synthetic gateID,"
                                                             " if the town code is unknown (for CityHTS)"):

                    cities_gate_ids = places_via_gate["KOD_LAU2"].dropna().values.tolist()
                    countries_gate_name = places_via_gate["State"].drop_duplicates().values.tolist()

                    f = (((df_trips["OriginCadastralAreaCode"] == "0")
                                    & (df_trips["OriginState"].isin(countries_gate_name)))
                                   | ((df_trips["OriginCadastralAreaCode"] != "0")
                                      & (df_trips["OriginCadastralAreaCode"].isin(cities_gate_ids))))
                    df_trips.loc[f, "OriginCadastralAreaCode"] = GATEosm_id
                    df_trips.loc[f, "OriginTownCode"] = GATEosm_id

                    f = (((df_trips["DestCadastralAreaCode"] == "0")
                                    & (df_trips["DestState"].isin(countries_gate_name)))
                                   | ((df_trips["DestCadastralAreaCode"] != "0")
                                      & (df_trips["DestCadastralAreaCode"].isin(cities_gate_ids))))
                    df_trips.loc[f, "DestCadastralAreaCode"] = GATEosm_id
                    df_trips.loc[f, "DestTownCode"] = GATEosm_id

        ### Code below only if considering peope living in the near areas out of the district
        # if df_ind == 0:
        #     df_trips['OriginCadastralAreaCode'] = df_trips.loc[:, 'OriginTownCode'].copy()
        #     df_trips['DestCadastralAreaCode'] = df_trips.loc[:, 'DestTownCode'].copy()
        ### Code above only if considering peope living in the near areas out of the district

        all_df_trips[df_ind] = df_trips
        all_df_persons[df_ind] = df_persons

        if df_ind == 0:
            df_persons.to_csv("%s/HTS/df_CzechiaHTS_filtered_ph.csv" % context.config("output_path"))
            commonFunctions.toXML(df_persons, "%s/HTS/df_CzechiaHTS_filtered_ph.xml" % context.config("output_path"))
            df_trips.to_csv("%s/HTS/df_CzechiaHTS_filtered_t.csv" % context.config("output_path"))
            commonFunctions.toXML(df_trips, "%s/HTS/df_CzechiaHTS_filtered_t.xml" % context.config("output_path"))
        else:
            df_persons.to_csv("%s/HTS/df_CityHTS_filtered_ph.csv" % context.config("output_path"))
            commonFunctions.toXML(df_persons, "%s/HTS/df_CityHTS_filtered_ph.xml" % context.config("output_path"))
            df_trips.to_csv("%s/HTS/df_CityHTS_filtered_t.csv" % context.config("output_path"))
            commonFunctions.toXML(df_trips, "%s/HTS/df_CityHTS_filtered_t.xml" % context.config("output_path"))

    return all_df_persons[0], all_df_persons[1], all_df_trips[0], all_df_trips[1]


