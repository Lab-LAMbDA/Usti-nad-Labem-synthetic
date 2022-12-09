import sys

import pandas as pd
import numpy as np
import synthesis.population.algo.hot_deck_matching

MINIMUM_SOURCE_SAMPLES = 3
UNMATCHABLE_MODE = "RANDOM" # options: RANDOM or DELETE

def configure(context):

    context.config("random_seed")
    context.stage("data.hts.cleaned")
    context.config("processes")
    context.stage("synthesis.population.sampled")

def validate(context):

    pass

def execute(context):

    # Get random seed to randomly assign unmatchable persons to HTS sample
    random = np.random.RandomState(context.config("random_seed"))

    # Source: HTS (both CzechiaHTS and CityHTS)
    df_source_CzechiaHTS, df_source_CityHTS = context.stage("data.hts.cleaned")[:2]
    number_of_threads = context.config("processes")

    # Target: Census
    df_census = context.stage("synthesis.population.sampled")
    df_target_municipalities = df_census[0].sort_values(by = "PersonID")
    df_target_city = df_census[1].sort_values(by = "PersonID")
    number_of_census_persons = [len(np.unique(df_target_municipalities["PersonID"])),
                                len(np.unique(df_target_city["PersonID"]))]

    # Match Census person's attributes with HTS person's attributes
    print("\nMatch Census person's attributes (from the municipalities around Ustí city) "
          "with CzechiaHTS person's attributes")
    synthesis.population.algo.hot_deck_matching.run(
        df_target_municipalities, "PersonID",
        df_source_CzechiaHTS, "PersonID", "Weight",
        [
         "AgeGroup",
         "TownSize",
         ], # mandatory fields above
        [
         # "NumPersonsAge00_05", "NumPersonsAge06_17", "NumPersonsAge18_99", # not at the moment
         "ActivityCzechiaHTS",
         "Gender",
         "Education",
         "RegionCode",
         "JourneyMainMode",
         # "DeclaredJourneyTime",
         "PrimaryLocRelationHome",
        ], # preferential fields above
        runners = number_of_threads,
        minimum_source_samples = MINIMUM_SOURCE_SAMPLES
    )

    print("\nMatch Census person's attributes (from Ustí city) with CityHTS person's attributes")
    synthesis.population.algo.hot_deck_matching.run(
        df_target_city, "PersonID",
        df_source_CityHTS, "PersonID", "Weight",
        [
         "AgeGroup",
        ],  # mandatory fields above
        [
         # "NumPersons", "NumPersonsAge06_18", # not at the moment
         "ActivityCityHTS",
         "Gender",
         "Education",
         "CadastralAreaCode",
         "JourneyMainMode",
         # "DeclaredJourneyTime",
         "PrimaryLocRelationHome",
        ],  # preferential fields above
        runners=number_of_threads,
        minimum_source_samples = MINIMUM_SOURCE_SAMPLES
    )

    # Fix inconsistencies and remove and track unmatchable persons
    all_df_matching = []
    all_not_matched_person_ids = []
    all_deleted_person_ids = []
    dfs = {"census": df_census, "target": [df_target_municipalities, df_target_city],
           "source": [df_source_CzechiaHTS, df_source_CityHTS]}
    num_dfs = len(dfs["census"])
    assert num_dfs == len(dfs["target"])
    for df_ind in range(0, num_dfs):
        df_census = dfs["census"][df_ind]
        df_target = dfs["target"][df_ind]
        df_source = dfs["source"][df_ind]

        # As HTS Czechia have AgeGroup = 2 including 18 years old, fix inconsistency that could assign HTS samples with
        # driving license to people younger than 18
        young_driving_person_selector = (df_target["hdm_source_id"] != -1) & (df_target["Age"].astype(int) < 18)
        num = 0
        for selector_ind, selector in enumerate(young_driving_person_selector):
            if selector:
                selector_bool = df_source.loc[df_source["PersonID"]
                                              == df_target["hdm_source_id"].iloc[selector_ind]]["DrivingLicense"] == '1'
                if selector_bool.values:
                    df_target["hdm_source_id"].iloc[selector_ind] = -1
                    num += 1
        print("Number of inconsistencies for df_ind", str(df_ind) + ":", str(num))

        # Define the unmatchable persons
        unmatchable_person_selector = df_target["hdm_source_id"] == -1
        umatchable_person_ids = set(df_target.loc[unmatchable_person_selector, "PersonID"].values)
        unmatchable_member_selector = df_census["PersonID"].isin(umatchable_person_ids)
        not_matched_person_ids = set(df_census.loc[unmatchable_member_selector, "PersonID"].values)
        all_not_matched_person_ids.append(not_matched_person_ids)

        # Decide what to do with unmatchable persons, either:
        if UNMATCHABLE_MODE == "RANDOM":
            # a) Assign randomly to each unmatchable person
            # Assign for people up to 17 years old HTS samples of people up to 18 years old without driving license
            unmatchable_person_selector_younger = (df_target["hdm_source_id"] == -1) & (df_target["Age"].astype(int) < 18)

            # Assign for people at age 18 years old or older HTS samples of people 19 years old or more
            unmatchable_person_selector_older = (df_target["hdm_source_id"] == -1) & (
                        df_target["Age"].astype(int) >= 18)

            if df_ind == 0:
                df_source_CzechiaHTS_younger = df_source_CzechiaHTS.loc[(df_source_CzechiaHTS["AgeGroup"].isin(('1', '2')))
                                                                        & (df_source_CzechiaHTS["DrivingLicense"] == '0')]
                df_target.loc[unmatchable_person_selector_younger, "hdm_source_id"] = \
                    df_target.loc[unmatchable_person_selector_younger, "hdm_source_id"].map(
                        lambda x: random.choice(df_source_CzechiaHTS_younger["PersonID"],
                                                p=df_source_CzechiaHTS_younger["Weight"] / sum(
                                                    df_source_CzechiaHTS_younger["Weight"]))).copy()

                df_source_CzechiaHTS_older = df_source_CzechiaHTS.loc[~df_source_CzechiaHTS["AgeGroup"].isin(('1', '2'))]
                df_target.loc[unmatchable_person_selector_older, "hdm_source_id"] = \
                    df_target.loc[unmatchable_person_selector_older, "hdm_source_id"].map(
                        lambda x: random.choice(df_source_CzechiaHTS_older["PersonID"],
                                                p=df_source_CzechiaHTS_older["Weight"] / sum(
                                                    df_source_CzechiaHTS_older["Weight"]))).copy()
            else:
                df_source_CityHTS_younger = df_source_CityHTS.loc[(df_source_CityHTS["AgeGroup"].isin(('1', '2')))
                                                                  & (df_source_CityHTS["DrivingLicense"] == '0')]
                df_target.loc[unmatchable_person_selector_younger, "hdm_source_id"] = \
                    df_target.loc[unmatchable_person_selector_younger, "hdm_source_id"].map(
                        lambda x: random.choice(df_source_CityHTS_younger["PersonID"],
                                                p=df_source_CityHTS_younger["Weight"] / sum(
                                                    df_source_CityHTS_younger["Weight"]))).copy()
                df_source_CityHTS_older = df_source_CityHTS.loc[~df_source_CityHTS["AgeGroup"].isin(('1', '2'))]
                df_target.loc[unmatchable_person_selector_older, "hdm_source_id"] = \
                    df_target.loc[unmatchable_person_selector_older, "hdm_source_id"].map(
                        lambda x: random.choice(df_source_CityHTS_older["PersonID"],
                                                p=df_source_CityHTS_older["Weight"] / sum(
                                                    df_source_CityHTS_older["Weight"]))).copy()

            deletable_person_selector = df_target["hdm_source_id"] == -1
            deletable_person_ids = set(df_target.loc[deletable_person_selector, "PersonID"].values)
            deletable_member_selector = df_census["PersonID"].isin(deletable_person_ids)

            deleted_person_ids = set(df_census.loc[deletable_member_selector, "PersonID"].values)
            all_deleted_person_ids.append(deleted_person_ids)
        else:
            deleted_person_ids = set()

        if UNMATCHABLE_MODE == "DELETE" or len(deleted_person_ids) > 0:
            # b) Delete unmatchable people
            initial_census_length = len(df_census)
            initial_target_length = len(df_target)

            df_target = df_target.loc[~unmatchable_person_selector, :]
            df_census = df_census.loc[~unmatchable_member_selector, :]

            not_matched_persons_count = sum(unmatchable_person_selector)
            not_matched_members_count = sum(unmatchable_member_selector)

            assert(len(df_target) == initial_target_length - not_matched_persons_count)
            assert(len(df_census) == initial_census_length - not_matched_members_count)

        # Get only the matching information
        df_matching = pd.merge(
            df_census[["PersonID"]],
            df_target[["PersonID", "hdm_source_id"]],
            on="PersonID", how="left")

        df_matching["hts_PersonID"] = df_matching["hdm_source_id"]
        del df_matching["hdm_source_id"]
        all_df_matching.append(df_matching)

        assert(len(df_matching) == len(df_census))

    if UNMATCHABLE_MODE == "RANDOM":
        print("Matching is done. In total, the following observations could not be matched "
                  "(they were assigned a random set of attributes from HTS, but only "
              "if samples are both younger or older than 18 years old: ")
        sum_not_matched = sum([len(pids) for pids in all_not_matched_person_ids])
        print("  Persons: %d (%.2f%%)" % (sum_not_matched,
                                          100.0 * sum_not_matched
                                          / sum(number_of_census_persons)))
        print("Regarding those not matching even if younger or older, the following observations could not be matched "
              "(they were removed from the results): ")
        sum_deleted = sum([len(pids) for pids in all_deleted_person_ids])
        print("  Persons: %d (%.2f%%)" % (sum_deleted,
                                          100.0 * sum_deleted
                                          / sum(number_of_census_persons)))
    elif UNMATCHABLE_MODE == "DELETE":
        print("Matching is done. In total, the following observations could not be matched "
              "(they were removed from the results): ")
        sum_not_matched = sum([len(pids) for pids in all_not_matched_person_ids])
        print("  Persons: %d (%.2f%%)" % (sum_not_matched,
                                          100.0 * sum_not_matched
                                          / sum(number_of_census_persons)))
    else:
        pass

    return all_df_matching