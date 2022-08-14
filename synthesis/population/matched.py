import pandas as pd
import numpy as np
import synthesis.population.algo.hot_deck_matching

MINIMUM_SOURCE_SAMPLES = 3

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

    # Remove and track unmatchable persons
    all_df_matching = []
    all_removed_person_ids = []
    dfs = {"census": df_census, "target": [df_target_municipalities, df_target_city]}
    num_dfs = len(dfs["census"])
    assert num_dfs == len(dfs["target"])
    for df_ind in range(0, num_dfs):
        df_census = dfs["census"][df_ind]
        df_target = dfs["target"][df_ind]

        unmatchable_person_selector = df_target["hdm_source_id"] == -1
        umatchable_person_ids = set(df_target.loc[unmatchable_person_selector, "PersonID"].values)
        unmatchable_member_selector = df_census["PersonID"].isin(umatchable_person_ids)

        removed_person_ids = set(df_census.loc[unmatchable_member_selector, "PersonID"].values)
        all_removed_person_ids.append(removed_person_ids)

        # Decide what to do with unmatchable persons, either:
        # a) Assign randomly to each unmatchable person
        if df_ind == 0:
            df_target.loc[unmatchable_person_selector, "hdm_source_id"] = \
                df_target.loc[unmatchable_person_selector, "hdm_source_id"].map(
                    lambda x: random.choice(df_source_CzechiaHTS["PersonID"],
                                               p=df_source_CzechiaHTS["Weight"] / sum(df_source_CzechiaHTS["Weight"]))).copy()
        else:
            df_target.loc[unmatchable_person_selector, "hdm_source_id"] = \
                df_target.loc[unmatchable_person_selector, "hdm_source_id"].map(
                    lambda x: random.choice(df_source_CityHTS["PersonID"],
                                               p=df_source_CityHTS["Weight"] / sum(df_source_CityHTS["Weight"]))).copy()

        # b) Delete unmatchable people
        # initial_census_length = len(df_census)
        # initial_target_length = len(df_target)
        #
        # df_target = df_target.loc[~unmatchable_person_selector, :]
        # df_census = df_census.loc[~unmatchable_member_selector, :]
        #
        # removed_persons_count = sum(unmatchable_person_selector)
        # removed_members_count = sum(unmatchable_member_selector)
        #
        # assert(len(df_target) == initial_target_length - removed_persons_count)
        # assert(len(df_census) == initial_census_length - removed_members_count)

        # Get only the matching information
        df_matching = pd.merge(
            df_census[[ "PersonID" ]],
            df_target[[ "PersonID", "hdm_source_id" ]],
            on = "PersonID", how = "left")

        df_matching["hts_PersonID"] = df_matching["hdm_source_id"]
        del df_matching["hdm_source_id"]
        all_df_matching.append(df_matching)

        assert(len(df_matching) == len(df_census))

    print("Matching is done. In total, the following observations could not be matched "
          "(optionally you can remove them, "
          "but at the moment they were assigned a random set of attributes from both HTS): ")
    sum_removed = sum([len(pids) for pids in all_removed_person_ids])
    print("  Persons: %d (%.2f%%)" % ( sum_removed,
                                       100.0 * sum_removed
                                       / sum(number_of_census_persons) ))

    return all_df_matching, all_removed_person_ids
