import pandas as pd
import os
from data import commonFunctions
import warnings

def configure(context):
    context.stage("data.census.raw")
    context.config("data_path")
    context.config("generalizations_file")
    context.config("routes_file")

def validate(context):
    data_path = context.config("data_path")
    generalizations_file = "%s/%s" % (context.config("data_path"), context.config("generalizations_file"))
    routes_file = "%s/%s" % (context.config("data_path"), context.config("routes_file"))

    if not os.path.isdir(data_path):
        raise RuntimeError("Input directory must exist: %s" % data_path)

    if not os.path.exists(generalizations_file):
        raise RuntimeError("Input file must exist: %s" % generalizations_file)

    if not os.path.exists(routes_file):
        raise RuntimeError("Input file must exist: %s" % routes_file)

def execute(context):

    print("Cleaning Census data")

    # Import census from previous stage
    df_census = context.stage("data.census.raw")

    # Drop records with missing values
    df_census = df_census.dropna()

    # Ignore warning when working on slices of dataframes
    pd.options.mode.chained_assignment = None

    # Ignore header warning when reading excel files
    warnings.filterwarnings('ignore', category=UserWarning, module='openpyxl')

    # Import generalizations
    df_age = pd.read_excel("%s/%s" % (context.config("data_path"), context.config("generalizations_file")),
                           header=None, skiprows=1, sheet_name='Person Age', dtype=str)
    df_edu = pd.read_excel("%s/%s" % (context.config("data_path"), context.config("generalizations_file")),
                           header=None, skiprows=1, sheet_name='Person Education', dtype=str)
    df_age_place = pd.read_excel("%s/%s" % (context.config("data_path"), context.config("generalizations_file")),
                           header=None, skiprows=1, sheet_name='EducationPlace FacilityType', dtype=str)
    # df_ap_type = pd.read_excel("%s/%s" % (context.config("data_path"), context.config("generalizations_file")),
    #                            header=None, skiprows=1, sheet_name='Household ApartmentType', dtype=str) # not at the moment
    df_activity_CityHTS = pd.read_excel("%s/%s" % (context.config("data_path"), context.config("generalizations_file")),
                                      header=None, skiprows=1, sheet_name='Person Activity (CityHTS)', dtype=str)
    df_primary_loc_home = pd.read_excel("%s/%s" % (context.config("data_path"), context.config("generalizations_file")),
                                        header=None, skiprows=1, sheet_name='Person PrimaryLocRelationHome', dtype=str)
    df_journey_mode = pd.read_excel("%s/%s" % (context.config("data_path"), context.config("generalizations_file")),
                                    header=None, skiprows=1, sheet_name='Person JourneyMainMode', dtype=str)
    df_journey_t = pd.read_excel("%s/%s" % (context.config("data_path"), context.config("generalizations_file")),
                                 header=None, skiprows=1, sheet_name='Person DeclaredTripTime', dtype=str)
    df_sector_activity = pd.read_excel("%s/%s" % (context.config("data_path"), context.config("generalizations_file")),
                                       header=None, skiprows=1, sheet_name='ActivitySector FacilityUsage', dtype=str)
    df_routes_gate = pd.read_excel("%s/%s" % (context.config("data_path"), context.config("routes_file")),
                                        header=0,
                                   # encoding="cp1250",
                                   dtype=str)[["KOD_LAU2", "Total_HourCostDrive", "GATEosm_id"]]
    df_routes_gate["Total_HourCostDrive"] = df_routes_gate["Total_HourCostDrive"].astype(float)
    df_routes_gate["Total_HourCostDrive"] = df_routes_gate["Total_HourCostDrive"] * 60
    df_routes_gate["Total_HourCostDrive"] = df_routes_gate["Total_HourCostDrive"].astype(int)

    ## Harmonize data by using common classification/groups between Census and HTS
    df_census['PersonID'] = df_census['PersonID'].astype(str)
    df_census['Age'] = df_census['Age'].astype(str)

    # Age groups (algorithm groups follow CzechiaHTS)
    alg_groups = df_age[0][:-1]
    input_groups = pd.Series([0] + [int(age.split('-')[1]) + 1 for age in df_age[1][:-2]] + [1000])
    df_census['AgeGroup'] = commonFunctions.mappingValCategories(alg_groups, input_groups, df_census['Age'])
    df_census['AgeGroup'] = df_census['AgeGroup'].astype(str)

    # Education groups (algorithm groups follow CityHTS)
    df_census['Education'] = commonFunctions.mappingStdCategories(df_edu[0], df_edu[6], df_census['Education'])

    # Apartment type groups (algorithm groups is a mix between Census and buildings register) # not at the moment
    # df_census['ApartmentType'] = commonFunctions.mappingStdCategories(df_ap_type[0], df_ap_type[4],
    #                                                                   df_census['ApartmentType'])

    # Activity groups (algorithm groups for CityHTS matching is a mix between CityHTS and Census)
    df_census['ActivityCityHTS'] = commonFunctions.mappingStdCategories(df_activity_CityHTS[0], df_activity_CityHTS[4],
                                                                        df_census['Activity'])

    # Rename standard activity group for CzechiaHTS
    df_census = df_census.rename(columns={'Activity': 'ActivityCzechiaHTS'}).copy()

    # Primary location in relation to home groups (algorithm groups is a mix between CzechiaHTS, CityHTS and Census)
    df_census['PrimaryLocRelationHome'] = commonFunctions.mappingStdCategories(df_primary_loc_home[0], df_primary_loc_home[6],
                                                               df_census['PrimaryLocRelationHome'])

    # Journey main mode groups (algorithm groups according to CzechiaHTS)
    df_census['JourneyMainMode'] = commonFunctions.mappingStdCategories(df_journey_mode[0], df_journey_mode[6],
                                                                        df_census['JourneyMainMode'])

    # Journey declared time groups (algorithm groups according to Census)
    df_census['DeclaredJourneyTime'] = commonFunctions.mappingStdCategories(df_journey_t[0], df_journey_t[6],
                                                                            df_census['DeclaredJourneyTime'])

    # Trip purpose groups for CzechiaHTS and CityHTS (algorithm groups follows a mix between CzechiaHTS and CityHTS)
    df_census['ActivitySector'] = commonFunctions.mappingStdCategories(df_sector_activity[0], df_sector_activity[4],
                                                                       df_census['ActivitySector'])

    # Education place based on age groups
    alg_groups = df_age_place[0][:-1]
    input_groups = pd.Series([0] + [int(age.split('-')[1]) + 1 for age in df_age_place[1][:-2]] + [1000])
    df_census['EducationPlace'] = commonFunctions.mappingValCategories(alg_groups, input_groups, df_census['Age'])
    df_census['EducationPlace'] = df_census['EducationPlace'].astype(str)

    # Clean up
    df_census = df_census[[
        'PersonID',
        # 'HouseholdID',
        # 'ApartmentID',
        # 'BuildingID',
        'ActivityCzechiaHTS',
        'ActivityCityHTS',
        'ActivitySector',
        'EducationPlace',
        'Age',
        'AgeGroup',
        'BasicSettlementCode',
        'CadastralAreaCode',
        'DistrictCode',
        'DeclaredJourneyTime',
        'Education',
        'Gender',
        'JourneyMainMode',
        'PrimaryLocTownCode',
        'PrimaryLocDistrictCode',
        'PrimaryLocRegionCode',
        'PrimaryLocStateCode',
        'PrimaryLocRelationHome',
        'TownCode',
        'TownSize',
        'RegionCode',
        # 'ApartmentType', # not at the moment
        # 'BuildingType', # not at the moment
    ]]


    return df_census
