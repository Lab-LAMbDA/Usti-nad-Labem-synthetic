import numpy as np
import pandas as pd
import os
from tqdm import tqdm
import warnings

def configure(context):
    context.config("data_path")
    context.config("census_file")
    context.config("territory_codes_file")

def validate(context):
    data_path = context.config("data_path")
    census_file = "%s/Census/%s" % (data_path, context.config("census_file"))
    routes_file = "%s/%s" % (context.config("data_path"), context.config("routes_file"))
    territory_codes_file = "%s/%s" % (context.config("data_path"), context.config("territory_codes_file"))

    if not os.path.isdir(data_path):
        raise RuntimeError("Input directory must exist: %s" % data_path)

    if not os.path.exists(census_file):
        raise RuntimeError("Input file must exist: %s" % census_file)

    if not os.path.exists(routes_file):
        raise RuntimeError("Input file must exist: %s" % routes_file)

    if not os.path.exists(territory_codes_file):
        raise RuntimeError("Input file must exist: %s" % territory_codes_file)

def execute(context):

    # Ignore header warning when reading excel files
    warnings.filterwarnings('ignore', category=UserWarning, module='openpyxl')

    original_columns = [  # If data already group persons per householdID, then only those uncommented
        # 'DVO_KEY',  # PersonID
        # 'HD_KEY',  # HouseholdID
        # 'DVB_KEY',  # ApartmentID
        # 'DVD_KEY',  # BuildingID
        'LIDEKAKTI',  # Activity
        'INFSEKAKTI',  # ActivitySector
        'LIDVEK',  # Age
        'ZSJ_DIL_MAX',  # BasicSettlementCode
        'LIDDOBADO',  # DeclaredJourneyTime
        'LIDSTVZDE',  # Education
        'LIDPOHLAV',  # Gender
        'LIDZPUSDO',  # JourneyMainMode
        'LIDAMPSOK',  # PrimaryLocDistrictCode
        'LIDKRAJPR',  # PrimaryLocRegionCode
        'LIDMPRAC',  # PrimaryLocRelationHome
        'LIDAMPSST',  # PrimaryLocStateCode
        'LIDAMPSOB',  # PrimaryLocTownCode
        'OBEC',  # TownCode
        'UZMVELSOBO'  # TownSize
        # 'BYTTYPBYT',  # ApartmentType - not at moment
        # 'DUMDRUHDO',  # BuildingType - not at moment
    ]

    # Chunk size of the partition of the full data
    CHUNK_SIZE = 500000

    # Define the code of the cities within Ustí nad Labem district (the study area)
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

    print("Reading Census and supporting files")

    # Read the file in chunks (two ways based on file format)
    # reader = pd.read_json("%s/Census/%s" % (context.config("data_path"), context.config("census_file")),
    #                       lines=True, chunksize=CHUNK_SIZE, encoding="cp1250")
    reader = pd.read_csv("%s/Census/%s" % (context.config("data_path"), context.config("census_file")),
                          delimiter=',', chunksize=CHUNK_SIZE, encoding="cp1250", dtype=str)

    # Get territorial codes
    df_codes = pd.read_excel("%s/%s" % (context.config("data_path"), context.config("territory_codes_file")),
                             header=0,
                             # encoding="cp1250",
                             dtype='str')

    # Get towns within 89 minutes of Ústí nad Labem (not at the moment)
    # df_routes_time_dist = pd.read_excel("%s/%s" % (context.config("data_path"), context.config("routes_file")),
    #                                     header=0,
    #                                     # encoding="cp1250",
    #                                     dtype='str')
    # df_routes_time_dist['Total_HourCostDrive'] = df_routes_time_dist['Total_HourCostDrive'].astype('float')
    # near_towns = df_routes_time_dist.loc[df_routes_time_dist['Total_HourCostDrive'] <= 1.483,
    #                                      'KOD_LAU2'].astype('str').tolist()

    # Renaming columns
    new_columns = [
        # 'PersonID'
        # 'HouseholdID',
        # 'ApartmentID',
        # 'BuildingID',
        'Activity',
        'ActivitySector',
        'Age',
        'BasicSettlementCode',
        'DeclaredJourneyTime',
        'Education',
        'Gender',
        'JourneyMainMode',
        'PrimaryLocDistrictCode',
        'PrimaryLocRegionCode',
        'PrimaryLocRelationHome',
        'PrimaryLocStateCode',
        'PrimaryLocTownCode',
        'TownCode',
        'TownSize'
        # 'ApartmentType',
        # 'BuildingType'
    ]

    # Get column names and create output dataframe
    df_census = pd.DataFrame(columns=new_columns)

    # Fill in the output dataframe with relevant observations from chunks
    i = CHUNK_SIZE
    for df in reader:
        # Keep only the population that:
        # a) lives within the Ustí nad Labem district (any town of the district), or

        ### Code below only if considering peope living in the near areas out of the district
        # b) go to there as primary location, or
        # c) did not respond where it is his/her primary location district but answered that:
        #   i) DeclaredJourneyTime as within 89 minutes inside Czechia, and its town is within 89 minutes from Ústí
        #   ii) PrimaryLocRelationHome is unknown or it is at least out of the home district
        ### Code above only if considering peope living in the near areas out of the district

        df = df[original_columns]

        # Get people that:
        df1 = df[(df["OBEC"].isin(cities_usti_district) #  live in a town within Ustí district; or
                 # | (df["LIDAMPSOK"] == '4214') # have primary location within Ustí district; or
                 # | ((df["LIDAMPSOK"] == '99999') # unknown place of primary location, but
                 #    & ((df["LIDDOBADO"].isin('1', '2', '3', '4', '5'))  # take up to 89 minutes commute; and
                 #       & (df["OBEC"].isin(near_towns))) # live within 89 minutes of Ústí nad Labem; and
                 #    & (df["LIDMPRAC"].isin('7', '8', '31', '41', '51', '99')) # it is at least out of the home district
                    )
        ]
        df1.columns = new_columns
        df_census = pd.concat([df_census, df1], sort=True)
        print("Processed " + repr(i) + " samples.")
        i = i + CHUNK_SIZE

    # In case there is no PersonID, add a column for it
    if "PersonID" not in df_census.columns:
        df_census["PersonID"] = list(range(1, len(df_census) + 1))

    # Add a new column for weight of the person in the sample
    if "Weight" not in df_census.columns:
        df_census['Weight'] = np.repeat(1, len(df_census["PersonID"]))

    # Add a new column for RegionCode, DistrictCode and CadastralAreaCode (this last only for Ustí city) for each person
    CadastralCodes = []
    DistrictCodes = []
    RegionCodes = []
    for person_ind, person_data in tqdm(df_census.iterrows(), total=len(df_census), desc="Adding territorial codes"):
        row_data = df_codes.loc[df_codes['KOD_ZSJ'] == person_data["BasicSettlementCode"]]
        assert len(row_data) == 1
        CadastralCodes.append(row_data.iloc[0]['KOD_KU'])
        DistrictCodes.append(row_data.iloc[0]['KOD_ORP_CSU'])
        RegionCodes.append(row_data.iloc[0]['KOD_KRAJ'])

    df_census["CadastralAreaCode"] = CadastralCodes
    df_census["DistrictCode"] = DistrictCodes
    df_census["RegionCode"] = RegionCodes

    return df_census
