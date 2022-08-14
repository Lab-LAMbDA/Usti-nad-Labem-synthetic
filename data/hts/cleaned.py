from tqdm import tqdm
import pandas as pd
import numpy as np
import xml.etree.ElementTree as ET
import geopy.distance
import os
from data import commonFunctions
import warnings

def configure(context):
    context.config("data_path")
    context.config("output_path")
    context.config("hts_CzechiaHTS_persons_file")
    context.config("hts_CzechiaHTS_households_file")
    context.config("hts_CzechiaHTS_trips_file")
    context.config("hts_CityHTS_persons_file")
    context.config("hts_CityHTS_households_file")
    context.config("hts_CityHTS_trips_file")
    context.config("territory_codes_file")
    context.config("generalizations_file")
    context.config("CzechiaHTS_persons_descr_file")
    context.config("CzechiaHTS_trips_descr_file")
    context.config("CzechiaHTS_households_descr_file")

def validate(context):
    data_path = context.config("data_path")
    output_path = context.config("output_path")
    hts_CzechiaHTS_persons_file = "%s/HTS/%s" %  (context.config("data_path"),
                                                  context.config("hts_CzechiaHTS_persons_file"))
    hts_CzechiaHTS_households_file = "%s/HTS/%s" %  (context.config("data_path"),
                                                     context.config("hts_CzechiaHTS_households_file"))
    hts_CzechiaHTS_trips_file = "%s/HTS/%s" %  (context.config("data_path"),
                                                context.config("hts_CzechiaHTS_trips_file"))
    hts_CityHTS_persons_file = "%s/HTS/%s" %  (context.config("data_path"),
                                               context.config("hts_CityHTS_persons_file"))
    hts_CityHTS_households_file = "%s/HTS/%s" %  (context.config("data_path"),
                                                  context.config("hts_CityHTS_households_file"))
    hts_CityHTS_trips_file = "%s/HTS/%s" %  (context.config("data_path"),
                                             context.config("hts_CityHTS_trips_file"))
    territory_codes_file = "%s/%s" % (context.config("data_path"), context.config("territory_codes_file"))
    generalizations_file = "%s/%s" % (context.config("data_path"), context.config("generalizations_file"))
    CzechiaHTS_persons_descr_file = "%s/HTS/%s" %  (context.config("data_path"),
                                                    context.config("CzechiaHTS_persons_descr_file"))
    CzechiaHTS_trips_descr_file = "%s/HTS/%s" %  (context.config("data_path"),
                                                  context.config("CzechiaHTS_trips_descr_file"))
    CzechiaHTS_households_descr_file = "%s/HTS/%s" %  (context.config("data_path"),
                                                       context.config("CzechiaHTS_households_descr_file"))

    if not os.path.isdir(data_path):
        raise RuntimeError("Input directory must exist: %s" % data_path)

    if not os.path.isdir(output_path):
        raise RuntimeError("Output directory must exist: %s" % output_path)

    if not os.path.exists(hts_CzechiaHTS_persons_file):
        raise RuntimeError("Input file must exist: %s" % hts_CzechiaHTS_persons_file)

    if not os.path.exists(hts_CzechiaHTS_households_file):
        raise RuntimeError("Input file must exist: %s" % hts_CzechiaHTS_households_file)

    if not os.path.exists(hts_CzechiaHTS_trips_file):
        raise RuntimeError("Input file must exist: %s" % hts_CzechiaHTS_trips_file)

    if not os.path.exists(hts_CityHTS_persons_file):
        raise RuntimeError("Input file must exist: %s" % hts_CityHTS_persons_file)

    if not os.path.exists(hts_CityHTS_households_file):
        raise RuntimeError("Input file must exist: %s" % hts_CityHTS_households_file)

    if not os.path.exists(hts_CityHTS_trips_file):
        raise RuntimeError("Input file must exist: %s" % hts_CityHTS_trips_file)

    if not os.path.exists(territory_codes_file):
        raise RuntimeError("Input file must exist: %s" % territory_codes_file)

    if not os.path.exists(generalizations_file):
        raise RuntimeError("Input file must exist: %s" % generalizations_file)

    if not os.path.exists(CzechiaHTS_persons_descr_file):
        raise RuntimeError("Input file must exist: %s" % CzechiaHTS_persons_descr_file)

    if not os.path.exists(CzechiaHTS_trips_descr_file):
        raise RuntimeError("Input file must exist: %s" % CzechiaHTS_trips_descr_file)

    if not os.path.exists(CzechiaHTS_households_descr_file):
        raise RuntimeError("Input file must exist: %s" % CzechiaHTS_households_descr_file)

def execute(context):

    # Ignore header warning when reading excel files
    warnings.filterwarnings('ignore', category=UserWarning, module='openpyxl')

    print("Reading Household Travel Survey (HTS) and supporting files")

    df_CzechiaHTS_p = pd.read_csv("%s/HTS/%s" %  (context.config("data_path"), context.config("hts_CzechiaHTS_persons_file")),
                           encoding="utf8", dtype=str)
    df_CzechiaHTS_h = pd.read_csv("%s/HTS/%s" %  (context.config("data_path"), context.config("hts_CzechiaHTS_households_file")),
                           encoding="cp1250", dtype=str)
    df_CzechiaHTS_t = pd.read_csv("%s/HTS/%s" %  (context.config("data_path"), context.config("hts_CzechiaHTS_trips_file")),
                           encoding="cp1250", dtype=str, delimiter=';')
    df_CityHTS_p = pd.read_csv("%s/HTS/%s" %  (context.config("data_path"), context.config("hts_CityHTS_persons_file")),
                             encoding="utf8", dtype=str)
    df_CityHTS_h = pd.read_csv("%s/HTS/%s" %  (context.config("data_path"), context.config("hts_CityHTS_households_file")),
                             encoding="utf8", dtype=str)
    df_CityHTS_t = pd.read_csv("%s/HTS/%s" %  (context.config("data_path"), context.config("hts_CityHTS_trips_file")),
                             encoding="utf8", dtype=str, delimiter=',')
    df_codes = pd.read_excel("%s/%s" % (context.config("data_path"), context.config("territory_codes_file")),
                             header=0,
                             # encoding="cp1250",
                             dtype=str)
    df_age = pd.read_excel("%s/%s" % (context.config("data_path"), context.config("generalizations_file")),
                           header=None, skiprows=1, sheet_name='Person Age', dtype=str)
    df_car_avail = pd.read_excel("%s/%s" % (context.config("data_path"), context.config("generalizations_file")),
                                 header=None, skiprows=1, sheet_name='Person AvailCar', dtype=str)
    df_bike_avail = pd.read_excel("%s/%s" % (context.config("data_path"), context.config("generalizations_file")),
                                  header=None, skiprows=1, sheet_name='Person AvailBike', dtype=str)
    df_pt_subs = pd.read_excel("%s/%s" % (context.config("data_path"), context.config("generalizations_file")),
                               header=None, skiprows=1, sheet_name='Person PtSubscription', dtype=str)
    df_driving_id = pd.read_excel("%s/%s" % (context.config("data_path"), context.config("generalizations_file")),
                               header=None, skiprows=1, sheet_name='Person DrivingLicense', dtype=str)
    df_edu = pd.read_excel("%s/%s" % (context.config("data_path"), context.config("generalizations_file")),
                                       header=None, skiprows=1, sheet_name='Person Education', dtype=str)
    df_activity_CzechiaHTS = pd.read_excel("%s/%s" % (context.config("data_path"), context.config("generalizations_file")),
                                       header=None, skiprows=1, sheet_name='Person Activity (CzechiaHTS)', dtype=str)
    df_activity_CityHTS = pd.read_excel("%s/%s" % (context.config("data_path"), context.config("generalizations_file")),
                                       header=None, skiprows=1, sheet_name='Person Activity (CityHTS)', dtype=str)
    df_trip_mode_times = pd.read_excel("%s/%s" % (context.config("data_path"), context.config("generalizations_file")),
                                       header=None, skiprows=1, sheet_name='Trip ModeTimes (Numerical)', dtype=str)
    df_journey_t = pd.read_excel("%s/%s" % (context.config("data_path"), context.config("generalizations_file")),
                                       header=None, skiprows=1, sheet_name='Person DeclaredTripTime', dtype=str)
    df_trip_purpose = pd.read_excel("%s/%s" % (context.config("data_path"), context.config("generalizations_file")),
                                    header=None, skiprows=1, sheet_name='Trip Purpose', dtype=str)

    descr_CzechiaHTS_persons = ET.parse("%s/HTS/%s" % (context.config("data_path"),
                                            context.config("CzechiaHTS_persons_descr_file"))).getroot()
    descr_CzechiaHTS_households = ET.parse("%s/HTS/%s" % (context.config("data_path"),
                                               context.config("CzechiaHTS_households_descr_file"))).getroot()
    descr_CzechiaHTS_trips = ET.parse("%s/HTS/%s" % (context.config("data_path"),
                                          context.config("CzechiaHTS_trips_descr_file"))).getroot()

    # Select only the necessary data
    df_CzechiaHTS_p_reduced = df_CzechiaHTS_p[['P_ID',
                                               'H_ID',
                                               'P_gender',
                                               'P_age',
                                               'P_education',
                                               'P_work',
                                               # 'P_work_time_0',
                                               # 'P_work_time_1',
                                               'P_work_time_2',
                                               # 'P_w_hours',
                                               'P_driving_licence_B',
                                               # 'P_driving_licence_C',
                                               # 'P_driving_licence_A',
                                               # 'P_driving_licence_none',
                                               'P_pt_0',
                                               # 'P_pt_1',
                                               # 'P_pt_2',
                                               # 'P_pt_3',
                                               # 'P_pt_4',
                                               # 'P_carsha_available',
                                               'P_caravail',
                                               'P_bikeavail',
                                               "WT_balanceSLDBvsCzechiaHTS",
                                               ]]
    df_CzechiaHTS_h_reduced = df_CzechiaHTS_h[['H_ID',
                                               # 'H_address_district_code',
                                               # 'H_address_district',
                                               'H_address_city_code',
                                               # 'H_address_city',
                                               'H_orp_code',
                                               # 'H_orp_name',
                                               'H_region',
                                               'H_munpop',
                                               # 'H_persons',   # not at the moment
                                               # 'H_persons_0005',  # not at the moment
                                               # 'H_persons_0617',  # not at the moment
                                               # 'H_persons_1899',  # not at the moment
                                               # 'H_venr_car_private',
                                               # 'H_venr_car_company',
                                               # 'H_venr_util',
                                               # 'H_venr_other',
                                               # 'H_venr_bike'
                                               ]]
    df_CzechiaHTS_t_reduced = df_CzechiaHTS_t[['T_ID',
                                               'P_ID',
                                               # 'H_ID',
                                               'T_ord',
                                               'T_modes_foot',
                                               'T_modes_bike',
                                               'T_modes_urbu',
                                               'T_modes_rebu',
                                               'T_modes_cobu',
                                               'T_modes_trol',
                                               'T_modes_tram',
                                               'T_modes_train',
                                               'T_modes_cadr',
                                               'T_modes_capa',
                                               'T_modes_metro',
                                               'T_modes_plane',
                                               'T_modes_other',
                                               'T_next',
                                               # 'T_O_orp_name',
                                               'T_O_orp_code',
                                               # 'T_O_address_city',
                                               'T_O_address_city_code',
                                               # 'T_O_address_district',
                                               # 'T_O_address_district_code',
                                               'T_O_time_hh',
                                               'T_O_time_min',
                                               # 'T_D_orp_name',
                                               'T_D_orp_code',
                                               # 'T_D_address_city',
                                               'T_D_address_city_code',
                                               # 'T_D_address_district',
                                               # 'T_D_address_district_code',
                                               'T_D_time_hh',
                                               'T_D_time_min',
                                               'T_D_purpose',
                                               'T_O_purpose',
                                               'T_last_trip',
                                               'T_dist_de',
                                               'T_dist_me',
                                               'T_dist_crow',
                                               'T_time_de',
                                               'T_time_me',
                                               'T_mainmode'
                                               ]]
    df_CityHTS_p_reduced = df_CityHTS_p[['os_ID',
                                         'dom_ID',
                                         'os_vek',
                                         'os_pohlavi',
                                         'os_vzdelani',
                                         'os_status',
                                         'os_pevna_prac_doba',
                                         'os_ridic',
                                         'os_kupon_mhd',
                                         # 'os_cip_karta',
                                         'os_auto_soukr',
                                         # 'os_auto_sluz',
                                         'os_vyuz_kolo',
                                         # 'os_park_auto',
                                         # 'os_park_kolo',
                                         "WT_BALANCE_CityHTSvsSLDB",
                                         ]]
    df_CityHTS_h_reduced = df_CityHTS_h[['dom_ID',
                                         # 'dom_lokalita',
                                         'dom_katastr',
                                         # 'dom_pocet_osob', # not at the moment
                                         # 'dom_pocet_deti', # not at the moment
                                         # 'dom_pocet_auto',
                                         # 'dom_pocet_moto',
                                         # 'dom_pocet_kol',
                                         # 'dom_vzdal_mhd',
                                         # 'dom_vzdal_vlak',
                                         ]]
    df_CityHTS_t_reduced = df_CityHTS_t[['ces_ID',
                                         'os_ID',
                                         'ces_z_misto',
                                         'ces_z_gps_sir',
                                         'ces_z_gps_del',
                                         'ces_z_stat',
                                         'ces_z_obec',
                                         'ces_z_ku',
                                         'ces_z_zsj',
                                         'ces_z_cas',
                                         'ces_do_misto',
                                         'ces_do_gps_sir',
                                         'ces_do_gps_del',
                                         'ces_do_stat',
                                         'ces_do_obec',
                                         'ces_do_ku',
                                         'ces_do_zsj',
                                         'ces_do_cas',
                                         'ces_doba',
                                         'ces_doba_ridic',
                                         'ces_doba_spol',
                                         'ces_doba_moto',
                                         'ces_doba_autob_mhd',
                                         'ces_doba_trolej',
                                         'ces_doba_vlak',
                                         'ces_doba_autob_prim',
                                         'ces_doba_kolo',
                                         'ces_doba_pesky',
                                         'ces_doba_taxi',
                                         'ces_doba_jine'
                                         ]]

    # Change column RegionName to RegionCode for each person in CzechiaHTS
    RegionCodes = []
    df_codes_KRAJ = df_codes[['KOD_KRAJ', 'NAZEV_KRAJ']].drop_duplicates()

    for _, person_data in tqdm(df_CzechiaHTS_h_reduced.iterrows(), total=len(df_CzechiaHTS_h_reduced),
                               desc="Changing column RegionName to RegionCode for each person in CzechiaHTS"):
        row_data = df_codes_KRAJ.loc[df_codes_KRAJ['NAZEV_KRAJ'] == person_data["H_region"]]
        assert len(row_data) == 1
        RegionCodes.append(row_data.iloc[0]['KOD_KRAJ'])
    df_CzechiaHTS_h_reduced = df_CzechiaHTS_h_reduced.rename(columns={'H_region': 'RegionCode'}).copy()
    df_CzechiaHTS_h_reduced["RegionCode"] = RegionCodes

    # For CzechiaHTS, some attributes are in text, change to numbers
    dfs = [df_CzechiaHTS_p_reduced, df_CzechiaHTS_h_reduced, df_CzechiaHTS_t_reduced]
    descrs = [descr_CzechiaHTS_persons, descr_CzechiaHTS_households, descr_CzechiaHTS_trips]
    for ind, df in tqdm(enumerate(dfs), total=len(dfs),
                               desc="For CzechiaHTS, some attributes are in text, changing to numbers"):
        df = df.copy()
        cols = df.columns
        for variable_children in descrs[ind].findall("variable"):
            column_name = variable_children.get('code')
            if column_name in cols and variable_children.get('type') == 'factor' and column_name in df.columns:
                codes = []
                for variable_child in variable_children.findall('factor'):
                    code = variable_child.get('code')
                    codes.append(code)
                    df[column_name].replace(variable_child.get('name'), code, inplace=True)
                assert len(np.unique(df[column_name].dropna())) == len(codes)
        dfs[ind] = df
    df_CzechiaHTS_p_reduced, df_CzechiaHTS_h_reduced, df_CzechiaHTS_t_reduced = dfs[:]

    # Rename columns
    df_CzechiaHTS_p_reduced.columns = ['PersonID',
                                       'HouseholdID',
                                       'Gender',
                                       'AgeGroup',
                                       'Education',
                                       'Activity',
                                       # 'Homeoffice', # potential useful info
                                       # 'FlexibleBegEndTime', # potential useful info
                                       'FlexibleHours',
                                       # 'WorkDuration', # potential useful info
                                       'DrivingLicense',
                                       'PtSubscription',
                                       # 'PtAnyDiscountTicket', # potential useful info
                                       # 'AvailCarSharing', # potential useful info
                                       'AvailCar',
                                       'AvailBike',
                                       "Weight"
                                       ]
    df_CzechiaHTS_h_reduced.columns = ['HouseholdID',
                                       # 'CityPartCode',
                                       # 'CityPartName',
                                       'TownCode',
                                       # 'TownName',
                                       'DistrictCode',
                                       # 'DistrictName',
                                       'RegionCode',
                                       'TownSize',
                                       # 'NumPersons', # not at the moment
                                       # 'NumPersonsAge00_05', # not at the moment
                                       # 'NumPersonsAge06_17', # not at the moment
                                       # 'NumPersonsAge18_99', # not at the moment
                                       # 'NumPrivateCar', # potential useful info
                                       # 'NumCompanyCar', # potential useful info
                                       # 'NumCommercialVeh', # potential useful info
                                       # 'NumMoto', # potential useful info
                                       # 'NumBikes' # potential useful info
                                       ]
    df_CzechiaHTS_t_reduced.columns = ['TripID',
                                       'PersonID',
                                       # 'HouseholdID', # not at the moment
                                       'TripOrderNum',
                                       'TimeFoot',
                                       'TimeBike',
                                       'TimeTownBus',
                                       'TimeRegionalBus',
                                       'TimeLongDistBus',
                                       'TimeTrolleyBus',
                                       'TimeTram',
                                       'TimeTrain',
                                       'TimeDriverCar',
                                       'TimePassengerCar',
                                       'TimeMetro',
                                       'TimePlane',
                                       'TimeOther',
                                       'ContinuingJourney',
                                       # 'OriginDistrictName',
                                       'OriginDistrictCode',
                                       # 'OriginTownName',
                                       'OriginTownCode',
                                       # 'OriginCityPartName',
                                       # 'OriginCityPartCode',
                                       'OriginStartHour',
                                       'OriginStartMin',
                                       # 'DestDistrictName',
                                       'DestDistrictCode',
                                       # 'DestTownName',
                                       'DestTownCode',
                                       # 'DestCityPartName',
                                       # 'DestCityPartCode',
                                       'DestEndHour',
                                       'DestEndMin',
                                       'DestPurpose',
                                       'OriginPurpose',
                                       'JourneyContinues',
                                       'DeclaredTripDist',
                                       'CalculatedTripDist',
                                       'CrowFliesTripDist',
                                       'DeclaredTripTime',
                                       'CalculatedTripTime',
                                       'TripMainMode'
                                       ]
    df_CityHTS_p_reduced.columns = ['PersonID',
                                    'HouseholdID',
                                    'Age',
                                    'Gender',
                                    'Education',
                                    'Activity',
                                    'FlexibleHours',
                                    'DrivingLicense',
                                    'PtSubscription',
                                    # 'PtAnyDiscountTicket', # potential useful info
                                    'AvailCar',  # 'AvailPrivateCar', # potential useful info
                                    # 'AvailCompanyCar', # potential useful info
                                    'AvailBike',
                                    # 'AvailCarParking', # potential useful info
                                    # 'AvailBikeParking', # potential useful info
                                    "Weight"
                                    ]
    df_CityHTS_h_reduced.columns = ['HouseholdID',
                                    # 'CitySiteID',
                                    'CadastralAreaCode',
                                    # 'NumPersons', # not at the moment
                                    # 'NumPersonsAge06_18', # not at the moment
                                    # 'NumCars', # potential useful info
                                    # 'NumMoto', # potential useful info
                                    # 'NumBikes', # potential useful info
                                    # 'WalkTimeToPT', # potential useful info
                                    # 'WalkTimeToTrain', # potential useful info
                                    ]
    df_CityHTS_t_reduced.columns = ['TripID',
                                    'PersonID',
                                    'OriginPurpose',
                                    'LatOrigin',
                                    'LonOrigin',
                                    'OriginState',
                                    'OriginTownCode',
                                    'OriginCadastralAreaCode',
                                    'OriginBasicSettlementCode',
                                    'OriginStartHourMin',
                                    'DestPurpose',
                                    'LatDest',
                                    'LonDest',
                                    'DestState',
                                    'DestTownCode',
                                    'DestCadastralAreaCode',
                                    'DestBasicSettlementCode',
                                    'DestEndHourMin',
                                    'DeclaredTripTime',
                                    'TimeDriverCar',
                                    'TimePassengerCar',
                                    'TimeMoto',
                                    'TimeTownBus',
                                    'TimeTrolleyBus',
                                    'TimeTrain',
                                    'TimeRegionalBus',
                                    'TimeBike',
                                    'TimeFoot',
                                    'TimeTaxi',
                                    'TimeOther'
                                    ]

    print("Merging and cleaning Household Travel Survey (HTS) data")
    df_CzechiaHTS_t_reduced = df_CzechiaHTS_t_reduced.copy()
    df_CityHTS_t_reduced = df_CityHTS_t_reduced.copy()

    # Merge persons database with household database
    df_CzechiaHTS_ph_reduced = pd.merge(df_CzechiaHTS_p_reduced, df_CzechiaHTS_h_reduced, on='HouseholdID').copy()
    df_CityHTS_ph_reduced = pd.merge(df_CityHTS_p_reduced, df_CityHTS_h_reduced, on='HouseholdID').copy()

    # Fill data mistakes
    df_CzechiaHTS_ph_reduced['FlexibleHours'] = df_CzechiaHTS_ph_reduced['FlexibleHours'].fillna("99")
    df_CzechiaHTS_ph_reduced['AvailCar'] = df_CzechiaHTS_ph_reduced['AvailCar'].fillna("1")
    df_CzechiaHTS_ph_reduced['AvailBike'] = df_CzechiaHTS_ph_reduced['AvailBike'].fillna("1")
    df_CzechiaHTS_ph_reduced['PtSubscription'] = df_CzechiaHTS_ph_reduced['PtSubscription'].fillna("1")
    df_CzechiaHTS_ph_reduced['DrivingLicense'] = df_CzechiaHTS_ph_reduced['DrivingLicense'].fillna("1")
    df_CzechiaHTS_t_reduced['ContinuingJourney'] = df_CzechiaHTS_t_reduced['ContinuingJourney'].fillna("0")

    df_CityHTS_ph_reduced['FlexibleHours'] = df_CityHTS_ph_reduced['FlexibleHours'].fillna("99")
    df_CityHTS_ph_reduced['AvailCar'] = df_CityHTS_ph_reduced['AvailCar'].fillna("2")
    df_CityHTS_ph_reduced['AvailBike'] = df_CityHTS_ph_reduced['AvailBike'].fillna("2")
    df_CityHTS_ph_reduced['PtSubscription'] = df_CityHTS_ph_reduced['PtSubscription'].fillna("2")
    df_CityHTS_ph_reduced['DrivingLicense'] = df_CityHTS_ph_reduced['DrivingLicense'].fillna("2")
    df_CityHTS_t_reduced['OriginBasicSettlementCode'] = df_CityHTS_t_reduced['OriginBasicSettlementCode'].fillna("0") # if none, it is trip to abroad
    df_CityHTS_t_reduced['DestBasicSettlementCode'] = df_CityHTS_t_reduced['DestBasicSettlementCode'].fillna("0") # if none, it is trip to abroad
    df_CityHTS_t_reduced['OriginCadastralAreaCode'] = df_CityHTS_t_reduced['OriginCadastralAreaCode'].fillna("0") # if none, it is trip to abroad
    df_CityHTS_t_reduced['DestCadastralAreaCode'] = df_CityHTS_t_reduced['DestCadastralAreaCode'].fillna("0") # if none, it is trip to abroad
    df_CityHTS_t_reduced['OriginTownCode'] = df_CityHTS_t_reduced['OriginTownCode'].fillna("0") # if none, it is trip to abroad
    df_CityHTS_t_reduced['DestTownCode'] = df_CityHTS_t_reduced['DestTownCode'].fillna("0") # if none, it is trip to abroad
    df_CityHTS_t_reduced['TimeDriverCar'] = df_CityHTS_t_reduced['TimeDriverCar'].fillna(0)
    df_CityHTS_t_reduced['TimePassengerCar'] = df_CityHTS_t_reduced['TimePassengerCar'].fillna(0)
    df_CityHTS_t_reduced['TimeMoto'] = df_CityHTS_t_reduced['TimeMoto'].fillna(0)
    df_CityHTS_t_reduced['TimeTownBus'] = df_CityHTS_t_reduced['TimeTownBus'].fillna(0)
    df_CityHTS_t_reduced['TimeTrolleyBus'] = df_CityHTS_t_reduced['TimeTrolleyBus'].fillna(0)
    df_CityHTS_t_reduced['TimeTrain'] = df_CityHTS_t_reduced['TimeTrain'].fillna(0)
    df_CityHTS_t_reduced['TimeTrolleyBus'] = df_CityHTS_t_reduced['TimeTrolleyBus'].fillna(0)
    df_CityHTS_t_reduced['TimeRegionalBus'] = df_CityHTS_t_reduced['TimeRegionalBus'].fillna(0)
    df_CityHTS_t_reduced['TimeBike'] = df_CityHTS_t_reduced['TimeBike'].fillna(0)
    df_CityHTS_t_reduced['TimeFoot'] = df_CityHTS_t_reduced['TimeFoot'].fillna(0)
    df_CityHTS_t_reduced['TimeTaxi'] = df_CityHTS_t_reduced['TimeTaxi'].fillna(0)
    df_CityHTS_t_reduced['TimeOther'] = df_CityHTS_t_reduced['TimeOther'].fillna(0)

    # Clean entries with missing key values
    CzechiaHTS_ph_Toclean = [
                      'PersonID', 'HouseholdID', 'Gender', 'AgeGroup', 'Education', 'Activity',
                      'TownSize',
                      # 'TownCode', # CzechiaHTS lacks data per municipality, use district only
                      'DistrictCode',
                      'RegionCode',
                      # 'NumPersonsAge00_05', 'NumPersonsAge06_17', 'NumPersonsAge18_99' # not at the moment
                      ]

    CzechiaHTS_t_Toclean = ['TripID', 'PersonID', 'TripOrderNum', 'ContinuingJourney',
                     'TimeFoot', 'TimeBike', 'TimeTownBus', 'TimeRegionalBus', 'TimeLongDistBus', 'TimeTrolleyBus',
                     'TimeTram', 'TimeTrain', 'TimeDriverCar', 'TimePassengerCar', 'TimeMetro', 'TimePlane',
                     'TimeOther', 'OriginStartHour', 'OriginStartMin', 'DestEndHour', 'DestEndMin',
                     'OriginDistrictCode', 'DestDistrictCode',
                     # 'OriginTownCode', 'DestTownCode', # CzechiaHTS lacks data per municipality, use district only
                     'DestPurpose', 'OriginPurpose', 'JourneyContinues',
                     # 'CrowFliesTripDist',
                     'DeclaredTripTime',
                     'TripMainMode']

    CityHTS_ph_Toclean = ['PersonID', 'HouseholdID', 'Age', 'Gender', 'Education', 'Activity',
                        'CadastralAreaCode',  # adding this makes that all non-residents of Ústí town to be discarded
                        # 'NumPersons', 'NumPersonsAge06_18' # not at the moment
                        ]

    CityHTS_t_Toclean = ['TripID', 'PersonID', 'OriginPurpose',
                       'OriginBasicSettlementCode', 'DestBasicSettlementCode',
                       'OriginCadastralAreaCode', 'DestCadastralAreaCode',
                       'OriginTownCode', 'DestTownCode',
                       'OriginState', 'DestState',
                       'OriginPurpose', 'DestPurpose',
                       'OriginStartHourMin', 'DestEndHourMin', 'DeclaredTripTime',
                       'TimeDriverCar', 'TimePassengerCar', 'TimeMoto', 'TimeTownBus', 'TimeTrolleyBus',
                       'TimeTrain', 'TimeRegionalBus', 'TimeBike', 'TimeFoot', 'TimeTaxi', 'TimeOther']

    df_CzechiaHTS_ph = df_CzechiaHTS_ph_reduced.dropna(subset=CzechiaHTS_ph_Toclean, inplace=False).copy()
    df_CzechiaHTS_t = df_CzechiaHTS_t_reduced.dropna(subset=CzechiaHTS_t_Toclean, inplace=False).copy()
    pids = set(df_CzechiaHTS_ph["PersonID"].values.tolist()).intersection(set(df_CzechiaHTS_t["PersonID"].values.tolist()))
    df_CzechiaHTS_ph = df_CzechiaHTS_ph[df_CzechiaHTS_ph['PersonID'].isin(pids)]
    df_CzechiaHTS_t = df_CzechiaHTS_t[df_CzechiaHTS_t['PersonID'].isin(pids)]

    df_CityHTS_ph = df_CityHTS_ph_reduced.dropna(subset=CityHTS_ph_Toclean, inplace=False).copy()
    df_CityHTS_t = df_CityHTS_t_reduced.dropna(subset=CityHTS_t_Toclean, inplace=False).copy()
    pids = set(df_CityHTS_ph["PersonID"].values.tolist()).intersection(set(df_CityHTS_t["PersonID"].values.tolist()))
    df_CityHTS_ph = df_CityHTS_ph[df_CityHTS_ph['PersonID'].isin(pids)]
    df_CityHTS_t = df_CityHTS_t[df_CityHTS_t['PersonID'].isin(pids)]

    print("Fixing and filtering Household Travel Survey (HTS) data as well as harmonizing it with Census data")

    # Initializing dataframe column types
    df_CityHTS_ph['Age'] = df_CityHTS_ph['Age'].astype(str)
    df_CityHTS_ph['Weight'] = df_CityHTS_ph['Weight'].astype(np.float)
    df_CityHTS_t['DeclaredTripTime'] = df_CityHTS_t['DeclaredTripTime'].astype(np.float)
    df_CityHTS_t['TimeFoot'] = df_CityHTS_t['TimeFoot'].astype(np.int)
    df_CityHTS_t['TimeBike'] = df_CityHTS_t['TimeBike'].astype(np.int)
    df_CityHTS_t['TimeTownBus'] = df_CityHTS_t['TimeTownBus'].astype(np.int)
    df_CityHTS_t['TimeTrolleyBus'] = df_CityHTS_t['TimeTrolleyBus'].astype(np.int)
    df_CityHTS_t['TimeRegionalBus'] = df_CityHTS_t['TimeRegionalBus'].astype(np.int)
    df_CityHTS_t['TimeTrain'] = df_CityHTS_t['TimeTrain'].astype(np.int)
    df_CityHTS_t['TimeDriverCar'] = df_CityHTS_t['TimeDriverCar'].astype(np.int)
    df_CityHTS_t['TimePassengerCar'] = df_CityHTS_t['TimePassengerCar'].astype(np.int)
    df_CityHTS_t['TimeTaxi'] = df_CityHTS_t['TimeTaxi'].astype(np.int)
    df_CityHTS_t['TimeMoto'] = df_CityHTS_t['TimeMoto'].astype(np.int)
    df_CityHTS_t['TimeOther'] = df_CityHTS_t['TimeOther'].astype(np.int)

    df_CzechiaHTS_ph['Weight'] = df_CzechiaHTS_ph['Weight'].astype(np.float)
    df_CzechiaHTS_t['DeclaredTripTime'] = df_CzechiaHTS_t['DeclaredTripTime'].astype(np.float)
    df_CzechiaHTS_t['CrowFliesTripDist'] = df_CzechiaHTS_t['CrowFliesTripDist'].astype(np.float)
    df_CzechiaHTS_t['CalculatedTripDist'] = df_CzechiaHTS_t['CalculatedTripDist'].astype(np.float)
    df_CzechiaHTS_t['DeclaredTripDist'] = df_CzechiaHTS_t['DeclaredTripDist'].astype(np.float)
    df_CzechiaHTS_t["OriginStartHour"] = df_CzechiaHTS_t["OriginStartHour"].astype(np.int)
    df_CzechiaHTS_t["OriginStartMin"] = df_CzechiaHTS_t["OriginStartMin"].astype(np.int)
    df_CzechiaHTS_t["DestEndHour"] = df_CzechiaHTS_t["DestEndHour"].astype(np.int)
    df_CzechiaHTS_t["DestEndMin"] = df_CzechiaHTS_t["DestEndMin"].astype(np.int)
    df_CzechiaHTS_t["TripOrderNum"] = df_CzechiaHTS_t["TripOrderNum"].astype(np.int)
    df_CzechiaHTS_t["TimeBike"] = df_CzechiaHTS_t["TimeBike"].astype(np.int)
    df_CzechiaHTS_t["TimeTownBus"] = df_CzechiaHTS_t["TimeTownBus"].astype(np.int)
    df_CzechiaHTS_t["TimeTrolleyBus"] = df_CzechiaHTS_t["TimeTrolleyBus"].astype(np.int)
    df_CzechiaHTS_t["TimeTram"] = df_CzechiaHTS_t["TimeTram"].astype(np.int)
    df_CzechiaHTS_t["TimeMetro"] = df_CzechiaHTS_t["TimeMetro"].astype(np.int)
    df_CzechiaHTS_t["TimeRegionalBus"] = df_CzechiaHTS_t["TimeRegionalBus"].astype(np.int)
    df_CzechiaHTS_t["TimeLongDistBus"] = df_CzechiaHTS_t["TimeLongDistBus"].astype(np.int)
    df_CzechiaHTS_t["TimeTrain"] = df_CzechiaHTS_t["TimeTrain"].astype(np.int)
    df_CzechiaHTS_t["TimeDriverCar"] = df_CzechiaHTS_t["TimeDriverCar"].astype(np.int)
    df_CzechiaHTS_t["TimePassengerCar"] = df_CzechiaHTS_t["TimePassengerCar"].astype(np.int)
    df_CzechiaHTS_t["TimePlane"] = df_CzechiaHTS_t["TimePlane"].astype(np.int)
    df_CzechiaHTS_t["TimeOther"] = df_CzechiaHTS_t["TimeOther"].astype(np.int)

    # Add columns OriginState and DestState for each trip in CzechiaHTS
    df_CzechiaHTS_t["OriginState"] = np.repeat('Česko', len(df_CzechiaHTS_t["TripID"]))
    df_CzechiaHTS_t["DestState"] = np.repeat('Česko', len(df_CzechiaHTS_t["TripID"]))

    # Add columns TownCode, RegionCode and TownSize for CityHTS
    df_CityHTS_ph["TownCode"] = np.repeat('554804', len(df_CityHTS_ph["PersonID"]))
    df_CityHTS_ph["DistrictCode"] = np.repeat('4214', len(df_CityHTS_ph["PersonID"]))
    df_CityHTS_ph["RegionCode"] = np.repeat('CZ042', len(df_CityHTS_ph["PersonID"]))
    df_CityHTS_ph["TownSize"] = np.repeat('5', len(df_CityHTS_ph["PersonID"]))
    df_CityHTS_t["OriginDistrictCode"] = np.repeat('4214', len(df_CityHTS_t["TripID"]))
    df_CityHTS_t["DestDistrictCode"] = np.repeat('4214', len(df_CityHTS_t["TripID"]))

    # Change Prague's district code from 1000 to 1100 according to code lists
    df_CzechiaHTS_ph["DistrictCode"] = df_CzechiaHTS_ph["DistrictCode"].replace({'1000': '1100'})
    df_CzechiaHTS_t["OriginDistrictCode"] = df_CzechiaHTS_t["OriginDistrictCode"].replace({'1000': '1100'})
    df_CzechiaHTS_t["DestDistrictCode"] = df_CzechiaHTS_t["DestDistrictCode"].replace({'1000': '1100'})

    # Generate TripOrderNum for each trip of each person for CityHTS data
    TripOrderNums = pd.DataFrame(columns=['TripID', 'TripOrderNum'])
    last_personID = None
    for _, trip_data in tqdm(df_CityHTS_t.sort_values(by=['TripID']).iterrows(), total=len(df_CityHTS_t),
                             desc="Generating TripOrderNum for each trip of each person for CityHTS data"):
        personID = trip_data['PersonID']
        tripID = trip_data['TripID']
        if personID != last_personID:
            order_num = 1
            last_personID = personID
        else:
            order_num += 1
        to_append = pd.Series([tripID, order_num], index=TripOrderNums.columns)
        TripOrderNums = TripOrderNums.append(to_append, ignore_index=True)

    df_CityHTS_t = pd.merge(df_CityHTS_t, TripOrderNums, on='TripID')

    # Update TripOrderNum for each trip of each person for CzechiaHTS data (in case any trip had been deleted)
    df_CzechiaHTS_t = df_CzechiaHTS_t.sort_values(by=["PersonID", "TripOrderNum"])
    trips_per_person = df_CzechiaHTS_t.groupby("PersonID").size().reset_index(name="count")["count"].values
    df_CzechiaHTS_t["TripOrderNum"] = np.hstack([np.arange(1, n + 1) for n in trips_per_person])

    # Remove persons (and all their trips) with inconsistent data
    dfs = {"persons": [df_CzechiaHTS_ph, df_CityHTS_ph], "trips": [df_CzechiaHTS_t, df_CityHTS_t]}
    num_dfs = len(dfs["persons"])
    assert num_dfs == len(dfs["trips"])
    for df_ind in range(0, num_dfs):
        df_trips = dfs["trips"][df_ind]
        pers_id = list(set(df_trips["PersonID"].values.tolist()))
        to_remove = set([])
        global broke_loop
        for pid in tqdm(pers_id, desc="Cleaning HTS trips for df " + str(df_ind + 1) + " out of " + str(num_dfs)):
            df_i = df_trips[df_trips["PersonID"] == pid]
            df_i.sort_values(by=["TripOrderNum"])
            purposes = []
            broke_loop = 0
            cols_to_check = ["OriginPurpose",
                             "DestPurpose",
                             'OriginDistrictCode',
                             'DestDistrictCode']
            new_cols = set(df_i.columns).intersection({"CrowFliesTripDist", "CalculatedTripDist", "DeclaredTripDist"})
            cols_to_check.extend(list(new_cols))
            for row in df_i[cols_to_check].itertuples(index=False):
                if len(new_cols) > 0:
                    pp = row[0]
                    fp = row[1]
                    origin_district_code = row[2]
                    dest_district_code = row[3]
                    dists = row[4:]
                    if np.isnan(dists).all():
                        # Remove if not known neither CrowFliesTripDist, CalculatedTripDist nor DeclaredTripDist
                        to_remove.add(pid)
                        broke_loop = 1
                        break
                else:
                    pp, fp, origin_district_code, dest_district_code = row
                if len(df_codes[df_codes['KOD_ORP_CSU'] == origin_district_code]) == 0 \
                        or len(df_codes[df_codes['KOD_ORP_CSU'] == dest_district_code]) == 0:
                    # Remove if not known the district code
                    to_remove.add(pid)
                    broke_loop = 1
                    break
                if len(purposes) > 0 and pp != purposes[-1]:
                    # Remove if missing purpose, likely to be a mistake
                    to_remove.add(pid)
                    broke_loop = 1
                    break
                purposes.append(pp)
                purposes.append(fp)
                if pp == fp:
                    # Remove if repeating purpose, likely to be a mistake or separation of the trip into legs
                    # (but incidence is about 0.2% of the trips, so don't consider these cases)
                    to_remove.add(pid)
                    broke_loop = 1
                    break
            if not broke_loop:
                if purposes[0] != '1' or purposes[-1] != '1':
                    # Remove if trip doesn't start and end at home
                    to_remove.add(pid)
                elif len(purposes) == 2:
                    # Remove if person has only one trip
                    to_remove.add(pid)

        # Adjust persons and trips dataframe
        dfs["persons"][df_ind] = dfs["persons"][df_ind][~dfs["persons"][df_ind]['PersonID'].isin(to_remove)]
        dfs["trips"][df_ind] = dfs["trips"][df_ind][~dfs["trips"][df_ind]['PersonID'].isin(to_remove)]

    df_CzechiaHTS_ph, df_CityHTS_ph = dfs["persons"]
    df_CzechiaHTS_t, df_CityHTS_t = dfs["trips"]

    df_CzechiaHTS_ph = df_CzechiaHTS_ph.copy()
    df_CityHTS_ph = df_CityHTS_ph.copy()
    df_CzechiaHTS_t = df_CzechiaHTS_t.copy()
    df_CityHTS_t = df_CityHTS_t.copy()

    # Adjust PersonID, HouseholdID and TripID to avoid having same names
    df_CzechiaHTS_ph["PersonID"] = "CZ" + df_CzechiaHTS_ph["PersonID"].astype(str)
    df_CityHTS_ph["PersonID"] = "Usti" + df_CityHTS_ph["PersonID"].astype(str)
    df_CzechiaHTS_t["PersonID"] = "CZ" + df_CzechiaHTS_t["PersonID"].astype(str)
    df_CityHTS_t["PersonID"] = "Usti" + df_CityHTS_t["PersonID"].astype(str)
    df_CzechiaHTS_ph["HouseholdID"] = "CZ" + df_CzechiaHTS_ph["HouseholdID"].astype(str)
    df_CityHTS_ph["HouseholdID"] = "Usti" + df_CityHTS_ph["HouseholdID"].astype(str)
    df_CzechiaHTS_t["TripID"] = "CZ" + df_CzechiaHTS_t["TripID"].astype(str)
    df_CityHTS_t["TripID"] = "Usti" + df_CityHTS_t["TripID"].astype(str)

    # Define the time (in seconds) of the trips
    df_CzechiaHTS_t["OriginStart"] = df_CzechiaHTS_t["OriginStartHour"] * 3600 + df_CzechiaHTS_t["OriginStartMin"] * 60
    df_CzechiaHTS_t["DestEnd"] = df_CzechiaHTS_t["DestEndHour"] * 3600 + df_CzechiaHTS_t["DestEndMin"] * 60
    start_time = pd.DatetimeIndex(df_CityHTS_t["OriginStartHourMin"])
    end_time = pd.DatetimeIndex(df_CityHTS_t["DestEndHourMin"])
    df_CityHTS_t["OriginStart"] = start_time.hour * 3600 + start_time.minute * 60
    df_CityHTS_t["DestEnd"] = end_time.hour * 3600 + end_time.minute * 60

    # Impute duration at trip's destination (not at the moment)
    # for df_ind in range(0, num_dfs):
    #     df_trips = dfs["trips"][df_ind]
    #
    #     df_duration = pd.DataFrame(df_trips[["PersonID", "TripOrderNum", "DestEnd"]], copy=True)
    #     df_following = pd.DataFrame(df_trips[["PersonID", "TripOrderNum", "OriginStart"]], copy=True)
    #
    #     df_following.columns = ["PersonID", "TripOrderNum", "NextOriginStart"]
    #     df_following["TripOrderNum"] = df_following["TripOrderNum"] - 1
    #
    #     df_duration = pd.merge(df_duration, df_following, on=["PersonID", "TripOrderNum"])
    #     df_duration["DestDuration"] = df_duration["NextOriginStart"] - df_duration["DestEnd"]
    #     df_duration.loc[df_duration["DestDuration"] < 0.0, "DestDuration"] += 24.0 * 3600.0
    #
    #     df_duration = df_duration[["PersonID", "TripOrderNum", "DestDuration"]]
    #     dfs["trips"][df_ind] = pd.merge(df_trips, df_duration, how="left", on=["PersonID", "TripOrderNum"])

    # Use only column CrowFliesTripDist for CzechiaHTS, but if not data, then use the following priority:
    # a) CalculatedTripDist
    # b) DeclaredTripDist
    for ind, trip_data in tqdm(df_CzechiaHTS_t.iterrows(), total= len(df_CzechiaHTS_t),
                               desc="Setting either CalculatedTripDist or DeclaredTripDist "
                                    "as CrowFliesTripDist for CzechiaHTS"):
        if pd.isna(trip_data['CrowFliesTripDist']):
            if pd.isna(trip_data['CalculatedTripDist']):
                df_CzechiaHTS_t.at[ind, 'CrowFliesTripDist'] = trip_data['DeclaredTripDist']
            else:
                df_CzechiaHTS_t.at[ind, 'CrowFliesTripDist'] = trip_data['CalculatedTripDist']
    df_CzechiaHTS_t.drop(columns=['CalculatedTripDist', 'DeclaredTripDist'])

    # Convert to meters and minutes
    df_CzechiaHTS_t['CrowFliesTripDist'] *= 1000
    df_CzechiaHTS_t['DeclaredTripTime'] *= 60

    # Calculate CrowFliesTripDist for CityHTS, using GPS coordinates
    CrowFliesTripDists = pd.DataFrame(columns=['TripID', 'CrowFliesTripDist'])
    for ind, trip_data in tqdm(df_CityHTS_t.iterrows(), total=len(df_CityHTS_t),
                               desc="Calculating CrowFliesTripDist for CityHTS, using GPS coordinates"):
        trip_id = trip_data["TripID"]
        trip_dist = geopy.distance.distance((trip_data["LatOrigin"], trip_data["LonOrigin"]),
                                            (trip_data["LatDest"], trip_data["LonDest"])).meters
        to_append = pd.Series([trip_id, trip_dist], index=CrowFliesTripDists.columns)
        CrowFliesTripDists = CrowFliesTripDists.append(to_append, ignore_index=True)

    df_CityHTS_t = pd.merge(df_CityHTS_t, CrowFliesTripDists, on='TripID')

    # Education groups for CzechiaHTS (algorithm groups follow CityHTS)
    df_CzechiaHTS_ph['Education'] = commonFunctions.mappingStdCategories(df_edu[0], df_edu[4],
                                                                         df_CzechiaHTS_ph['Education'])

    # Car availability for CzechiaHTS and CityHTS
    df_CzechiaHTS_ph['AvailCar'] = commonFunctions.mappingStdCategories(df_car_avail[0], df_car_avail[4],
                                                                        df_CzechiaHTS_ph['AvailCar'])
    df_CityHTS_ph['AvailCar'] = commonFunctions.mappingStdCategories(df_car_avail[0], df_car_avail[2],
                                                                     df_CityHTS_ph['AvailCar'])

    # Bike availability for CzechiaHTS and CityHTS
    df_CzechiaHTS_ph['AvailBike'] = commonFunctions.mappingStdCategories(df_bike_avail[0], df_bike_avail[4],
                                                                         df_CzechiaHTS_ph['AvailBike'])
    df_CityHTS_ph['AvailBike'] = commonFunctions.mappingStdCategories(df_bike_avail[0], df_bike_avail[2],
                                                                      df_CityHTS_ph['AvailBike'])

    # Public transport subscription
    df_CzechiaHTS_ph['PtSubscription'] = commonFunctions.mappingStdCategories(df_pt_subs[0], df_pt_subs[4],
                                                                              df_CzechiaHTS_ph['PtSubscription'])
    df_CityHTS_ph['PtSubscription'] = commonFunctions.mappingStdCategories(df_pt_subs[0], df_pt_subs[2],
                                                                           df_CityHTS_ph['PtSubscription'])

    # Driving license ownership
    df_CzechiaHTS_ph['DrivingLicense'] = commonFunctions.mappingStdCategories(df_driving_id[0], df_driving_id[4],
                                                                              df_CzechiaHTS_ph['DrivingLicense'])
    df_CityHTS_ph['DrivingLicense'] = commonFunctions.mappingStdCategories(df_driving_id[0], df_driving_id[2],
                                                                           df_CityHTS_ph['DrivingLicense'])

    # Activity groups for CzechiaHTS (algorithm groups for CzechiaHTS matching groups follows Census)
    df_CzechiaHTS_ph['Activity'] = commonFunctions.mappingStdCategories(df_activity_CzechiaHTS[0],
                                                                        df_activity_CzechiaHTS[2],
                                                                        df_CzechiaHTS_ph['Activity'])

    # Activity groups for CityHTS (algorithm groups for CityHTS matching is a mix between CityHTS and Census)
    df_CityHTS_ph['Activity'] = commonFunctions.mappingStdCategories(df_activity_CityHTS[0], df_activity_CityHTS[2],
                                                                     df_CityHTS_ph['Activity'])

    # Trip purpose groups for CzechiaHTS and CityHTS (algorithm groups follows a mix between CzechiaHTS and CityHTS)
    df_CzechiaHTS_t['OriginPurpose'], df_CityHTS_t['OriginPurpose'] = commonFunctions.mappingStdCategories(
        df_trip_purpose[0],
        [df_trip_purpose[4],
         df_trip_purpose[2]],
        [df_CzechiaHTS_t['OriginPurpose'],
         df_CityHTS_t['OriginPurpose']])

    # Trip purpose groups for CzechiaHTS and CityHTS (algorithm groups follows a mix between CzechiaHTS and CityHTS)
    df_CzechiaHTS_t['DestPurpose'], df_CityHTS_t['DestPurpose'] = commonFunctions.mappingStdCategories(
        df_trip_purpose[0],
        [df_trip_purpose[4],
         df_trip_purpose[2]],
        [df_CzechiaHTS_t['DestPurpose'],
         df_CityHTS_t['DestPurpose']])

    # Age groups for CityHTS (algorithm groups follow CzechiaHTS)
    alg_groups = df_age[0][:-1]
    input_groups = pd.Series([0] + [int(age.split('-')[1]) + 1 for age in df_age[1][:-2]] + [1000])
    df_CityHTS_ph['Age'] = commonFunctions.mappingValCategories(alg_groups, input_groups, df_CityHTS_ph['Age'])
    df_CityHTS_ph = df_CityHTS_ph.rename(columns={'Age': 'AgeGroup'}).copy()
    df_CityHTS_ph['AgeGroup'] = df_CityHTS_ph['AgeGroup'].astype(str)

    # Aggregate values of mode times columns for CzechiaHTS and CityHTS
    df_CzechiaHTS_t, df_CityHTS_t = commonFunctions.aggregateColumns(df_trip_mode_times[1],
                                                                     [df_trip_mode_times[5], df_trip_mode_times[3]],
                                                                     [df_CzechiaHTS_t, df_CityHTS_t])

    ## Generate the following missing data (i.e. add columns) to match CzechiaHTS with Census data:
    # a) PrimaryLocRelationHome for each person
    # b) DeclaredJourneyTime for each person (the trip to primary location)
    # c) PrimaryLocTownCodes for each person (the trip to primary location)
    # d) PrimaryLocStateName for each person (the trip to primary location)
    # e) PrimaryLocDistrictCodes for each person (the trip to primary location)
    # f) PrimaryLocCrowFliesTripDist for each person (the trip to primary location)
    # g) JourneyMainMode for each person
    # h) IsPassenger for each trip (the trip to primary location)
    PrimaryLocRelationHomes = pd.DataFrame(columns=['PersonID', 'PrimaryLocRelationHome'])
    JourneyMainModes = pd.DataFrame(columns=['PersonID', 'JourneyMainMode'])
    DeclaredJourneyTimes = pd.DataFrame(columns=['PersonID', 'DeclaredJourneyTime'])
    PrimaryLocTownCodes = pd.DataFrame(columns=['PersonID', 'PrimaryLocTownCode'])
    PrimaryLocStateNames = pd.DataFrame(columns=['PersonID', 'PrimaryLocStateName'])
    PrimaryLocDistrictCodes = pd.DataFrame(columns=['PersonID', 'PrimaryLocDistrictCode'])
    PrimaryLocCrowFliesTripDists = pd.DataFrame(columns=['PersonID', 'PrimaryLocCrowFliesTripDist'])
    IsPassengers = pd.DataFrame(columns=['PersonID', 'IsPassenger'])
    HasWorkTrips = pd.DataFrame(columns=['PersonID', 'HasWorkTrip'])
    HasEducationTrips = pd.DataFrame(columns=['PersonID', 'HasEducationTrip'])
    max_DeclaredTripTime = df_CzechiaHTS_t['DeclaredTripTime'].max()
    for person_id, trips_data in tqdm(df_CzechiaHTS_t.groupby('PersonID'), total=len(df_CzechiaHTS_ph),
                               desc="Generating missing data (i.e. add columns) to match CzechiaHTS with Census data"):
        person_row = df_CzechiaHTS_ph.loc[df_CzechiaHTS_ph['PersonID'] == person_id]
        home_town_code = person_row.iloc[0]['TownCode']
        home_district_code = person_row.iloc[0]['DistrictCode']
        home_region_code = person_row.iloc[0]['RegionCode']

        if person_row.iloc[0]['Activity'] in ('3', '8'):
            mainly_student = True
        else:
            mainly_student = False
        has_work_trip = False
        has_edu_trip = False
        if "4" in trips_data['DestPurpose'].array:
            has_work_trip = True
        if "5" in trips_data['DestPurpose'].array:
            has_edu_trip = True

        if has_work_trip or has_edu_trip:
            if (not mainly_student or (mainly_student and not has_edu_trip)) and has_work_trip:
                # Give preference to work as main journey
                trip_data = trips_data.loc[trips_data['DestPurpose'] == '4']
            else:
                # Give preference to education as main journey
                trip_data = trips_data.loc[trips_data['DestPurpose'] == '5']

            trip_data = trip_data.iloc[0] # take only the first trip of the main journey
            dest_town_code = trip_data['DestTownCode']
            dest_state_name = trip_data['DestState']
            dest_district_code = trip_data['DestDistrictCode']
            dest_row = df_codes.loc[df_codes['KOD_ORP_CSU'] == dest_district_code]
            dest_region_code = dest_row.iloc[0]['KOD_KRAJ']
            if int(trip_data['DeclaredTripTime']) <= 5:
                # If works/studies at residence (if it takes 5 minutes or less)
                to_append = pd.Series([person_id, '6'], index=PrimaryLocRelationHomes.columns)
                PrimaryLocRelationHomes = PrimaryLocRelationHomes.append(to_append, ignore_index=True)
            elif dest_town_code == home_town_code:
                # If works/studies at the same town
                to_append = pd.Series([person_id, '4'], index=PrimaryLocRelationHomes.columns)
                PrimaryLocRelationHomes = PrimaryLocRelationHomes.append(to_append, ignore_index=True)
            elif dest_district_code == home_district_code:
                # If works/studies at the same district
                to_append = pd.Series([person_id, '1'], index=PrimaryLocRelationHomes.columns)
                PrimaryLocRelationHomes = PrimaryLocRelationHomes.append(to_append, ignore_index=True)
            elif dest_region_code == home_region_code:
                # If works/studies at the same region
                to_append = pd.Series([person_id, '2'], index=PrimaryLocRelationHomes.columns)
                PrimaryLocRelationHomes = PrimaryLocRelationHomes.append(to_append, ignore_index=True)
            else:
                # If works/studies at another region
                to_append = pd.Series([person_id, '3'], index=PrimaryLocRelationHomes.columns)
                PrimaryLocRelationHomes = PrimaryLocRelationHomes.append(to_append, ignore_index=True)

            DeclaredTripTime = trip_data['DeclaredTripTime']
            CrowFliesTripDist = trip_data['CrowFliesTripDist']
            trip_main_mode = trip_data['TripMainMode']
            passenger_bool = trip_main_mode == '7'
            to_append = pd.Series([person_id, trip_main_mode], index=JourneyMainModes.columns)
            JourneyMainModes = JourneyMainModes.append(to_append, ignore_index=True)
            to_append = pd.Series([person_id, DeclaredTripTime], index=DeclaredJourneyTimes.columns)
            DeclaredJourneyTimes = DeclaredJourneyTimes.append(to_append, ignore_index=True)
            to_append = pd.Series([person_id, dest_town_code], index=PrimaryLocTownCodes.columns)
            PrimaryLocTownCodes = PrimaryLocTownCodes.append(to_append, ignore_index=True)
            to_append = pd.Series([person_id, dest_district_code], index=PrimaryLocDistrictCodes.columns)
            PrimaryLocDistrictCodes = PrimaryLocDistrictCodes.append(to_append, ignore_index=True)
            to_append = pd.Series([person_id, dest_state_name], index=PrimaryLocStateNames.columns)
            PrimaryLocStateNames = PrimaryLocStateNames.append(to_append, ignore_index=True)
            to_append = pd.Series([person_id, CrowFliesTripDist], index=PrimaryLocCrowFliesTripDists.columns)
            PrimaryLocCrowFliesTripDists = PrimaryLocCrowFliesTripDists.append(to_append, ignore_index=True)
            to_append = pd.Series([person_id, passenger_bool], index=IsPassengers.columns)
            IsPassengers = IsPassengers.append(to_append, ignore_index=True)

            if has_work_trip:
                to_append = pd.Series([person_id, True], index=HasWorkTrips.columns)
                HasWorkTrips = HasWorkTrips.append(to_append, ignore_index=True)
            else:
                to_append = pd.Series([person_id, False], index=HasWorkTrips.columns)
                HasWorkTrips = HasWorkTrips.append(to_append, ignore_index=True)
            if has_edu_trip:
                to_append = pd.Series([person_id, True], index=HasEducationTrips.columns)
                HasEducationTrips = HasEducationTrips.append(to_append, ignore_index=True)
            else:
                to_append = pd.Series([person_id, False], index=HasEducationTrips.columns)
                HasEducationTrips = HasEducationTrips.append(to_append, ignore_index=True)
        else:
            # No work and school trip, treat as not economically active people
            to_append = pd.Series([person_id, '88'], index=PrimaryLocRelationHomes.columns)
            PrimaryLocRelationHomes = PrimaryLocRelationHomes.append(to_append, ignore_index=True)
            to_append = pd.Series([person_id, '999'], index=JourneyMainModes.columns)
            JourneyMainModes = JourneyMainModes.append(to_append, ignore_index=True)
            to_append = pd.Series([person_id, max_DeclaredTripTime + 1], index=DeclaredJourneyTimes.columns)
            DeclaredJourneyTimes = DeclaredJourneyTimes.append(to_append, ignore_index=True)
            to_append = pd.Series([person_id, home_town_code], index=PrimaryLocTownCodes.columns)
            PrimaryLocTownCodes = PrimaryLocTownCodes.append(to_append, ignore_index=True)
            to_append = pd.Series([person_id, home_district_code], index=PrimaryLocDistrictCodes.columns)
            PrimaryLocDistrictCodes = PrimaryLocDistrictCodes.append(to_append, ignore_index=True)
            to_append = pd.Series([person_id, 'Česko'], index=PrimaryLocStateNames.columns)
            PrimaryLocStateNames = PrimaryLocStateNames.append(to_append, ignore_index=True)
            to_append = pd.Series([person_id, 0], index=PrimaryLocCrowFliesTripDists.columns)
            PrimaryLocCrowFliesTripDists = PrimaryLocCrowFliesTripDists.append(to_append, ignore_index=True)
            to_append = pd.Series([person_id, False], index=IsPassengers.columns)
            IsPassengers = IsPassengers.append(to_append, ignore_index=True)
            to_append = pd.Series([person_id, False], index=HasWorkTrips.columns)
            HasWorkTrips = HasWorkTrips.append(to_append, ignore_index=True)
            to_append = pd.Series([person_id, False], index=HasEducationTrips.columns)
            HasEducationTrips = HasEducationTrips.append(to_append, ignore_index=True)

    df_CzechiaHTS_ph = pd.merge(PrimaryLocRelationHomes, df_CzechiaHTS_ph, on='PersonID')
    df_CzechiaHTS_ph = pd.merge(JourneyMainModes, df_CzechiaHTS_ph, on='PersonID')
    df_CzechiaHTS_ph = pd.merge(DeclaredJourneyTimes, df_CzechiaHTS_ph, on='PersonID')
    df_CzechiaHTS_ph = pd.merge(PrimaryLocTownCodes, df_CzechiaHTS_ph, on='PersonID')
    df_CzechiaHTS_ph = pd.merge(PrimaryLocDistrictCodes, df_CzechiaHTS_ph, on='PersonID')
    df_CzechiaHTS_ph = pd.merge(PrimaryLocStateNames, df_CzechiaHTS_ph, on='PersonID')
    df_CzechiaHTS_ph = pd.merge(PrimaryLocCrowFliesTripDists, df_CzechiaHTS_ph, on='PersonID')
    df_CzechiaHTS_ph = pd.merge(IsPassengers, df_CzechiaHTS_ph, on='PersonID')
    df_CzechiaHTS_ph = pd.merge(HasWorkTrips, df_CzechiaHTS_ph, on='PersonID')
    df_CzechiaHTS_ph = pd.merge(HasEducationTrips, df_CzechiaHTS_ph, on='PersonID')

    alg_groups = pd.Series(df_journey_t[0][:-2].to_list())
    input_groups = pd.Series([0, 15, 30, 45, 60, 90, 90 + max_DeclaredTripTime + 1])
    df_CzechiaHTS_ph['DeclaredJourneyTime'] = commonFunctions.mappingValCategories(alg_groups, input_groups,
                                                                                   df_CzechiaHTS_ph['DeclaredJourneyTime'])

    ## Generate the following missing data (i.e. add columns) to match CityHTS with Census data:
    # a) PrimaryLocRelationHome for each person
    # b) DeclaredJourneyTime for each person (the trip to primary location)
    # c) PrimaryLocTownCodes for each person (the trip to primary location)
    # d) PrimaryLocDistrictCodes for each person (the trip to primary location)
    # e) PrimaryLocCrowFliesTripDist for each person (the trip to primary location)
    # f) PrimaryLocStateName for each person (the trip to primary location)
    # g) JourneyMainMode for each person
    # h) TripMainMode for each trip of each person
    # i) IsPassenger for each trip (the trip to primary location)
    PrimaryLocRelationHomes = pd.DataFrame(columns=['PersonID', 'PrimaryLocRelationHome'])
    JourneyMainModes = pd.DataFrame(columns=['PersonID', 'JourneyMainMode'])
    DeclaredJourneyTimes = pd.DataFrame(columns=['PersonID', 'DeclaredJourneyTime'])
    PrimaryLocTownCodes = pd.DataFrame(columns=['PersonID', 'PrimaryLocTownCode'])
    PrimaryLocStateNames = pd.DataFrame(columns=['PersonID', 'PrimaryLocStateName'])
    PrimaryLocDistrictCodes = pd.DataFrame(columns=['PersonID', 'PrimaryLocDistrictCode'])
    PrimaryLocCrowFliesTripDists = pd.DataFrame(columns=['PersonID', 'PrimaryLocCrowFliesTripDist'])
    max_DeclaredTripTime = df_CityHTS_t['DeclaredTripTime'].max()
    TripMainModes = pd.DataFrame(columns=['TripID', 'TripMainMode'])
    IsPassengers = pd.DataFrame(columns=['PersonID', 'IsPassenger'])
    HasWorkTrips = pd.DataFrame(columns=['PersonID', 'HasWorkTrip'])
    HasEducationTrips = pd.DataFrame(columns=['PersonID', 'HasEducationTrip'])
    df_group = df_CityHTS_t.groupby('PersonID')
    for person_id, trips_data in tqdm(df_group, total=len(df_group),
                                      desc="Generating missing data (i.e. add columns) "
                                           "to match CityHTS with Census data"):
        person_row = df_CityHTS_ph.loc[df_CityHTS_ph['PersonID'] == person_id]
        home_town_code = person_row.iloc[0]['TownCode']
        home_district_code = person_row.iloc[0]['DistrictCode']
        home_region_code = person_row.iloc[0]['RegionCode']
        for _, trip_data in trips_data.iterrows():
            trip_id = trip_data["TripID"]
            time_modes = trip_data[[
                'TimeFoot', 'TimeBike', 'TimeMHD', 'TimeRegionalBus', 'TimeRegionalTrain', 'TimeDriverCar',
                'TimePassengerCar', 'TimeOther',
            ]]
            main_mode = pd.to_numeric(time_modes).idxmax()
            to_append = pd.Series([trip_id, str(list(time_modes.keys()).index(main_mode) + 1)],
                                  index=TripMainModes.columns)

            assert trip_id not in TripMainModes['TripID']

            TripMainModes = TripMainModes.append(to_append, ignore_index=True)

        if person_row.iloc[0]['Activity'] == '2':
            mainly_student = True
        else:
            mainly_student = False
        has_work_trip = False
        has_edu_trip = False
        if "4" in trips_data['DestPurpose'].array:
            has_work_trip = True
        if "5" in trips_data['DestPurpose'].array:
            has_edu_trip = True

        if has_work_trip or has_edu_trip:
            if (not mainly_student or (mainly_student and not has_edu_trip)) and has_work_trip:
                # Give preference to work as main journey
                trip_data = trips_data.loc[trips_data['DestPurpose'] == '4']
            else:
                # Give preference to education as main journey
                trip_data = trips_data.loc[trips_data['DestPurpose'] == '5']

            trip_data = trip_data.iloc[0] # take only the first trip of the main journey
            dest_town_code = trip_data['DestTownCode']
            dest_state_name = trip_data['DestState']
            dest_row = df_codes.loc[df_codes['KOD_OBEC'] == dest_town_code]
            try:
                dest_district_code = dest_row.iloc[0]['KOD_ORP_CSU']
                dest_region_code = dest_row.iloc[0]['KOD_KRAJ']
            except IndexError:
                dest_district_code = '0'
                dest_region_code = '0'

            if int(trip_data['DeclaredTripTime']) <= 5:
                # If works/studies at residence (if it takes 5 minutes or less)
                to_append = pd.Series([person_id, '6'], index=PrimaryLocRelationHomes.columns)
                PrimaryLocRelationHomes = PrimaryLocRelationHomes.append(to_append, ignore_index=True)
            elif dest_town_code == home_town_code:
                # If works/studies at the same town
                to_append = pd.Series([person_id, '4'], index=PrimaryLocRelationHomes.columns)
                PrimaryLocRelationHomes = PrimaryLocRelationHomes.append(to_append, ignore_index=True)
            elif dest_district_code == home_district_code:
                # If works/studies at the same district
                to_append = pd.Series([person_id, '1'], index=PrimaryLocRelationHomes.columns)
                PrimaryLocRelationHomes = PrimaryLocRelationHomes.append(to_append, ignore_index=True)
            elif dest_region_code == home_region_code:
                # If works/studies at the same region
                to_append = pd.Series([person_id, '2'], index=PrimaryLocRelationHomes.columns)
                PrimaryLocRelationHomes = PrimaryLocRelationHomes.append(to_append, ignore_index=True)
            else:
                # If works/studies at another region
                to_append = pd.Series([person_id, '3'], index=PrimaryLocRelationHomes.columns)
                PrimaryLocRelationHomes = PrimaryLocRelationHomes.append(to_append, ignore_index=True)
            journeyTimes = trip_data[['TimeFoot', 'TimeBike', 'TimeMHD', 'TimeRegionalBus',
                                     'TimeRegionalTrain', 'TimeDriverCar', 'TimePassengerCar', 'TimeOther']]
            DeclaredTripTime = trip_data['DeclaredTripTime']
            CrowFliesTripDist = trip_data['CrowFliesTripDist']
            main_mode = pd.to_numeric(journeyTimes).idxmax()
            passenger_bool = main_mode == 'TimePassengerCar'
            to_append = pd.Series([person_id, list(journeyTimes.keys()).index(main_mode) + 1],
                                  index=JourneyMainModes.columns)
            JourneyMainModes = JourneyMainModes.append(to_append, ignore_index=True)

            to_append = pd.Series([person_id, DeclaredTripTime], index=DeclaredJourneyTimes.columns)
            DeclaredJourneyTimes = DeclaredJourneyTimes.append(to_append, ignore_index=True)
            to_append = pd.Series([person_id, dest_town_code], index=PrimaryLocTownCodes.columns)
            PrimaryLocTownCodes = PrimaryLocTownCodes.append(to_append, ignore_index=True)
            to_append = pd.Series([person_id, dest_district_code], index=PrimaryLocDistrictCodes.columns)
            PrimaryLocDistrictCodes = PrimaryLocDistrictCodes.append(to_append, ignore_index=True)
            to_append = pd.Series([person_id, dest_state_name], index=PrimaryLocStateNames.columns)
            PrimaryLocStateNames = PrimaryLocStateNames.append(to_append, ignore_index=True)
            to_append = pd.Series([person_id, CrowFliesTripDist], index=PrimaryLocCrowFliesTripDists.columns)
            PrimaryLocCrowFliesTripDists = PrimaryLocCrowFliesTripDists.append(to_append, ignore_index=True)
            to_append = pd.Series([person_id, passenger_bool], index=IsPassengers.columns)
            IsPassengers = IsPassengers.append(to_append, ignore_index=True)

            if has_work_trip:
                to_append = pd.Series([person_id, True], index=HasWorkTrips.columns)
                HasWorkTrips = HasWorkTrips.append(to_append, ignore_index=True)
            else:
                to_append = pd.Series([person_id, False], index=HasWorkTrips.columns)
                HasWorkTrips = HasWorkTrips.append(to_append, ignore_index=True)
            if has_edu_trip:
                to_append = pd.Series([person_id, True], index=HasEducationTrips.columns)
                HasEducationTrips = HasEducationTrips.append(to_append, ignore_index=True)
            else:
                to_append = pd.Series([person_id, False], index=HasEducationTrips.columns)
                HasEducationTrips = HasEducationTrips.append(to_append, ignore_index=True)
        else:
            # Not economically active people
            to_append = pd.Series([person_id, '88'], index=PrimaryLocRelationHomes.columns)
            PrimaryLocRelationHomes = PrimaryLocRelationHomes.append(to_append, ignore_index=True)
            to_append = pd.Series([person_id, '999'], index=JourneyMainModes.columns)
            JourneyMainModes = JourneyMainModes.append(to_append, ignore_index=True)
            to_append = pd.Series([person_id, max_DeclaredTripTime + 1], index=DeclaredJourneyTimes.columns)
            DeclaredJourneyTimes = DeclaredJourneyTimes.append(to_append, ignore_index=True)
            to_append = pd.Series([person_id, home_town_code], index=PrimaryLocTownCodes.columns)
            PrimaryLocTownCodes = PrimaryLocTownCodes.append(to_append, ignore_index=True)
            to_append = pd.Series([person_id, home_district_code], index=PrimaryLocDistrictCodes.columns)
            PrimaryLocDistrictCodes = PrimaryLocDistrictCodes.append(to_append, ignore_index=True)
            to_append = pd.Series([person_id, 'Česko'], index=PrimaryLocStateNames.columns)
            PrimaryLocStateNames = PrimaryLocStateNames.append(to_append, ignore_index=True)
            to_append = pd.Series([person_id, 0], index=PrimaryLocCrowFliesTripDists.columns)
            PrimaryLocCrowFliesTripDists = PrimaryLocCrowFliesTripDists.append(to_append, ignore_index=True)
            to_append = pd.Series([person_id, False], index=IsPassengers.columns)
            IsPassengers = IsPassengers.append(to_append, ignore_index=True)
            to_append = pd.Series([person_id, False], index=HasWorkTrips.columns)
            HasWorkTrips = HasWorkTrips.append(to_append, ignore_index=True)
            to_append = pd.Series([person_id, False], index=HasEducationTrips.columns)
            HasEducationTrips = HasEducationTrips.append(to_append, ignore_index=True)

    df_CityHTS_ph = pd.merge(PrimaryLocRelationHomes, df_CityHTS_ph, on='PersonID')
    df_CityHTS_ph = pd.merge(JourneyMainModes, df_CityHTS_ph, on='PersonID')
    df_CityHTS_ph = pd.merge(DeclaredJourneyTimes, df_CityHTS_ph, on='PersonID')
    df_CityHTS_ph = pd.merge(PrimaryLocTownCodes, df_CityHTS_ph, on='PersonID')
    df_CityHTS_ph = pd.merge(PrimaryLocStateNames, df_CityHTS_ph, on='PersonID')
    df_CityHTS_ph = pd.merge(PrimaryLocDistrictCodes, df_CityHTS_ph, on='PersonID')
    df_CityHTS_ph = pd.merge(PrimaryLocCrowFliesTripDists, df_CityHTS_ph, on='PersonID')
    df_CityHTS_t = pd.merge(TripMainModes, df_CityHTS_t, on='TripID')
    df_CityHTS_ph = pd.merge(IsPassengers, df_CityHTS_ph, on='PersonID')
    df_CityHTS_ph = pd.merge(HasWorkTrips, df_CityHTS_ph, on='PersonID')
    df_CityHTS_ph = pd.merge(HasEducationTrips, df_CityHTS_ph, on='PersonID')

    alg_groups = pd.Series(df_journey_t[0][:-2].to_list())
    input_groups = pd.Series([0, 15, 30, 45, 60, 90, 90 + max_DeclaredTripTime + 1])
    df_CityHTS_ph['DeclaredJourneyTime'] = commonFunctions.mappingValCategories(alg_groups, input_groups,
                                                                                df_CityHTS_ph['DeclaredJourneyTime'])

    print("Finishing up Travel Survey (HTS) data")

    # Finishing up
    df_CzechiaHTS_ph = df_CzechiaHTS_ph.rename(columns={'Activity': 'ActivityCzechiaHTS'}).copy()
    df_CzechiaHTS_ph = df_CzechiaHTS_ph[[
        "PersonID", "Weight", "AgeGroup", "Gender", "Education", "ActivityCzechiaHTS", "TownSize",
        "TownCode", "RegionCode", "DistrictCode",
        # "NumPersonsAge00_05", "NumPersonsAge06_17", "NumPersonsAge18_99", # not at the moment
        "JourneyMainMode", "DeclaredJourneyTime", "PrimaryLocRelationHome",
        'PrimaryLocTownCode', 'PrimaryLocDistrictCode', 'PrimaryLocStateName', 'PrimaryLocCrowFliesTripDist',
        "HasWorkTrip", "HasEducationTrip",
        # 'Homeoffice', # potential useful info
        # 'FlexibleBegEndTime', # potential useful info
        'FlexibleHours',
        # 'AvailCarSharing', # potential useful info
        "AvailCar", "AvailBike", "DrivingLicense", "PtSubscription", "IsPassenger",
    ]]

    df_CzechiaHTS_t = df_CzechiaHTS_t[['TripID', 'PersonID', 'TripOrderNum', 'OriginStart', 'DestEnd', 'TripMainMode',
                         # 'TimeFoot', 'TimeBike', 'TimeMHD', 'TimeRegionalBus', 'TimeRegionalTrain', 'TimeDriverCar',
                         # 'TimePassengerCar', 'TimeOther',
                         'OriginTownCode', 'DestTownCode', 'OriginState', 'DestState',
                         "OriginDistrictCode", "DestDistrictCode",
                         'CrowFliesTripDist', 'OriginPurpose', 'DestPurpose',
                         # 'DestDuration', # not at the moment
                         'DeclaredTripTime',
                         ]]

    df_CityHTS_ph = df_CityHTS_ph.rename(columns={'Activity': 'ActivityCityHTS'})
    df_CityHTS_ph = df_CityHTS_ph[[
        "PersonID", "Weight", "AgeGroup", "Gender", "Education", "ActivityCityHTS",
        "CadastralAreaCode", "TownCode", "RegionCode", "DistrictCode",
        # "NumPersons", "NumPersonsAge06_18", # not at the moment
        "JourneyMainMode", "DeclaredJourneyTime", "PrimaryLocRelationHome",
        'PrimaryLocTownCode', 'PrimaryLocDistrictCode', 'PrimaryLocStateName', 'PrimaryLocCrowFliesTripDist',
        "HasWorkTrip", "HasEducationTrip",
        'FlexibleHours',
        "AvailCar", "AvailBike", "DrivingLicense", "PtSubscription", "IsPassenger",
    ]]

    df_CityHTS_t = df_CityHTS_t[['TripID', 'PersonID', 'TripOrderNum', 'OriginStart', 'DestEnd', 'TripMainMode',
                             # 'TimeFoot', 'TimeBike', 'TimeMHD', 'TimeRegionalBus', 'TimeRegionalTrain', 'TimeDriverCar',
                             # 'TimePassengerCar', 'TimeOther',
                             'OriginCadastralAreaCode', 'DestCadastralAreaCode',
                             "OriginDistrictCode", "DestDistrictCode",
                             'OriginTownCode', 'DestTownCode',  'OriginState', 'DestState',
                             'CrowFliesTripDist', 'OriginPurpose', 'DestPurpose',
                             # 'DestDuration', # potential useful info
                             'DeclaredTripTime',
                             ]]

    df_CzechiaHTS_ph = df_CzechiaHTS_ph.drop_duplicates()
    df_CzechiaHTS_t = df_CzechiaHTS_t.drop_duplicates()
    df_CityHTS_ph = df_CityHTS_ph.drop_duplicates()
    df_CityHTS_t = df_CityHTS_t.drop_duplicates()

    df_CzechiaHTS_ph.to_csv("%s/HTS/df_CzechiaHTS_ph.csv" % context.config("output_path") )
    df_CzechiaHTS_t.to_csv("%s/HTS/df_CzechiaHTS_t.csv" % context.config("output_path") )
    df_CityHTS_ph.to_csv("%s/HTS/df_CityHTS_ph.csv" % context.config("output_path"))
    df_CityHTS_t.to_csv("%s/HTS/df_CityHTS_t.csv" % context.config("output_path"))

    commonFunctions.toXML(df_CzechiaHTS_ph, "%s/HTS/df_CzechiaHTS_ph.xml" % context.config("output_path"))
    commonFunctions.toXML(df_CzechiaHTS_t, "%s/HTS/df_CzechiaHTS_t.xml" % context.config("output_path"))
    commonFunctions.toXML(df_CityHTS_ph, "%s/HTS/df_CityHTS_ph.xml" % context.config("output_path"))
    commonFunctions.toXML(df_CityHTS_t, "%s/HTS/df_CityHTS_t.xml" % context.config("output_path"))

    return df_CzechiaHTS_ph, df_CityHTS_ph, df_CzechiaHTS_t, df_CityHTS_t
