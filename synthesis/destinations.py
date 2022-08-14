from tqdm import tqdm
import pandas as pd
import numpy as np
import data.spatial.utils
import geopandas as gpd
import os
from scipy.spatial import cKDTree
from data import commonFunctions
import warnings

def configure(context):
    context.config("data_path")
    context.config("output_path")
    context.stage("data.spatial.zones")
    context.stage("data.osm.extract_facilities")
    context.config("generalizations_file")
    context.config("facilities_edu_file")
    context.config("facilities_work_home_secondary_file")
    context.config("facilities_work_home_file")
    context.config("facilities_osm_file")
    context.config("facilities_area_file")
    context.config("buildings_occupancy_file")
    context.config("routes_file")

def validate(context):
    data_path = context.config("data_path")
    output_path = context.config("output_path")
    generalizations_file = "%s/%s" % (context.config("data_path"), context.config("generalizations_file"))
    routes_file = "%s/%s" % (context.config("data_path"), context.config("routes_file"))
    facilities_edu_file = "%s/Facilities/%s" % (context.config("data_path"),
                                                context.config("facilities_edu_file"))
    facilities_work_home_file = "%s/Facilities/%s" % (context.config("data_path"),
                                                      context.config("facilities_work_home_file"))
    facilities_area_file = "%s/Facilities/%s" % (context.config("data_path"),
                                                 context.config("facilities_area_file"))
    buildings_occupancy_file = "%s/Facilities/%s" % (context.config("data_path"),
                                                     context.config("buildings_occupancy_file"))

    if not os.path.isdir(data_path):
        raise RuntimeError("Input directory must exist: %s" % data_path)

    if not os.path.isdir(output_path):
        raise RuntimeError("Output directory must exist: %s" % output_path)

    if not os.path.exists(generalizations_file):
        raise RuntimeError("Input file must exist: %s" % generalizations_file)

    if not os.path.exists(routes_file):
        raise RuntimeError("Input file must exist: %s" % routes_file)

    if not os.path.exists(facilities_edu_file):
        raise RuntimeError("Input file must exist: %s" % facilities_edu_file)

    if not os.path.exists(facilities_work_home_file):
        raise RuntimeError("Input file must exist: %s" % facilities_work_home_file)

    if not os.path.exists(facilities_area_file):
        raise RuntimeError("Input file must exist: %s" % facilities_area_file)

    if not os.path.exists(buildings_occupancy_file):
        raise RuntimeError("Input file must exist: %s" % buildings_occupancy_file)

def ckdnearest(gdA, gdB):
    """"Get the closest facility from gdB to gdA by proximity using the quick nearest-neighbor lookup"""

    try:
        nA = np.array(list(gdA.geometry.apply(lambda x: (x.x, x.y))))
    except:
        # If facility geometry is an area/polygon, use its centroid
        nA = np.array(list(gdA.geometry.apply(lambda x: (x.centroid.x, x.centroid.y))))
    try:
        nB = np.array(list(gdB.geometry.apply(lambda x: (x.x, x.y))))
    except:
        # If facility geometry is an area/polygon, use its centroid
        nB = np.array(list(gdB.geometry.apply(lambda x: (x.centroid.x, x.centroid.y))))
    ids = gdA.index
    btree = cKDTree(nB)
    dist, idx = btree.query(nA, k=1) # get only the first closest neighbour
    gdB_nearest = gdB.iloc[idx].drop(columns="geometry").reset_index(drop=True)
    gdf = pd.concat(
        [
            gdA["IDOB"].reset_index(drop=True),
            gdB_nearest["osmID"], gdB_nearest["FacilityType"],
            pd.Series(dist, name='dist')
        ],
        axis=1)

    gdf.index = ids

    return gdf

def execute(context):
    
    # Ignore header warning when reading excel files
    warnings.filterwarnings('ignore', category=UserWarning, module='openpyxl')

    # Ignore warning when working on slices of dataframes
    pd.options.mode.chained_assignment = None

    print("Reading Facilities Files")

    # Get inputs
    df_zones_gates = pd.read_excel("%s/%s" % (context.config("data_path"), context.config("routes_file")),
                                   header=0,
                                   # encoding="cp1250",
                                   dtype=str)[["GATEosm_id"]].drop_duplicates().dropna().astype(int)
    df_zones_gates = df_zones_gates.rename(columns={'GATEosm_id': 'ZoneID'}).astype(str)

    df_obec_zones, df_ku_zones, df_zsj_zones, df_outer_zones = context.stage("data.spatial.zones")
    df_zones_gates = df_outer_zones[df_outer_zones["ZoneID"].isin(df_zones_gates['ZoneID'])]

    df_purpose = pd.read_excel("%s/%s" % (context.config("data_path"), context.config("generalizations_file")),
                           header=None, skiprows=1, sheet_name='Purpose FacilityPurpose',
                               dtype=str)
    df_purpose = df_purpose.astype(str)
    df_usage = pd.read_excel("%s/%s" % (context.config("data_path"), context.config("generalizations_file")),
                           header=None, skiprows=1, sheet_name='ActivitySector FacilityUsage', dtype=str)
    df_usage = df_usage.astype(str)
    df_edu_place = pd.read_excel("%s/%s" % (context.config("data_path"), context.config("generalizations_file")),
                                 header=None, skiprows=1, sheet_name='EducationPlace FacilityType', dtype=str)

    # Define which attributes will be necessary
    all_cols = ["BasicSettlementCode", "CadastralAreaCode", "TownCode",
                "IDOB", "osmID",
                "FacilityPurpose", "FacilityUsage", "FacilityType", "EducationPlace",
                'Area', "Shape", "Inhabitants", "WorkPlaces", "StudyPlaces", "Visitors",
                'offers_home', 'offers_work', 'offers_education',
                'offers_freetime', 'offers_shopping', 'offers_errands',
                "geometry"]

    # Get facilities for education based on School Census + Universities (manually)
    df_facilities_edu = gpd.read_file("%s/Facilities/%s" % (context.config("data_path"),
                                                            context.config("facilities_edu_file")),
                                      dtypes=str)[['SchoolCode', 'CAPACITY', "geometry"]]
    df_facilities_edu.crs = "epsg:5514"

    # Renaming columns
    df_facilities_edu.columns = ["FacilityType", "StudyPlaces", "geometry"]

    # Getting Zones
    print("Defining educational facilities BasicSettlementCode")
    df_facilities_edu = gpd.overlay(df_facilities_edu, df_zsj_zones, how='intersection')
    df_facilities_edu = df_facilities_edu.rename(columns={'ZoneID': 'BasicSettlementCode'})
    print("Defining educational facilities CadastralCode")
    df_facilities_edu = gpd.overlay(df_facilities_edu, df_ku_zones, how='intersection')
    df_facilities_edu = df_facilities_edu.rename(columns={'ZoneID': 'CadastralAreaCode'})
    print("Defining educational facilities TownCode")
    df_facilities_edu = gpd.overlay(df_facilities_edu, df_obec_zones, how='intersection')
    df_facilities_edu = df_facilities_edu.rename(columns={'ZoneID': 'TownCode'})

    # Define facility missing attributes
    df_facilities_edu["IDOB"] = np.nan
    df_facilities_edu["osmID"] = np.nan
    df_facilities_edu["Area"] = np.nan
    df_facilities_edu["Shape"] = np.nan
    df_facilities_edu["FacilityPurpose"] = '5'
    df_facilities_edu["FacilityPurpose"] = df_facilities_edu["FacilityPurpose"].apply(lambda x: tuple(x.split(",")))
    df_facilities_edu["FacilityUsage"] = '8'
    df_facilities_edu["FacilityUsage"] = df_facilities_edu["FacilityUsage"].apply(lambda x: tuple(x.split(",")))
    df_facilities_edu["Inhabitants"] = 0
    df_facilities_edu["WorkPlaces"] = 0 # workplaces of educational facilities are from Facility Census database
    df_facilities_edu["Visitors"] = 0 # visitors of educational facilities are from Facility Census database
    df_facilities_edu["StudyPlaces"] = df_facilities_edu["StudyPlaces"].astype(int)

    # Trip purpose groups for CzechiaHTS and CityHTS (algorithm groups follows a mix between CzechiaHTS and CityHTS)
    df_facilities_edu['EducationPlace'] = commonFunctions.mappingStdCategories(df_edu_place[0], df_edu_place[4],
                                                                               df_facilities_edu['FacilityType'])

    # Write down what each facility offers based on their building purpose
    df_facilities_edu["offers_education"] = True
    df_facilities_edu["offers_home"] = False
    df_facilities_edu["offers_freetime"] = False
    df_facilities_edu["offers_shopping"] = False
    df_facilities_edu["offers_work"] = False
    df_facilities_edu["offers_errands"] = False

    # Clean up
    df_facilities_edu = df_facilities_edu[all_cols]

    # Transform each element of a list-like to a row, replicating index values
    df_facilities_edu = df_facilities_edu.explode()

    # Get facilities for home, work and secondary locations based on Facility Census data and OpenStreetMaps
    try:
        # Try to get already processed database
        df_facilities_work_home_secondary = gpd.read_file("%s/Facilities/%s" % (context.config("data_path"),
                                                                                context.config(
                                                                                    "facilities_work_home_secondary_file")),
                                                          dtypes=str)[["KOD_ZSJ", "KOD_KU", "KOD_LAU2",
                                                                       "IDOB", "osmID",
                                                                       "purpose", "usage", "type", "place",
                                                                       'area', "shape", "population", "workers", "students",
                                                                       "visitors",
                                                                       'home', 'work', 'education',
                                                                       'freetime', 'shopping', 'errands',
                                                                       "geometry"]]
        df_facilities_work_home_secondary.crs = "epsg:5514"

        # Transform string to tuple
        df_facilities_work_home_secondary["purpose"] = \
            df_facilities_work_home_secondary["purpose"].apply(lambda x: tuple(x.split(",")))
        df_facilities_work_home_secondary["place"] = \
            df_facilities_work_home_secondary["place"].apply(lambda x: tuple(x.split(",")))
        df_facilities_work_home_secondary["usage"] = \
            df_facilities_work_home_secondary["usage"].apply(lambda x: tuple(x.split(",")) if x != None else [x])

        df_facilities_work_home_secondary.columns = all_cols
    except:
        # Not found database, get data from Facility Census data and OpenStreetMaps

        # Get facilities from Facility Census
        df_facilities_work_home = gpd.read_file("%s/Facilities/%s" % (context.config("data_path"),
                                                                      context.config("facilities_work_home_file")),
                                                dtypes=str)[["KOD_ZSJ", "KOD_KU_A", "KOD_OBEC",
                                                             "IDOB",
                                                             "ZPVYBU",
                                                             "BUDOBYEV",
                                                             "geometry"]]

        # If missing FacilityPurpose, define as any
        df_facilities_work_home['ZPVYBU'] = df_facilities_work_home['ZPVYBU'].fillna('99')
        df_facilities_work_home.crs = "epsg:5514"

        # Shift the work_home position to conform with OSM position
        df_facilities_work_home["geometry"] = df_facilities_work_home["geometry"].translate(xoff=-8.593000000109896,
                                                                                            yoff=-7.990000000107102)

        print("Saving corrected coordinates of Facilities Census data")
        df_facilities_work_home.to_file("%s/Facilities/%s" % (context.config("data_path"),
                                                              "fixed_" + context.config("facilities_work_home_file")))
        print("Saved corrected coordinates of Facilities Census data")

        df_facilities_work_home.columns = ["BasicSettlementCode", "CadastralAreaCode", "TownCode",
                                           "IDOB",
                                           "FacilityPurpose", "Inhabitants", "geometry"]
        df_facilities_work_home["Inhabitants"] = df_facilities_work_home["Inhabitants"].astype(int)


        # Harmonize FacilityPurpose and FacilityUsage according to generalizations.xlsx file
        df_facilities_work_home["FacilityPurpose"] = df_facilities_work_home["FacilityPurpose"].astype(str)
        df_facilities_work_home["FacilityUsage"] = df_facilities_work_home["FacilityPurpose"].astype(str)
        df_facilities_work_home['FacilityPurpose'] = \
            commonFunctions.mappingStdCategories(df_purpose[0], df_purpose[2],
                                                 df_facilities_work_home['FacilityPurpose'])
        df_facilities_work_home['FacilityUsage'] = \
            commonFunctions.mappingStdCategories(df_usage[0], df_usage[2],
                                                 df_facilities_work_home['FacilityUsage'])

        # Get home and work places
        required_purposes = {"1", "4"}
        ids = df_facilities_work_home["FacilityPurpose"].apply(
                lambda x: True if (type(x) == tuple and x == ("99",) or len(set(x).intersection(required_purposes)))
                                   or (type(x) != tuple and x == "99" or x in required_purposes) > 0
                else False)
        df_facilities_work_home = df_facilities_work_home[ids]

        # Define facilities attributes
        df_facilities_work_home["StudyPlaces"] = 0  # studyplaces of (educational) work facilities
                                                    # are from school census database

        # Get area of facilities
        df_facilities_area = gpd.read_file("%s/Facilities/%s" % (context.config("data_path"),
                                                                 context.config("facilities_area_file")))[["osm_id",
                                                                                                           "RSO_IDOB",
                                                                                                           "hpp",
                                                                                                           "geometry"]]
        df_facilities_area.crs = "epsg:5514"

        # Shift the facility areas position to conform with OSM position
        df_facilities_area["geometry"] = df_facilities_area["geometry"].translate(xoff=-8.593000000109896,
                                                                                  yoff=-7.990000000107102)

        print("Saving corrected coordinates of Facilities Area data")
        df_facilities_area.to_file("%s/Facilities/%s" % (context.config("data_path"),
                                                         "fixed_" + context.config("facilities_area_file")))
        print("Saved corrected coordinates of Facilities Area data")

        df_facilities_area.columns = ["osmID", "IDOB", "Area", "geometry"]
        df_facilities_area["osmID"] = df_facilities_area["osmID"].astype(int)

        # Get area of Facility Census facilities
        df_facilities_work_home = df_facilities_work_home.merge(df_facilities_area, on='IDOB', how="left")
        # For those without osmID, assign the area (and consequently the osmID) it is located inside and sum its inhabitants.
        # This is done because some polygons of buildings have multiple point with inhabitants
        df_facilities_work_home["geometry"] = df_facilities_work_home["geometry_x"]
        df_facilities_work_home["Shape"] = df_facilities_work_home["geometry_y"]
        del df_facilities_work_home["geometry_x"]
        del df_facilities_work_home["geometry_y"]
        df_facilities_work_home = gpd.GeoDataFrame(df_facilities_work_home)
        df_facilities_work_home.crs = "epsg:5514"
        df_facilities_na = gpd.sjoin(df_facilities_work_home[df_facilities_work_home.osmID.isna()],
                                     df_facilities_area,
                                     how="inner",
                                     op='intersects')
        df_facilities_na["IDOB"] = df_facilities_na["IDOB_left"]
        df_facilities_na["osmID"] = df_facilities_na["osmID_right"]
        del df_facilities_na["IDOB_left"]
        del df_facilities_na["osmID_left"]
        del df_facilities_na["Area_left"]
        del df_facilities_na["index_right"]
        del df_facilities_na["osmID_right"]
        del df_facilities_na["IDOB_right"]
        del df_facilities_na["Area_right"]
        for na_ind,na_data in df_facilities_na.iterrows():
            work_home_osm_ind = df_facilities_work_home.osmID == na_data.osmID
            work_home_idob_ind = df_facilities_work_home.IDOB == na_data.IDOB
            if np.count_nonzero(work_home_osm_ind) > 0:
                df_facilities_work_home.at[work_home_osm_ind, "Inhabitants"] = \
                    df_facilities_work_home[work_home_osm_ind]["Inhabitants"].values.sum() + na_data["Inhabitants"]
            elif np.count_nonzero(work_home_idob_ind) > 0:
                if not np.isnan(na_data["osmID"]):
                    df_facilities_work_home.at[work_home_idob_ind, "osmID"] = na_data["osmID"]
                df_facilities_work_home.at[work_home_idob_ind, "Inhabitants"] = \
                    df_facilities_work_home[work_home_idob_ind]["Inhabitants"].values.sum() + na_data["Inhabitants"]
            else:
                df_facilities_work_home.at[na_ind, "osmID"] = na_data["osmID"]
                df_facilities_work_home.at[na_ind, "IDOB"] = na_data["IDOB"]

        # For those without an area (or area without osmID) remove them
        df_facilities_work_home = df_facilities_work_home.dropna(subset=["osmID"])
        df_facilities_work_home = df_facilities_work_home.dropna(subset=["geometry"])

        # Get facilities from OpenStreetMaps in order to match primary locations and for getting secondary locations
        try:
            # Try to get already processed database
            df_facilities_osm = gpd.read_file("%s/Facilities/%s" % (context.config("data_path"),
                                                                          context.config("facilities_osm_file")),
                                                    dtypes=str)[["KOD_ZSJ", "KOD_KU", "KOD_LAU2",
                                                                 "osmID",
                                                                 "purpose", "type",
                                                                 "area", "shape", "geometry"]]
            df_facilities_osm.crs = "epsg:5514"
            df_facilities_osm.columns = ["BasicSettlementCode", "CadastralAreaCode", "TownCode",
                                         "osmID",
                                         "FacilityPurpose", "FacilityType",
                                         "Area", "Shape", "geometry"]
        except:
            # Not found database, get data from OpenStreetMaps and find the zones of each place
            df_facilities_home, \
            df_facilities_freetime, \
            df_facilities_shopping, \
            df_facilities_work, \
            df_facilities_education, \
            df_facilities_errands = context.stage("data.osm.extract_facilities")

            df_facilities_osm = pd.concat([
                df_facilities_home,
                df_facilities_freetime,
                df_facilities_shopping,
                df_facilities_work,
                # df_facilities_education, # not necessary
                df_facilities_errands
            ])
            df_facilities_osm = df_facilities_osm[["lat", "lon", "id",
                                                   "type", "tagkey", "tagvalue",
                                                   "purpose"]]
            # Transform from pandas dataframe to geoPandas dataframe
            df_facilities_osm = data.spatial.utils.toGPD(df_facilities_osm,
                                                         x='lon', y='lat', crs="epsg:4326")
            df_facilities_osm = df_facilities_osm.to_crs(epsg=5514)
            df_facilities_osm = df_facilities_osm[["id",
                                                   "type", "tagkey", "tagvalue",
                                                   "purpose",
                                                   "geometry"]]

            df_facilities_osm = df_facilities_osm.rename(columns={'id': 'osmID'})
            df_facilities_osm = df_facilities_osm.rename(columns={'type': 'osmType'})
            df_facilities_osm = df_facilities_osm.rename(columns={'tagkey': 'osmKey'})
            df_facilities_osm = df_facilities_osm.rename(columns={'tagvalue': 'osmValue'})

            # Define FacilityType
            df_facilities_osm["type"] = df_facilities_osm["osmValue"].copy()
            df_facilities_osm.loc[df_facilities_osm["osmKey"].isin(['leisure', 'natural', 'tourism', 'hiking']),
                                  "type"] = 'park'
            df_facilities_osm.loc[df_facilities_osm["osmKey"].isin(['shop']), "type"] = 'shop'
            df_facilities_osm.loc[df_facilities_osm["osmKey"].isin(['office']), "type"] = 'office'

            # Merge FacilityPurpose for duplicated osmID
            duplicated_osmIDs = df_facilities_osm[df_facilities_osm.duplicated("osmID", keep=False)].osmID.unique()
            new_df_facilities_osm = pd.DataFrame(columns=df_facilities_osm.columns)
            cols_no_purpose = list(df_facilities_osm.columns)
            cols_no_purpose.remove("purpose")

            progress = tqdm(duplicated_osmIDs, total=len(duplicated_osmIDs),
                            desc="Merging FacilityPurpose for duplicated osmID")
            for duplicated_osmID in progress:
                duplicated_indices = df_facilities_osm.osmID == duplicated_osmID
                purposes = set(df_facilities_osm[duplicated_indices].purpose)
                facility_data = df_facilities_osm[duplicated_indices].drop_duplicates(subset=cols_no_purpose)
                facility_data["purpose"] = ",".join(sorted(purposes))
                new_df_facilities_osm = pd.concat([new_df_facilities_osm,
                                                   facility_data],
                                                  axis=0, ignore_index=True, sort=False)
                progress.update()

            not_duplicated_data = df_facilities_osm.loc[~df_facilities_osm.osmID.isin(duplicated_osmIDs)]
            df_facilities_osm = pd.concat([not_duplicated_data,
                                           new_df_facilities_osm],
                                          axis=0, ignore_index=True, sort=False)

            # Get area of OpenStreetMaps facilities
            # For those OSM facilities with known area (usually buildings), match them
            df_facilities_osm = df_facilities_osm.merge(df_facilities_area, on='osmID', how = "left")
            df_facilities_osm["geometry"] = df_facilities_osm["geometry_x"]
            df_facilities_osm["Shape"] = df_facilities_osm["geometry_y"]
            del df_facilities_osm["geometry_x"]
            del df_facilities_osm["geometry_y"]
            df_facilities_osm = df_facilities_osm.dropna(subset=["geometry"])
            df_facilities_osm = gpd.GeoDataFrame(df_facilities_osm)
            df_facilities_osm.crs = "epsg:5514"
            # For those OSM facilities with unknown area (shops, services, restaurants, etc. inside buildings),
            # add their purposes to the known OSM facilities (when they are inside the area), and remove them from the list
            # Notice that OSM facilities outside buildings (e.g. parks,) remain,
            # but their visitor's capacity will be infinite and will have not inhabitants neither workplaces
            known_osmIDs = df_facilities_osm[~df_facilities_osm["Area"].isna()].osmID
            unknown_osmIDs = df_facilities_osm[df_facilities_osm["Area"].isna()].osmID

            area_data = df_facilities_area[df_facilities_area["osmID"].isin(known_osmIDs)][["osmID", "geometry"]].copy()
            known_osm_data = df_facilities_osm[df_facilities_osm["osmID"].isin(known_osmIDs)][["osmID", "purpose"]].copy()
            unknown_osm_data = df_facilities_osm[df_facilities_osm["osmID"].isin(unknown_osmIDs)][["osmID", "purpose",
                                                                                                   "geometry"]].copy()
            new_known_purposes = dict()
            progress = tqdm(unknown_osm_data.geometry.items(), total=len(unknown_osm_data),
                                             desc="Assigning the purposes of places inside buildings "
                                                  "to buildings' purposes")
            for ind,unknown_osm_geometry in progress:
                try:
                    assigned_osmID = \
                        area_data.osmID.iloc[np.where(area_data.geometry.contains(unknown_osm_geometry))[0]].values[0]
                    unknown_purpose = unknown_osm_data.purpose[ind].split(",")
                    try:
                        new_known_purposes[assigned_osmID].update(unknown_purpose)
                    except KeyError:
                        old_purposes = set(known_osm_data.loc[known_osm_data.osmID
                                                              == assigned_osmID].purpose.values[0].split(","))
                        new_known_purposes[assigned_osmID] = old_purposes.union(unknown_purpose)
                    df_facilities_osm.drop(df_facilities_osm.index[df_facilities_osm.osmID
                                                                   == unknown_osm_data.osmID[ind]], inplace = True)
                except IndexError:
                    skip = 1

                progress.update()

            for assigned_osmID in new_known_purposes:
                df_facilities_osm.at[df_facilities_osm.index[df_facilities_osm.osmID == assigned_osmID].values[0],
                                     "purpose"] = tuple(sorted(new_known_purposes[assigned_osmID]))

            df_facilities_osm["purpose"] = df_facilities_osm["purpose"].apply(lambda x: ','.join([str(i) for i in x])
                                                                                        if type(x) == tuple else x)

            print("Defining OSM facilities BasicSettlementCode")
            df_facilities_osm = gpd.overlay(df_facilities_osm, df_zsj_zones, how='intersection')
            df_facilities_osm = df_facilities_osm.rename(columns={'ZoneID': 'KOD_ZSJ'})
            print("Defining OSM facilities CadastralCode")
            df_facilities_osm = gpd.overlay(df_facilities_osm, df_ku_zones, how='intersection')
            df_facilities_osm = df_facilities_osm.rename(columns={'ZoneID': 'KOD_KU'})
            print("Defining OSM facilities TownCode")
            df_facilities_osm = gpd.overlay(df_facilities_osm, df_obec_zones, how='intersection')
            df_facilities_osm = df_facilities_osm.rename(columns={'ZoneID': 'KOD_LAU2'})

            # Save dataframe
            print("Saving facilities_osm_file")
            df_facilities_osm.Shape = df_facilities_osm.Shape.astype(str)
            df_facilities_osm = df_facilities_osm.rename(columns={'Area': 'area'})
            df_facilities_osm = df_facilities_osm.rename(columns={'Shape': 'shape'})
            df_facilities_osm.to_file("%s/Facilities/%s" % (context.config("data_path"),
                                                            context.config("facilities_osm_file")))
            print("Saved facilities_osm_file")

            df_facilities_osm = df_facilities_osm[["KOD_ZSJ", "KOD_KU", "KOD_LAU2",
                                                   "osmID",
                                                   "purpose", "type",
                                                   "area", "shape", "geometry"]]
            df_facilities_osm.columns = ["BasicSettlementCode", "CadastralAreaCode", "TownCode",
                                         "osmID",
                                         "FacilityPurpose", "FacilityType",
                                         "Area", "Shape", "geometry"]

        df_facilities_osm["FacilityPurpose"] = df_facilities_osm["FacilityPurpose"].apply(lambda x: tuple(x.split(",")))
        df_facilities_osm["FacilityUsage"] = '99' # OSM data don't follow RUIAN codes, make usage as any
        df_facilities_osm["FacilityUsage"] = df_facilities_osm["FacilityUsage"].apply(lambda x: tuple(x.split(",")))
        df_facilities_osm["StudyPlaces"] = 0 # OSM data don't provide such information
        df_facilities_osm["Inhabitants"] = 0 # OSM data don't provide such information

        # Define work and home location facilities and attributes from Facilities Census
        required_purposes = {"1", "4"}
        ids = df_facilities_osm["FacilityPurpose"].apply(
            lambda x: True if (type(x) == tuple and x == ("99",) or len(set(x).intersection(required_purposes)))
                              or (type(x) != tuple and x == "99" or x in required_purposes) > 0
            else False)
        df_facilities_osm_work_home = df_facilities_osm[ids]

        # Match home and work locations between Facility Census and OpenStreetMaps
        nearest = ckdnearest(df_facilities_work_home, df_facilities_osm_work_home)
        matching_osmIDs = df_facilities_work_home["osmID"] == nearest["osmID"]
        not_matching_osmIDs = (df_facilities_work_home["osmID"] != nearest["osmID"]) & (nearest["dist"] < 10)
        df_facilities_work_home["FacilityType"] = '99'
        df_facilities_work_home.loc[matching_osmIDs, "FacilityType"] = nearest[matching_osmIDs]["FacilityType"]
        df_facilities_work_home.loc[not_matching_osmIDs, "FacilityType"] = nearest[not_matching_osmIDs]["FacilityType"]

        # Merge FacilityPurpose when same osmID, all the rest get from df_facilities_work_home
        matched_osmIDs = set(df_facilities_osm.osmID).intersection(set(df_facilities_work_home.osmID))
        shorter_all_cols = all_cols[:]
        shorter_all_cols.remove("WorkPlaces")
        shorter_all_cols.remove("Visitors")
        matched_work_home_secondary = pd.DataFrame(columns=shorter_all_cols)

        progress = tqdm(enumerate(matched_osmIDs), total=len(matched_osmIDs),
                        desc="Merging FacilityPurpose when same osmID at work_home and secondary facilities")
        for ind_matched, matched_osmID in progress:
            ind_work_home = df_facilities_work_home.index[df_facilities_work_home.osmID == matched_osmID]
            ind_osm = df_facilities_osm.index[df_facilities_osm.osmID == matched_osmID]
            matched_work_home_secondary = pd.concat([matched_work_home_secondary,
                                                     df_facilities_work_home.loc[ind_work_home]],
                                                      axis=0, ignore_index=True, sort=False)
            work_home_purposes = set(df_facilities_work_home["FacilityPurpose"].loc[ind_work_home].values.all())
            secondary_purposes = set(df_facilities_osm["FacilityPurpose"].loc[ind_osm].values.all())
            matched_work_home_secondary.at[ind_matched, "FacilityPurpose"] = \
                tuple(sorted(work_home_purposes.union(secondary_purposes)))
            progress.update()

        unmatched_work_home = df_facilities_work_home.loc[~df_facilities_work_home.osmID.isin(matched_osmIDs)]
        unmatched_osm = df_facilities_osm.loc[~df_facilities_osm.osmID.isin(matched_osmIDs)]
        df_facilities_work_home_secondary = pd.concat([matched_work_home_secondary,
                                                       unmatched_work_home,
                                                       unmatched_osm],
                                                      axis=0, ignore_index=True, sort=False)

        # Write down what each facility offers based on their facility purposes
        required_purposes = {"1"}
        df_facilities_work_home_secondary["offers_home"] = df_facilities_work_home_secondary["FacilityPurpose"].apply(
            lambda x: True if x == ("99",) or len(set(x).intersection(required_purposes)) > 0
            else False)

        required_purposes = {"2"}
        df_facilities_work_home_secondary["offers_freetime"] = df_facilities_work_home_secondary["FacilityPurpose"].apply(
            lambda x: True if x == ("99",) or len(set(x).intersection(required_purposes)) > 0
            else False)

        required_purposes = {"3"}
        df_facilities_work_home_secondary["offers_shopping"] = df_facilities_work_home_secondary["FacilityPurpose"].apply(
            lambda x: True if x == ("99",) or len(set(x).intersection(required_purposes)) > 0
            else False)

        required_purposes = {"4"}
        df_facilities_work_home_secondary["offers_work"] = df_facilities_work_home_secondary["FacilityPurpose"].apply(
            lambda x: True if x == ("99",) or len(set(x).intersection(required_purposes)) > 0
            else False)

        df_facilities_work_home_secondary["offers_education"] = False

        required_purposes = {"6"}
        df_facilities_work_home_secondary["offers_errands"] = df_facilities_work_home_secondary["FacilityPurpose"].apply(
            lambda x: True if x == ("99",) or len(set(x).intersection(required_purposes)) > 0
            else False)

        # Get the number of workplaces and visitors per FacilityType
        df_buildings_occupancy = pd.read_excel("%s/Facilities/%s" % (context.config("data_path"),
                                                          context.config("buildings_occupancy_file")),
                                 header=0,
                                 # encoding="cp1250",
                                 dtype='str')[["type", "workers", "visitors"]].drop_duplicates()
        df_buildings_occupancy.columns = ["FacilityType", "WorkPlacesPerArea", "VisitorsPerArea"]
        df_facilities_work_home_secondary = pd.merge(df_facilities_work_home_secondary, df_buildings_occupancy,
                                                     on="FacilityType", how="left")
        df_facilities_work_home_secondary["WorkPlacesPerArea"] = \
            df_facilities_work_home_secondary["WorkPlacesPerArea"].astype(float)
        df_facilities_work_home_secondary["VisitorsPerArea"] = \
            df_facilities_work_home_secondary["VisitorsPerArea"].astype(float)

        # Calculate Workplaces based on FacilityType
        df_facilities_work_home_secondary["WorkPlaces"] = 0
        df_facilities_work_home_secondary["WorkPlaces"][df_facilities_work_home_secondary["offers_work"]] = \
            df_facilities_work_home_secondary["WorkPlacesPerArea"][df_facilities_work_home_secondary["offers_work"]] \
            * df_facilities_work_home_secondary["Area"][df_facilities_work_home_secondary["offers_work"]]
        df_facilities_work_home_secondary["WorkPlaces"] = df_facilities_work_home_secondary["WorkPlaces"].fillna(float("inf"))

        # Calculate Visitors based on FacilityType
        df_facilities_work_home_secondary["Visitors"] = 0
        offers_secondary = (df_facilities_work_home_secondary["offers_freetime"]
                            | df_facilities_work_home_secondary["offers_shopping"]
                            | df_facilities_work_home_secondary["offers_errands"])
        df_facilities_work_home_secondary["Visitors"][offers_secondary] = \
            df_facilities_work_home_secondary["VisitorsPerArea"] * df_facilities_work_home_secondary["Area"]
        df_facilities_work_home_secondary["Visitors"] = df_facilities_work_home_secondary["Visitors"].fillna(float("inf"))

        del df_facilities_work_home_secondary["WorkPlacesPerArea"]
        del df_facilities_work_home_secondary["VisitorsPerArea"]

        # Merge Work, Home and Secondary facilities
        df_facilities_work_home_secondary["EducationPlace"] = [('99',)] * len(df_facilities_work_home_secondary)
        df_facilities_work_home_secondary = df_facilities_work_home_secondary[all_cols]

        df_facilities_work_home_secondary = gpd.GeoDataFrame(df_facilities_work_home_secondary,
                                                             geometry=df_facilities_work_home_secondary.geometry)
        df_facilities_work_home_secondary.crs = "epsg:5514"

        print("Saving facilities_work_home_secondary_file")
        df_facilities_work_home_secondary.Shape = df_facilities_work_home_secondary.Shape.astype(str)
        copied_df_facilities_work_home_secondary = df_facilities_work_home_secondary.copy()
        copied_df_facilities_work_home_secondary.columns = ["KOD_ZSJ", "KOD_KU", "KOD_LAU2",
                                                      "IDOB", "osmID",
                                                      "purpose", "usage", "type", "place",
                                                      'area', "shape", "population", "workers", "students",
                                                      "visitors",
                                                      'home', 'work', 'education',
                                                      'freetime', 'shopping', 'errands',
                                                      "geometry"]

        # Transform tuple to string in order to save
        copied_df_facilities_work_home_secondary["purpose"] = \
            copied_df_facilities_work_home_secondary["purpose"].apply(lambda x: ', '.join([str(i) for i in x]))
        copied_df_facilities_work_home_secondary["usage"] = \
            copied_df_facilities_work_home_secondary["usage"].apply(lambda x: ', '.join([str(i) for i in x])
                                                             if type(x) == tuple else x)
        copied_df_facilities_work_home_secondary["place"] = \
            copied_df_facilities_work_home_secondary["place"].apply(lambda x: ', '.join([str(i) for i in x]))

        # Transform each element of a list-like to a row, replicating index values
        copied_df_facilities_work_home_secondary = copied_df_facilities_work_home_secondary.explode()
        copied_df_facilities_work_home_secondary.to_file("%s/Facilities/%s" % (context.config("data_path"),
                                                                        context.config(
                                                                            "facilities_work_home_secondary_file")))
        print("Saved facilities_work_home_secondary_file")

    # Merge and clean facility databases
    df_facilities = pd.concat([df_facilities_work_home_secondary, df_facilities_edu],
                              axis=0, ignore_index=True, sort=False)
    df_facilities = df_facilities.dropna(subset=["BasicSettlementCode", "TownCode",
                                                 'CadastralAreaCode', "FacilityPurpose"], inplace=False).copy()
    df_facilities["BasicSettlementCode"] = df_facilities["BasicSettlementCode"].astype(int)
    df_facilities["CadastralAreaCode"] = df_facilities["CadastralAreaCode"].astype(int)
    df_facilities["TownCode"] = df_facilities["TownCode"].astype(int)

    df_facilities["BasicSettlementCode"] = df_facilities["BasicSettlementCode"].astype(str) + '0'
    df_facilities["CadastralAreaCode"] = df_facilities["CadastralAreaCode"].astype(str)
    df_facilities["TownCode"] = df_facilities["TownCode"].astype(str)

    # Municipalities data
    df_facilities.loc[(df_facilities["TownCode"] != '554804'), "ZoneID"] = df_facilities["TownCode"]

    # Ustí city data
    df_facilities.loc[(df_facilities["TownCode"] == '554804'), "ZoneID"] = df_facilities["CadastralAreaCode"]

    df_facilities.drop(columns=["TownCode"])
    df_facilities.drop(columns=["CadastralAreaCode"])

    df_facilities = df_facilities.dropna(subset=["ZoneID"], inplace=False).copy()

    # Add one facility for any attributes and without capacities on the synthetic gates of Ustí nad Labem district
    # for trips outside the study area
    for _,gate_data in tqdm(df_zones_gates.iterrows(), total=len(df_zones_gates),
                             desc="Adding one facility for each FacilityPurpose and FacilityUsage on the gates of the "
                                  "study area"):
        gateID = gate_data["ZoneID"]
        df_facilities = df_facilities.append({
            "BasicSettlementCode": gateID,
            "ZoneID": gateID,
            "IDOB": np.nan,
            "osmID": np.nan,
            "FacilityPurpose": ('99',),
            "FacilityUsage": ('99',),
            "FacilityType": 99,
            "EducationPlace": ('99',),
            'Area': np.nan,
            'Shape': np.nan,
            "Inhabitants": float("inf"),
            "WorkPlaces": float("inf"),
            "StudyPlaces": float("inf"),
            "Visitors": float("inf"),
            'offers_home': True,
            'offers_work': True,
            'offers_education': True,
            'offers_freetime': True,
            'offers_shopping': True,
            'offers_errands': True,
            "geometry": gate_data["geometry"]
            # "BuildingType": 99, # 99 for any type # not at the moment
            # "ApartmentType" 99: # 9 for any type # not at the moment
        },
            ignore_index=True)


    # Define the facility IDs
    df_facilities["LocationID"] = np.arange(len(df_facilities))
    df_facilities["LocationID"] = df_facilities["LocationID"].astype(str)

    # Clean up
    df_facilities = df_facilities[["LocationID",
                                   "BasicSettlementCode", "ZoneID",
                                   "FacilityPurpose", "FacilityUsage", "FacilityType", "EducationPlace",
                                   # "BuildingType", "ApartmentType" # not at the moment
                                   "Inhabitants", "WorkPlaces", "StudyPlaces", "Visitors",
                                   'offers_home', 'offers_work', 'offers_education',
                                   'offers_freetime', 'offers_shopping', 'offers_errands',
                                   "Shape", "geometry"]]

    print("Saving all_locations.csv")

    df_facilities.to_csv("%s/Locations/all_locations.csv" % context.config("output_path"))
    print("Saving all_locations.gpkg")
    copied_df_facilities = df_facilities.copy()
    copied_df_facilities["FacilityPurpose"] = copied_df_facilities["FacilityPurpose"].apply(
        lambda x: ', '.join([str(i) for i in x]))
    copied_df_facilities["FacilityUsage"] = copied_df_facilities["FacilityUsage"].apply(
        lambda x: ', '.join([str(i) for i in x])
        if type(x) == tuple else x)
    copied_df_facilities["EducationPlace"] = copied_df_facilities["EducationPlace"].apply(
        lambda x: ', '.join([str(i) for i in x]))
    copied_df_facilities.to_file("%s/Locations/all_locations.gpkg" % context.config("output_path"), driver="GPKG")

    print("Saved all_locations files")

    return df_facilities
