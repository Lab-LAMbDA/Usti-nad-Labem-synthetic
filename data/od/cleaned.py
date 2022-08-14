import pandas as pd
import numpy as np
import os
from data import commonFunctions
import warnings

def configure(context):
    context.config("data_path")
    context.stage("data.spatial.zones")
    context.stage("data.hts.filtered")
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
    
    # Ignore header warning when reading excel files
    warnings.filterwarnings('ignore', category=UserWarning, module='openpyxl')

    print("Compute OD proportions between zones")

    # Get gates' artificial zones
    zone_id_gates = pd.read_excel("%s/%s" % (context.config("data_path"), context.config("routes_file")),
                                   header=0,
                                  # encoding="cp1250",
                                  dtype=str)[["GATEosm_id"]].drop_duplicates().dropna().astype(int)
    zone_id_gates = set(np.unique(zone_id_gates["GATEosm_id"]))

    # Get zones and HTS
    df_zones_municipalities, df_zones_cadastral_city = context.stage("data.spatial.zones")[:2]
    df_persons_CzechiaHTS, df_persons_CityHTS = context.stage("data.hts.filtered")[:2]
    df_trips_CzechiaHTS, df_trips_CityHTS = context.stage("data.hts.filtered")[2:]
    zone_id_CzechiaHTS = set(np.unique(df_zones_municipalities["ZoneID"].astype(int)))
    zone_id_CityHTS = set(np.unique(df_zones_cadastral_city["ZoneID"].astype(int)))
    zone_ids = zone_id_CzechiaHTS.union(zone_id_CityHTS, zone_id_gates)

    # Get work trips
    df_trips_persons_CzechiaHTS = pd.merge(df_trips_CzechiaHTS, df_persons_CzechiaHTS, on=["PersonID"], how='left')
    df_trips_persons_CzechiaHTS = df_trips_persons_CzechiaHTS[(df_trips_persons_CzechiaHTS["DestPurpose"] == '4')]  # select work
    df_trips_persons_CzechiaHTS.drop_duplicates(subset=["PersonID"])
    df_trips_persons_CzechiaHTS = df_trips_persons_CzechiaHTS.rename(columns={'TownCode': 'OriginID'}).copy()
    df_trips_persons_CzechiaHTS = df_trips_persons_CzechiaHTS.rename(columns={'DestTownCode': 'DestID'}).copy()

    df_trips_persons_CityHTS = pd.merge(df_trips_CityHTS, df_persons_CityHTS, on=["PersonID"], how='left')
    df_trips_persons_CityHTS = df_trips_persons_CityHTS[(df_trips_persons_CityHTS["DestPurpose"] == '4')]  # select work
    df_trips_persons_CityHTS.drop_duplicates(subset=["PersonID"])
    df_trips_persons_CityHTS = df_trips_persons_CityHTS.rename(columns={'CadastralAreaCode': 'OriginID'}).copy()
    df_trips_persons_CityHTS = df_trips_persons_CityHTS.rename(columns={'DestCadastralAreaCode': 'DestID'}).copy()

    df_trips_persons_CzechiaHTS = df_trips_persons_CzechiaHTS.groupby(["OriginID", "DestID"]).sum()["Weight"].reset_index()
    df_trips_persons_CityHTS = df_trips_persons_CityHTS.groupby(["OriginID", "DestID"]).sum()["Weight"].reset_index()

    df_work = pd.concat([df_trips_persons_CzechiaHTS, df_trips_persons_CityHTS], sort=True)

    # Get study trips
    df_trips_persons_CzechiaHTS = pd.merge(df_trips_CzechiaHTS, df_persons_CzechiaHTS, on=["PersonID"], how='left')
    df_trips_persons_CzechiaHTS = df_trips_persons_CzechiaHTS[(df_trips_persons_CzechiaHTS["DestPurpose"] == '5')]  # select education
    df_trips_persons_CzechiaHTS.drop_duplicates(subset=["PersonID"])
    df_trips_persons_CzechiaHTS = df_trips_persons_CzechiaHTS.rename(columns={'TownCode': 'OriginID'}).copy()
    df_trips_persons_CzechiaHTS = df_trips_persons_CzechiaHTS.rename(columns={'DestTownCode': 'DestID'}).copy()

    df_trips_persons_CityHTS = pd.merge(df_trips_CityHTS, df_persons_CityHTS, on=["PersonID"], how='left')
    df_trips_persons_CityHTS = df_trips_persons_CityHTS[(df_trips_persons_CityHTS["DestPurpose"] == '5')]  # select education
    df_trips_persons_CityHTS.drop_duplicates(subset=["PersonID"])
    df_trips_persons_CityHTS = df_trips_persons_CityHTS.rename(columns={'CadastralAreaCode': 'OriginID'}).copy()
    df_trips_persons_CityHTS = df_trips_persons_CityHTS.rename(columns={'DestCadastralAreaCode': 'DestID'}).copy()

    df_education = pd.concat([df_trips_persons_CzechiaHTS, df_trips_persons_CityHTS], sort=True)
    
    # Compute totals
    df_work_totals = df_work[["OriginID", "Weight"]].groupby("OriginID").sum().reset_index()
    df_work_totals["Total"] = df_work_totals["Weight"]
    del df_work_totals["Weight"]

    df_education_totals = df_education[["OriginID", "Weight"]].groupby("OriginID").sum().reset_index()
    df_education_totals["Total"] = df_education_totals["Weight"]
    del df_education_totals["Weight"]

    # Impute totals
    #df_work = pd.merge(df_work, df_work_totals, on = ["OriginID", "TripMainMode"]) # so far ODs not based on modes, but rather aggregated
    df_work = pd.merge(df_work, df_work_totals, on = "OriginID")
    df_education = pd.merge(df_education, df_education_totals, on = "OriginID")

    # Compute probabilities
    df_work["Weight"] /= df_work["Total"]
    df_education["Weight"] /= df_education["Total"]

    assert(sum(df_work_totals["Total"] == 0.0) == 0)
    assert(sum(df_education_totals["Total"] == 0.0) == 0)

    # Cleanup
    df_work = df_work[["OriginID", "DestID", "Weight"]]
    df_education = df_education[["OriginID", "DestID", "Weight"]]

    # Fix missing zones
    existing_work_ids = set(np.unique(df_work["OriginID"]))
    missing_work_ids = zone_ids - existing_work_ids
    existing_education_ids = set(np.unique(df_education["OriginID"]))
    missing_education_ids = zone_ids - existing_education_ids

    # Distribute evenly the proportion of missing zones
    work_rows = []
    for origin_id in missing_work_ids:
        work_rows.append((origin_id, origin_id, 1.0 / len(missing_work_ids)))
    df_work = pd.concat([df_work, pd.DataFrame.from_records(work_rows,
                                                            columns=["OriginID", "DestID", "Weight"])],
                        sort=True)
    education_rows = []
    for origin_id in missing_education_ids:
        education_rows.append((origin_id, origin_id, 1.0 / len(existing_education_ids)))
    df_education = pd.concat([df_education, pd.DataFrame.from_records(education_rows,
                                                                      columns=["OriginID", "DestID",
                                                                               "Weight"])], sort=True)
   
    df_total = df_work[["OriginID", "Weight"]].groupby("OriginID").sum().rename({"Weight": "Total"}, axis = 1)
    df_work = pd.merge(df_work, df_total, on = "OriginID")
    df_work["Weight"] /= df_work["Total"]
    del df_work["Total"]
    df_total = df_education[["OriginID", "Weight"]].groupby("OriginID").sum().rename({"Weight": "Total"}, axis=1)
    df_education = pd.merge(df_education, df_total, on="OriginID")
    df_education["Weight"] /= df_education["Total"]
    del df_education["Total"]

    # Save OD proportions
    df_work = df_work.astype({'OriginID': 'str'})
    df_work = df_work.astype({'DestID': 'str'})
    df_education = df_education.astype({'OriginID': 'str'})
    df_education = df_education.astype({'DestID': 'str'})

    df_work.to_csv("%s/ODs/od_work.csv" % context.config("output_path"))
    commonFunctions.toXML(df_work, "%s/ODs/od_work.xml" % context.config("output_path"))
    df_education.to_csv("%s/ODs/od_edu.csv" % context.config("output_path"))
    commonFunctions.toXML(df_education, "%s/ODs/od_edu.xml" % context.config("output_path"))

    return df_work, df_education
