import sys

from tqdm import tqdm
import pandas as pd
import numpy as np
import os
import shapely.geometry as geo
import geopandas as gpd
from sklearn.neighbors import KDTree
from data import commonFunctions

# Define globals
SAMPLE_SIZE = 1000
DIST_POINTS = 5000
ALL_FACILITY_USAGES = {'0', '1', '2', '3', '4', '5', '6', '7', '8', '9'}
ALL_EDUCATION_PLACES = {'0', '1', '2', '3', '4'}

def configure(context):
    context.config("data_path")
    context.config("generalizations_file")
    context.stage("synthesis.population.spatial.by_person.primary_zones")
    context.stage("data.spatial.zones")
    context.stage("synthesis.destinations")
    context.stage("synthesis.population.sociodemographics")
    context.stage("synthesis.population.sampled")
    context.stage("synthesis.population.trips")
    context.config("processes")
    context.stage("data.hts.cleaned")
    context.config("output_path")

def validate(context):
    data_path = context.config("data_path")
    output_path = context.config("output_path")
    generalizations_file = "%s/%s" % (context.config("data_path"), context.config("generalizations_file"))

    if not os.path.isdir(data_path):
        raise RuntimeError("Input directory must exist: %s" % data_path)

    if not os.path.isdir(output_path):
        raise RuntimeError("Output directory must exist: %s" % output_path)

    if not os.path.exists(generalizations_file):
        raise RuntimeError("Input file must exist: %s" % generalizations_file)

def assign_agents(df_persons, commute_coordinates, commute_caps, purpose, progress):

    if "HomeX" in df_persons.columns:
        # If known where is the home location, ordering based on the primary location distance from home and capacities
        home_coordinates = df_persons[["HomeX", "HomeY"]].values
        commute_distances = df_persons["PrimaryLocCrowFliesTripDist"].values
        try:
            activities = df_persons["Activity"].values
        except KeyError:
            try:
                activities = df_persons["ActivityCityHTS"].values
            except KeyError:
                activities = df_persons["ActivityCzechiaHTS"].values
        indices,commute_caps = heuristic_primary_ordering(home_coordinates, commute_coordinates, activities,
                                                          commute_caps, commute_distances, purpose, progress)
        return indices,commute_caps
    else:
        # If not know where is the home location, ordering only based on the capacities
        num_persons = len(df_persons["PersonID"])
        indices,commute_caps = heuristic_home_ordering(num_persons, commute_caps, progress)
        return indices,commute_caps

def heuristic_home_ordering(num_persons, home_caps, progress):
    indices = []

    for _ in range(0, num_persons):
        # Select the home location with the highest number of available capacity
        # (i.e. the highest number of inhabitants not assigned yet)
        index = np.argmax(home_caps)
        # Reduce the number of available capacity (i.e. number of inhabitants not assigned yet)
        home_caps[index] -= 1
        indices.append(index)
        progress.update(1)

    return indices, home_caps

def heuristic_primary_ordering(home_coordinates, commute_coordinates, activities, commute_caps, commute_distances,
                               purpose, progress):

    indices = []

    for home_coordinate, commute_distance, activity in zip(home_coordinates, commute_distances, activities):
        # Calculate the distance of the filtered facilities to home location
        distances = np.sqrt(np.sum((commute_coordinates - home_coordinate)**2, axis = 1))
        # Set up the cost as the difference between the facility distance (from home location) with the crow flies distance
        costs = np.abs(distances - commute_distance)
        # For those facilities that don't have available capacity yet, set their cost as infinite
        costs[commute_caps < 1] = np.inf
        if min(costs) == np.inf:
            # If all filtered facilities have no available capacity, distribute evenly new assignments among them
            index = np.argmax(commute_caps)
        else:
            # If at least one filtered facility with available capacity, choose the one with lowest cost
            index = np.argmin(costs)
        # Reduce the number of available capacity (i.e. number of persons not assigned yet)
        # For people with education trips but not mainly students (i.e. parents taking kids to school), don't reduce
        if purpose != "education" or activity == '2':
            commute_caps[index] -= 1
        indices.append(index)
        progress.update(1)

    return indices,commute_caps

def impute_diff_zone_locations(df_persons, df_zones, df_locations, purpose):

    df_counts = df_persons[["ZoneID"]].groupby("ZoneID").size().reset_index(name="count")
    df_zones = pd.merge(df_zones, df_counts, on = "ZoneID", how = "inner").drop_duplicates(subset=["ZoneID"])
    df_impute = df_zones[["ZoneID", "count", "geometry"]].values

    person_dfs = []

    all_counts = sum([zone_count for zone_count in df_counts["count"]])

    progress = tqdm(df_impute, position=0, leave=False, total=all_counts, ascii=True)
    progress.set_description("Sampling coordinates for different zones")
    for zone_id, count, shape in progress:
        if count > 0:
            # If at least one person/agent to be assigned location
            points = []
            ids = []

            if df_locations is None or np.count_nonzero(df_locations["ZoneID"] == zone_id) == 0:
                no_facilities = True
                # When no filtered facility in certain zone,
                # assign people/agents to random points inside the zone of the primary location
                minx, miny, maxx, maxy = shape.bounds
                num_points = int(shape.area / DIST_POINTS) # 1 point per 5 square kilometres (time consuming process)
                counter = 0
                while len(points) < num_points:
                    candidates = np.random.random(size=(SAMPLE_SIZE, 2))
                    candidates[:, 0] = minx + candidates[:, 0] * (maxx - minx)
                    candidates[:, 1] = miny + candidates[:, 1] * (maxy - miny)
                    candidates = [geo.Point(*point) for point in candidates]
                    candidates = [(str(zone_id) + "_" + str(ind), point) for ind, point in enumerate(candidates)
                                  if shape.contains(point)]
                    candidatesIDs, candidates = zip(*candidates)
                    counter += len(candidates)
                    points += candidates
                    ids += candidatesIDs
                    progress.set_postfix({'Status': "ZoneID: " + zone_id +
                                                    " - Generated " + str(counter) + " of " + str(num_points)})

                points, ids = points[:num_points], ids[:num_points]
                points = np.array([np.array([point.x, point.y]) for point in points])
                ids = np.array([str(zone_id) + "_" + str(ind) for ind in range(len(points))])
                caps = np.array([float('inf') for _ in range(len(points))])
            else:
                # When there are filtered facilities in the zone, assign agents/people to them
                no_facilities = False
                df_zone_locations = df_locations[df_locations["ZoneID"] == zone_id]

                points = pd.DataFrame(columns=['x', 'y'])
                locations = df_zone_locations['geometry']
                counter = 0
                for point in locations.geometry:
                    counter += 1
                    try:
                        points = pd.concat([points, pd.DataFrame(data={'x': [point.x], 'y': [point.y]})],
                                           sort=False)
                    except AttributeError:
                        # If facility's geometry is an area/polygon, use its centroid
                        points = pd.concat([points, pd.DataFrame(data={'x': [point.centroid.x],
                                                                       'y': [point.centroid.y]})], sort=False)

                    progress.set_postfix({'Status': "ZoneID: " + zone_id +
                                                    " - Retrieved " + str(counter) + " of " + str(len(locations))})
                points = points.values
                ids = df_zone_locations["LocationID"].values
                # Define which attribute to use as capacity of the facility
                if purpose == "home":
                    caps = df_zone_locations["Inhabitants"].values
                elif purpose == "work":
                    caps = df_zone_locations["WorkPlaces"].values
                else:
                    caps = df_zone_locations["StudyPlaces"].values

            f = df_persons["ZoneID"] == zone_id

            # Assign the locations (either real ones or random points when none available)
            indices,caps = assign_agents(df_persons[f], points, caps, purpose, progress)

            df_persons.loc[f, "x"] = points[indices, 0]
            df_persons.loc[f, "y"] = points[indices, 1]
            df_persons.loc[f, "LocationID"] = ids[indices]

            assert not df_persons[f].isnull().any()['x']
            assert not df_persons[f].isnull().any()['y']

            person_dfs.append(df_persons[f])

            if no_facilities is False:
                # If there were filtered facilities in the zone, update their available capacities
                indx = pd.Series(ids).apply(lambda x:
                                            df_locations["LocationID"].index[df_locations["LocationID"]
                                                                             == x].values[0]).tolist()

                if purpose == "home":
                    df_locations.at[indx, "Inhabitants"] = caps
                elif purpose == "work":
                    df_locations.at[indx, "WorkPlaces"] = caps
                else:
                    df_locations.at[indx, "StudyPlaces"] = caps

    sys.stdout.write("\r") # Clean tqdm progress

    if len(person_dfs) > 0:
        return pd.concat(person_dfs), df_locations
    else:
        return pd.DataFrame(), df_locations

def impute_primary_locations_same_zone(hts_trips, df_ag, df_candidates, purpose):

    with tqdm(total=len(df_ag), desc="Sampling coordinates same zones",
              leave=False, position=0, ascii=True) as progress:

        hts_trip = hts_trips.copy()

        # Assign a given radius to each person based on a histogram of all trips
        # from the chosen HTS survey (CityHTS or CzechiaHTS)
        hist_cp, bins_cp = np.histogram(hts_trip["PrimaryLocCrowFliesTripDist"], weights = hts_trip["Weight"], bins = 500)

        df_agents = df_ag.copy()
        df_agents_cp = df_agents

        home_coordinates_cp = list(zip(df_agents_cp["HomeX"], df_agents_cp["HomeY"]))

        dest_coordinates = pd.DataFrame(columns=['x', 'y'])
        dest_cap = []
        if purpose == "work":
            locations_cap = df_candidates[['geometry', "WorkPlaces"]]
        else:
            locations_cap = df_candidates[['geometry', "StudyPlaces"]]
        for _,location in locations_cap.iterrows():
            point = location['geometry']
            if purpose == "work":
                dest_cap.append(location['WorkPlaces'])
            else:
                dest_cap.append(location['StudyPlaces'])


            try:
                dest_coordinates = pd.concat([dest_coordinates, pd.DataFrame(data={'x': [point.x], 'y': [point.y]})],
                                             sort=False)
            except AttributeError:
               # If an area/polygon, get the centroid
               dest_coordinates = pd.concat([dest_coordinates, pd.DataFrame(data={'x': [point.centroid.x],
                                                                                  'y': [point.centroid.y]})],
                                            sort=False)

        # Order the facilities based on the proximity to the assigned radius
        bin_midpoints = bins_cp[:-1] + np.diff(bins_cp)/2
        cdf = np.cumsum(hist_cp)
        cdf = cdf / cdf[-1]
        values = np.random.rand(len(df_agents_cp))
        value_bins = np.searchsorted(cdf, values)
        random_from_cdf_cp = bin_midpoints[value_bins] # in meters

        tree = KDTree(dest_coordinates)
        indices_cp, distances_cp = tree.query_radius(home_coordinates_cp, r=random_from_cdf_cp,
                                                     return_distance = True, sort_results=True)

        for i in range(len(indices_cp)):
            l = indices_cp[i]
            if len(l) == 0:
                # In some cases no facility was found for certain persons within the given radius,
                # assign the nearest facility
                dist, ind = tree.query(np.array(home_coordinates_cp[i]).reshape(1,-1), 2,
                                       return_distance = True, sort_results=True)
                fac = ind[0][1]
                indices_cp[i] = [fac]
                distances_cp[i] = [dist[0][1]]
            else:
                # When found facilities, remove those with no available capacity and go to the next nearest one
                for indice_cp in l:
                    # If found a nearest facility with available capacity, stop checking and decrease its capacity
                    if dest_cap[indice_cp] >= 1:
                        dest_cap[indice_cp] -= 1
                        break
                    else:
                        indices_cp[i] = indices_cp[i][1:]
                else:
                    # If found facilities but none with available capacity, assign the nearest facility
                    dist, ind = tree.query(np.array(home_coordinates_cp[i]).reshape(1, -1), 2,
                                           return_distance=True, sort_results=True)
                    fac = ind[0][1]
                    indices_cp[i] = [fac]
                    distances_cp[i] = [dist[0][1]]

            progress.update()


        indices_cp = [l[-1] for l in indices_cp]
        distances_cp = [d[-1] for d in distances_cp]

        df_return_cp = df_agents_cp.copy()
        df_return_cp["x"] = dest_coordinates.iloc[indices_cp]["x"].values
        df_return_cp["y"] = dest_coordinates.iloc[indices_cp]["y"].values
        df_return_cp["LocationID"] = df_candidates.iloc[indices_cp]["LocationID"].values

        # Update the available capacities of the facilities
        if purpose == "home":
            df_candidates["Inhabitants"] = dest_cap
        elif purpose == "work":
            df_candidates["WorkPlaces"] = dest_cap
        else:
            df_candidates["StudyPlaces"] = dest_cap

        df_return = df_return_cp
        assert len(df_return) == len(df_agents)

    sys.stdout.write("\r")  # Clean tqdm progress

    return df_return, df_candidates


def execute(context):

    # Ignore warning when working on slices of dataframes
    pd.options.mode.chained_assignment = None

    # Get zones and their area coordinates
    df_zones_municipalities, df_zones_cadastral_city, \
    df_zones_zsj_city, df_zones_gates = context.stage("data.spatial.zones")
    df_zones_municipalities = df_zones_municipalities[["ZoneID", "geometry"]]
    df_zones_cadastral_city = df_zones_cadastral_city[["ZoneID", "geometry"]]
    df_zones_zsj_city = df_zones_zsj_city[["ZoneID", "geometry"]]
    df_zones_gates = df_zones_gates[["ZoneID", "geometry"]]

    df_zones_home = df_zones_zsj_city.append(df_zones_gates)
    df_zones_primary = df_zones_municipalities.append(df_zones_cadastral_city)
    df_zones_primary = df_zones_primary.append(df_zones_gates)
    df_zones_primary["ZoneID"] = df_zones_primary["ZoneID"].astype(float).astype(int).astype(str)
    df_zones_home["ZoneID"] = df_zones_zsj_city["ZoneID"].astype(float).astype(int).astype(str)

    # Get all locations/facilities
    df_facilities = context.stage("synthesis.destinations")
    df_facilities["Inhabitants"] = df_facilities["Inhabitants"].astype(float)
    df_facilities["WorkPlaces"] = df_facilities["WorkPlaces"].astype(float)
    df_facilities["StudyPlaces"] = df_facilities["StudyPlaces"].astype(float)

    # Get the attributes of the population
    df_commute = context.stage("synthesis.population.sociodemographics")[["PersonID",
                                                                          "PrimaryLocCrowFliesTripDist",
                                                                          "hts_PersonID"]]

    # The BasicSettlementCode will filter the facilities that offer home
    df_home_facilities = df_facilities[df_facilities["offers_home"] == True]
    df_home_facilities = df_home_facilities.drop(columns=['ZoneID'])
    df_home_facilities = df_home_facilities.rename(columns={'BasicSettlementCode': 'ZoneID'}).copy()

    # The FacilityUsage and ActivitySector attributes will filter the facilities that offer work
    df_work_locations = df_facilities[df_facilities["offers_work"] == True]

    # The AgeGroup and EducationLevel attributes will filter the facilities that offer education
    df_education_locations = df_facilities[df_facilities["offers_education"] == True]

    # Get the zones of households (at the moment it is not known the households, so using 1 person = 1 household)
    df_households = context.stage("synthesis.population.spatial.by_person.primary_zones")[0].copy()

    # Get the zones of persons/agents who have work trips
    df_work_zones = context.stage("synthesis.population.spatial.by_person.primary_zones")[1].copy()
    df_hw = pd.merge(df_work_zones.rename(columns={"ZoneID": "WorkID"}),
                     df_households.rename(columns={"ZoneID": "HomeID"}), on=["PersonID"], how='left')
    df_work_zones = pd.merge(df_hw, df_commute)

    # Get the zones of persons/agents who have education trips
    df_education_zones = context.stage("synthesis.population.spatial.by_person.primary_zones")[2].copy()
    df_hw = pd.merge(df_education_zones.rename(columns={"ZoneID": "EducationID"}),
                     df_households.rename(columns={"ZoneID": "HomeID"}), on=["PersonID"], how='left')

    df_education_zones = pd.merge(df_hw, df_commute)

    print("Imputing home locations ...")

    # Get population and their zone of residence
    df_hhl = context.stage("synthesis.population.sampled")
    df_hhl = pd.concat(df_hhl)
    df_hhl = df_hhl[[
                     "PersonID",
                     "BasicSettlementCode"
                     ]].copy()
    df_hhl.rename(columns={"BasicSettlementCode": "ZoneID"}, inplace=True)

    # Define home locations
    df_home, df_home_facilities = impute_diff_zone_locations(df_hhl,
                                                             df_zones_home,
                                                             df_home_facilities,
                                                             "home")
    df_home = df_home[["PersonID", "x", "y", "LocationID"]]

    # Enhance the home locations with population data
    df_hhl = context.stage("synthesis.population.sampled")
    df_hhl = pd.concat(df_hhl)
    df_home = pd.merge(df_hhl, df_home, on=["PersonID"], how="left")
    df_home = pd.merge(df_home, df_households[["PersonID"]], on=["PersonID"], how='left')
    df_home = df_home.copy()
    assert len(df_households) == len(df_home)

    # Update the number of available inhabitants
    df_facilities["Inhabitants"].update(df_home_facilities["Inhabitants"])

    print("Imputing different zone work locations ...")

    # Enhance the persons/agents who have work trips with home data
    df_work_zones = pd.merge(df_work_zones, df_home.rename({"x": "HomeX", "y": "HomeY"}, axis=1))

    # Select only persons/agents that work on a different zone than their home zone
    df_work_different_zone = df_work_zones.copy()
    df_work_different_zone = df_work_different_zone[df_work_different_zone["WorkID"]
                                                    != df_work_different_zone["HomeID"]]
    df_work_different_zone.rename(columns={"WorkID": "ZoneID"}, inplace=True)

    # Filter the facilities to be assigned according to activity sector the person works and facility usage of facilities
    for build_usage in ALL_FACILITY_USAGES:
        print(" For facility usage", build_usage)
        required_usages = {build_usage}
        ids = df_work_locations["FacilityUsage"].apply(
                lambda x: True if (type(x) == tuple and x == ("99",) or len(set(x).intersection(required_usages)))
                                   or (type(x) != tuple and x == "99" or x in required_usages) > 0
                else False)
        filtered_df_work_locations = df_work_locations[ids]

        required_sectors = {build_usage}
        try:
            # If already a single dataframe, filter facilities and avoid reassigning already assigned persons/agents
            ids = df_work_different_zone["ActivitySector"].apply(
                lambda x: True if (type(x) == tuple and x == ("99",) or len(set(x).intersection(required_sectors)))
                                   or (type(x) != tuple and x == "99" or x in required_sectors) > 0
                else False)
            filtered_df_work_different_zone = df_work_different_zone[(ids)
                                                                     & ~(df_work_different_zone["PersonID"].isin(df_work["PersonID"]))]
        except NameError:
            ids = df_work_different_zone["ActivitySector"].apply(
                lambda x: True if (type(x) == tuple and x == ("99",) or len(set(x).intersection(required_sectors)))
                                   or (type(x) != tuple and x == "99" or x in required_sectors) > 0
                else False)
            filtered_df_work_different_zone = df_work_different_zone[ids]

        if len(filtered_df_work_different_zone) > 0:
            # Define the work locations
            df_work_diff_zone, filtered_df_work_locations = impute_diff_zone_locations(filtered_df_work_different_zone,
                                                                                       df_zones_primary,
                                                                                       filtered_df_work_locations,
                                                                                       "work")
            df_work_diff_zone = df_work_diff_zone[["PersonID", "x", "y", "LocationID"]]

            # Update the available capacities of work locations
            df_facilities["WorkPlaces"].update(filtered_df_work_locations["WorkPlaces"])

            # Merge the assigned facilities for each facility usage to a single dataframe
            try:
                df_work = df_work.append(df_work_diff_zone, sort=False)
            except NameError:
                df_work = df_work_diff_zone.copy()

    assert len(df_work_different_zone) == len(df_work)

    print("Imputing same zone work locations ...")

    df_work_same_zone = df_work_zones.copy()
    df_work_same_zone = df_work_same_zone[df_work_same_zone["WorkID"] == df_work_same_zone["HomeID"]]

    # Get both HTS data
    all_df_hts_persons = context.stage("data.hts.cleaned")[:2]
    all_df_hts_trips = context.stage("data.hts.cleaned")[2:]

    for df_ind in range(0, len(all_df_hts_persons)):
        print(" For HTS", df_ind)
        df_hts_persons = all_df_hts_persons[df_ind]
        df_hts_trips = all_df_hts_trips[df_ind]

        df_hts = pd.merge(df_hts_trips, df_hts_persons, on=["PersonID"])
        hts_trips_work = df_hts[df_hts["DestPurpose"] == '4']

        # Select only trips with same zone (if CzechiaHTS town codes and if CityHTS cadastral area)
        if df_ind == 0:
            # If in the municipalities around Ustí city
            hts_trips_work = hts_trips_work[(hts_trips_work["OriginTownCode"] == hts_trips_work["DestTownCode"])]
        else:
            # If within Ustí city
            hts_trips_work = hts_trips_work[hts_trips_work["OriginCadastralAreaCode"]
                                            == hts_trips_work["DestCadastralAreaCode"]]

        # Filter the facilities to be assigned according to activity sector the person works and facility usage of facilities
        for build_usage in ALL_FACILITY_USAGES:
            print("     For facility usage", build_usage)

            required_usages = {build_usage}
            ids = df_work_locations["FacilityUsage"].apply(
                lambda x: True if (type(x) == tuple and x == ("99",) or len(set(x).intersection(required_usages)))
                                   or (type(x) != tuple and x == "99" or x in required_usages) > 0
                else False)
            filtered_df_work_locations = df_work_locations[ids]

            required_sectors = {build_usage}
            ids = df_work_same_zone["ActivitySector"].apply(
                lambda x: True if (type(x) == tuple and x == ("99",) or len(set(x).intersection(required_sectors)))
                                   or (type(x) != tuple and x == "99" or x in required_sectors) > 0
                else False)

            if df_ind == 0:
                # If in the municipalities around Ustí city
                filtered_df_work_same_zone = df_work_same_zone[(ids)
                    & ~(df_work_same_zone["PersonID"].isin(df_work["PersonID"])) & (df_work_same_zone["TownCode"] != '554804')]
            else:
                # If within Ustí city
                filtered_df_work_same_zone = df_work_same_zone[(ids)
                    & ~(df_work_same_zone["PersonID"].isin(df_work["PersonID"])) & (df_work_same_zone["TownCode"] == '554804')]

            if len(filtered_df_work_same_zone) > 0 and len(hts_trips_work) > 0:
                # Define work locations
                work_locations, filtered_df_work_locations = impute_primary_locations_same_zone(hts_trips_work,
                                                                                                filtered_df_work_same_zone,
                                                                                                filtered_df_work_locations,
                                                                                                "work")
                work_locations = work_locations[["PersonID", "x", "y", "LocationID"]]

                # Update the available capacities of work locations
                df_facilities["WorkPlaces"].update(filtered_df_work_locations["WorkPlaces"])

                # Merge the assigned facilities for each facility usage to a single dataframe
                df_work = df_work.append(work_locations, sort=False)

    assert len(df_work_different_zone) + len(df_work_same_zone) == len(df_work)

    print("Imputing different zone education locations ...")

    # Enhance the persons/agents who have education trips with home data
    df_education_zones = pd.merge(df_education_zones, df_home.rename({"x": "HomeX", "y": "HomeY"}, axis=1))

    # Select only persons/agents that have a edution trip on a different zone than their home zone
    df_education_different_zone = df_education_zones.copy()
    df_education_different_zone = df_education_different_zone[df_education_different_zone["EducationID"]
                                                              != df_education_different_zone["HomeID"]]
    df_education_different_zone.rename(columns={"EducationID": "ZoneID"}, inplace=True)

    # Filter the facilities to be assigned according to age the person and education place of facilities
    for education_place in ALL_EDUCATION_PLACES:
        print(" For education place", education_place)
        required_type = {education_place}
        ids = df_education_locations["EducationPlace"].apply(
                lambda x: True if (type(x) == tuple and x == ("99",) or len(set(x).intersection(required_type)))
                                   or (type(x) != tuple and x == "99" or x in required_type) > 0
                else False)
        filtered_df_education_locations = df_education_locations[ids]

        try:
            # If already a single dataframe, filter facilities and avoid reassigning already assigned persons/agents
            ids = df_education_different_zone["EducationPlace"].apply(
                lambda x: True if (type(x) == tuple and x == ("99",) or len(set(x).intersection(required_type)))
                                   or (type(x) != tuple and x == "99" or x in required_type) > 0
                else False)
            filtered_df_education_different_zone = df_education_different_zone[(ids)
                                                    & ~(df_education_different_zone["PersonID"].isin(df_education["PersonID"]))]
        except NameError:
            ids = df_education_different_zone["EducationPlace"].apply(
                lambda x: True if (type(x) == tuple and x == ("99",) or len(set(x).intersection(required_type)))
                                   or (type(x) != tuple and x == "99" or x in required_type) > 0
                else False)
            filtered_df_education_different_zone = df_education_different_zone[ids]

        if len(filtered_df_education_different_zone) > 0:
            # Define education locations
            df_education_diff_zone, filtered_df_education_locations = impute_diff_zone_locations(
                filtered_df_education_different_zone,
                df_zones_primary,
                filtered_df_education_locations,
                "education")
            df_education_diff_zone = df_education_diff_zone[["PersonID", "x", "y", "LocationID"]]

            # Update the available capacities of study locations
            df_facilities["StudyPlaces"].update(filtered_df_education_locations["StudyPlaces"])

            # Merge the assigned facilities for each education place to a single dataframe
            try:
                df_education = df_education.append(df_education_diff_zone, sort=False)
            except NameError:
                df_education = df_education_diff_zone.copy()

    assert len(df_education_different_zone) == len(df_education)

    print("Imputing same zone education locations ...")

    df_education_same_zone = df_education_zones.copy()
    df_education_same_zone = df_education_same_zone[df_education_same_zone["EducationID"]
                                                    == df_education_same_zone["HomeID"]]

    # Get both HTS data
    all_df_hts_persons = context.stage("data.hts.cleaned")[:2]
    all_df_hts_trips = context.stage("data.hts.cleaned")[2:]

    for df_ind in range(0, len(all_df_hts_persons)):
        print(" For HTS", df_ind)
        df_hts_persons = all_df_hts_persons[df_ind]
        df_hts_trips = all_df_hts_trips[df_ind]

        df_hts = pd.merge(df_hts_trips, df_hts_persons, on=["PersonID"])
        hts_trips_education = df_hts[df_hts["DestPurpose"] == '5']

        # Select only trips with same zone (if CzechiaHTS town codes and if CityHTS cadastral area)
        if df_ind == 0:
            # If in the municipalities around Ustí city
            hts_trips_education = hts_trips_education[hts_trips_education["OriginTownCode"]
                                                      == hts_trips_education["DestTownCode"]]
        else:
            # If within Ustí city
            hts_trips_education = hts_trips_education[hts_trips_education["OriginCadastralAreaCode"]
                                                      == hts_trips_education["DestCadastralAreaCode"]]

        # Filter the facilities to be assigned according to age the person and education place of facilities
        for education_place in ALL_EDUCATION_PLACES:
            print("     For school type", education_place)

            required_type = {education_place}
            ids = df_education_locations["EducationPlace"].apply(
                lambda x: True if (type(x) == tuple and x == ("99",) or len(set(x).intersection(required_type)))
                                   or (type(x) != tuple and x == "99" or x in required_type) > 0
                else False)
            filtered_df_education_locations = df_education_locations[ids]

            ids = df_education_same_zone["EducationPlace"].apply(
                lambda x: True if (type(x) == tuple and x == ("99",) or len(set(x).intersection(required_type)))
                                   or (type(x) != tuple and x == "99" or x in required_type) > 0
                else False)

            if df_ind == 0:
                # If in the municipalities around Ustí city
                filtered_df_education_same_zone = df_education_same_zone[(ids)
                                                                    & ~(
                    df_education_same_zone["PersonID"].isin(df_education["PersonID"])) & (
                                                                            df_education_same_zone[
                                                                                "TownCode"] != '554804')]
            else:
                # If within Ustí city
                filtered_df_education_same_zone = df_education_same_zone[(ids)
                                                                    & ~(
                    df_education_same_zone["PersonID"].isin(df_education["PersonID"])) & (
                                                                            df_education_same_zone[
                                                                                "TownCode"] == '554804')]

            if len(filtered_df_education_same_zone) > 0 and len(hts_trips_education) > 0:
                # Define education locations
                education_locations, filtered_df_education_locations = impute_primary_locations_same_zone(
                    hts_trips_education,
                    filtered_df_education_same_zone,
                    filtered_df_education_locations,
                    # df_trips,
                    "education")
                education_locations = education_locations[["PersonID", "x", "y", "LocationID"]]
                df_facilities["StudyPlaces"].update(filtered_df_education_locations["StudyPlaces"])

                # Merge the assigned facilities for each education place to a single dataframe
                df_education = df_education.append(education_locations, sort=False)

    assert len(df_education_different_zone) + len(df_education_same_zone) == len(df_education)

    print("Saving primary locations")

    df_home = df_home[["PersonID", "Age", "Gender", "Education",
                       "x", "y", "LocationID"]]
    df_home = gpd.GeoDataFrame(df_home, geometry=gpd.points_from_xy(df_home.x, df_home.y))
    df_home.crs = "epsg:5514"
    df_home = df_home[['PersonID', "Age", "Gender", "Education",
                       'LocationID', 'geometry']]

    df_work = gpd.GeoDataFrame(df_work, geometry=gpd.points_from_xy(df_work.x, df_work.y))
    df_work.crs = "epsg:5514"
    df_work = df_work[['PersonID', 'LocationID', 'geometry']]
    df_work = pd.merge(df_work, df_hhl, how="left", on=["PersonID"
                                                        ])[["PersonID", "ActivitySector", "JourneyMainMode",
                                                            "LocationID", 'geometry']]

    df_education = gpd.GeoDataFrame(df_education, geometry=gpd.points_from_xy(df_education.x, df_education.y))
    df_education.crs = "epsg:5514"
    df_education = df_education[['PersonID', 'LocationID', 'geometry']]
    df_education = pd.merge(df_education, df_hhl, how="left", on=["PersonID"
                                                                  ])[["PersonID", "AgeGroup", "JourneyMainMode",
                                                                      "LocationID", 'geometry']]

    df_home.to_csv("%s/Locations/locations_home.csv" % context.config("output_path"))
    commonFunctions.toXML(df_home, "%s/Locations/locations_home.xml" % context.config("output_path"))
    df_home.to_file("%s/Locations/locations_home.gpkg" % context.config("output_path"), driver="GPKG")

    df_work.to_csv("%s/Locations/locations_work.csv" % context.config("output_path"))
    commonFunctions.toXML(df_work, "%s/Locations/locations_work.xml" % context.config("output_path"))
    df_work.to_file("%s/Locations/locations_work.gpkg" % context.config("output_path"), driver="GPKG")

    df_education.to_csv("%s/Locations/locations_edu.csv" % context.config("output_path"))
    commonFunctions.toXML(df_education, "%s/Locations/locations_edu.xml" % context.config("output_path"))
    df_education.to_file("%s/Locations/locations_edu.gpkg" % context.config("output_path"), driver = "GPKG")

    print("Saved primary locations")

    return df_home, df_work, df_education
