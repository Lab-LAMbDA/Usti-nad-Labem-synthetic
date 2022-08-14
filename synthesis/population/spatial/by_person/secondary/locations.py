import numpy as np
import pandas as pd
import os
import sys
import shapely.geometry as geo
import geopandas as gpd
from tqdm import tqdm
from synthesis.population.spatial.by_person.secondary.problems import find_assignment_problems
from synthesis.population.spatial.by_person.secondary.rda import AssignmentSolver, DiscretizationErrorObjective, GravityChainSolver
from synthesis.population.spatial.by_person.secondary.components import CustomDistanceSampler, CustomDiscretizationSolver
from data import commonFunctions

def configure(context):
    context.stage("synthesis.population.trips")
    context.stage("synthesis.population.sociodemographics")
    context.stage("synthesis.population.sampled")
    context.stage("synthesis.population.spatial.by_person.primary_locations")
    context.stage("synthesis.population.spatial.by_person.secondary.distance_distributions")
    context.stage("synthesis.destinations")
    context.config("random_seed")
    context.config("output_path")

def validate(context):
    output_path = context.config("output_path")

    if not os.path.isdir(output_path):
        raise RuntimeError("Output directory must exist: %s" % output_path)

def prepare_locations(context):

    # Load persons and their primary locations
    df_home, df_work, df_education = context.stage("synthesis.population.spatial.by_person.primary_locations")

    # Home
    df_home["1"] = df_home["geometry"]

    # Work
    df_work["4"] = df_work["geometry"]

    # Education
    df_education["5"] = df_education["geometry"]

    all_df_locations = []
    all_df_persons = context.stage("synthesis.population.sampled")
    for df_persons in all_df_persons:
        df_persons = df_persons[["PersonID",
                                 # "HouseholdID" # not at the moment
                                 ]]
        df_locations = pd.merge(df_home, df_persons, how = "right", on = ["PersonID",
                                                                         # "HouseholdID" # not at the moment
                                                                         ])
        df_locations = pd.merge(df_locations, df_work[["PersonID", "4"]], how = "left", on = "PersonID")
        df_locations = pd.merge(df_locations, df_education[["PersonID", "5"]], how = "left", on = "PersonID")
        df_locations = df_locations[["PersonID",
                                     "1",  # home
                                     "4",  # work
                                     "5"]  # education
        ].sort_values(by="PersonID").copy()
        all_df_locations.append(df_locations)

    return all_df_locations

def prepare_destinations(context):

    df_destinations = context.stage("synthesis.destinations")
    identifiers = df_destinations["LocationID"].values
    locations = np.vstack(df_destinations["geometry"].centroid.apply(lambda x: np.array([x.x, x.y])).values)
    capacities = df_destinations["Visitors"].values

    df_destinations = df_destinations.drop(["FacilityPurpose"], axis=1)

    f = ((df_destinations["offers_freetime"] == True)
        | (df_destinations["offers_shopping"] == True)
        | (df_destinations["offers_errands"] == True)) \
        & (df_destinations["Visitors"] >= 1)
    data = dict(
        identifiers=identifiers[f].tolist(),
        locations=locations[f].tolist(),
        capacities=capacities[f].tolist()
    )

    return data

def resample_cdf(cdf, factor):
    if factor >= 0.0:
        cdf = cdf * (1.0 + factor * np.arange(1, len(cdf) + 1) / len(cdf))
    else:
        cdf = cdf * (1.0 + abs(factor) - abs(factor) * np.arange(1, len(cdf) + 1) / len(cdf))

    cdf /= cdf[-1]
    return cdf

def resample_distributions(distributions, factors):
    for mode, mode_distributions in distributions.items():
        for distribution in mode_distributions["distributions"]:
            distribution["cdf"] = resample_cdf(distribution["cdf"], factors[mode])

def process(context, destinations, distance_distributions, arguments):

    df_trips, df_primary, number_of_persons, random_seed = arguments

    with tqdm(total=number_of_persons, desc="Assigning secondary locations to persons", ascii=True,
              leave=False, miniters=1, position=0) as progress:

        progress.set_description("Assigning secondary locations to persons")
        # Set up RNG
        random = np.random.RandomState(context.config("random_seed"))

        # Set up distance sampler
        distance_sampler = CustomDistanceSampler(
            maximum_iterations=1000,
            random=random,
            distributions=distance_distributions)

        # Set up relaxation solver
        relaxation_solver = GravityChainSolver(
            random=random,
            # eps=10.0,
            lateral_deviation=10.0,
            # alpha=0.3
        )

        # Maximum error per mode to consider the location assignment as valid
        thresholds = {
            "1": 100,  # on foot
            "2": 100,  # bike
            "3": 200,  # city public transport
            "4": 200,  # bus (except city public transport)
            "5": 200,  # train (except city public transport)
            "6": 200,  # auto-driver
            "7": 200,  # auto-passenger
            "8": 200,  # other
            "999": 200  # Not identified
        }

        # Initialize objective for discretization solver
        assignment_objective = DiscretizationErrorObjective(thresholds=thresholds)

        # Initialize discretization solver
        discretization_solver = CustomDiscretizationSolver(destinations)
        assignment_solver = AssignmentSolver(
            distance_sampler=distance_sampler,
            relaxation_solver=relaxation_solver,
            discretization_solver=discretization_solver,
            objective=assignment_objective,
            maximum_iterations=20
        )

        df_locations = []
        df_convergence = []

        last_person_id = None

        for problem in find_assignment_problems(df_trips, df_primary):

            # Define the secondary locations
            result = assignment_solver.solve(problem)

            for trip_index, (identifier, location, location_index) in enumerate(
                    zip(result["discretization"]["identifiers"],
                        result["discretization"]["locations"],
                        result["discretization"]["indices"])):

                # Decrease the available capacity of the assigned location
                discretization_solver.data["capacities"][location_index] -= 1

                if discretization_solver.data["capacities"][location_index] < 1:
                    # If assigned location has no capacity anymore, remove it from the list of possible locations
                    discretization_solver.update(location_index)

                df_locations.append((
                    problem["PersonID"],
                    problem["TripIDs"][trip_index],
                    problem["TripOrderNums"][trip_index],
                    problem["modes"][trip_index],
                    problem["purposes"][trip_index],
                    identifier,
                    geo.Point(location)
                ))

            df_convergence.append((
                problem["PersonID"], result["valid"], problem["size"]
            ))

            if problem["PersonID"] != last_person_id:
                last_person_id = problem["PersonID"]
                progress.update()

        df_locations = pd.DataFrame.from_records(df_locations, columns=["PersonID",
                                                                        "TripID",
                                                                        "TripOrderNum",
                                                                        "TripMainMode",
                                                                        "DestPurpose",
                                                                        "LocationID", "geometry"])
        df_locations = gpd.GeoDataFrame(df_locations, crs="epsg:5514")

        df_convergence = pd.DataFrame.from_records(df_convergence, columns=["PersonID", "valid", "size"])

    sys.stdout.write("\r")

    return df_locations, df_convergence

def execute(context):

    print("Imputing secondary locations ...")

    # Load trips and primary locations
    df_trips = context.stage("synthesis.population.trips").sort_values(by = ["PersonID", "TripOrderNum"])

    # Get primary locations
    all_df_primary = prepare_locations(context)

    # Prepare data
    df_locations = pd.DataFrame(columns=["PersonID",
                                         "TripID",
                                         "TripOrderNum",
                                         "TripMainMode",
                                         "DestPurpose",
                                         "LocationID",
                                         "geometry"])
    df_convergence = pd.DataFrame(columns=["PersonID", "valid", "size"])
    destinations = prepare_destinations(context)
    hts_distance_distributions = context.stage("synthesis.population.spatial.by_person.secondary.distance_distributions")

    for df_ind, (df_primary, hts_distributions) in enumerate(zip(all_df_primary, hts_distance_distributions)):
        print(" For HTS", df_ind)

        # Resample the DeclaredTripTime distribution per trip mode for adjustments when results not conforming
        # (not at the moment)
        resample_distributions(hts_distributions, {
            "1": 0.0,  # on foot
            "2": 0.0,  # bike
            "3": 0.0,  # city public transport
            "4": 0.0,  # bus (except city public transport)
            "5": 0.0,  # train (except city public transport)
            "6": 0.0,  # auto-driver
            "7": 0.0,  # auto-passenger
            "8": 0.0,  # other
            "999": 0.0  # Not identified
        })

        random = np.random.RandomState(context.config("random_seed"))
        random_seeds = random.randint(10000, size = 1)

        df_hts_trips = df_trips[df_trips["PersonID"].isin(df_primary["PersonID"])]

        unique_person_ids = df_hts_trips["PersonID"].unique()

        number_of_persons = len(unique_person_ids)
        batch = (df_hts_trips, df_primary, number_of_persons, random_seeds[0])

        # Run algorithm in one single batch
        df_hts_locations, df_hts_convergence = process(context, destinations, hts_distributions, batch)
        df_locations = pd.concat([df_locations, df_hts_locations],
                                 axis=0, ignore_index=True, sort=False)
        df_convergence = pd.concat([df_convergence, df_hts_convergence],
                                   axis=0, ignore_index=True, sort=False)

    val = "{:.0%}".format(df_convergence["valid"].mean())
    print("Success rate:", val + ".",
          "For those invalid, using the location of last iteration. "
          "Consider increasing \"maximum_iterations\" in the \"AssignmentSolver\" "
          "and thresholds in the \"DiscretizationErrorObjective\".")

    print("Saving secondary locations")

    df_locations = gpd.GeoDataFrame(df_locations, crs="epsg:5514")
    df_convergence.to_csv("%s/Locations/convergence_secondary.csv" % context.config("output_path"))
    df_locations.to_csv("%s/Locations/locations_secondary.csv" % context.config("output_path"))
    commonFunctions.toXML(df_locations, "%s/Locations/locations_secondary.xml" % context.config("output_path"))
    df_locations.to_file("%s/Locations/locations_secondary.gpkg" % context.config("output_path"), driver="GPKG")

    print("Saved secondary locations")

    return df_locations, df_convergence


