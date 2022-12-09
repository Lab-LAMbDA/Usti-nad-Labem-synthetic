import numpy as np
import pandas as pd

BIN_SIZE = 20

def configure(context):
    context.stage("data.hts.cleaned")

def validate(context):

    pass

def calculate_bounds(values, BIN_SIZE):
    values = np.sort(values)

    bounds = []
    current_count = 0
    previous_bound = None

    for value in values:
        if value == previous_bound:
            continue

        if current_count < BIN_SIZE:
            current_count += 1
        else:
            current_count = 0
            bounds.append(value)
            previous_bound = value

    bounds[-1] = np.inf
    return bounds

def execute(context):

    df_persons_CzechiaHTS, df_persons_CityHTS, df_trips_CzechiaHTS, df_trips_CityHTS = context.stage("data.hts.cleaned")
    distance_column = "CrowFliesTripDist"
    distributions = []
    primary_activities = ["1",  # Home
                          "4",  # Work
                          "5"  # Education
                          ]

    print("Defining distributions of CrowFliesTripDist for DeclaredTripTime intervals per trip mode")

    # Define distributions of DeclaredTripTime per trip mode
    for (df_persons, df_trips) in [[df_persons_CzechiaHTS, df_trips_CzechiaHTS], [df_persons_CityHTS, df_trips_CityHTS]]:
        distributions.append({})
        df_trips = pd.merge(df_trips, df_persons[["PersonID", "Weight"]])
        df = df_trips[["TripMainMode", "DeclaredTripTime", distance_column,
                       "Weight", "OriginPurpose", "DestPurpose"]].rename(columns={distance_column: "distance"})

        # Filtering only primary activities
        df = df[~(
            df["OriginPurpose"].isin(primary_activities) &
            df["DestPurpose"].isin(primary_activities)
        )]

        # Calculate distributions of primary activities per trip mode
        modes = df["TripMainMode"].unique()

        for mode in modes:
            # First, calculate bounds by unique values
            f_mode = df["TripMainMode"] == mode

            # As some modes have too little samples, allow these exceptions
            bounds = calculate_bounds(df[f_mode]["DeclaredTripTime"].values,
                                      min(BIN_SIZE, len(df[f_mode]["DeclaredTripTime"].values) - 1))

            distributions[-1][mode] = dict(bounds = np.array(bounds), distributions = [])

            # Second, calculate distribution per band
            for lower_bound, upper_bound in zip([-np.inf] + bounds[:-1], bounds):
                f_bound = (df["DeclaredTripTime"] > lower_bound) & (df["DeclaredTripTime"] <= upper_bound)

                # Set up distribution
                values = df[f_mode & f_bound]["distance"].values
                weights = df[f_mode & f_bound]["Weight"].values

                sorter = np.argsort(values)
                values = values[sorter]
                weights = weights[sorter]

                cdf = np.cumsum(weights)
                cdf /= cdf[-1]

                # Write distribution
                distributions[-1][mode]["distributions"].append(dict(cdf = cdf, values = values, weights = weights))

    return distributions
