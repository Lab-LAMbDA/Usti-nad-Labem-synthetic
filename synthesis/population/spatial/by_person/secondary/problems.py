import numpy as np

# Define globals
FIELDS = ["PersonID", "TripID", "TripOrderNum", "OriginPurpose", "DestPurpose", "TripMainMode", "DeclaredTripTime"]
FIXED_PURPOSES = ["1", # home
                  "4", # work
                  "5"] # location
LOCATION_FIELDS = ["PersonID",
                   "1", # home
                   "4", # work
                   "5"] # location

def find_bare_assignment_problems(df):
    problem = None

    for row in df[FIELDS].itertuples(index = False):
        PersonID, TripID, TripOrderNum, preceeding_purpose, following_purpose, mode, travel_time = row

        if not problem is None and PersonID != problem["PersonID"]:
            # We switch person, but we're still tracking a problem. This is a tail!
            yield problem
            problem = None

        if problem is None:
            # Start a new problem
            problem = dict(
                PersonID = PersonID, purposes = [preceeding_purpose],
                TripIDs = [], TripOrderNums = [], modes = [], travel_times = []
            )

        problem["purposes"].append(following_purpose)
        problem["modes"].append(mode)
        problem["travel_times"].append(travel_time)
        problem["TripIDs"].append(TripID)
        problem["TripOrderNums"].append(TripOrderNum)

        if problem["purposes"][-1] in FIXED_PURPOSES:
            # The current chain (or initial tail) ends with a fixed purpose
            yield problem
            problem = None

def find_assignment_problems(df, df_locations):
    """
        Enriches assignment problems with:
          - Locations of the fixed activities
          - Size of the problem
          - Reduces purposes to the variable ones
    """
    location_iterator = df_locations[LOCATION_FIELDS].itertuples(index = False)
    current_location = None    

    for problem in find_bare_assignment_problems(df):
        origin_purpose = problem["purposes"][0]
        destination_purpose = problem["purposes"][-1]

        # Remove home purposes
        if origin_purpose in FIXED_PURPOSES and destination_purpose in FIXED_PURPOSES:
            problem["purposes"] = problem["purposes"][1:-1]

        elif origin_purpose in FIXED_PURPOSES:
            problem["purposes"] = problem["purposes"][1:]

        elif destination_purpose in FIXED_PURPOSES:
            problem["purposes"] = problem["purposes"][:-1]

        else:
            raise RuntimeError("The presented 'problem' is neither a chain nor a tail")

        # Define size
        problem["size"] = len(problem["purposes"])

        if problem["size"] == 0:
            continue # We can skip if there are no secondary activities

        # Advance location iterator until we arrive at the current problem's person
        while current_location is None or current_location[0] != problem["PersonID"]:
            current_location = next(location_iterator)

        # Define origin and destination locations if they have fixed purposes
        problem["origin"] = None
        problem["destination"] = None

        if origin_purpose in FIXED_PURPOSES:
            try:
                problem["origin"] = current_location[LOCATION_FIELDS.index(origin_purpose)]  # Shapely POINT
                problem["origin"] = np.array([[problem["origin"].x, problem["origin"].y]])
            except AttributeError:
                continue # We can skip if there is no place for the origin

        if destination_purpose in FIXED_PURPOSES:
            try:
                problem["destination"] = current_location[LOCATION_FIELDS.index(destination_purpose)] # Shapely POINT
                problem["destination"] = np.array([[problem["destination"].x, problem["destination"].y]])
            except AttributeError:
                continue # We can skip if there is no place for the destination

        yield problem



