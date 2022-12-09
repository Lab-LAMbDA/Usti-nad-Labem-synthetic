import io, gzip
import os
from tqdm import tqdm
import matsim.writers as writers


# Define globals
FIELDS = ["HouseholdID", "PersonID", "HouseholdIncome", "AvailCar"]

def configure(context):
    context.config("output_path")
    context.stage("synthesis.population.sociodemographics")

def validate(context):
    output_path = context.config("output_path")

    if not os.path.isdir(output_path):
        raise RuntimeError("Output directory must exist: %s" % output_path)

def add_household(writer, household, member_ids):
    writer.start_household(household[FIELDS.index("HouseholdID")])
    writer.add_members(member_ids)

    writer.start_attributes()
    writer.add_attribute("carAvailability", "java.lang.String", "always" if household[FIELDS.index("AvailCar")] else "never")
    writer.add_attribute("household_income", "java.lang.Double", household[FIELDS.index("HouseholdIncome")])
    writer.end_attributes()

    writer.end_household()

def execute(context):
    output_path = "%s/households.xml.gz" % context.config("output_path")

    df_persons = context.stage("synthesis.population.sociodemographics")
    df_persons = df_persons.sort_values(by = ["HouseholdID", "PersonID"])
    df_persons = df_persons[FIELDS]

    current_members = []
    current_household_id = None
    current_household = None

    # Write MATSim input file
    with gzip.open(output_path, 'wb+') as writer:
        with io.BufferedWriter(writer, buffer_size = 2 * 1024**3) as writer:
            writer = writers.HouseholdsWriter(writer)
            writer.start_households()

            for item in tqdm(df_persons.itertuples(index = False),
                               desc="Writing households ...", ascii=True,
                               position=0, leave=False):
                if current_household_id != item[FIELDS.index("HouseholdID")]:
                    if not current_household_id is None:
                        add_household(writer, current_household, current_members)

                    current_household = item
                    current_household_id = item[FIELDS.index("HouseholdID")]
                    current_members = [item[FIELDS.index("PersonID")]]
                else:
                    current_members.append(item[FIELDS.index("PersonID")])

            if not current_household_id is None:
                add_household(writer, current_household, current_members)

            writer.end_households()

    return "households.xml.gz"
