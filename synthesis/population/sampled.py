import pandas as pd
import numpy as np
import os
from data import commonFunctions

def configure(context):

    context.config("random_seed")
    context.stage("data.census.cleaned")
    context.config("sampling_rate")
    context.config("output_path")

def validate(context):
    output_path = context.config("output_path")

    if not os.path.isdir(output_path):
        raise RuntimeError("Output directory must exist: %s" % output_path)

def execute(context):

    # Get random seed for defining the persons that will have trips based on trip frequencies
    random = np.random.RandomState(context.config("random_seed"))

    df_census = context.stage("data.census.cleaned")

    persons_ustí_city = df_census['TownCode'] == '554804'
    df_census = [df_census.loc[~persons_ustí_city], df_census.loc[persons_ustí_city]]

    print("Defining the persons that will have trips based on trip frequencies per employment status and by country")

    # Define the persons that will have trips based on trip frequencies per employment status and by country
    for df_ind in range(0, len(df_census)):
        all_people = len(df_census[df_ind])
        if df_ind == 0:
            col_name = "ActivityCzechiaHTS"

            prob_everyday_trips = {"1": 0.80,  # Employees, employers, self-employed, helping
                                   "2": 0.80,  # Working pensioners
                                   "3": 0.80,  # Working students and apprentices
                                   "4": 0.39,  # Women on maternity leave (28 or 37 weeks)
                                   "6": 0.27,  # Non-working pensioners
                                   "7": 0.39,  # Others with their own source of livelihood
                                   "8": 0.69,  # Pupils, students, apprentices
                                   "11": 0.38,  # Unemployed looking for their first job
                                   "12": 0.38,  # Other unemployed
                                   "13": 0.39,  # Households, preschool children, other dependents
                                   "99": 0.65  # Not identified
                                   }

            prob_somedays_trips = {"1": 0.15,  # Employees, employers, self-employed, helping
                                   "2": 0.15,  # Working pensioners
                                   "3": 0.15,  # Working students and apprentices
                                   "4": 0.34,  # Women on maternity leave (28 or 37 weeks)
                                   "6": 0.44,  # Non-working pensioners
                                   "7": 0.34,  # Others with their own source of livelihood
                                   "8": 0.23,  # Pupils, students, apprentices
                                   "11": 0.39,  # Unemployed looking for their first job
                                   "12": 0.39,  # Other unemployed
                                   "13": 0.34,  # Households, preschool children, other dependents
                                   "99": 0.23  # Not identified
                                   }
        else:
            col_name = "ActivityCityHTS"

            prob_everyday_trips = {"1": 0.80,  # Employees, employers, self-employed, helping
                                   "2": 0.80,  # Working students and apprentices
                                   "3": 0.27,  # Non - working pensioners
                                   "4": 0.39,  # Any unemployed
                                   "5": 0.39,  # Households, preschool children, other dependents
                                   "99": 0.65  # Not identified
                                   }

            prob_somedays_trips = {"1": 0.15,  # Employees, employers, self-employed, helping
                                   "2": 0.15,  # Working students and apprentices
                                   "3": 0.44,  # Non - working pensioners
                                   "4": 0.39,  # Any unemployed
                                   "5": 0.34,  # Households, preschool children, other dependents
                                   "99": 0.23  # Not identified
                                   }
        f_first = df_census[df_ind][col_name].apply(lambda x: random.choice([True, False],
                                                                              p=[prob_everyday_trips[x],
                                                                                 1 - prob_everyday_trips[x]]))

        f_final = df_census[df_ind][col_name].apply(lambda x: random.choice([True, False],
                                                                              p=[prob_somedays_trips[x],
                                                                                 1 - prob_somedays_trips[x]]))
        f = (f_first == True) | (f_final == True)
        df_census[df_ind] = df_census[df_ind][f]
        moving_people = len(df_census[df_ind])

        if df_ind == 0:
            print("For town:")
        else:
            print("For municipalities:")
        print(" Total number of people:", all_people)
        print(" Number of people with trips:", moving_people)
        print(" Number of people without trips:", all_people - moving_people)
        print(" % of people with trips:", round((moving_people / all_people) * 100, 1))
        print(" % of people without trips:", round(((all_people - moving_people) / all_people) * 100, 1))

    ### BELOW NOT AT THE MOMENT
    # NumPersons = pd.DataFrame(columns=['HouseholdID', 'NumPersons'])
    # NumPersonsAge06_18 = pd.DataFrame(columns=['HouseholdID', 'NumPersonsAge06_18'])
    # for household_id, household_data in df_census[0].groupby(['HouseholdID']):
    #     NumPersons.append([household_id, len(household_data['Age'])])
    #     NumPersonsAge06_18.append([household_id, len(household_data.loc[6 <= household_data['Age'] <= 18])])
    # df_census[1]['NumPersons'] = pd.merge(df_census[1], NumPersons, on='HouseholdID')
    # df_census[1]['NumPersonsAge06_18'] = pd.merge(df_census[1], NumPersonsAge06_18, on='HouseholdID')
    #
    # NumPersonsAge00_05 = pd.DataFrame(columns=['HouseholdID', 'NumPersonsAge00_05'])
    # NumPersonsAge06_17 = pd.DataFrame(columns=['HouseholdID', 'NumPersonsAge06_17'])
    # NumPersonsAge18_99 = pd.DataFrame(columns=['HouseholdID', 'NumPersonsAge18_99'])
    # for household_id, household_data in df_census[0].groupby(['HouseholdID']):
    #     NumPersonsAge00_05.append([household_id, len(household_data.loc[household_data['Age'] <= 5])])
    #     NumPersonsAge06_17.append([household_id, len(household_data.loc[6 <= household_data['Age'] <= 17])])
    #     NumPersonsAge18_99.append([household_id, len(household_data.loc[18 <= household_data['Age']])])
    # df_census[0]['NumPersonsAge00_05'] = pd.merge(df_census[0], NumPersonsAge00_05, on='HouseholdID')
    # df_census[0]['NumPersonsAge06_17'] = pd.merge(df_census[0], NumPersonsAge06_17, on='HouseholdID')
    # df_census[0]['NumPersonsAge18_99'] = pd.merge(df_census[0], NumPersonsAge18_99, on='HouseholdID')
    ### ABOVE NOT AT THE MOMENT

    if context.config("sampling_rate"):
        probability = context.config("sampling_rate")
        if probability < 1:
            for df_ind in range(0, len(df_census)):
                df = df_census[df_ind]
                print("Downsampling (%f)" % probability)

                num_person_ids = len(df["PersonID"])
                print("  Initial number of persons:", num_person_ids)

                f = np.random.random(size = (num_person_ids,)) < probability
                remaining_person_ids = df["PersonID"][f]
                print("  Sampled number of persons:", len(remaining_person_ids))

                df_census[df_ind] = df[df["PersonID"].isin(remaining_person_ids)]

    print("\nSaving sampled population")

    for df_ind in range(0, len(df_census)):
        df = df_census[df_ind]

        if df_ind == 0:
            df.to_csv("%s/Census/sampled_population%s.csv" % (context.config("output_path"), "_city"))
            commonFunctions.toXML(df, "%s/Census/sampled_population%s.xml"
                                  % (context.config("output_path"), "_city"))
        else:
            df.to_csv("%s/Census/sampled_population%s.csv" % (context.config("output_path"),
                                                              "_municipalities"))
            commonFunctions.toXML(df, "%s/Census/sampled_population%s.xml"
                                  % (context.config("output_path"), "_municipalities"))

    print("\nSaved sampled population")

    return df_census

