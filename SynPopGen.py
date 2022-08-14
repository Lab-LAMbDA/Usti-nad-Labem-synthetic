import pickle
import os

import data.census.raw
import data.spatial.zones
import data.hts.cleaned
import data.hts.filtered
import data.census.cleaned
import data.od.cleaned
import synthesis.population.sampled
import synthesis.population.matched
import synthesis.population.sociodemographics
import synthesis.population.trips
import synthesis.population.spatial.by_person.primary_zones
import data.osm.extract_facilities
import synthesis.destinations
import synthesis.population.spatial.by_person.primary_locations
import synthesis.population.spatial.by_person.secondary.distance_distributions
import synthesis.population.spatial.by_person.secondary.locations
import synthesis.population.activities
import synthesis.population.spatial.locations
import synthesis.output
import matsim.scenario.population
# import matsim.scenario.households  # not at the moment
import matsim.scenario.facilities


class contexts:
    
    configs = dict()
    stages = dict()
    cwd = os.getcwd()

    configs.update({"processes": -1}) # -1 will use the number of cores of the CPU instead of fixed amount

    # Define sampling rate and random seed for the output population
    configs.update({"sampling_rate": 1.00})
    configs.update({"random_seed": 1234})

    # Paths to the input data and where the output should be stored
    configs.update({"data_path": cwd + "/input"})
    configs.update({"output_path": cwd + "/output"})
    configs.update({"analysis_path": cwd + "/output/Analysis"})

    configs.update({"territory_codes_file": "/0_Code_Lists/territory_codes.xlsx"})
    configs.update({"generalizations_file": "generalizations.xlsx"})
    configs.update({"routes_file": "RoutesGatesMunicipalities.xlsx"})

    configs.update({"census_file": "lide_2016.csv"})

    configs.update({"shapefile_municipalities_name": "obec.shp"})
    configs.update({"shapefile_zsj_city_name": "zsj.shp"})
    configs.update({"shapefile_cadastral_city_name": "ku.shp"})
    configs.update({"shapefile_gates": "gates.shp"})

    configs.update({"hts_CzechiaHTS_persons_file": "CzechiaHTS_P_weighted.csv"})
    configs.update({"hts_CzechiaHTS_households_file": "CzechiaHTS_H.csv"})
    configs.update({"hts_CzechiaHTS_trips_file": "CzechiaHTS_T.csv"})
    configs.update({"hts_CityHTS_persons_file": "CityHTS_P_weighted.csv"})
    configs.update({"hts_CityHTS_households_file": "CityHTS_H.csv"})
    configs.update({"hts_CityHTS_trips_file": "CityHTS_T.csv"})

    configs.update({"CzechiaHTS_persons_descr_file": "CzechiaHTS_P_popis.xml"})
    configs.update({"CzechiaHTS_households_descr_file": "CzechiaHTS_H_popis.xml"})
    configs.update({"CzechiaHTS_trips_descr_file": "CzechiaHTS_T_popis.xml"})

    configs.update({"facilities_work_home_secondary_file": "allUstiORP.shp"})
    configs.update({"facilities_work_home_file": "budovy_07_2016_usti_district.dbf"})
    configs.update({"facilities_edu_file": "EduUstiORP.shp"})
    configs.update({"facilities_osm_file": "osmUstiORP.shp"})
    configs.update({"facilities_area_file": "POI_workers_visitors.shp"})
    configs.update({"buildings_occupancy_file": "aktivity_people.xlsx"})

    configs.update({"osm_file": "usti_orp.osm.xml"})
    configs.update({"osm_matsim_file": "usti_orp.osm.gz"})

    def config(self, name):

        value = self.configs[name]

        return value
    
    def stage(self, name):

        try:
            value = self.stages[name]
        except:
            try:
                context.stages[name] = pickle.load(open(cwd + "/cache/" + name + ".p", "rb"))
            except:
                try:
                    exec(name + ".validate(context)")
                except:
                    skip = 1
                exec("context.stages[name] = " + name + ".execute(context)")
                pickle.dump(context.stages[name], open(cwd + "/cache/" + name + ".p", "wb"))
            value = self.stages[name]

        return value

# Initiate the pipeline stages
context = contexts()
cwd = os.getcwd()

try:
    context.stages["data.spatial.zones"] = pickle.load(open(cwd + "/cache/data.spatial.zones.p", "rb" ))
except:
    data.spatial.zones.validate(context)
    context.stages["data.spatial.zones"] = data.spatial.zones.execute(context)
    pickle.dump(context.stages["data.spatial.zones"], open(cwd + "/cache/data.spatial.zones.p", "wb" ))

try:
    context.stages["data.census.raw"] = pickle.load(open(cwd + "/cache/data.census.raw.p", "rb" ))
except:
    data.census.raw.validate(context)
    context.stages["data.census.raw"] = data.census.raw.execute(context)
    pickle.dump(context.stages["data.census.raw"], open(cwd + "/cache/data.census.raw.p", "wb" ))

try:
    context.stages["data.census.cleaned"] = pickle.load(open(cwd + "/cache/data.census.cleaned.p", "rb" ))
except:
    data.census.cleaned.validate(context)
    context.stages["data.census.cleaned"] = data.census.cleaned.execute(context)
    pickle.dump(context.stages["data.census.cleaned"], open(cwd + "/cache/data.census.cleaned.p", "wb" ))

try:
    context.stages["data.hts.cleaned"] = pickle.load(open(cwd + "/cache/data.hts.cleaned.p", "rb" ))
except:
    data.hts.cleaned.validate(context)
    context.stages["data.hts.cleaned"] = data.hts.cleaned.execute(context)
    pickle.dump(context.stages["data.hts.cleaned"], open(cwd + "/cache/data.hts.cleaned.p", "wb" ))

try:
    context.stages["data.hts.filtered"] = pickle.load(open(cwd + "/cache/data.hts.filtered.p", "rb" ))
except:
    data.hts.filtered.validate(context)
    context.stages["data.hts.filtered"] = data.hts.filtered.execute(context)
    pickle.dump(context.stages["data.hts.filtered"], open(cwd + "/cache/data.hts.filtered.p", "wb" ))

try:
    context.stages["synthesis.population.trips"] = pickle.load(open(cwd + "/cache/synthesis.population.trips.p", "rb" ))
except:
    synthesis.population.trips.validate(context)
    context.stages["synthesis.population.trips"] = synthesis.population.trips.execute(context)
    pickle.dump(context.stages["synthesis.population.trips"], open(cwd + "/cache/synthesis.population.trips.p", "wb" ))

try:
    context.stages["synthesis.destinations"] = pickle.load(open(cwd + "/cache/synthesis.destinations.p", "rb" ))
except:
    synthesis.destinations.validate(context)
    context.stages["synthesis.destinations"] = synthesis.destinations.execute(context)
    pickle.dump(context.stages["synthesis.destinations"], open(cwd + "/cache/synthesis.destinations.p", "wb" ))

try:
    context.stages["synthesis.population.spatial.by_person.primary_zones"] = \
        pickle.load(open(cwd + "/cache/synthesis.population.spatial.by_person.primary_zones.p", "rb" ))
except:
    synthesis.population.spatial.by_person.primary_zones.validate(context)
    context.stages["synthesis.population.spatial.by_person.primary_zones"] = \
        synthesis.population.spatial.by_person.primary_zones.execute(context)
    pickle.dump(context.stages["synthesis.population.spatial.by_person.primary_zones"],
                open(cwd + "/cache/synthesis.population.spatial.by_person.primary_zones.p", "wb" ))

try:
    context.stages["synthesis.population.spatial.by_person.primary_locations"] = \
        pickle.load(open(cwd + "/cache/synthesis.population.spatial.by_person.primary_locations.p", "rb" ))
except:
    synthesis.population.spatial.by_person.primary_locations.validate(context)
    context.stages["synthesis.population.spatial.by_person.primary_locations"] = \
        synthesis.population.spatial.by_person.primary_locations.execute(context)
    pickle.dump(context.stages["synthesis.population.spatial.by_person.primary_locations"],
                open(cwd + "/cache/synthesis.population.spatial.by_person.primary_locations.p", "wb" ))
    
try:
    context.stages["synthesis.population.spatial.by_person.secondary.distance_distributions"] = \
        pickle.load(open(cwd + "/cache/synthesis.population.spatial.by_person.secondary.distance_distributions.p", "rb" ))
except:
    synthesis.population.spatial.by_person.secondary.distance_distributions.validate(context)
    context.stages["synthesis.population.spatial.by_person.secondary.distance_distributions"] = \
        synthesis.population.spatial.by_person.secondary.distance_distributions.execute(context)
    pickle.dump(context.stages["synthesis.population.spatial.by_person.secondary.distance_distributions"],
                open(cwd + "/cache/synthesis.population.spatial.by_person.secondary.distance_distributions.p", "wb" ))

try:
    context.stages["synthesis.population.spatial.by_person.secondary.locations"] = \
        pickle.load(open(cwd + "/cache/synthesis.population.spatial.by_person.secondary.locations.p", "rb" ))
except:
    synthesis.population.spatial.by_person.secondary.locations.validate(context)
    context.stages["synthesis.population.spatial.by_person.secondary.locations"] = \
        synthesis.population.spatial.by_person.secondary.locations.execute(context)
    pickle.dump(context.stages["synthesis.population.spatial.by_person.secondary.locations"],
                open(cwd + "/cache/synthesis.population.spatial.by_person.secondary.locations.p", "wb" ))

try:
    context.stages["matsim.scenario.population"] = pickle.load(open(cwd + "/cache/matsim.scenario.population.p", "rb" ))
except:
    matsim.scenario.population.validate(context)
    context.stages["matsim.scenario.population"] = matsim.scenario.population.execute(context)
    pickle.dump(context.stages["matsim.scenario.population"], open(cwd + "/cache/matsim.scenario.population.p", "wb" ))

# Not at the moment
# try:
#     context.stages["matsim.scenario.households"] = pickle.load(open(cwd + "/cache/matsim.scenario.households.p", "rb" ))
# except:
#     matsim.scenario.households.validate(context)
#     context.stages["matsim.scenario.households"] = matsim.scenario.households.execute(context)
#     pickle.dump(context.stages["matsim.scenario.households"], open(cwd + "/cache/matsim.scenario.households.p", "wb" ))

try:
    context.stages["matsim.scenario.facilities"] = pickle.load(open(cwd + "/cache/matsim.scenario.facilities.p", "rb" ))
except:
    matsim.scenario.facilities.validate(context)
    context.stages["matsim.scenario.facilities"] = matsim.scenario.facilities.execute(context)
    pickle.dump(context.stages["matsim.scenario.facilities"], open(cwd + "/cache/matsim.scenario.facilities.p", "wb" ))

try:
    context.stages["synthesis.output"] = pickle.load(open(cwd + "/cache/synthesis.output.p", "rb" ))
except:
    synthesis.output.validate(context)
    context.stages["synthesis.output"] = synthesis.output.execute(context)
    pickle.dump(context.stages["synthesis.output"], open(cwd + "/cache/synthesis.output.p", "wb" ))