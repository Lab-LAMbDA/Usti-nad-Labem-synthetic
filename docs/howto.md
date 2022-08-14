# How to create a scenario

The following sections describe three steps of using the pipeline. To generate
the synthetic population, first all necessary data must be gathered. Afterwards,
the pipeline can be run to create a synthetic population in *CSV*, *XML* and *GPKG*
format.

- [Running the pipeline](#section-data)

## <a name="section-data"/>Running the pipeline

To set up all dependencies, we recommend setting up a Python environment using [Anaconda](https://www.anaconda.com/):

```bash
conda env create -f environment.yml
```

This will create a new Anaconda environment with the name `usti_syn_gen`. (In
case you don't want to use Anaconda, we also provide a `requirements.txt` to
install all dependencies in a `virtualenv` using `pip install -r requirements.txt`).

To activate the environment, run:

```bash
conda activate usti_syn_gen
```

Now have a look at `SynPopGen.py` which is the main file of the pipeline. For the moment, it is important to adjust
two configuration values inside of `SynPopGen.py`:

- `data_path`: This should be the path to the folder where you were collecting
and arranging all the raw data sets as described above.
It must exist and contain the sub-folders: Census, Facilities, HTS, and Spatial.
- `output_path`: This should be the path to the folder where the output data 
of the pipeline should be stored. 
It must exist and contain the sub-folders: Analysis, Census, HTS, Locations, 
ODs, Population and Trips
- `analysis_path`: This should be the path to the folder where you will have 
results of your data analysis.

If you had cloned the repository, nothing is needed. However, if you prefer to set up your own data and output 
directories, make sure your have created all the following directories in your working directory:

```bash
mkdir cache
mkdir input
mkdir output
mkdir input/0_Code_Lists
mkdir input/Census
mkdir input/Facilities
mkdir input/HTS
mkdir input/Spatial
mkdir output/Analysis
mkdir output/Census
mkdir output/HTS
mkdir output/Locations
mkdir output/ODs
mkdir output/Population
mkdir output/Trips
```

Everything is set now to run the pipeline (i.e. run or debug the file `SynPopGen.py`) using your preferred IDE, such as Pycharm. 
This file (somehow) replicates the [synpp](https://github.com/eqasim-org/synpp) pipeline that was the original approach in the previous pipelines.

The necessary raw files in the sub-folders of the `data` folder are:
- `lide_2016.csv` (in `input/Census`) contains the estimated population (the process we call 'demographic transition') 
of the study area and their respective sociodemographic attributes for the year of 2016.
- Set of shapefile files `osmUstiORP`, `EduUstiORP`, `budovy_07_2016_usti_district`, and `POI_workers_visitors`, 
representing mainly the data necessary for secondary locations, educational locations, home and work locations, and area 
of buildings, respectively (in `input/Facilities`). As the datasets for home and work locations, as well as the area of 
buildings were not open, they are not included in the repository. However, a processed input including all the necessary
facilities is available in the shapefiles `allUstiORP`.
- `aktivity_people` (in `input/Facilities`) defining the number of workers and visitors to different type of buildings 
per square meter.
- Sets of `.csv` files `CityHTS_.csv` and `CzechiaHTS_.csv` (in `input/Census`) with the answers of the household travel
surveys. Notice that the set of files `CityHTS_.csv` could not be made public, thus only the files `CzechiaHTS_.csv` are
in the repository.
- Set of shapefile files `zsj`, `ku`, `cast`, `obec`, `gates` for the basic settlement units, cadastral units, city parts,
towns, and synthetic gates (in `input/Spatial`).
- `territory_codes.xlsx` (in `input/0_Code_Lists`) defining the territorial codes of every area in the Czech Republic.
- `generalizations.xlsx` for the harmonisation of the different datasets throughout the project.
- `RoutesGatesMunicipalities.xlsx` for the travel time, distance (from/to Ustí nad Labem town) and population of other
towns in the Czech Republic.

The resulting files in the sub-folders of the `output` folder are:

- `meta.json` contains some meta data, e.g. with which random seed or sampling
rate the population was created and when.
- `persons.csv` and (optionally) `households.csv` (in `output/Population`) contain all persons 
and households in the population with their respective sociodemographic attributes.
- `activities.csv` and `trips.csv` (in `output/Trips`)  contain all activities and trips in the
daily mobility patterns of these people including attributes on the purposes
of activities or transport modes for the trips.
- `activities.gpkg` and `trips.gpkg` (in `output/Trips`) represent the same trips and
activities, but in the spatial *GPKG* format. Activities and trips contain point and line
geometries to indicate where they happen.
- `all_locations.csv` contains the information about all facilities, buildings and amenities in the model,
while `locations_edu.csv`, `locations_home.csv`, `locations_work.csv`, `locations_secondary.csv` 
and their respective *GPKG* format (in `output/Locations`)  contain all assigned locations of each person
- `od_edu.csv` and `od_work.csv` (in `output/ODs`) contain the weight of trip distribution between
the zones.
- In other folders `output/Census` and `output/HTS` you find the sampled persons from the Census and
used data from the HTS, respectively. 
- In the `output` folder, input files for MATSim simulation:
  - `population.xml.gz` containing the agents and their daily plans.
  - `facilities.xml.gz` containing all businesses, services, etc.
  - (optional) `households.xml.gz` containing additional household information