import osmium as osm
import pandas as pd
import shapely.wkb as wkblib
import os

wkbfab = osm.geom.WKBFactory()

def configure(context):
    context.config("data_path")    
    context.config("osm_file")

def validate(context):
    data_path = context.config("data_path")
    osm_file = context.config("osm_file")

    if not os.path.isdir(data_path):
        raise RuntimeError("Input directory must exist: %s" % data_path)

    if not os.path.exists(osm_file):
        raise RuntimeError("Input directory must exist: %s" % osm_file)

class OSMHandler(osm.SimpleHandler):
    def __init__(self):
        osm.SimpleHandler.__init__(self)
        self.osm_data = []

    def tag_inventory(self, elem, elem_type):
        for tag in elem.tags:
            self.osm_data.append([elem_type, 
                                   elem.id,                                   
                                   tag.k, 
                                   tag.v])

    def way(self, w):
        try:
            wkb = wkbfab.create_linestring(w)
            line = wkblib.loads(wkb, hex=True)

            for tag in w.tags:
                if tag.k in ('public_transport', 'amenity', 'building', 'office',
                             'shop', 'leisure', "natural", "tourism", "hiking", 'residential', 'living_street'):
                    if (tag.k != 'tourism' or tag.v != "information") and (tag.k != 'building' or tag.v != "yes"):
                        self.osm_data.append(['way',
                                              w.id,
                                              tag.k,
                                              tag.v, line.centroid.x, line.centroid.y])
                        break
            else:
                for tag in w.tags:
                    if tag.k == "building:ruian:type":
                        self.osm_data.append(['way',
                                              w.id,
                                              'building',
                                              tag.v, line.centroid.x, line.centroid.y])
                        break
                # else:
                #     for tag in w.tags:
                #         if "ref:ruian" in tag.k:
                #             self.osm_data.append(['way',
                #                                   w.id,
                #                                   'building',
                #                                   'residential', line.centroid.x, line.centroid.y])
                #             break
        except Exception:
            pass
    def node(self, w):
        try:
            for tag in w.tags:
                if tag.k in ('public_transport', 'amenity', 'building', 'office',
                             'shop', 'leisure', "natural", "tourism", "hiking", 'residential', 'living_street'):
                    if (tag.k != 'tourism' or tag.v != "information") and (tag.k != 'building' or tag.v != "yes"):
                        self.osm_data.append(['node',
                                       w.id,
                                       tag.k,
                                       tag.v, w.location.lon, w.location.lat])
                        break
            else:
                for tag in w.tags:
                    if tag.k == "building:ruian:type":
                        self.osm_data.append(['node',
                                              w.id,
                                              'building',
                                              tag.v, w.location.lon, w.location.lat])
                        break
                # else:
                #     for tag in w.tags:
                #         if "ref:ruian" in tag.k:
                #             self.osm_data.append(['node',
                #                                   w.id,
                #                                   'building',
                #                                   'residential', w.location.lon, w.location.lat])
                #             break
        except Exception:
            pass

def execute(context):

    print("Extracting Facilities from OpenStreetMaps")

    # For RUIAN codes, see ./input/0_Code_Lists/STAVBA_building_usage.xlsx

    # Scan the input file and fills the handler list accordingly
    osmhandler = OSMHandler()
    osmhandler.apply_file("%s/Facilities/%s" % (context.config("data_path"),
                                                context.config("osm_file")), locations=True)

    # Transform the list into a pandas DataFrame
    data_colnames = ['type', 'id', 'tagkey', 'tagvalue', 'lon', 'lat']
    df_osm = pd.DataFrame(osmhandler.osm_data, columns=data_colnames)

    # Define the name of amenities and buildings for each trip purpose
    amenity_freetime = ['social_facility',
                        'theatre', 'swimming_pool',
                        'place_of_worship', 'library', 'science_park', 'social_centre',
                        'arts_centre', 'community_centre', 'restaurant', 'events_centre', 'fast_food', 'pub', 'cafe',
                        'commercial', 'cinema', 'winery', 'bar', 'amphitheatre', 'concert_hall', 'studio', 'nightclub',
                        'food_court', 'language_school',
                        'bbq', 'music_venue', 'senior_center', 'pool', 'casino',
                        'events_venue', 'spa', 'boat_rental',
                        'senior_centre',
                        'music_venue;bar', 'community_center', 'ice_cream', 'church', 'park', 'stripclub',
                        'swingerclub',
                        'biergarten',
                        'music_rehearsal_place', 'cafeteria', 'meditation_centre', 'gym',
                        'planetarium', 'clubhouse', 'dive_centre', 'community_hall',
                        'event_hall', 'bicycle_rental', 'club', 'gambling']
    amenity_shopping = ['pharmacy', 'convenience_store', 'commercial', 'marketplace', 'winery', 'food_court',
                        'convenience']
    amenity_work = ['school',
                    'bank', 'hospital', 'social_facility', 'police', 'pharmacy',
                    'theatre', 'university', 'college', 'swimming_pool',
                    'place_of_worship', 'library', 'clinic', 'science_park',
                    'conference_centre', 'trailer_park', 'social_centre',
                    'arts_centre', 'courthouse', 'post_office', 'community_centre',
                    'car_rental', 'restaurant', 'ranger_station',
                    'events_centre', 'convenience_store', 'townhall', 'mortuary',
                    'fuel', 'car_wash', 'fast_food', 'pub', 'fire_station', 'cafe',
                    'doctors', 'commercial', 'nursing_home', 'marketplace', 'cinema',
                    'public_building', 'winery',
                    'dentist', 'bar', 'amphitheatre', 'ferry_terminal',
                    'concert_hall', 'studio', 'nightclub', 'kindergarten',
                    'civic', 'food_court', 'childcare', 'prison',
                    'caravan_rental', 'monastery', 'dialysis', 'veterinary',
                    'music_venue', 'senior_center', 'pool', 'casino',
                    'events_venue', 'preschool',
                    'animal_shelter', 'spa', 'boat_rental',
                    'senior_centre', 'brokerage', 'vehicle_inspection', 'healthcare',
                    'music_venue;bar', 'community_center', 'embassy', 'ice_cream',
                    'tailor', 'coworking_space', 'church',
                    'storage_rental', 'stripclub', 'swingerclub',
                    'office', 'biergarten',
                    'music_rehearsal_place', 'cafeteria', 'truck_rental',
                    'sperm_bank', 'meditation_centre',
                    'funeral_parlor', 'cruise_terminal',
                    'crematorium', 'gym',
                    'planetarium', 'clubhouse',
                    'language_school', 'convenience', 'music_school', 'dive_centre',
                    'community_hall',
                    'event_hall', 'research_institute',
                    'club', 'gambling',
                    'retirement_village']
    amenity_education = ['school', 'university', 'college', 'kindergarten']
    amenity_errands = ['bank', 'hospital', 'clinic', 'post_office', 'doctors', 'nursing_home', 'public_building',
                       'dentist', 'dialysis', 'veterinary', 'vehicle_inspection', 'healthcare', 'tailor', 'sperm_bank',
                       'funeral_parlor', 'crematorium']

    building_home = ['2', '3', '6', '7', '8', '11', '20', '99', # RUIAN codes when OSM categories not available
                     'apartments', 'bungalow', 'cabin', 'detached', 'dormitory', 'farm', 'house', 'houseboat',
                     'residential', 'semidetached_house', 'static_caravan', 'terrace']
    building_freetime = ['8', '9', '15', '20', '99', # RUIAN codes when OSM categories not available
                         "mixed", 'mixed_use', 'arena', 'museum', 'theatre', 'stadium', 'sports_centre', 'church',
                         'civic', 'temple', 'mixed_use', 'amphitheatre', 'cinema', 'cathedral', 'synagogue',
                         'community_centre', 'chapel', 'castle', 'clubhouse']
    building_shopping = ['10', '20', '99', # RUIAN codes when OSM categories not available
                         "retails", "apartments;commerical", "mixed", 'mixed_use', "kiosk", "supermarket",
                         "mall", "commercial", "shop", "retail"]
    building_work = ['1', '2', '4', '5', '9', '10', '11', '12', '13', '14', '15', '16', '17', '20', '21', '28', '99',
                     # RUIAN codes when OSM categories not available
                     'hotel', 'tower', 'police_station',
                     'retail', 'shop', 'arena', 'transportation',
                     'office', 'commercial', 'hangar', 'industrial',
                     'terminal', 'mall', 'warehouse', 'multi_level_parking',
                     'university', 'dormitory', 'museum', 'theatre',
                     'stadium', 'fire_station', 'control_tower',
                     'manufacture', 'sports_centre', 'hospital', 'train_station',
                     'civic', 'church',
                     'gymnasium', 'temple', 'mixed_use',
                     'central_office', 'amphitheatre',
                     'Business', 'barn', 'data_center', 'cinema',
                     'service', 'supermarket', 'weapon_armory',
                     'cathedral', 'farm_auxiliary', 'factory',
                     'station', 'library', 'farm', 'mosque', 'stable', 'historic_building',
                     'synagogue', 'convent',
                     'mortuary',
                     'prison',
                     'brewery', 'Office',
                     'monastery', 'clinic', 'kiosk', 'carpark',
                     'motel', 'research', 'charity', 'medical', 'offices', 'community_centre',
                     'Athletic_field_house', 'depot', 'Laundry', 'chapel',
                     'lighthouse',
                     'clubhouse', 'guardhouse', 'bungalow', 'retails', 'tech_cab',
                     'commerical', 'gasstation', 'yes;offices', 'castle']
    building_education = ['30', '99', # RUIAN codes when OSM categories not available
                          'college', 'kindergarten', 'school', 'university']
    building_errands = ['5', '14', '15', '20', '99', # RUIAN codes when OSM categories not available
                        "mixed", 'mixed_use', 'hospital', 'service', 'clinic', 'medical']

    # Option below only useful when few buildings classified as homes (not the case of the OSM data for Ustí district)
    # It basically gets the streets that are residential
    # df_home = df_osm[df_osm['type'] == 'way']
    # df_home = df_home[df_home['tagkey'] == 'highway']
    # df_home = df_home[(df_home['tagvalue'] == 'residential') | (df_home['tagvalue'] == 'living_street')]

    # Option when many buildings classified as homes (the case of the OSM data for Ustí district)
    df_home = df_osm[(df_osm["tagkey"] == 'building') & (df_osm["tagvalue"].isin(building_home))]
    df_home = df_home.assign(purpose='1')

    df_freetime = df_osm[((df_osm["tagkey"] == 'amenity') & (df_osm["tagvalue"].isin(amenity_freetime)))
                         | ((df_osm["tagkey"] == 'building') & (df_osm["tagvalue"].isin(building_freetime)))
                         | (df_osm["tagkey"] == 'leisure')
                         | (df_osm["tagkey"] == 'natural')
                         | (df_osm["tagkey"] == 'tourism')
                         | (df_osm["tagkey"] == 'hiking')]
    df_freetime = df_freetime.assign(purpose='2')

    df_shopping = df_osm[((df_osm["tagkey"]=='amenity') & (df_osm["tagvalue"].isin(amenity_shopping)))
                         | ((df_osm["tagkey"]=='building') & (df_osm["tagvalue"].isin(building_shopping)))
                         | (df_osm["tagkey"] == 'shop')]
    df_shopping = df_shopping.assign(purpose='3')

    df_work = df_osm[((df_osm["tagkey"] == 'amenity') & (df_osm["tagvalue"].isin(amenity_work)))
                         | ((df_osm["tagkey"] == 'building') & (df_osm["tagvalue"].isin(building_work)))
                         | (df_osm["tagkey"] == 'office')]
    df_work = df_work.assign(purpose='4')

    df_education = df_osm[((df_osm["tagkey"] == 'amenity') & (df_osm["tagvalue"].isin(amenity_education)))
                     | ((df_osm["tagkey"] == 'building') & (df_osm["tagvalue"].isin(building_education)))]
    df_education = df_education.assign(purpose='5')

    df_errands = df_osm[((df_osm["tagkey"] == 'amenity') & (df_osm["tagvalue"].isin(amenity_errands)))
                         | ((df_osm["tagkey"] == 'building') & (df_osm["tagvalue"].isin(building_errands)))]
    df_errands = df_errands.assign(purpose='6')


    return df_home, df_freetime, df_shopping, df_work, df_education, df_errands
    
