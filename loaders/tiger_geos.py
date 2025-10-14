"""
downloads state, county shapes from tiger/line,
converts to geojson,
loads to db.
"""
#TODO optimize

import geopandas as gpd
import shapely.wkt
from settings import data_dir
import json
from models import State, County, db

### START CONFIG ###
# tiger/line 2024 state boundaries zipped shapefile
# key names must be consistent in spelling and case to peewee models
shape_urls = {'State': 'https://www2.census.gov/geo/tiger/TIGER2024/STATE/tl_2024_us_state.zip',
              'County': 'https://www2.census.gov/geo/tiger/TIGER2024/COUNTY/tl_2024_us_county.zip'}
# output files go in data/geos
geos_dir = data_dir + 'geos/'
### END CONFIG ###


def construct_path(locale):
    """
    given a locale,
    returns path to geojson
    """
    return geos_dir + locale + '.geojson'


def get_shapes(simplify=True):
    """
    get fresh shapes from tiger/lines
    save as geojson.
    simplify=False for production viz (as needed)
    """
    # loop thru state and county urls
    for locale in shape_urls:
        # geopandas can read from url
        shapes = gpd.read_file(shape_urls[locale])
        # simplify for research
        if simplify:
            # not sure how many vertices to reduce ...
            shapes['geometry'] = shapes['geometry'].simplify(tolerance=0.001,preserve_topology=True)
            # ... same with rounding precision
            shapes['geometry'] = shapes['geometry'].apply(lambda geom: shapely.wkt.loads(shapely.wkt.dumps(geom, rounding_precision=4)))

        # save as geojson
        shapes.to_file(construct_path(locale), driver="GeoJSON", indent=None)
        print('saved',construct_path(locale))


def load(get=False):
    """
    load saved geojson
    look up locale 
    save to db
    """
    # optional - get shapes from internet 
    if get:
        get_shapes()
    # figure out which code to use for locale look up
    fips_field_names = {"State": "STATEFP", "County": "GEOID"}
    # iterate through state and counties using ObjClass variable
    for ObjClass in [State, County]:
        # check if this is a state or county
        class_name = ObjClass.__name__
        # ... and load geo data accordingly
        geojson = json.load(open(construct_path(class_name)))
        # for bulk update
        objs = []
        # roll through each state dict
        for feature in geojson['features']:
            # fips for db lookup
            # infer fips field name
            fips_field = fips_field_names[class_name]
            # get fips
            fips = feature['properties'][fips_field]
            try:
                # get state from db
                obj = ObjClass.get(ObjClass.fips==fips)
                obj.geometry = feature['geometry']
                objs.append(obj)
            except Exception as e:
                print(feature['properties']['NAME'],e)
        # stay in transaction
        with db.atomic():
            # bulk update
            ObjClass.bulk_update(objs,
                              fields=['geometry'],
                              batch_size=50)
                
