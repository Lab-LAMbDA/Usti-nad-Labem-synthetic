import shapely.geometry as geo
from tqdm import tqdm
import geopandas as gpd

def toGPD(df, x ="lon", y ="lat", crs ="epsg:5514"):
    """Transforme pandas dataframe into geopandas dataframe"""

    df = df.assign(geometry=[geo.Point(*coord) for coord in tqdm(zip(df[x], df[y]), total=len(df), ascii=True,
                                                                 desc="Converting coordinates")])
    df = gpd.GeoDataFrame(df)
    df.crs = crs

    return df