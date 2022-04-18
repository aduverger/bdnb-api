from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import geopandas as gpd
import math
from geopy.geocoders import Nominatim
from pyproj import Transformer
import os

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Allows all origins
    allow_credentials=True,
    allow_methods=["*"],  # Allows all methods
    allow_headers=["*"],  # Allows all headers
)


@app.get("/")
def index():
    return {"greeting": "Hello world"}


@app.get("/getbbox")
def get_bbox(xmin: float, xmax: float, ymin: float, ymax: float):
    gdf = gpd.read_file(
        os.path.join(
            os.path.dirname(os.path.dirname(os.path.realpath(__file__))),
            os.path.join("data", "bnb_export.gpkg"),
        ),
        bbox=(xmin, ymin, xmax, ymax),
    )
    gdf = gdf[
        [
            "geometry",
            "cerffo2020_annee_construction",
            "adedpe202006_mean_class_conso_ener",
            "adedpe202006_mean_conso_ener",
            "adedpe202006_mean_class_estim_ges",
            "adedpe202006_mean_estim_ges",
        ]
    ]
    gdf.rename(
        columns={
            "cerffo2020_annee_construction": "Année de construction",
            "adedpe202006_mean_class_conso_ener": "Etiquette énergétique (DPE)",
            "adedpe202006_mean_conso_ener": "Consommations énergétiques, kWhEP/m².an (DPE)",
            "adedpe202006_mean_class_estim_ges": "Etiquette carbone (DPE)",
            "adedpe202006_mean_estim_ges": "Emissions de GES, kgC02eq/m².an (DPE)",
        },
        inplace=True,
    )
    return gdf.to_json()


@app.get("/getaddress")
def get_address(address: str, radius: int):
    geolocator = Nominatim(user_agent="bdnb")
    location = geolocator.geocode(address)
    x, y = location.latitude, location.longitude
    xmin = x - radius / (2 * 110.574)
    xmax = x + radius / (2 * 110.574)
    ymin = y - radius / (2 * 111.320 * math.cos(math.pi * x / 180))
    ymax = y + radius / (2 * 111.320 * math.cos(math.pi * x / 180))
    transformer = Transformer.from_crs("epsg:4326", "epsg:2154")
    xmin, ymin = transformer.transform(xmin, ymin)
    xmax, ymax = transformer.transform(xmax, ymax)

    return get_bbox(xmin, xmax, ymin, ymax)
