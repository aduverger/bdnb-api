import json
import math
import os
import re

import geopandas as gpd
import numpy as np
import pandas as pd
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from geopy.geocoders import Nominatim
from pyproj import Transformer

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
            "data",
            "bnb_export.gpkg",
        ),
        bbox=(xmin, ymin, xmax, ymax),
    )
    gdf["Surface habitable (estimée)"] = gdf[
        [
            "cerffo2020_nb_log",
            "adedpe202006_logtype_s_hab",
            "adedpe202006_logtype_type_batiment",
        ]
    ].apply(lambda x: calculate_SHAB(*x), axis=1)
    gdf.adedpe202006_logtype_ch_type_ener_corr.replace(
        to_replace="", value="N.C.", inplace=True
    )
    gdf.adedpe202006_logtype_ecs_type_ener.replace(
        to_replace="", value="N.C.", inplace=True
    )
    gdf.fillna("N.C.", inplace=True)

    gdf["Types d'énergie"] = gdf[
        ["adedpe202006_logtype_ch_type_ener_corr", "adedpe202006_logtype_ecs_type_ener"]
    ].apply(lambda x: get_ener_type(*x), axis=1)

    gdf["etaban202111_label"] = gdf["etaban202111_label"].apply(get_street)

    gdf = gdf[
        [
            "geometry",
            "etaban202111_label",
            "adedpe202006_logtype_type_batiment",
            "cerffo2020_annee_construction",
            "Surface habitable (estimée)",
            "cerffo2020_nb_log",
            "adedpe202006_mean_class_conso_ener",
            "adedpe202006_mean_conso_ener",
            "adedpe202006_mean_class_estim_ges",
            "adedpe202006_mean_estim_ges",
            "Types d'énergie",
            "mtedle2019_elec_conso_tot",
            "mtedle2019_gaz_conso_tot",
            "adedpe202006_logtype_ch_gen_lib_princ",
            "adedpe202006_logtype_ecs_gen_lib_princ",
        ]
    ]
    gdf = gdf.rename(
        columns={
            "etaban202111_label": "Adresse",
            "adedpe202006_logtype_type_batiment": "Type de batiment",
            "cerffo2020_annee_construction": "Année de construction",
            "cerffo2020_nb_log": "Nombre de logements",
            "adedpe202006_mean_class_conso_ener": "Etiquette énergétique (DPE)",
            "adedpe202006_mean_conso_ener": "Conso énergétique [kWhEP/m².an] (DPE)",
            "adedpe202006_mean_class_estim_ges": "Etiquette carbone (DPE)",
            "adedpe202006_mean_estim_ges": "Emissions de GES [kgC02eq/m².an] (DPE)",
            "mtedle2019_elec_conso_tot": "Conso électrique [kwhEF/an] (MTEDLE)",
            "mtedle2019_gaz_conso_tot": "Conso de gaz [kwhEF/an] (MTEDLE)",
            "adedpe202006_logtype_ch_gen_lib_princ": "Générateurs de chauffage",
            "adedpe202006_logtype_ecs_gen_lib_princ": "Générateurs d'ECS",
        }
    )
    gdf["Etiquette énergétique (DPE)"].replace(
        to_replace="N", value="N.C.", inplace=True
    )
    gdf["Etiquette carbone (DPE)"].replace(to_replace="N", value="N.C.", inplace=True)

    return json.loads(gdf.to_json())


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


def calculate_SHAB(nb_log, shab, type_bat):
    if type_bat in ("Non résidentiel", "Logements collectifs") or (
        shab > 400 and nb_log > 50
    ):
        return shab
    return nb_log * shab


def get_ener_type(type_chauf, type_ecs):
    if type_chauf == type_ecs:
        return type_chauf
    if type_chauf == "N.C.":
        return type_ecs
    if type_ecs == "N.C.":
        return type_chauf
    type_chauf = type_chauf.split(" + ")
    type_ecs = type_ecs.split(" + ")
    return " + ".join(set(type_chauf + type_ecs))


def get_street(address: str):
    street = re.findall(r".*(?=\d{5})", address)
    if len(street) > 0:
        return street[0][:-1]
    return address
