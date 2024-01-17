"""Collection of functions for connecting to MapBox Tilesets API"""
import json
import logging
import os
import tempfile
import time

import requests
from dotenv import load_dotenv
from requests_toolbelt import MultipartEncoder, MultipartEncoderMonitor

from mapbox_api import (
    concurrent_runner,
    create_callback,
    create_tileset,
    get_files_full_path,
)
from utils import normalize

load_dotenv()
logging.basicConfig(format="%(asctime)s - %(message)s", level=logging.INFO)
RECIPES_FOLDER = os.path.join(os.path.dirname(os.path.abspath(__file__)), "recipes")
GEOJSON_FOLDER = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "data/geojson"
)
region = "ph" # Name of Area, PH if tileset is nationwide
hazard_type = "fh" # Hazard Type
hazard_level = "100yr" # Hazard Level


def generate_container_recipe(tileset_id, source_name):  # On top for visibility
    """Function to generate recipe for a container"""
    recipe = {
        "version": 1,
        "layers": {
            f"{source_name}": {
                "source": tileset_id,
                "minzoom": 7,
                "maxzoom": 13,
                # Use simplification value of 1 for zoom >= 10. Use default 4 below that
                # "features": {"simplification": ["case", [">=", ["zoom"], 13], 1, 13]},
            }
        },
    }

    with open(
        recipe_path := os.path.join(RECIPES_FOLDER, f"{source_name}.json"), "w"
    ) as recipe_file:
        json.dump(recipe, recipe_file, indent=4)
    return recipe_path


def create_multilayer_tls_src(geo_file, replace=False):
    """
    Creates the tilesource in mapbox. Basically, uploads the multiple geojson into
    MapBox's server for processing.

    Args:
        geo_file (str): Full path of the geojson being uploaded
        replace (bool): Defaults to False. Setting to True will enable the script to
                        replace the source file.
    """
    # reg = os.path.dirname(geo_file).split("/")[-3]
    # haztype = os.path.dirname(geo_file).split("/")[-2]
    # hazlevel = os.path.dirname(geo_file).split("/")[-1]
    source_name = f"{region}_{hazard_type}_{hazard_level}"
    url = f"https://api.mapbox.com/tilesets/v1/sources/{os.getenv('USER')}/{source_name}?access_token={os.getenv('MAPBOX_ACCESS_TOKEN')}"  # noqa: E501
    features = normalize(geo_file)
    with tempfile.TemporaryFile() as file:
        for feature in features:
            file.write(
                (json.dumps(feature, separators=(",", ":")) + "\n").encode("utf-8")
            )

        file.seek(0)
        multipart_encoded_file = MultipartEncoder(fields={"file": ("file", file)})
        callback = create_callback(multipart_encoded_file)
        monitor = MultipartEncoderMonitor(multipart_encoded_file, callback)

        method = "POST"
        if replace:
            method = "PUT"

        response = requests.request(
            method,
            url,
            data=monitor,
            headers={
                "Content-Disposition": "multipart/form-data",
                "Content-type": monitor.content_type,
            },
        )
        logging.info(response.json())


def bulk_multilayer_tls_src(folder):
    """Bulk process in createing tileset source"""
    files = get_files_full_path(folder)
    concurrent_runner(create_multilayer_tls_src, files)


def single_container_pipeline(region, hazard_type, hazard_level):
    """Pipeline for running multiple tileset in one source"""
    geojson_folder = f"{GEOJSON_FOLDER}/{region}/{hazard_type}/{hazard_level}/"
    print(geojson_folder)
    if os.path.isdir(geojson_folder):
        print(os.path.isdir(geojson_folder))
        bulk_multilayer_tls_src(geojson_folder)
    else:
        pass


if __name__ == "__main__":
    t0 = time.time()
    sleep_counter = 0
    # Bulk Run
    # for x in range(1, 18):
    #     if x < 10:
    #         ph = region.replace("PH00", f"PH0{x}")
    #         print(ph)
    #         single_container_pipeline(ph, hazard_type, hazard_level)
    #         sleep_counter += 1
    #         if sleep_counter % 5:
    #             time.sleep(0.005)

    #     else:
    #         ph = region.replace("PH00", f"PH{x}")
    #         print(ph)
    #         single_container_pipeline(ph, hazard_type, hazard_level)
    #         sleep_counter += 1

    #         if sleep_counter % 5:
    #             time.sleep(0.005)

    #Single Run
    single_container_pipeline(region, hazard_type, hazard_level)

    t1 = time.time()
    logging.info(f"Elapsed time: {t1-t0:.2f}s")
