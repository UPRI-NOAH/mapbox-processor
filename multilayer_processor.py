"""Collection of functions for connecting to MapBox Tilesets API"""
import json
import logging
import os
import time

import requests
from dotenv import load_dotenv

from mapbox_api import bulk_create_tileset_source

load_dotenv()
logging.basicConfig(format="%(asctime)s - %(message)s", level=logging.INFO)
RECIPES_FOLDER = os.path.join(os.path.dirname(os.path.abspath(__file__)), "recipes")
GEOJSON_FOLDER = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "data/geojson"
)


def generate_multilayer_recipe(multilayer_folder):  # On top for visibility
    """Function to generate recipe with multiple layer"""
    geo_folder = GEOJSON_FOLDER + multilayer_folder
    recipe_name = geo_folder.split("geojson/")[-1].rsplit("/", 1)[0]
    ext = ".geojson"
    geojson_file_list = [
        i for i in os.listdir(geo_folder) if os.path.splitext(i)[1] == ext
    ]

    layer = {}

    for file in sorted(geojson_file_list):
        filename = file.rsplit(".", 1)[0]
        norm_filename = filename.lower()
        tls_src = {
            "source": f"mapbox://tileset-source/{os.getenv('USER')}/{norm_filename}",
            "minzoom": 4,
            "maxzoom": 13,
            # Use simplification value of 1 for zoom >= 10. Use default 4 below that
            "features": {"simplification": ["case", [">=", ["zoom"], 7], 1, 4]},
        }

        layer[norm_filename] = tls_src

    geojson_dict = {"version": 1, "layers": layer}

    with open(
        recipe_path := os.path.join(RECIPES_FOLDER, f"{recipe_name}.json"), "w"
    ) as recipe_file:
        json.dump(geojson_dict, recipe_file, indent=4)
    return recipe_name, recipe_path


def publish_multilayer_tileset(recipe):
    """Function to process and publish created tileset."""
    tileset_name = recipe + "_tls"
    url = f"https://api.mapbox.com/tilesets/v1/{os.getenv('USER')}.{tileset_name}/publish?access_token={os.getenv('MAPBOX_ACCESS_TOKEN')}"  # noqa: E501
    response = requests.request("POST", url=url)
    logging.info(f"{response.status_code}:{response.text}")
    if response.status_code != 200:
        logging.info(f"retrying {tileset_name}")
        time.sleep(30)
        response = requests.request("POST", url=url)
        logging.info(f"retried {tileset_name}: {response.status_code}:{response.text}")


def create_multilayer_tileset(recipe, recipe_url, publish=True):
    """
    Function to create an empty tileset using a multi layer recipe. Need to publish
    tileset for it to be usable.
    Args:
        recipe (str): recipe name to be set as tileset and mapbox name.
        recipe_url (str): Full path of the recipe file.
        publish (bool): Specify if you want to publish directly after creating the
                        tileset. Defaults to True so that we are able to do bulk
                        operations explicitly.
    """
    tileset_name = recipe + "_tls"  # Mapbox tileset identifier
    mapbox_name = recipe  # Mapbox verbose name
    url = f"https://api.mapbox.com/tilesets/v1/{os.getenv('USER')}.{tileset_name}?access_token={os.getenv('MAPBOX_ACCESS_TOKEN')}"  # noqa: E501
    payload = {}
    payload["name"] = mapbox_name
    payload["description"] = f"Tiles for {mapbox_name}."
    payload["private"] = False
    with open(recipe_url) as json_recipe:
        payload["recipe"] = json.load(json_recipe)

    response = requests.request("POST", url=url, json=payload)
    logging.info(response.text)
    if response.status_code != 200:
        logging.info(f"retrying {tileset_name}")
        time.sleep(30)
        response = requests.request("POST", url=url, json=payload)
        logging.info(f"retried {tileset_name}: {response.status_code}:{response.text}")

    # Run publish
    if publish:
        publish_multilayer_tileset(recipe)


def bulk_upload_pipeline(multilayer_folder):
    """Pipeline for running multi layer process"""
    geo_folder = GEOJSON_FOLDER + multilayer_folder
    bulk_create_tileset_source(geo_folder)
    recipe, recipe_url = generate_multilayer_recipe(multilayer_folder)
    create_multilayer_tileset(recipe, recipe_url)


if __name__ == "__main__":
    t0 = time.time()
    multilayer_folder = "/ph_lh_lh2/"  # folder name contains multiple geojson
    bulk_upload_pipeline(multilayer_folder)

    t1 = time.time()
    logging.info(f"Elapsed time: {t1-t0:.2f}s")
