"""Collection of functions for connecting to MapBox Tilesets API"""
import concurrent.futures as concurr
import json
import logging
import os
import tempfile
import time

import requests
from clint.textui.progress import Bar as ProgressBar
from dotenv import load_dotenv
from requests_toolbelt import MultipartEncoder, MultipartEncoderMonitor

from utils import normalize

load_dotenv()
logging.basicConfig(format="%(asctime)s - %(message)s", level=logging.INFO)
RECIPES_FOLDER = os.path.join(os.path.dirname(os.path.abspath(__file__)), "recipes")


def generate_recipe(tileset_id, geo_file):  # On top for visibility
    filename = generate_tileset_name(geo_file)
    recipe = {
        "version": 1,
        "layers": {
            f"{filename}": {
                "source": tileset_id,
                "minzoom": int(os.getenv("MIN_ZOOM", 0)),
                "maxzoom": int(os.getenv("MAX_ZOOM", 5)),
                # Use simplification value of 1 for zoom >= 10. Use default 4 below that
                "features": {"simplification": ["case", [">=", ["zoom"], 10], 1, 4]},
            }
        },
    }

    with open(
        recipe_path := os.path.join(RECIPES_FOLDER, f"{filename}.json"), "w"
    ) as recipe_file:
        json.dump(recipe, recipe_file, indent=4)
    return recipe_path


def create_callback(encoder):
    encoder_len = encoder.len
    bar = ProgressBar(expected_size=encoder_len, filled_char="=")

    def callback(monitor):
        bar.show(monitor.bytes_read)

    return callback


def get_files_full_path(folder):
    file_paths = []
    for file in os.listdir(folder):
        full_path = os.path.join(folder, file)
        file_paths.append(full_path)
    return file_paths


def determine_source_name(file):
    """
    This function returns the specific source name to be used for
    a specific geojson file to be uploaded.

    Convention: <prov>_<hazard>
    File-naming assumption: <Province>_<Hazard>_Type.geojson.
        i.e. Aklan_Flood_100year.geojson
    """
    return "_".join(file.split("_")[:2]).lower()


def concurrent_runner(func, iterable, num_workers=5):
    """
    Executor/runner function to run functions in a ThreadPool.
    """
    with concurr.ThreadPoolExecutor(max_workers=num_workers) as executor:
        # Create mapping of executable task for each item in the iterable
        future_to_data = {executor.submit(func, param): param for param in iterable}
        for future in concurr.as_completed(future_to_data):
            file = future_to_data[future]
            try:
                data = future.result()
                print(data)
            except Exception as e:
                logging.exception(f"Exception for {os.path.basename(file)}: {e}")
            else:
                logging.info(f"Successful operation for {os.path.basename(file)}.")


def generate_tileset_name(file):
    """Returns input filename without extension, all lowercase."""
    file = os.path.basename(file)
    return "_".join(file.split(".")[:-1]).lower()


def tileset_name_to_source(tileset_name):
    return " ".join(tileset_name.split("_")).title()


def create_tileset_source(geo_file):
    """
    Creates the tilesource in mapbox. Basically, uploads the geojson into MapBox's
    server for processing.
    """
    source_name = determine_source_name(os.path.basename(geo_file))
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

        response = requests.request(
            "POST",
            url,
            data=monitor,
            headers={
                "Content-Disposition": "multipart/form-data",
                "Content-type": monitor.content_type,
            },
        )
        logging.info(js_resp := response.json()["message"])
    if response.status_code == 200:
        tileset_id = js_resp.get("id")
        recipe_path = generate_recipe(tileset_id, geo_file)
        return recipe_path


def get_layer_name(recipe):
    with open(recipe) as recipe_file:
        recipe_json = json.load(recipe_file)
        return list(recipe_json["layers"].keys())[0]


def create_tileset(recipe):
    """
    Function to create an empty tileset using a recipe. Need to publish
    tileset for it to be usable.
    Args:
        recipe (str): Full path of the recipe file.
    """
    layer_name = get_layer_name(recipe)  # Mapbox layer name
    tileset_name = layer_name + "_tiles"  # Mapbox tileset identifier
    mapbox_name = tileset_name_to_source(layer_name)  # Mapbox verbose name
    url = f"https://api.mapbox.com/tilesets/v1/{os.getenv('USER')}.{tileset_name}?access_token={os.getenv('MAPBOX_ACCESS_TOKEN')}"  # noqa: E501
    payload = {}
    payload["name"] = mapbox_name
    payload["description"] = f"Tiles for {mapbox_name}."
    payload["private"] = False
    with open(recipe) as json_recipe:
        payload["recipe"] = json.load(json_recipe)
    logging.info(url)
    response = requests.request("POST", url=url, json=payload)
    logging.info(response.text)


def update_tileset_recipe(recipe):
    """Function to update the recipe of a tileset. Call publish after updating recipe"""
    tileset_name = get_layer_name(recipe) + "_tiles"
    url = f"https://api.mapbox.com/tilesets/v1/{os.getenv('USER')}.{tileset_name}/recipe?access_token={os.getenv('MAPBOX_ACCESS_TOKEN')}"  # noqa: E501

    with open(recipe) as json_recipe:
        payload = json.load(json_recipe)
    response = requests.request("PATCH", url=url, json=payload)
    logging.info(response)


def publish_tileset(geo_file):
    """Function to process and publish created tileset."""
    tileset_name = generate_tileset_name(geo_file) + "_tiles"
    url = f"https://api.mapbox.com/tilesets/v1/{os.getenv('USER')}.{tileset_name}/publish?access_token={os.getenv('MAPBOX_ACCESS_TOKEN')}"  # noqa: E501
    response = requests.request("POST", url=url)
    logging.info(response.text)


def bulk_create_tileset_source(folder):
    """
    Args:
        folder (str): The folder containing geojson files. It will not
                      include non-geojson files and sub-dirs.
    """
    files = get_files_full_path(folder)
    concurrent_runner(create_tileset_source, files)


def bulk_create_tilesets_from_recipes(geo_folder, recipe_folder):
    """
    Bulk create tilesets from existing geojson and
    """
    recipes = get_files_full_path(recipe_folder)
    concurrent_runner(create_tileset, recipes)


if __name__ == "__main__":
    t0 = time.time()
    folder = "/home/cloud/projects/noah/noah-gis/data/geojson"
    # bulk_create_tileset_source(folder)
    file = "data/populated_places.geojson.ld"
    recipe = "recipes/aklan_flood_100year.json"
    # update_tileset_recipe(recipe)
    # update_tileset_recipe(file, recipe)
    # publish_tileset(file)
    # generate_recipe(tileset_id="aklan_flood", geo_file="Aklan_Flood_100year.js")
    create_tileset(recipe)
    publish_tileset(geo_file="data/geojson/Aklan_Flood_100year.geojson")
    t1 = time.time()
    print(f"Elapsed time: {t1-t0:.2f}s")
