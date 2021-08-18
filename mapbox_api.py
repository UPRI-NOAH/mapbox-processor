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
                "minzoom": 4,  # int(os.getenv("MIN_ZOOM", 4)),
                "maxzoom": 13,  # int(os.getenv("MAX_ZOOM", 13)),
                # Use simplification value of 1 for zoom >= 10. Use default 4 below that
                "features": {"simplification": ["case", [">=", ["zoom"], 7], 1, 4]},
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


def generate_tileset_name(file):
    """Returns input filename without extension, all lowercase."""
    file = os.path.basename(file)
    return "_".join(file.split(".")[:-1]).lower()


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


def tileset_name_to_source(tileset_name):
    return " ".join(tileset_name.split("_")).title()


def create_tileset_source(geo_file, replace=False):
    """
    Creates the tilesource in mapbox. Basically, uploads the geojson into MapBox's
    server for processing.

    Args:
        geo_file (str): Full path of the geojson being uploaded
        replace (bool): Defaults to False. Setting to True will enable the script to
                        replace the source file.
    """
    source_name = generate_tileset_name(os.path.basename(geo_file))
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
        logging.info(js_resp := response.json())

    if response.status_code == 200:
        tileset_id = js_resp.get("id")
        recipe_path = generate_recipe(tileset_id, geo_file)
        return recipe_path


def get_layer_name(recipe):
    with open(recipe) as recipe_file:
        recipe_json = json.load(recipe_file)
        return list(recipe_json["layers"].keys())[0]


def create_tileset(recipe, publish=True):
    """
    Function to create an empty tileset using a recipe. Need to publish
    tileset for it to be usable.
    Args:
        recipe (str): Full path of the recipe file.
        publish (bool): Specify if you want to publish directly after creating the
                        tileset. Defaults to True so that we are able to do bulk
                        operations explicitly.
    """
    layer_name = get_layer_name(recipe)  # Mapbox layer name
    tileset_name = layer_name + "_tls"  # Mapbox tileset identifier
    mapbox_name = tileset_name_to_source(layer_name)  # Mapbox verbose name
    url = f"https://api.mapbox.com/tilesets/v1/{os.getenv('USER')}.{tileset_name}?access_token={os.getenv('MAPBOX_ACCESS_TOKEN')}"  # noqa: E501
    payload = {}
    payload["name"] = mapbox_name
    payload["description"] = f"Tiles for {mapbox_name}."
    payload["private"] = False
    with open(recipe) as json_recipe:
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
        publish_tileset(recipe)


def update_tileset_recipe(recipe):
    """Function to update the recipe of a tileset. Call publish after updating recipe"""
    tileset_name = get_layer_name(recipe) + "_tls"
    url = f"https://api.mapbox.com/tilesets/v1/{os.getenv('USER')}.{tileset_name}/recipe?access_token={os.getenv('MAPBOX_ACCESS_TOKEN')}"  # noqa: E501

    with open(recipe) as json_recipe:
        payload = json.load(json_recipe)
    response = requests.request("PATCH", url=url, json=payload)
    logging.info(response)


def publish_tileset(recipe):
    """Function to process and publish created tileset."""
    tileset_name = get_layer_name(recipe) + "_tls"
    url = f"https://api.mapbox.com/tilesets/v1/{os.getenv('USER')}.{tileset_name}/publish?access_token={os.getenv('MAPBOX_ACCESS_TOKEN')}"  # noqa: E501
    response = requests.request("POST", url=url)
    logging.info(f"{response.status_code}:{response.text}")
    if response.status_code != 200:
        logging.info(f"retrying {tileset_name}")
        time.sleep(30)
        response = requests.request("POST", url=url)
        logging.info(f"retried {tileset_name}: {response.status_code}:{response.text}")


def bulk_create_tileset_source(folder):
    """
    Args:
        folder (str): The folder containing geojson files. It will not
                      include non-geojson files and sub-dirs.
    """
    files = get_files_full_path(folder)
    concurrent_runner(create_tileset_source, files)


def bulk_create_tilesets_from_recipes(recipe_folder):
    """
    Bulk create tilesets from existing geojson and publishes the created tilesets.
    """
    recipes = get_files_full_path(recipe_folder)
    concurrent_runner(create_tileset, recipes)


def bulk_publish_tilesets_from_recipes(recipe_folder):
    recipes = get_files_full_path(recipe_folder)
    for recipe in recipes:
        publish_tileset(recipe)
        time.sleep(30)


def single_upload_pipeline(geo_file, replace=True):
    # Upload source file
    recipe_path = create_tileset_source(geo_file, replace=replace)

    # Create tileset from recipe and
    create_tileset(recipe_path)


def bulk_upload_pipeline(geojson_folder, recipe_folder):
    bulk_create_tileset_source(folder=geojson_folder)
    bulk_create_tilesets_from_recipes(recipe_folder=recipe_folder)
    bulk_publish_tilesets_from_recipes(recipe_folder=recipe_folder)


if __name__ == "__main__":
    t0 = time.time()
    file = "/home/dev-hpc/noahv2/noah-api/sensors.geojson"
    single_upload_pipeline(file, replace=True)
    # geojson_folder = "data/geojson/"
    # recipe_folder = "recipes/"
    # bulk_upload_pipeline(geojson_folder, recipe_folder)
    t1 = time.time()
    logging.info(f"Elapsed time: {t1-t0:.2f}s")
