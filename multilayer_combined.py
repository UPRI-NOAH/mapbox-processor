import json
import os
import time

from multilayer_processor import create_multilayer_tileset

RECIPES_FOLDER = os.path.join(os.path.dirname(os.path.abspath(__file__)), "recipes")


def generate_combined_recipe():
    """Function generate multilayer recipe."""

    with open("data/source.json") as f:  # array of tileset source
        js = json.load(f)

    hazard_types = [
        "FH_100yr",
        "FH_25yr",
        "FH_5yr",
        "LH_lh1",
        "SSH_ssa1",
        "SSH_ssa2",
        "SSH_ssa3",
        "SSH_ssa4",
    ]  # noqa: E501
    for hazard in hazard_types:
        layers = {}
        for source in js:
            layer_config = {
                "minzoom": 4,
                "maxzoom": 13,
                "features": {"simplification": ["case", [">=", ["zoom"], 7], 1, 4]},
            }

            if hazard in (source_url := source["url"]):
                print(source_url)
                layer_config["source"] = source_url
                layer_key = source["url"].split("/")[-1]
                layers[layer_key] = layer_config

        recipe = {"version": 1, "layers": layers}
        with open(f"recipes/ph_{hazard.lower()}.json", "w") as rcp:
            json.dump(recipe, rcp, indent=4)


def create_combined_tileset():
    """
    Function to process and publish multilayer
    tileset using multiple source reciper.
    """
    recipes = os.listdir(RECIPES_FOLDER)

    for recipe in recipes:
        recipe_name = recipe.rsplit(".", 1)[0]
        recipe_url = f"{RECIPES_FOLDER}/{recipe}"
        create_multilayer_tileset(recipe_name, recipe_url)


if __name__ == "__main__":
    t0 = time.time()
    generate_combined_recipe()
    create_combined_tileset()
    sleep_counter = 0
