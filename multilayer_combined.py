import json

with open("data/sources.json") as f:
    js = json.load(f)

haz_types = ["FH_100yr", "SSH_ssa4"]
for haz in haz_types:
    layers = {}
    for source in js:
        layer_config = {
            "minzoom": 4,
            "maxzoom": 13,
            "features": {"simplification": ["case", [">=", ["zoom"], 7], 1, 4]},
        }
        # print(source)
        if haz in (source_url := source["id"]):
            # if "FH_100yr" in (source_url := source["id"]):
            # print(source_url, "\n")
            layer_config["source"] = source_url
            layer_key = source["id"].split("/")[-1]
            layers[layer_key] = layer_config

    recipe = {"version": 1, "layers": layers}
    with open(f"recipes/ph_{haz.lower()}_recipe.json", "w") as rcp:
        json.dump(recipe, rcp, indent=4)
