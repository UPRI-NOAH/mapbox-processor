import logging
import multiprocessing
import os

import geopandas as gpd

fpath = os.path.dirname(os.path.abspath(__file__))
logging.basicConfig(format="%(asctime)s - %(message)s", level=logging.INFO)


def generate_file_paths(shp_folder, geo_folder):
    file_paths = []
    for file in os.listdir(shp_folder):
        if file.endswith(".shp"):
            paths = (
                os.path.join(fpath, shp_folder, file),
                dst := os.path.join(fpath, geo_folder, f"{file.split('.')[0]}.geojson"),
            )
            if not os.path.isfile(dst):  # Do not regenerate existing geojson
                file_paths.append(paths)
    return file_paths


def shp_to_geojson(input_shp, output_file):
    logging.info(f"Processing {input_shp}.")
    shp = gpd.read_file(input_shp)
    print(shp.columns)
    # Add here the function to check if file has correct var column
    shp.to_file(output_file, driver="GeoJSON")
    logging.info(f"Converted to {output_file}")


def convert_folder_contents(paths, num_cores=os.cpu_count()):
    with multiprocessing.Pool(num_cores) as pool:
        pool.starmap(shp_to_geojson, paths)


if __name__ == "__main__":
    shp_folder = "data/shp/"
    geo_folder = "data/geojson/"
    # shp_files = [p for p in os.listdir(shp_folder) if p.endswith(".shp")]
    paths = generate_file_paths(shp_folder, geo_folder)
    convert_folder_contents(paths)
