import logging
import multiprocessing
import os
import json
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
    shp2wgs = shp.to_crs(epsg=4326)
    print(shp2wgs.columns)
    # Add here the function to check if file has correct var column
    shp2wgs.to_file(output_file, driver="GeoJSON")
    logging.info(f"Converted to {output_file}")


def convert_folder_contents(paths, num_cores=5):
    with multiprocessing.Pool(num_cores) as pool:
        pool.starmap(shp_to_geojson, paths)


if __name__ == "__main__":
    #Bulk Conversion
    # shp_folder = "path to shapefile folder"
    # geo_folder = "path to geojson folder"
    # paths = generate_file_paths(shp_folder, geo_folder)
    # convert_folder_contents(paths)
    shp_file = "/home/noahdev-hpc/Documents/git/random_processor/output/merged_df.shp"
    geojson_file = "/home/noahdev-hpc/Documents/git/mapbox-processor/data/geojson/LH/LH22024/PH_LH_DF.geojson"
    shp_to_geojson(shp_file, geojson_file)



























































