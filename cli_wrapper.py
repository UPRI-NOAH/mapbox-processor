import asyncio
import json
import logging
import os
import subprocess
import time

import aiofiles
from dotenv import load_dotenv

load_dotenv()
FPATH = os.path.dirname(os.path.abspath(__file__))
logging.basicConfig(format="%(asctime)s - %(message)s", level=logging.DEBUG)

region = "PH000000000"
hazard_type = "SSH"
hazard_level = "ssa4"

def call_tilesets_cli(parameters):
    retcode = subprocess.call(
        parameters,
        stderr=subprocess.STDOUT,
    )
    print(retcode)


async def run_tilesets_cli(cmd):
    proc = await asyncio.create_subprocess_shell(
        cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
    )
    stdout, stderr = await proc.communicate()
    if stderr:
        logging.info(f"[stderr]\n{stderr.decode()}")
        return None
    logging.info(f"[stdout]\n{stdout.decode()}")
    result = stdout.decode().strip()
    return result


async def get_files_full_path(folder):
    file_paths = []
    for file in os.listdir(folder):
        full_path = os.path.join(folder, file)
        file_paths.append(full_path)
    return file_paths


async def write_to_file(cmd, file, precision):
    res = await run_tilesets_cli(cmd=cmd.format(file=file, precision=precision))
    logging.info(res)
    if res is None:
        return None
    result_js = json.loads(res)
    async with aiofiles.open("1m_estimates.txt", "a") as f:
        await f.write(f"{file}\t{result_js.get('km2')}\t{result_js.get('precision')}\n")
    logging.info("Wrote results.")


async def estimate_all_tileset_area(folder, precision):
    """
    Wrapper for the tilesets estimate-area command. This will instead estimate
    the area of all files in a folder
    """
    cmd = "tilesets estimate-area {file} -p {precision}"
    files = await get_files_full_path(folder)
    tasks = []
    for file in files:
        tasks.append(write_to_file(cmd=cmd, file=file, precision=precision))
    await asyncio.gather(*tasks)


async def determine_source_name(file):
    """
    This function returns the specific source name to be used for
    a specific geojson file to be uploaded.

    Convention: <prov>_<hazard>
    File-naming assumption: <Province>_<Hazard>_Type.geojson.
        i.e. Aklan_Flood_100year.geojson
    """
    return "_".join(file.split("_")[:2]).lower()


async def create_all_tileset_source(geo_folder):
    cmd = "tilesets upload-source {user} {source_name} {path}"
    tasks = []
    for file in os.listdir(geo_folder):
        if file.endswith(".geojson"):
            full_path = os.path.join(geo_folder, file)
            source_name = await determine_source_name(file)
            tasks.append(
                run_tilesets_cli(
                    cmd=cmd.format(
                        user=os.getenv("USER"),
                        source_name=source_name,
                        path=full_path,
                    )
                )
            )
    await asyncio.gather(*tasks)


async def create_all_tilesets():
    pass


async def publish_all_tilesets():
    pass


def main_estimater(folder, precision="10m"):
    asyncio.run(estimate_all_tileset_area(folder=folder, precision=precision))


def main_upload_source(folder):
    asyncio.run(create_all_tileset_source(geo_folder=folder))


if __name__ == "__main__":
    t0 = time.time()
    folder = "/path_to/mapbox-processor/data/geojson/hazfolder"
    main_estimater(folder, precision="1m")

    # FOR BULK RUN
    # for x in range(1, 19):
    #     if x < 10:
    #         ph = region.replace("PH000000000", f"PH0{x}0000000")
    #         print(ph)
    #         folder = f"data/geojson/multisource/{ph}/{hazard_type}/{hazard_level}"
    #         if os.path.exists(folder):
    #             print("True")
    #             main_estimater(folder, precision="1m")
    #         else:
    #             continue

    #     else:
    #         ph = region.replace("PH00", f"PH{x}")
    #         print(ph)
    #         folder = f"data/geojson/multisource/{ph}/{hazard_type}/{hazard_level}"
    #         if os.path.exists(folder):
    #             print("True")
    #             main_estimater(folder, precision="1m")
    #         else:
    #             continue
 
    t1 = time.time()
    print(f"Elapsed time async: {t1-t0:.2f}s")
