
# MapBox Toolkit

This project is a collection of functions and CLI wrapper for interacting with the Mapbox Tile Service.

## Installation

This project requires Python 3.9 and `pipenv` to be installed globally. To start, install all the required libraries on Pipfile.

First, install non dev libraries.
```bash
pipenv install
```
I've added geopandas as dev package since it requires the correct version of GDAL to be installed. To install GDAL, make sure you are inside
the virtual environment before running this script:
```bash
pipenv shell
sh install_gdal.sh
```
This will install the necessary libraries for you. The python wrapper will also be installed via `pip`.
Lastly, install the dev packages.
```bash
pipenv install --dev
```



## Environment Variables

A sample environment variable `.env.sample` is provided to run this project. Follow these steps to get your Mapbox access token:
1. Create a mapbox account [here](https://account.mapbox.com/).
1. Create a new [access token](https://account.mapbox.com/access-tokens/) that has the scopes `tilesets:write`, `tilesets:read`, and `tilesets:list`. Do not share this token! Copy the token value to your env file.
1. Also add your username to the env.

## Documentation


There are 3 important scripts on the project:
```
cli_wrapper.py
mapbox_api.py
shp_converter.py
```

### cli_wrapper
This script contains functions to wrap the `tilesets` Mapbox CLI. This is done for us to be able to do bulk operations optimally by using async operations.
Currently,only the area estimation is fully implemented since the rest of integration is done via API.

### mapbox_api
This script contains functions to interact with the MTS API.
The current operations implemented are:
1. Creating a tile source by uploading a geojson file. Bulk operation also implemented.
1. Create tilesets from a generated recipe.
1. Update tileset recipe.
1. Publish created tileset.

### shp_converter
Converts a directory of shapefiles to geojson which is required by MTS.
