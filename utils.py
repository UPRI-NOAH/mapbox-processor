"""Lifted from mapbox cli for normalizing data"""
import json
from itertools import chain


def normalize(file):
    with open(file, encoding="utf-8") as src:
        yield from iter_features(iter(src))


def to_feature(obj):
    """Converts an object to a GeoJSON Feature
    Returns feature verbatim or wraps geom in a feature with empty
    properties.
    Raises
    ------
    ValueError
    Returns
    -------
    Mapping
        A GeoJSON Feature represented by a Python mapping
    """
    if obj["type"] == "Feature":
        return obj
    elif "coordinates" in obj:
        return {"type": "Feature", "properties": {}, "geometry": obj}
    else:
        raise ValueError("Object is not a feature or geometry")


def iter_features(geojsonfile, func=None):
    """Extract GeoJSON features from a text file object.
    Given a file-like object containing a single GeoJSON feature
    collection text or a sequence of GeoJSON features, iter_features()
    iterates over lines of the file and yields GeoJSON features.
    Parameters
    ----------
    geojsonfile: a file-like object
        The geojsonfile implements the iterator protocol and yields
        lines of JSON text.
    func: function, optional
        A function that will be applied to each extracted feature. It
        takes a feature object and may return a replacement feature or
        None -- in which case iter_features does not yield.
    Yields
    ------
    Mapping
        A GeoJSON Feature represented by a Python mapping
    """
    func = func or (lambda x: x)
    first_line = next(geojsonfile)

    # Does the geojsonfile contain RS-delimited JSON sequences?
    if first_line.startswith("\x1e"):
        text_buffer = first_line.strip("\x1e")
        for line in geojsonfile:
            if line.startswith("\x1e"):
                if text_buffer:
                    obj = json.loads(text_buffer)
                    if "coordinates" in obj:
                        obj = to_feature(obj)
                    newfeat = func(obj)
                    if newfeat:
                        yield newfeat
                text_buffer = line.strip("\x1e")
            else:
                text_buffer += line
        # complete our parsing with a for-else clause.
        else:
            obj = json.loads(text_buffer)
            if "coordinates" in obj:
                obj = to_feature(obj)
            newfeat = func(obj)
            if newfeat:
                yield newfeat

    # If not, it may contains LF-delimited GeoJSON objects or a single
    # multi-line pretty-printed GeoJSON object.
    else:
        # Try to parse LF-delimited sequences of features or feature
        # collections produced by, e.g., `jq -c ...`.
        try:
            obj = json.loads(first_line)
            if obj["type"] == "Feature":
                newfeat = func(obj)
                if newfeat:
                    yield newfeat
                for line in geojsonfile:
                    newfeat = func(json.loads(line))
                    if newfeat:
                        yield newfeat
            elif obj["type"] == "FeatureCollection":
                for feat in obj["features"]:
                    newfeat = func(feat)
                    if newfeat:
                        yield newfeat
            elif "coordinates" in obj:
                newfeat = func(to_feature(obj))
                if newfeat:
                    yield newfeat
                for line in geojsonfile:
                    newfeat = func(to_feature(json.loads(line)))
                    if newfeat:
                        yield newfeat

        # Indented or pretty-printed GeoJSON features or feature
        # collections will fail out of the try clause above since
        # they'll have no complete JSON object on their first line.
        # To handle these, we slurp in the entire file and parse its
        # text.
        except ValueError:
            text = "".join(chain([first_line], geojsonfile))
            obj = json.loads(text)
            if obj["type"] == "Feature":
                newfeat = func(obj)
                if newfeat:
                    yield newfeat
            elif obj["type"] == "FeatureCollection":
                for feat in obj["features"]:
                    newfeat = func(feat)
                    if newfeat:
                        yield newfeat
            elif "coordinates" in obj:
                newfeat = func(to_feature(obj))
                if newfeat:
                    yield newfeat
