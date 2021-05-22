from docx import Document
from docx.shared import Inches
from pathlib import Path
from dagster import pipeline, solid, AssetMaterialization, Output, EventMetadata
import os
from dagster import sensor, RunRequest, repository
import glob

import json

from pathdef import *


# https://docs.dagster.io/concepts/partitions-schedules-sensors/sensors
@solid(config_schema={"filepath": str})
def load_document(_context):
    filepath = _context.solid_config["filepath"]
    filename = filepath.split('/')[-1]
    _context.log.info("Load document: %s" % filepath)
    doc = Document(filepath)
    return {
        "name": filename,
        "paragraphs": [p.text for p in doc.paragraphs],
    }  # , "serial":doc.__dict__}


@solid
def dict_to_json(_context, d):
    data = json.dumps(d)
    return data


@solid
def write_to_json(_context, data):
    json_path = str(processed_data) + f"/{_context.run_id}.json"

    with open(json_path, "w") as fp:
        json.dump(data, fp, ensure_ascii=False, indent=4)

    yield AssetMaterialization(
        asset_key="json_path",
        description="Cereals data frame sorted by caloric content",
        metadata={"json_path": EventMetadata.path(json_path)},
    )
    yield Output(None)


@pipeline
def process_docx():
    write_to_json(dict_to_json(load_document()))



@sensor(pipeline_name="process_docx")
def add_docx_sensor(_context):
    types = (str(external_data) + '/*.docx', str(interim_data) + '/*.docx') # the tuple of file types
    files_grabbed = []
    for files in types:
        files_grabbed.extend(glob.glob(files))

    for filepath in files_grabbed:
        
        if os.path.isfile(filepath):
            filename = filepath.split('/')[-1]
            yield RunRequest(
                run_key=filename,
                run_config={"solids": {"load_document": {"config": {"filepath": filepath}}}},
            )



@repository
def sensor_repo():
    return [process_docx, add_docx_sensor]

