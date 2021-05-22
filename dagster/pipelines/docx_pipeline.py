from docx import Document
from docx.shared import Inches
from pathlib import Path
from dagster import pipeline, solid, AssetMaterialization, Output, EventMetadata
import json
from pathdef import *


# https://docs.dagster.io/concepts/partitions-schedules-sensors/sensors
@solid(config_schema={"filename": str})
def load_document(_context):
    name = _context.solid_config["filename"]
    doc = Document(external_data / name)
    return {"name": name, "paragraphs": [p.text for p in doc.paragraphs]} #, "serial":doc.__dict__}


@solid
def dict_to_json(_context, d):
    data = json.dumps(d)
    return data


@solid
def write_to_json(_context, data):
    json_path = str(processed_data) + f"_{_context.run_id}.json"

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
