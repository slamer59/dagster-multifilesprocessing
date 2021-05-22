from dagster.core.definitions.events import Output
from docx import Document
from docx.shared import Inches
from pathlib import Path
from dagster import (
    pipeline,
    solid,
    AssetMaterialization,
    Field,
    EventMetadata,
    repository,
    file_relative_path,
)
import glob
from dagster import sensor, RunRequest, repository

from dagster.experimental import DynamicOutput, DynamicOutputDefinition
import subprocess
import uuid
import os
from pathdef import *


@solid(config_schema={"filepath": str})
def doc2docx(_context):
    filepath = _context.solid_config["filepath"]
    docx_file = interim_data / Path(filepath).with_suffix(".docx").name
    _context.log.info(f"Doc2docx {filepath} to {docx_file}")
    cmd = f"soffice --headless --convert-to docx --outdir {interim_data} {filepath}"
    result = subprocess.run(cmd.split())

    yield AssetMaterialization(
        asset_key="doc2docx",
        description="Conversion from doc to docx",
        metadata={"docx_file": EventMetadata.path(str(docx_file))},
    )
    yield Output(None)


@pipeline
def doc2docx_pipeline():
    files = doc2docx()



@sensor(pipeline_name="doc2docx_pipeline")
def add_doc_sensor(_context):
    types = (str(external_data) + '/*.doc') # the tuple of file types
    files_grabbed = []
    for files in types:
        files_grabbed.extend(glob.glob(files))

    for filepath in files_grabbed:
        
        if os.path.isfile(filepath):
            filename = filepath.split('/')[-1]
            yield RunRequest(
                run_key=filename,
                run_config={"solids": {"doc2docx": {"config": {"filepath": filepath}}}},
            )



@repository
def doc_repo():
    return [add_doc_sensor,doc2docx_pipeline]
