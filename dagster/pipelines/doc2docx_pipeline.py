from dagster.core.definitions.events import Output
from docx import Document
from docx.shared import Inches
from pathlib import Path
from dagster import pipeline, solid, AssetMaterialization, Field, EventMetadata, repository, file_relative_path
from dagster.experimental import DynamicOutput, DynamicOutputDefinition
import subprocess
import sys
import time
import uuid

from pathdef import *

@solid(output_defs=[DynamicOutputDefinition(str)])
def doc2docx(_context):
    # path = _context.solid_config["path"]
    for pdf in external_data.glob("*.doc"):
        docx_file = processed_data / pdf.with_suffix(".docx").name
        _context.log.info(f"Process {pdf} to {docx_file}")
        cmd = f"soffice --headless --convert-to docx --outdir {processed_data} {pdf}"
        _context.log.info(str(cmd))
        result = subprocess.run(cmd.split())
        
        yield AssetMaterialization(
            asset_key="doc2docx",
            description="Conversion from pdf to docx",
            metadata={"docx_file": EventMetadata.path(str(docx_file))},
        )
        yield DynamicOutput(str(processed_data.name), mapping_key=str(processed_data.name) + str(uuid.uuid4()).replace('-','_'))


@pipeline
def doc2docx_pipeline():
    files = doc2docx()
    

@repository
def pipelines():
    return [doc2docx_pipeline]