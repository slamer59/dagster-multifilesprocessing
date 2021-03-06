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
from dagster.experimental import DynamicOutput, DynamicOutputDefinition
import subprocess
import uuid

from pathdef import *


@solid(output_defs=[DynamicOutputDefinition(str)])
def pdf2docx_s(_context):
    for pdf in external_data.glob("*.pdf"):
        docx_file = interim_data / pdf.with_suffix(".docx").name
        _context.log.info(f"Process {pdf} to {docx_file}")
        cmd = f"pdf2docx convert {pdf} {docx_file}".split()
        result = subprocess.run(cmd)

        yield AssetMaterialization(
            asset_key="pdf2docx",
            description="Converstion from pdf to docx",
            metadata={"docx_file": EventMetadata.path(str(docx_file))},
        )
        yield DynamicOutput(
            str(interim_data.name),
            mapping_key=str(interim_data.name) + str(uuid.uuid4()).replace("-", "_"),
        )


@pipeline
def pdf2docx_pipeline():
    files = pdf2docx_s()


@repository
def pipelines():
    return [pdf2docx_pipeline]
