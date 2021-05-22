import os
from dagster import sensor, RunRequest, repository
import glob
from pipelines import process_docx
from pathdef import *

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
                run_config={"solids": {"process_file": {"config": {"filepath": filepath}}}},
            )


@repository
def sensor_repo():
    return [
        add_docx_sensor
    ]