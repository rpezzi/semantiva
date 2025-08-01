# Copyright 2025 Semantiva authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import argparse
from pathlib import Path
from fastapi import FastAPI, HTTPException
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from semantiva import Pipeline, load_pipeline_from_yaml
from semantiva.tools.pipeline_inspector import PipelineInspector

app = FastAPI()


def build_pipeline_json(pipeline: Pipeline) -> dict:
    # Use the centralized pipeline analysis from PipelineInspector
    return PipelineInspector.get_pipeline_json(pipeline)


@app.get("/pipeline")
def get_pipeline():
    if not hasattr(app.state, "pipeline") or app.state.pipeline is None:
        raise HTTPException(
            status_code=404, detail="Pipeline not found. Please load a pipeline first."
        )
    return build_pipeline_json(app.state.pipeline)


@app.get("/")
def index():
    return FileResponse(Path(__file__).parent / "web_gui" / "index.html")


@app.get("/debug")
def debug():
    return FileResponse(Path(__file__).parent / "web_gui" / "debug.html")


def main():
    parser = argparse.ArgumentParser(description="Semantiva Pipeline GUI server")
    parser.add_argument("yaml", help="Path to pipeline YAML")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8000)
    args = parser.parse_args()

    # Load the pipeline
    config = load_pipeline_from_yaml(args.yaml)
    app.state.pipeline = Pipeline(config)

    # Print inspection information
    from semantiva.tools.pipeline_inspector import PipelineInspector

    print("Pipeline Inspector:", PipelineInspector.inspect_pipeline(app.state.pipeline))
    print("-" * 40)
    print(
        "Extended Pipeline Inspection:",
        PipelineInspector.inspect_pipeline_extended(app.state.pipeline),
    )

    static_dir = Path(__file__).parent / "web_gui"
    app.mount("/static", StaticFiles(directory=static_dir), name="static")

    import uvicorn

    uvicorn.run(app, host=args.host, port=args.port)


if __name__ == "__main__":
    main()
