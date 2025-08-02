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

"""Pipeline Visualization Web Server.

This module provides a lightweight web server for visualizing Semantiva pipelines
using the new modular inspection system. The server integrates with the inspection
module to provide consistent pipeline data representation across CLI and web interfaces.

Key Features:
- **Inspection Integration**: Uses `build_pipeline_inspection()` and `json_report()`
  for consistent data generation
- **Error Resilient**: Can visualize even invalid pipeline configurations
- **Parameter Tracking**: Shows parameter resolution and context flow
- **Multi-format Support**: Provides both web UI and CLI text output

The server has been updated to use the new inspection architecture, ensuring
that web visualizations use the same data structures as CLI inspection tools.
"""

import argparse
from typing import Union, List, Dict
from pathlib import Path
from fastapi import FastAPI, HTTPException
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from semantiva import Pipeline, load_pipeline_from_yaml
from semantiva.inspection import (
    build_pipeline_inspection,
    json_report,
    summary_report,
    extended_report,
)

app = FastAPI()


def build_pipeline_json(pipeline_or_config: Union[Pipeline, List[Dict]]) -> dict:
    """Generate JSON representation of pipeline using the inspection system.

    This function serves as the integration point between the web GUI and the
    new inspection architecture. It ensures that web visualizations use the
    same data structures and analysis as CLI tools.

    Args:
        pipeline_or_config: Either a Semantiva Pipeline object or raw configuration data
            (List[Dict]). The inspection system can handle both valid and invalid
            configurations without raising exceptions.

    Returns:
        Dictionary containing nodes and edges data for web visualization

    Note:
        This function replaces the previous direct pipeline analysis with
        a call to the inspection system's json_report() function, ensuring
        consistency across all pipeline introspection tools. It can handle
        invalid configurations that would fail during Pipeline construction.
    """
    # Use the new inspection system for consistent data generation
    inspection = build_pipeline_inspection(pipeline_or_config)

    # Run validation to populate errors in the inspection (but don't raise exceptions here)
    try:
        from semantiva.inspection.validator import validate_pipeline

        validate_pipeline(inspection)
    except Exception:
        # Validation failed, but errors are now populated in the inspection data
        pass

    # Convert inspection data to JSON format suitable for web visualization
    return json_report(inspection)


@app.get("/api/pipeline")
def get_pipeline_api():
    # Check for configuration data first (new approach)
    if hasattr(app.state, "config") and app.state.config is not None:
        return build_pipeline_json(app.state.config)

    # Fall back to pipeline object for backward compatibility
    if hasattr(app.state, "pipeline") and app.state.pipeline is not None:
        return build_pipeline_json(app.state.pipeline)

    # Neither configuration nor pipeline available
    raise HTTPException(
        status_code=404,
        detail="Pipeline configuration not found. Please load a pipeline first.",
    )


@app.get("/pipeline")
def get_pipeline_legacy():
    """Legacy endpoint retained for backward compatibility."""
    return get_pipeline_api()


@app.get("/")
def index():
    return FileResponse(Path(__file__).parent / "web_gui" / "index.html")


def main():
    parser = argparse.ArgumentParser(description="Semantiva Pipeline GUI server")
    parser.add_argument("yaml", help="Path to pipeline YAML")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8000)
    args = parser.parse_args()

    # Load the pipeline configuration (doesn't create Pipeline object)
    config = load_pipeline_from_yaml(args.yaml)
    app.state.config = config

    # Print inspection information using the raw configuration
    # This works even for invalid configurations that would fail Pipeline construction
    inspection = build_pipeline_inspection(config)

    # Run validation to populate errors in the inspection (but don't raise exceptions here)
    try:
        from semantiva.inspection.validator import validate_pipeline

        validate_pipeline(inspection)
    except Exception:
        # Validation failed, but errors are now populated in the inspection data
        pass

    print("Pipeline Inspector:", summary_report(inspection))
    print("-" * 40)
    print("Extended Pipeline Inspection:", extended_report(inspection))

    # Also try to create a Pipeline object for backward compatibility, but don't fail if it doesn't work
    app.state.pipeline = None
    try:
        app.state.pipeline = Pipeline(config)
        print("-" * 40)
        print("Pipeline object created successfully - configuration is valid")
    except Exception as e:
        print("-" * 40)
        print(f"Pipeline object creation failed: {e}")
        print("Continuing with inspection-only mode for invalid configuration")

    static_dir = Path(__file__).parent / "web_gui" / "static"
    app.mount("/static", StaticFiles(directory=static_dir), name="static")

    import uvicorn

    uvicorn.run(app, host=args.host, port=args.port)


if __name__ == "__main__":
    main()
