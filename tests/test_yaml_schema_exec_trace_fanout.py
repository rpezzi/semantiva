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

from semantiva.configurations import load_pipeline_from_yaml


def test_yaml_parses_execution_trace_and_fanout(tmp_path):
    yaml_content = """
execution:
  orchestrator: LocalSemantivaOrchestrator
  executor: SequentialSemantivaExecutor
  transport: InMemorySemantivaTransport
  options:
    retries: 2
trace:
  driver: jsonl
  output_path: ./ser/out.ser.jsonl
  options:
    detail: all
fanout:
  multi:
    value: [1, 2]
    factor: [3, 4]
pipeline:
  nodes:
    - processor: FloatValueDataSource
    - processor: FloatMockDataSink
      parameters:
        path: ./fanout.txt
"""
    cfg_path = tmp_path / "schema.yaml"
    cfg_path.write_text(yaml_content)

    pipeline_cfg = load_pipeline_from_yaml(str(cfg_path))

    assert pipeline_cfg.execution.orchestrator == "LocalSemantivaOrchestrator"
    assert pipeline_cfg.execution.executor == "SequentialSemantivaExecutor"
    assert pipeline_cfg.execution.transport == "InMemorySemantivaTransport"
    assert pipeline_cfg.execution.options["retries"] == 2

    assert pipeline_cfg.trace.driver == "jsonl"
    assert pipeline_cfg.trace.output_path == "./ser/out.ser.jsonl"
    assert pipeline_cfg.trace.options["detail"] == "all"

    assert pipeline_cfg.fanout.multi == {"value": [1, 2], "factor": [3, 4]}
    assert pipeline_cfg.fanout.mode == "zip"
    assert len(pipeline_cfg.nodes) == 2
