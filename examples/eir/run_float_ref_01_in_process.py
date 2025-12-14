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

from __future__ import annotations

from pathlib import Path

from semantiva.context_processors.context_types import ContextType
from semantiva.data_types import NoDataType
from semantiva.eir.compiler import compile_eir_v1
from semantiva.eir.execution_scalar import execute_eir_v1_scalar_plan
from semantiva.pipeline.payload import Payload


def _repo_root() -> Path:
    """Resolve repo root deterministically for examples and tests."""
    return Path(__file__).resolve().parents[2]


def main() -> float:
    """
    Compile and execute the EIR reference suite pipeline float_ref_01 in-process.

    This is a minimal developer example:
    - compile YAML -> EIRv1 dict
    - execute classic scalar plan via execute_eir_v1_scalar_plan(...)
    """
    spec_path = _repo_root() / "tests" / "eir_reference_suite" / "float_ref_01.yaml"
    eir = compile_eir_v1(str(spec_path))

    payload = Payload(
        NoDataType(),
        ContextType({"value": 1.0, "addend": 2.0}),
    )

    out = execute_eir_v1_scalar_plan(eir, payload)
    return float(out.data.data)


if __name__ == "__main__":
    print(main())
