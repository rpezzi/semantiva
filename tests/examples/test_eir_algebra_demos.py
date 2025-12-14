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

import json
import runpy
from pathlib import Path

import pytest


DEMO_DIR = Path(__file__).parents[2] / "examples" / "eir_algebra_demos"
GOLDEN_PATH = Path(__file__).with_name("golden_eir_algebra_demos.json")


def _run_demo(module_filename: str) -> dict:
    # Execute the file, then call its run() function.
    # This keeps demos import-free even though examples/ is not a Python package.
    scope = runpy.run_path(str(DEMO_DIR / module_filename))
    assert "run" in scope and callable(scope["run"])
    return scope["run"]()


def test_demo_folder_exists():
    assert DEMO_DIR.exists(), f"Missing demos folder: {DEMO_DIR}"


# --- Unit tests (helpers are pure + deterministic) ---
def test_demo_1_normalization_is_float_only():
    scope = runpy.run_path(str(DEMO_DIR / "algebra_demos.py"))
    normalize = scope["normalize_uint8_like_to_float"]
    from semantiva.examples.test_utils import FloatDataType

    assert normalize(FloatDataType(255.0)).data == pytest.approx(1.0)


# --- Integration tests (execute each demo) ---
def test_demos_execute_and_return_expected_shape():
    demo_1 = _run_demo("demo_1_rewrite.py")
    demo_2 = _run_demo("demo_2_composition.py")
    demo_3 = _run_demo("demo_3_ref_anchored.py")

    assert demo_1["channels"] == ["img", "mask", "ref"]
    assert demo_2["channels"] == ["A.feat_a", "A.ref", "B.feat_b", "B.ref"]
    assert demo_3["channels"] == ["feat", "ref"]


# --- Golden test (prevents silent drift in educational examples) ---
def test_demos_match_golden_snapshot():
    golden = json.loads(GOLDEN_PATH.read_text(encoding="utf-8"))

    got = {
        "demo_1_rewrite": _run_demo("demo_1_rewrite.py"),
        "demo_2_composition": {
            k: v
            for k, v in _run_demo("demo_2_composition.py").items()
            if k != "context"
        },
        "demo_3_ref_anchored": _run_demo("demo_3_ref_anchored.py"),
    }

    assert got == golden
