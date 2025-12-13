from __future__ import annotations

import subprocess
import sys
import textwrap
from pathlib import Path
from typing import Any, Dict

from semantiva.contracts.expectations import validate_component
from semantiva.data_types import BaseDataType


class InT(BaseDataType[float]):
    def validate(self, data: float) -> bool:  # pragma: no cover - trivial
        return isinstance(data, (int, float))


class OutT(BaseDataType[float]):
    def validate(self, data: float) -> bool:  # pragma: no cover - trivial
        return isinstance(data, (int, float))


def _codes(diags) -> set[str]:
    return {d.code for d in diags}


def test_sva220_passes_when_io_is_inferable_from_annotations() -> None:
    class OpInferable:
        """Inferable IO via _process_logic annotations."""

        @classmethod
        def get_metadata(cls) -> Dict[str, Any]:
            return {
                "class_name": cls.__name__,
                "docstring": cls.__doc__ or "",
                "component_type": "DataOperation",
                "parameters": {},
            }

        def _process_logic(self, data: InT) -> OutT:
            return OutT(data.data)

    diags = validate_component(OpInferable)
    assert "SVA220" not in _codes(diags)


def test_sva220_fails_when_neither_declared_nor_inferable() -> None:
    class OpMissing:
        """No declared IO and no annotations."""

        @classmethod
        def get_metadata(cls) -> Dict[str, Any]:
            return {
                "class_name": cls.__name__,
                "docstring": cls.__doc__ or "",
                "component_type": "DataOperation",
                "parameters": {},
            }

        def _process_logic(self, data):  # type: ignore[no-untyped-def]
            return data

    diags = validate_component(OpMissing)
    assert "SVA220" in _codes(diags)


def test_sva222_errors_on_declared_vs_inferred_mismatch() -> None:
    class OpMismatch:
        """Declared IO disagrees with annotations."""

        @classmethod
        def get_metadata(cls) -> Dict[str, Any]:
            return {
                "class_name": cls.__name__,
                "docstring": cls.__doc__ or "",
                "component_type": "DataOperation",
                "parameters": {},
                "input_data_type": "InT",
                "output_data_type": "OutT",
            }

        def _process_logic(self, data: OutT) -> InT:
            return InT(data.data)

    diags = validate_component(OpMismatch)
    assert "SVA222" in _codes(diags)


def test_dev_lint_cli_surfaces_sva222_for_mismatch(tmp_path: Path) -> None:
    mod = tmp_path / "bad_op.py"
    mod.write_text(
        textwrap.dedent(
            """
            from typing import Dict, Any
            from semantiva.data_types import BaseDataType
            from semantiva.data_processors.data_processors import DataOperation

            class InT(BaseDataType[float]):
                def validate(self, data: float) -> bool:
                    return True

            class OutT(BaseDataType[float]):
                def validate(self, data: float) -> bool:
                    return True

            class OpMismatch(DataOperation):
                \"""Declared IO disagrees with annotations.\"""

                @classmethod
                def input_data_type(cls) -> type[InT]:
                    return InT

                @classmethod
                def output_data_type(cls) -> type[OutT]:
                    return OutT

                def _process_logic(self, data: OutT) -> InT:
                    return InT(data.data)
            """
        ),
        encoding="utf-8",
    )

    cmd = [sys.executable, "-m", "semantiva.cli", "dev", "lint", "--paths", str(mod)]
    res = subprocess.run(cmd, capture_output=True, text=True)

    assert res.returncode == 3
    assert "SVA222" in (res.stdout + res.stderr)
