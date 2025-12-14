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

"""Metadata-only slot inference for data processors.

Phase 1 PoC utility that extracts data slot candidates from `_process_logic`
annotations. Runtime semantics remain unchanged.
"""

from __future__ import annotations

import inspect
import importlib
import typing
from collections import OrderedDict
from dataclasses import dataclass
from typing import Optional, Type, get_args, get_origin

from semantiva.data_types import BaseDataType


def _safe_issubclass(t: object, base: type) -> bool:
    """Return ``True`` if ``t`` is a subclass of ``base`` without raising."""
    try:
        return isinstance(t, type) and issubclass(t, base)
    except Exception:
        return False


def _normalize_datatype_annotation(ann: object) -> Optional[type]:
    """
    Best-effort normalize an annotation to a concrete class.

    Accepts:
      - Concrete classes
      - Optional[T] / Union[T, None] where T is a single concrete class
    Rejects:
      - Any, TypeVar, ambiguous unions, missing annotations
    """

    if ann is None or ann is inspect._empty:
        return None
    if ann is typing.Any:
        return None

    origin = get_origin(ann)
    if origin is None:
        return ann if isinstance(ann, type) else None

    if origin is typing.Union:
        args = [a for a in get_args(ann) if a is not type(None)]  # noqa: E721
        concrete = [a for a in args if isinstance(a, type)]
        if len(concrete) == 1:
            return concrete[0]
        return None

    return None


@dataclass(frozen=True)
class SlotInference:
    """Deterministic slot inference result for a processor's ``_process_logic``."""

    inputs: "OrderedDict[str, Type[BaseDataType]]"
    output: Optional[Type[BaseDataType]]

    def to_dict(self) -> dict:
        """Serialize slots to a dict with stable ordering for testing."""

        return {
            "inputs": [{"name": k, "type": v.__name__} for k, v in self.inputs.items()],
            "output": self.output.__name__ if self.output else None,
        }


def infer_data_slots(processor_cls: type) -> SlotInference:
    """
    Infer data slots from a processor class's ``_process_logic`` signature.

    Metadata-only utility: this does not affect runtime behavior.

    Rules:
      - Any typed parameter (except ``self``) that is a BaseDataType subclass is an input slot.
      - Typed return annotation that is a BaseDataType subclass is the output slot.
      - Slot ordering follows the signature parameter order.
    """

    fn = getattr(processor_cls, "_process_logic", None)
    if fn is None:
        return SlotInference(inputs=OrderedDict(), output=None)

    module = importlib.import_module(processor_cls.__module__)
    try:
        hints = typing.get_type_hints(
            fn, globalns=vars(module), localns=dict(vars(processor_cls))
        )
    except Exception:
        hints = getattr(fn, "__annotations__", {}) or {}

    try:
        sig = inspect.signature(fn)
    except Exception:
        return SlotInference(inputs=OrderedDict(), output=None)

    inputs: "OrderedDict[str, Type[BaseDataType]]" = OrderedDict()
    for name, param in sig.parameters.items():
        if name == "self":
            continue
        ann = hints.get(name, param.annotation)
        t = _normalize_datatype_annotation(ann)
        if _safe_issubclass(t, BaseDataType):
            inputs[str(name)] = typing.cast(Type[BaseDataType], t)

    out_ann = hints.get("return", sig.return_annotation)
    out_t = _normalize_datatype_annotation(out_ann)
    output: Optional[Type[BaseDataType]] = None
    if _safe_issubclass(out_t, BaseDataType):
        output = typing.cast(Type[BaseDataType], out_t)

    return SlotInference(inputs=inputs, output=output)
