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

"""Public entry point for resolving processor symbols."""

from __future__ import annotations

import importlib
from typing import Any, Type, Union

from .bootstrap import DEFAULT_MODULES
from .name_resolver_registry import NameResolverRegistry
from .processor_registry import ProcessorRegistry


def _ensure_defaults_loaded() -> None:
    ProcessorRegistry.ensure_default_modules(DEFAULT_MODULES)


class UnknownProcessorError(LookupError):
    """Raised when a processor symbol cannot be resolved."""


class AmbiguousProcessorError(LookupError):
    """Raised when a processor symbol resolves to multiple candidates."""


def _format_ambiguity(symbol: str, candidates: list[str]) -> str:
    """Format an ambiguity error message with fix instructions."""
    cand_str = "\n".join(f"  - {c}" for c in candidates)
    return (
        f"Ambiguous processor symbol: {symbol!r}\n"
        f"Candidates:\n{cand_str}\n"
        f"Fix: use an explicit processor_ref (dotted FQN), e.g. "
        f'{{"processor_ref": "{candidates[0]}"}}'
    )


def _try_import_dotted_fqn(symbol: str) -> Type[Any] | None:
    """Try to import a dotted FQN (pkg.mod.Qualname) with nested qualname support."""
    parts = symbol.split(".")
    if len(parts) < 2:
        return None

    # Try importing the longest module prefix, then traverse remaining qualname.
    for i in range(len(parts) - 1, 0, -1):
        module_name = ".".join(parts[:i])
        qual_parts = parts[i:]
        try:
            module = importlib.import_module(module_name)
        except Exception:
            continue

        obj: Any = module
        ok = True
        for attr in qual_parts:
            if not hasattr(obj, attr):
                ok = False
                break
            obj = getattr(obj, attr)
        if ok and isinstance(obj, type):
            return obj
    return None


def resolve_symbol(name_or_type: Union[str, Type]) -> Type:
    """Resolve a processor symbol to a concrete class.

    Strings are processed in phases:
    1. Prefix-based name resolvers (``rename:``, ``delete:``, etc.)
    2. Registered processors via :class:`ProcessorRegistry` (may raise AmbiguousProcessorError)
    3. Fully qualified ``module:Class`` imports (auto-registered on success)
    4. Dotted FQN imports (``pkg.module.Qualname`` with nested qualname support)
    """

    if isinstance(name_or_type, type):
        return name_or_type

    symbol = str(name_or_type)

    _ensure_defaults_loaded()

    resolved = NameResolverRegistry.resolve(symbol)
    if resolved is not None:
        return resolved

    try:
        return ProcessorRegistry.get_processor(symbol)
    except AmbiguousProcessorError as e:
        candidates = ProcessorRegistry.get_candidates(symbol)
        raise AmbiguousProcessorError(_format_ambiguity(symbol, candidates)) from e
    except KeyError:
        pass

    if ":" in symbol:
        module_name, _, class_name = symbol.partition(":")
        if "." in module_name:
            module = importlib.import_module(module_name)
            candidate = getattr(module, class_name)
            if isinstance(candidate, type):
                ProcessorRegistry.register_processor(class_name, candidate)
                return candidate

    # Try dotted FQN import (pkg.module.Qualname with nested qualname support)
    imported = _try_import_dotted_fqn(symbol)
    if imported is not None:
        # Register safe alias: key is the exact dotted-FQN string (does not create
        # short-name collisions), allowing stable repeated resolutions.
        ProcessorRegistry.register_processor(symbol, imported)
        return imported

    # Safety net: try simple dotted format (module.Class) as fallback
    if "." in symbol and ":" not in symbol:
        mod_name, _, cls_name = symbol.rpartition(".")
        if mod_name and cls_name:
            try:
                module = importlib.import_module(mod_name)
                candidate = getattr(module, cls_name, None)
                if isinstance(candidate, type):
                    ProcessorRegistry.register_processor(cls_name, candidate)
                    return candidate
            except Exception:
                pass

    raise UnknownProcessorError(f"Cannot resolve processor symbol: {symbol!r}")
