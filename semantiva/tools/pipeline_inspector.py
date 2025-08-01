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

from typing import List, Any, Dict, Protocol, Union
import inspect
from semantiva import Pipeline
from semantiva.logger import Logger
from semantiva.exceptions import PipelineConfigurationError
from semantiva.pipeline.nodes.nodes import (
    _PipelineNode,
    _ProbeContextInjectorNode,
    _ContextProcessorNode,
)
from semantiva.execution.transport import (
    SemantivaTransport,
)
from semantiva.execution.orchestrator.orchestrator import (
    SemantivaOrchestrator,
)


class PipelineInspector:
    """
    A utility class for inspecting pipeline nodes and configurations.

    Provides methods to analyze the structure, requirements, and behavior of a pipeline,
    producing human-readable summaries and extended reports for debugging and introspection.
    """

    @classmethod
    def _inspect_nodes(cls, nodes: List[_PipelineNode]) -> str:
        """
        Inspect a list of _PipelineNode objects and return a summary of the pipeline.

        The summary includes:
        - Node details: Class names and operations of the nodes.
        - Parameters: Differentiates between pipeline configuration and context parameters.
        - Context updates: Keywords created, modified, or deleted in the context.
        - Required context keys: Context parameters necessary for the pipeline.
        - Errors for invalid states, such as requiring deleted context keys.

        Args:
            nodes (List[_PipelineNode]): A list of pipeline nodes in execution order.

        Returns:
            str: A formatted report describing the pipeline composition and relevant details.
        """

        # Create a temporary pipeline-like object for analysis
        class TempPipeline:
            def __init__(self, nodes: List[Any]) -> None:
                self.nodes = nodes

        temp_pipeline = TempPipeline(nodes)
        analysis = cls.analyze_pipeline(temp_pipeline)

        summary_lines = ["Pipeline Structure:"]
        summary_lines.append(
            f"\tRequired context keys: {cls._format_set(analysis['required_context_keys'])}"
        )

        for node_data in analysis["nodes"]:
            node_summary_lines = [
                f"\n\t{node_data['index']}. Node: {node_data['processor_class_name']} ({node_data['component_type']})",
                f"\t\tParameters: {cls._format_set(node_data['operation_params'])}",
                f"\t\t\tFrom pipeline configuration: {cls._format_pipeline_config(node_data['processor_config'])}",
                f"\t\t\tFrom context: {cls._format_context_params(node_data['context_params'], analysis['key_origin'])}",
                f"\t\tContext additions: {cls._format_set(node_data['created_keys'])}",
            ]

            if node_data["suppressed_keys"]:
                node_summary_lines.append(
                    f"\t\tContext suppressions: {cls._format_set(node_data['suppressed_keys'])}"
                )

            summary_lines.extend(node_summary_lines)

        return "\n".join(summary_lines)

    @staticmethod
    def _format_set(values: set[str] | list[str]) -> str:
        """
        Format a set or list of strings into a sorted, comma-separated string.

        Args:
            values (set[str] | list[str]): A collection of string values.

        Returns:
            str: Comma-separated sorted values or "None" if empty.
        """
        return ", ".join(sorted(values)) if values else "None"

    @classmethod
    def _build_node_summary(
        cls,
        node: _PipelineNode,
        index: int,
        deleted_keys: set[str],
        all_required_params: set[str],
        injected_or_created_keywords: set[str],
        key_origin: dict[str, int],
    ) -> list[str]:
        """
        Build a summary for a single node, updating tracking sets.

        Args:
            node (_PipelineNode): The pipeline node to summarize.
            index (int): The node's position in the pipeline.
            deleted_keys (set[str]): Keys deleted by previous nodes.
            all_required_params (set[str]): Context keys required by the pipeline.
            injected_or_created_keywords (set[str]): Context keys created or injected by nodes.
            key_origin (dict[str, int]): Mapping of context keys to the node index that created them.

        Returns:
            list[str]: Human-readable lines describing the node.
        """
        operation_params = set(node.processor.get_processing_parameter_names())
        config_params = set(node.processor_config.keys())
        context_params = operation_params - config_params

        all_required_params.update(context_params)

        created_keys = node.processor.get_created_keys()
        injected_or_created_keywords.update(created_keys)

        if isinstance(node, _ProbeContextInjectorNode):
            injected_or_created_keywords.add(node.context_keyword)
            created_keys.append(node.context_keyword)
            key_origin[node.context_keyword] = index

        for key in created_keys:
            if key not in key_origin:
                key_origin[key] = index

        cls._validate_deleted_keys(index, operation_params, config_params, deleted_keys)

        node_summary_lines = [
            f"\n\t{index}. Node: {node.processor.__class__.__name__} ({node.get_metadata().get('component_type', 'Unknown')})",
            f"\t\tParameters: {cls._format_set(operation_params)}",
            f"\t\t\tFrom pipeline configuration: {cls._format_pipeline_config(node.processor_config)}",
            f"\t\t\tFrom context: {cls._format_context_params(context_params, key_origin)}",
            f"\t\tContext additions: {cls._format_set(created_keys)}",
        ]

        if isinstance(node, _ContextProcessorNode):
            suppressed_keys = node.get_suppressed_keys()
            deleted_keys.update(suppressed_keys)
            node_summary_lines.append(
                f"\t\tContext suppressions: {cls._format_set(suppressed_keys)}"
            )

        return node_summary_lines

    @staticmethod
    def _validate_deleted_keys(
        index: int,
        operation_params: set[str],
        config_params: set[str],
        deleted_keys: set[str],
    ) -> None:
        """
        Validate that a node does not require context keys that were previously deleted.

        Args:
            index (int): The node's position in the pipeline.
            operation_params (set[str]): Parameters required by the node.
            config_params (set[str]): Parameters provided by the node's configuration.
            deleted_keys (set[str]): Keys deleted by previous nodes.

        Raises:
            PipelineConfigurationError: If a node requires keys that were deleted and not restored.
        """
        missing_deleted_keys = operation_params & deleted_keys
        if not missing_deleted_keys.issubset(config_params):
            raise PipelineConfigurationError(
                f"Node {index} requires context keys previously deleted: {sorted(missing_deleted_keys)}"
            )

    @staticmethod
    def _determine_required_context_keys(
        all_required_params: set[str],
        injected_or_created_keywords: set[str],
    ) -> set[str]:
        """
        Determine context keys required by the pipeline but not created or injected.

        Args:
            all_required_params (set[str]): Context keys required across the pipeline.
            injected_or_created_keywords (set[str]): Context keys created or injected by nodes.

        Returns:
            set[str]: Context keys required but not provided by any node.
        """
        return all_required_params - injected_or_created_keywords

    @staticmethod
    def _format_pipeline_config(processor_config: dict[str, Any]) -> str:
        """
        Format parameters explicitly set in the pipeline configuration.

        Args:
            processor_config (dict[str, Any]): Processor configuration key-value pairs.

        Returns:
            str: Comma-separated 'key=value' pairs or "None" if empty.
        """
        if processor_config:
            return ", ".join(
                f"{key}={value}" for key, value in processor_config.items()
            )
        return "None"

    @staticmethod
    def _format_context_params(
        context_params: set[str], key_origin: dict[str, int]
    ) -> str:
        """
        Format context parameters required by a node, including their origin if known.

        Args:
            context_params (set[str]): Context parameters required by the node.
            key_origin (dict[str, int]): Mapping of context keys to the node index that created them.

        Returns:
            str: Comma-separated list of parameters with origins or "None" if empty.
        """
        if not context_params:
            return "None"
        parts = []
        for param in sorted(context_params):
            if param in key_origin:
                origin_node = key_origin[param]
                parts.append(f"{param} (from Node {origin_node})")
            else:
                parts.append(param)
        return ", ".join(parts)

    @classmethod
    def inspect_pipeline_extended(cls, pipeline: Pipeline) -> str:
        """
        Perform an extended inspection of a pipeline, providing verbose details.

        Includes per-node details, overall required context keys, and processor docstrings.

        Args:
            pipeline (Pipeline): The pipeline to inspect.

        Returns:
            str: An extended inspection report.
        """
        analysis = cls.analyze_pipeline(pipeline)

        summary_lines = ["Extended Pipeline Inspection:"]
        summary_lines.append(
            f"\tRequired context keys: {cls._format_set(analysis['required_context_keys'])}"
        )

        footnotes: Dict[str, str] = {}

        for node_data in analysis["nodes"]:
            # Format context parameters with origins
            context_params_with_origins = cls._format_context_params(
                node_data["context_params"], analysis["key_origin"]
            )

            # Format config parameters
            config_params_str = (
                ", ".join(f"{k}={v}" for k, v in node_data["processor_config"].items())
                if node_data["processor_config"]
                else "None"
            )

            # Format data types
            input_type_name = (
                node_data["input_data_type"].__name__
                if node_data["input_data_type"]
                else "None"
            )
            output_type_name = (
                node_data["output_data_type"].__name__
                if node_data["output_data_type"]
                else "None"
            )

            summary_lines.extend(
                [
                    f"\nNode {node_data['index']}: {node_data['processor_class_name']} ({node_data['node_class_name']})",
                    f"    - Component type: {node_data['component_type']}",
                    f"    - Input data type: {input_type_name}",
                    f"    - Output data type: {output_type_name}",
                    f"    - Parameters from pipeline configuration: {config_params_str}",
                    f"    - Parameters from context: {context_params_with_origins}",
                    f"    - Context additions: {cls._format_set(node_data['created_keys'])}",
                    f"    - Context suppressions: {cls._format_set(node_data['suppressed_keys'])}",
                ]
            )

            # Add footnote for processor docstring
            footnote_key = node_data["processor_class_name"]
            if footnote_key not in footnotes:
                footnotes[footnote_key] = node_data["docstring"]

        summary_lines.append("\nFootnotes:")
        for name, doc in footnotes.items():
            summary_lines.extend([f"[{name}]", doc, ""])

        return "\n".join(summary_lines)

    @classmethod
    def get_pipeline_json(cls, pipeline: Pipeline) -> dict:
        """
        Get a JSON-serializable representation of the pipeline analysis.

        This method provides the data structure used by the web GUI and other
        external consumers that need structured pipeline information.

        Args:
            pipeline (Pipeline): The pipeline to analyze.

        Returns:
            dict: JSON-serializable pipeline data including nodes and edges.
        """
        analysis = cls.analyze_pipeline(pipeline)

        nodes = []
        edges = []

        for node_data in analysis["nodes"]:
            # Build parameter resolution info
            from_config = {}
            for key, value in node_data["processor_config"].items():
                from_config[key] = str(value)

            from_context = {}
            for param in node_data["context_params"]:
                if param in analysis["key_origin"]:
                    source_node_idx = analysis["key_origin"][param]
                    from_context[param] = {
                        "value": None,
                        "source": f"Node {source_node_idx}",
                        "source_idx": source_node_idx,
                    }
                else:
                    from_context[param] = {
                        "value": None,
                        "source": "Initial Context",
                        "source_idx": -1,
                    }

            parameter_resolution = {
                "required_params": list(node_data["operation_params"]),
                "from_pipeline_config": from_config,
                "from_context": from_context,
            }

            # Format data types
            input_type_name = (
                node_data["input_data_type"].__name__
                if node_data["input_data_type"]
                else None
            )
            output_type_name = (
                node_data["output_data_type"].__name__
                if node_data["output_data_type"]
                else None
            )

            node_info = {
                "id": node_data["index"],
                "label": node_data["processor_class_name"],
                "component_type": node_data["component_type"],
                "input_type": input_type_name,
                "output_type": output_type_name,
                "docstring": node_data["docstring"],
                "parameters": node_data["processor_config"],
                "parameter_resolution": parameter_resolution,
                "created_keys": list(node_data["created_keys"]),
                "required_keys": list(node_data["operation_params"]),
                "suppressed_keys": list(node_data["suppressed_keys"]),
                "pipelineConfigParams": list(node_data["config_params"]),
                "contextParams": list(node_data["context_params"]),
            }

            nodes.append(node_info)

            # Create edges
            if node_data["index"] < len(analysis["nodes"]):
                edges.append(
                    {"source": node_data["index"], "target": node_data["index"] + 1}
                )

        return {"nodes": nodes, "edges": edges}

    @classmethod
    def get_nodes_semantic_ids_report(cls, nodes: List[_PipelineNode]) -> str:
        """
        Generate a report of semantic IDs for each node in the pipeline.

        Args:
            nodes (List[_PipelineNode]): A list of pipeline nodes.

        Returns:
            str: A report containing semantic IDs for each node.
        """
        report = ""

        for index, node in enumerate(nodes, start=1):
            report += f"\nNode {index}:\n"
            report += node.semantic_id()
        return report

    @classmethod
    def inspect_pipeline(cls, pipeline: Pipeline) -> str:
        """
        Inspect an initialized Pipeline instance.

        Args:
            pipeline (Pipeline): The pipeline to inspect.

        Returns:
            str: A summary of the pipeline.
        """
        return cls._inspect_nodes(pipeline.nodes)

    @classmethod
    def inspect_config(
        cls,
        config: List[Dict],
        logger: Logger | None = None,
        transport: SemantivaTransport | None = None,
        orchestrator: SemantivaOrchestrator | None = None,
    ) -> str:
        """
        Initialize a Pipeline from a configuration dictionary list and inspect it.

        Args:
            config (List[Dict]): Pipeline configuration as a list of dictionaries.
            logger (Logger | None): Optional logger instance.
            transport (SemantivaTransport | None): Optional transport instance.
            orchestrator (SemantivaOrchestrator | None): Optional orchestrator instance.

        Returns:
            str: A summary of the pipeline.
        """
        pipeline = Pipeline(
            config, logger=logger, transport=transport, orchestrator=orchestrator
        )
        return cls.inspect_pipeline(pipeline)

    @classmethod
    def inspect_config_extended(
        cls,
        config: List[Dict],
        logger: Logger | None = None,
        transport: SemantivaTransport | None = None,
        orchestrator: SemantivaOrchestrator | None = None,
    ) -> str:
        """
        Initialize a Pipeline from a configuration dictionary list and perform an extended inspection.

        Args:
            config (List[Dict]): Pipeline configuration as a list of dictionaries.
            logger (Logger | None): Optional logger instance.
            transport (SemantivaTransport | None): Optional transport instance.
            orchestrator (SemantivaOrchestrator | None): Optional orchestrator instance.

        Returns:
            str: An extended inspection report of the pipeline.
        """
        pipeline = Pipeline(
            config, logger=logger, transport=transport, orchestrator=orchestrator
        )
        return cls.inspect_pipeline_extended(pipeline)

    @classmethod
    def analyze_pipeline(cls, pipeline: Union[Pipeline, Any]) -> dict:
        """
        Perform a comprehensive analysis of a pipeline, returning structured data.

        This is the single source of truth for all pipeline inspection functionality.
        All other inspection methods should use this data.

        Args:
            pipeline (Pipeline): The pipeline to analyze.

        Returns:
            dict: Comprehensive pipeline analysis including:
                - nodes: List of node analysis data
                - required_context_keys: Set of keys required by pipeline but not provided
                - key_origin: Mapping of context keys to the node that created them
        """
        nodes = pipeline.nodes
        result: Dict[str, Any] = {
            "nodes": [],
            "required_context_keys": set(),
            "key_origin": {},
            "deleted_keys": set(),
        }

        # Track context key lifecycle
        key_origin: dict[str, int] = {}
        deleted_keys: set[str] = set()
        all_required_params: set[str] = set()
        all_created_keys: set[str] = set()

        # Analyze each node
        for index, node in enumerate(nodes, start=1):
            metadata = node.get_metadata()
            processor = node.processor

            # Get all processing parameters (the authoritative source)
            operation_params = set()
            if hasattr(processor, "get_processing_parameter_names"):
                operation_params = set(processor.get_processing_parameter_names())

            # Get configuration parameters
            config_params = set(node.processor_config.keys())

            # Context parameters are those not provided by configuration
            context_params = operation_params - config_params

            # Get created keys
            created_keys = set()
            if hasattr(processor, "get_created_keys"):
                created_keys.update(processor.get_created_keys())

            # Handle probe nodes specially
            if isinstance(node, _ProbeContextInjectorNode):
                created_keys.add(node.context_keyword)
                # Only set key origin if this is the first time we see this key
                if node.context_keyword not in key_origin:
                    key_origin[node.context_keyword] = index

            # Track key origins for all created keys
            for key in created_keys:
                # If a key was deleted but is now recreated, update its origin
                if key in deleted_keys:
                    deleted_keys.remove(key)
                    key_origin[key] = index  # Update to the new creator
                elif key not in key_origin:
                    key_origin[key] = index

            # Handle context processor nodes (rename/delete operations)
            suppressed_keys = set()
            if isinstance(node, _ContextProcessorNode):
                suppressed_keys = set(node.get_suppressed_keys())
                # For context processor nodes, they also have required keys
                if hasattr(node, "get_required_keys"):
                    context_params.update(node.get_required_keys())
                deleted_keys.update(suppressed_keys)

            # Validate deleted keys only for non-context processor nodes
            # Context processor nodes have special handling for required keys
            if not isinstance(node, _ContextProcessorNode):
                cls._validate_deleted_keys(
                    index, operation_params, config_params, deleted_keys
                )

            # Build node analysis
            node_analysis = {
                "index": index,
                "processor_class_name": processor.__class__.__name__,
                "node_class_name": node.__class__.__name__,
                "component_type": metadata.get("component_type", "Unknown"),
                "input_data_type": getattr(node, "input_data_type", lambda: None)(),
                "output_data_type": getattr(node, "output_data_type", lambda: None)(),
                "processor_config": dict(node.processor_config),
                "operation_params": operation_params,
                "config_params": config_params,
                "context_params": context_params,
                "created_keys": created_keys,
                "suppressed_keys": suppressed_keys,
                "docstring": inspect.getdoc(processor.__class__)
                or "No description provided.",
            }

            result["nodes"].append(node_analysis)
            all_required_params.update(context_params)
            all_created_keys.update(created_keys)

        # Determine final required context keys
        result["required_context_keys"] = all_required_params - all_created_keys
        result["key_origin"] = key_origin
        result["deleted_keys"] = deleted_keys

        return result

    @classmethod
    def get_node_parameter_resolutions(cls, pipeline: Pipeline) -> list[dict]:
        """
        Get structured parameter resolution information for each node.

        This method returns a list of dictionaries, each containing detailed information
        about parameter resolution for a node in the pipeline:
        - Which parameters are required
        - Which parameters come from pipeline configuration
        - Which parameters come from context, and from which node they originated

        Args:
            pipeline (Pipeline): The pipeline to inspect.

        Returns:
            list[dict]: A list of dictionaries, one for each node, containing parameter resolution info.
        """
        try:
            # Use the centralized analysis
            analysis = cls.analyze_pipeline(pipeline)
            result = []

            for node_data in analysis["nodes"]:
                # Format parameters from pipeline configuration
                from_config = {}
                for key, value in node_data["processor_config"].items():
                    from_config[key] = str(value)

                # Format parameters from context
                from_context = {}
                for param in node_data["context_params"]:
                    if param in analysis["key_origin"]:
                        source_node_idx = analysis["key_origin"][param]
                        from_context[param] = {
                            "value": None,  # We don't have the actual value here
                            "source": f"Node {source_node_idx}",
                            "source_idx": source_node_idx,
                        }
                    else:
                        from_context[param] = {
                            "value": None,
                            "source": "Initial Context",
                            "source_idx": -1,
                        }

                # Build the result for this node (0-indexed for compatibility)
                node_info = {
                    "id": node_data["index"] - 1,  # Convert to 0-indexed
                    "parameter_resolution": {
                        "required_params": list(node_data["operation_params"]),
                        "from_pipeline_config": from_config,
                        "from_context": from_context,
                    },
                }

                result.append(node_info)

            return result
        except Exception as e:
            print(f"Error in get_node_parameter_resolutions: {e}")
            # Return empty data on error to not break the UI
            return [
                {
                    "id": i,
                    "parameter_resolution": {
                        "required_params": [],
                        "from_pipeline_config": {},
                        "from_context": {},
                    },
                }
                for i in range(len(pipeline.nodes))
            ]
