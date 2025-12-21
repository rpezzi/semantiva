import pytest

from semantiva.pipeline.cpsv1.canonicalize import canonicalize_yaml_to_cpsv1
from semantiva.pipeline.cpsv1.validation import validate_cpsv1_semantics


def test_semantics_rejects_unknown_bind_target():
    spec = {
        "extensions": ["semantiva-examples"],
        "pipeline": {
            "nodes": [
                {"processor": "FloatValueDataSource"},
                {
                    "processor": "FloatAddTwoInputsOperation",
                    "bind": {"other_typo": "primary"},
                },
            ]
        },
    }

    cps = canonicalize_yaml_to_cpsv1(spec)

    with pytest.raises(ValueError, match=r"binds unknown parameter"):
        validate_cpsv1_semantics(cps)
