import inspect
from typing import Any, Tuple, get_args, get_origin

import semantiva.eir.payload_algebra_contracts as contracts


def test_bindresolver_resolve_param_signature_and_return():
    sig = inspect.signature(contracts.BindResolver.resolve_param)
    params = list(sig.parameters.values())

    # positional args
    assert params[0].name == "self"
    assert params[1].name == "param_name"

    # keyword-only args (in exact order)
    kw_params = [p for p in params if p.kind == inspect.Parameter.KEYWORD_ONLY]
    expected_kw = ["binds", "node_params", "context", "channels", "default"]
    assert [p.name for p in kw_params] == expected_kw

    # return annotation should be Tuple[Any, ParameterSource]
    ret = sig.return_annotation
    # Handle postponed / stringified annotations (PEP 563) as well as typing.Tuple
    if isinstance(ret, str):
        assert "Tuple" in ret and "ParameterSource" in ret
    else:
        assert get_origin(ret) in (tuple, Tuple)
        args = get_args(ret)
        assert len(args) == 2
        assert args[0] is Any
    # ParameterSource is a Literal alias; comparing by presence is sufficient
    assert getattr(contracts, "ParameterSource", None) is not None


def test_channelstore_and_publishplan_signatures():
    # ChannelStore methods
    cs = contracts.ChannelStore
    sig_get = inspect.signature(cs.get)
    assert list(sig_get.parameters.keys()) == ["self", "name"]

    sig_set = inspect.signature(cs.set)
    assert list(sig_set.parameters.keys()) == ["self", "name", "value"]

    sig_seed = inspect.signature(cs.seed_primary)
    assert list(sig_seed.parameters.keys()) == ["self", "value"]

    # PublishPlan.apply signature
    pp = contracts.PublishPlan
    sig_apply = inspect.signature(pp.apply)
    assert [p.name for p in sig_apply.parameters.values()] == [
        "self",
        "output_value",
        "channels",
    ]


def test_contract_module_has_no_runtime_provenance_dataclasses():
    # Ensure no runtime provenance dataclasses leaked into the contract module
    for name in ("ProducerRef", "ChannelEntry", "ResolvedParam"):
        assert not hasattr(
            contracts, name
        ), f"{name} should not be exported from contracts module"
