"""Dashboard contract helpers."""

from core.dashboard_contracts import position_exposure_class


def test_position_exposure_class_includes_stakedao_in_portfolio() -> None:
    assert (
        position_exposure_class(
            {"include_in_yield": False, "capital_bucket": "strategy_deployed"},
            "stakedao",
        )
        == "core_lending"
    )


def test_position_exposure_class_keeps_other_ops_protocols_out_of_portfolio() -> None:
    assert position_exposure_class({"include_in_yield": False}, "traderjoe_lp") == "other"
    assert position_exposure_class({"include_in_yield": False}, "etherex") == "other"
