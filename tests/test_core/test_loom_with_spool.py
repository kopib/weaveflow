from dataclasses import dataclass
from pathlib import Path

import pandas as pd
from numpy import inf

from weaveflow._decorators import spool, weave
from weaveflow.core import Loom


@spool(path=Path(__file__).parent.parent / "data")
@dataclass
class Constants:
    """Data class for collection all variables from registry config files."""

    taxation: float
    threshold_low: int
    threshold_middle: int
    threshold_high: int
    threshold_very_high: int


@weave(outputs=["net_worth", "status"], params_from=Constants)
def how_rich_are_you(
    stocks: float,
    investments: float,
    rest: float,
    taxation: float,
    threshold_low: int,
    threshold_middle: int,
    threshold_high: int,
    threshold_very_high: int,
    margin_of_safety: float = 0.1,
) -> float:
    """Calculate the fair value of a stock based on its ratios."""
    net_worth = (stocks - investments) * (1 - taxation) + investments + rest
    net_worth *= 1 - margin_of_safety

    return (
        net_worth,
        pd.cut(
            net_worth,
            bins=[
                -inf,
                0,
                threshold_low,
                threshold_middle,
                threshold_high,
                threshold_very_high,
                inf,
            ],
            labels=[
                "broke",
                "poor",
                "middle class",
                "rich",
                "very rich",
                "extremely rich",
            ],
        ),
    )


def test_spool_weave(finacial_dataframe):
    margin_of_safety = 0.2
    loom = Loom(finacial_dataframe, [how_rich_are_you], margin_of_safety=0.2)
    loom.run()
    assert loom.database.shape == (len(finacial_dataframe), 6)

    df = finacial_dataframe.copy()

    net_worth = (
        (df["stocks"] - df["investments"]) * (1 - 0.42) + df["investments"] + df["rest"]
    )
    net_worth *= 1 - margin_of_safety

    df["net_worth"] = net_worth
    df["status"] = pd.cut(
        net_worth,
        bins=[-inf, 0, 10_000, 100_000, 1_000_000, 10_000_000, inf],
        labels=["broke", "poor", "middle class", "rich", "very rich", "extremely rich"],
    )

    pd.testing.assert_frame_equal(
        loom.database[["net_worth", "status"]], df[["net_worth", "status"]]
    )
