from pathlib import Path
from dataclasses import dataclass
from pandas import DataFrame
from krystallizer import weave_refine, weave, spool, PandasWeave


@spool(path=Path(__file__).parent / "data", include="costs")
@dataclass
class People:
    city_dict: dict[str, int]
    children_dict: dict[int, int]
    subscription_int: int


@weave("total_costs", params_from=People)
def get_total_costs(
    city: str,
    children: int,
    has_subscription: bool,
    city_dict: dict[str, int], 
    children_dict: dict[int, int], 
    subscription_int: int
    ) -> int:

    children_costs = children.map(children_dict)
    city_costs = city.map(city_dict)
    subscription_costs = has_subscription * subscription_int * (children + 1)

    return children_costs + city_costs + subscription_costs


@weave("surplus")
def get_surplus(total_costs: int, income_thousands: int) -> int:
    return income_thousands * 1_000 - total_costs


@weave_refine(on_method="clean")
class DataClenar:

    def __init__(self, df: DataFrame):
        self.df = df

    def clean(self):
        self.df = self.df.dropna()
        return self.df


@weave_refine(on_method="group")
class DataGrouper:

    def __init__(self, df: DataFrame):
        self.df = df

    def group(self):
        return self.df.groupby("city").sum()


def test_basics(personal_data):
    pw = PandasWeave(personal_data, [get_total_costs, get_surplus]) 
    # TODO: Add DataClenar and DataGrouper in the future
    pw.run()
