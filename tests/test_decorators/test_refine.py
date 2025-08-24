from pathlib import Path
from dataclasses import dataclass
from pandas import DataFrame, Series, Index, read_csv
from pandas.testing import assert_series_equal, assert_frame_equal
import pytest
from weaveflow import refine, weave, spool_asset, Loom
from weaveflow.options import set_weaveflow_option


# Set asset path and include spool data from specified folder
# Set include_spool to consider files containing basename "costs" in their name
set_weaveflow_option(
    ["asset_path", "include_spool"],
    [Path(__file__).parent.parent / "data", "costs"],
)


@spool_asset
@dataclass
class People:
    city_dict: dict[str, int]
    children_dict: dict[int, int]
    subscription_int: int


@spool_asset(custom_engine={"csv": read_csv})
@dataclass
class CitySurplus:
    costs: DataFrame


@weave("total_costs", params_from=People)
def get_total_costs(
    city: str,
    children: int,
    has_subscription: bool,
    city_dict: dict[str, int],
    children_dict: dict[int, int],
    subscription_int: int,
) -> int:
    # Transform to string to match the dict keys as by default
    # toml always treats keys as strings
    children_costs = children.astype(str).map(children_dict)
    city_costs = city.astype(str).map(city_dict)

    subscription_costs = has_subscription * subscription_int * (children + 1)

    return children_costs + city_costs + subscription_costs


@weave("surplus")
def get_surplus(total_costs: int, income_thousands: int) -> int:
    return income_thousands * 1_000 - total_costs


@weave("city_costs", params_from=CitySurplus)
def get_total_city_costs(city: str, costs: DataFrame) -> int:
    return city.map(costs.set_index("city")["costs_million_eur"])


@refine
def clean_data(df: DataFrame) -> DataFrame:
    return df.dropna()


@refine(on_method="clean")
class DataCleaner:
    def __init__(self, df: DataFrame):
        self.df = df

    def clean(self):
        self.df = self.df.dropna()
        return self.df


class DataCleanerStatic:
    @refine  # Also works for static methods
    @staticmethod
    def clean(df: DataFrame):
        return df.dropna()


@refine(on_method="group")
class DataGrouper:
    def __init__(self, df: DataFrame):
        self.df = df

    def group(self):
        return self.df.groupby("city")["surplus"].mean()


def test_weave_spool_basics():
    # Check meta data of weave with spooled params
    meta = getattr(get_total_costs, "_weave_meta")
    assert meta._rargs == ["city", "children", "has_subscription"]
    assert meta._oargs == []
    assert meta._outputs == ["total_costs"]
    assert meta._params == {
        "city_dict": {
            "Cologne": 1000,
            "Berlin": 1250,
            "Munich": 1500,
            "Hamburg": 1210,
            "Frankfurt": 1380,
        },
        "children_dict": {"0": 0, "1": 400, "2": 700, "3": 950},
        "subscription_int": 45,
    }


@pytest.mark.parametrize("refiner_task", [DataCleaner, DataCleanerStatic.clean])
def test_consistency_in_refiner(personal_data, refiner_task):
    # Define expected loom using `clean_data` refiner
    expected_loom = Loom(personal_data, [get_total_costs, get_surplus, clean_data])
    expected_loom.run()

    loom_to_test = Loom(personal_data, [get_total_costs, get_surplus, refiner_task])
    loom_to_test.run()

    assert_frame_equal(expected_loom.database, loom_to_test.database)


def test_loomflow_with_refiner(personal_data):
    # Define loom with clean_data refiner
    loom = Loom(personal_data, [get_total_costs, get_surplus, clean_data, get_total_city_costs])
    loom.run()

    # assert that 2 rows (number 7 and 9) are dropped by clean_data refiner due to nan values
    assert len(loom.database) == len(personal_data) - 2
    assert loom.database.index.tolist() == [0, 1, 2, 3, 4, 5, 6, 8, 10, 11, 12, 13, 14]
    # Assert new columns are added by weave functions
    assert all(c in loom.database.columns for c in ["total_costs", "surplus", "city_costs"])

    # Define expected total cost column based on data inputs and spooled params
    total_cost_expected = Series(
        [1045, 1950, 1990, 2160, 1045, 1740, 2080, 1045, 2380, 1870, 1545, 1400, 2085],
        name="total_costs",
        index=loom.database.index,
    )
    assert_series_equal(loom.database["total_costs"], total_cost_expected)
    # Define expected surplus column based on data inputs and total cost
    surplus = Series(
        [
            54455.0,
            87050.0,
            70110.0,
            103340.0,
            40955.0,
            74060.0,
            79120.0,
            60455.0,
            89620.0,
            108330.0,
            46955.0,
            64600.0,
            83015.0,
        ],
        name="surplus",
        index=loom.database.index,
    )
    assert_series_equal(loom.database["surplus"], surplus)

    city_costs = Series(
        [200, 600, 300, 400, 200, 600, 200, 200, 600, 200, 300, 200, 600],
        name="city_costs",
        index=loom.database.index,
    )
    assert_series_equal(loom.database["city_costs"], city_costs)


def test_loomflow_with_several_refiners(personal_data):
    loom = Loom(
        personal_data,
        [get_total_costs, get_surplus, get_total_city_costs, DataCleaner, DataGrouper],
    )
    loom.run()

    # Assert that groupby statement worked by checking number of rows
    assert len(loom.database) == personal_data["city"].nunique()
    # Assert that data base is series now after application of groupby statement
    assert isinstance(loom.database, Series) and loom.database.name == "surplus"
    # Assert result of groupby statement is as expected
    assert_series_equal(
        loom.database,
        Series(
            [83436.25, 55116.25, 93725.0, 103340.0, 58532.5],
            index=Index(
                data=["Berlin", "Cologne", "Frankfurt", "Hamburg", "Munich"],
                name="city",
            ),
            name="surplus",
        ),
    )
