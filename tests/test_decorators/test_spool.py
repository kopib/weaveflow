from dataclasses import dataclass
from pathlib import Path
from pandas import DataFrame, read_csv
from pandas.testing import assert_frame_equal
from weaveflow._decorators import spool_asset
from weaveflow._decorators import SPoolRegistry
from weaveflow.options import set_weaveflow_option


set_weaveflow_option("asset_path", Path(__file__).parent.parent / "data")


@spool_asset
def stock_data(
    price: float,
    pe_ratio: float,
    ps_ratio: float,
    peg_ratio: float,
    pb_ratio: float,
) -> SPoolRegistry:
    """Returns numeric constants from the registry."""


@spool_asset(file="stock.toml")
@dataclass
class StockData:
    """Data class for collection all variables from registry config files."""

    price: float
    pe_ratio: float
    ps_ratio: float
    peg_ratio: float
    pb_ratio: float


@spool_asset
@dataclass
class DummyDataAsset:
    """Dummy asset class for testing."""

    dummy1: int
    dummy2: int
    dummy3: int
    id1: float
    id2: float
    id3: float


@spool_asset(custom_engine={"csv": read_csv})
@dataclass
class CityCosts:
    costs: DataFrame


def test_spool_toml_function():
    """Test spool decorator with TOML files."""
    # Use a different variable name for the result
    data = stock_data()

    assert hasattr(stock_data, "_spool")
    assert isinstance(data, SPoolRegistry)
    assert data.price == 233.33
    assert data.pe_ratio == 35.41
    assert data.ps_ratio == 8.62
    assert data.peg_ratio == 116.31
    assert data.pb_ratio == 52.6
    assert data.__dict__ == {
        "price": 233.33,
        "pe_ratio": 35.41,
        "ps_ratio": 8.62,
        "peg_ratio": 116.31,
        "pb_ratio": 52.6,
    }


def test_spool_toml_class():
    """Test spool decorator with TOML files."""
    # Use a different variable name for the result
    data = StockData()

    assert hasattr(StockData, "_spool")
    assert isinstance(data, StockData)
    assert data.price == 233.33
    assert data.pe_ratio == 35.41
    assert data.ps_ratio == 8.62
    assert data.peg_ratio == 116.31
    assert data.pb_ratio == 52.6
    assert data.__dict__ == {
        "price": 233.33,
        "pe_ratio": 35.41,
        "ps_ratio": 8.62,
        "peg_ratio": 116.31,
        "pb_ratio": 52.6,
    }


def test_spool_asset():
    """Test spool_asset decorator."""
    data = DummyDataAsset()

    assert hasattr(DummyDataAsset, "_spool")
    assert isinstance(data, DummyDataAsset)
    assert data.dummy1 == 10
    assert data.dummy2 == 20
    assert data.dummy3 == 30
    assert data.id1 == 1.4
    assert data.id2 == 2.3
    assert data.id3 == 1.2


def test_custom_engine_in_spool():
    """Test custom engine in spool decorator."""
    city_costs = CityCosts()
    assert isinstance(city_costs.costs, DataFrame)
    expected_data = read_csv(Path(__file__).parent.parent / "data" / "costs.csv")
    assert_frame_equal(city_costs.costs, expected_data)
