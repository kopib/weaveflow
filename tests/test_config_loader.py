from pathlib import Path
from pandas import DataFrame, read_csv
from pandas.testing import assert_frame_equal
import pytest
from weaveflow._decorators._spool import (
    _handle_files_from_iterable,
    _load_config_data,
)
from tests.data.dummy.dummy import dummy_function


def test_handle_elements_from_iterable():
    """Test the handle_elements_from_iterable function."""
    # Test with a single string pattern
    assert _handle_files_from_iterable(["a", "b", "c"], None) == ["a", "b", "c"]
    assert _handle_files_from_iterable(["a", "b", "c"], "a") == ["a"]
    assert _handle_files_from_iterable(("a", "b", "c"), ("a", "b")) == ["a", "b"]
    assert _handle_files_from_iterable(["a", "b", "c"], "d") == []
    assert _handle_files_from_iterable(["a", "b", "c"], ["a", "b"], include=False) == ["c"]


def test_handle_elements_from_iterable_with_path():
    """Test the handle_elements_from_iterable function with Path objects."""
    from pathlib import Path

    files = [
        Path("/home/user/file_registry.json"),
        Path("/home/user/file_registry.toml"),
        Path("/home/user/file_registry2.yaml"),
        Path("/home/user/file_spool.yml"),
        Path("/home/user/spool/somefile.txt"),
    ]
    assert _handle_files_from_iterable(files, "spool") == [Path("/home/user/file_spool.yml")]
    assert _handle_files_from_iterable(files, "registry") == [
        Path("/home/user/file_registry.json"),
        Path("/home/user/file_registry.toml"),
        Path("/home/user/file_registry2.yaml"),
    ]


def test_load_config_data_specific_file_not_found(test_data_path: Path):
    """Test error when specific file doesn't exist."""
    with pytest.raises(FileNotFoundError, match="Specified config file not found"):
        _load_config_data(path=test_data_path, specific_file="nonexistent.json")


def test_load_config_data_path_not_found(test_data_path):
    """Test error when path doesn't exist."""
    with pytest.raises(
        FileNotFoundError,
        match=f"Specified path not found: {test_data_path / 'no_dir'}",
    ):
        _load_config_data(path=test_data_path / "no_dir")


def test_load_config_data_no_config_files(test_data_path):
    """Test error when no config files found in directory."""
    with pytest.raises(
        FileNotFoundError,
        match=f"No config files found in {test_data_path / 'empty_dir'}.",
    ):
        _load_config_data(path=test_data_path / "empty_dir")


def test_load_config_data_both_include_exclude_error():
    """Test error when both include and exclude are specified."""
    with pytest.raises(ValueError, match="Cannot specify both 'exclude' and 'include'"):
        _load_config_data(path="/tmp", include=["a"], exclude=["b"])


def test_load_config_data_specific_file_with_include_exclude_error():
    """Test error when specific_file is used with include/exclude."""
    with pytest.raises(
        ValueError, match="Cannot specify both 'specific_file' and 'exclude/include'"
    ):
        _load_config_data(path="/tmp", specific_file="config.json", include=["a"])


def test_load_config_data_empty_configs(test_data_path):
    """Test error when only empty config files found in directory."""
    with pytest.raises(ValueError, match="Config files found, but no data found in config files."):
        _load_config_data(
            path=test_data_path, exclude=["dummy_spool"]
        )  # dummy_spool.yaml is not empty, exclude to raise error


def test_custom_config_loader_type_error(test_data_path):
    """Test custom config loader."""
    with pytest.raises(
        TypeError,
        match="Custom engine must be a dict mapping file extensions to read function.",
    ):
        _load_config_data(path=test_data_path, custom_engine="nonexistent")


def test_load_config_data_no_obj_or_path_error():
    """Test error when neither obj nor path is specified."""
    with pytest.raises(ValueError, match="Either 'obj' or 'path' must be specified"):
        _load_config_data()


def test_load_config_data_with_obj():
    """Test loading config data with an object."""
    data = _load_config_data(obj=dummy_function)
    assert data == {"a": 1, "b": 2, "c": 3, "d": 4}


def test_load_config_data_with_path(test_data_path):
    """Test loading config data with a path."""
    data = _load_config_data(path=test_data_path)
    assert data == {"a": 1, "b": 2, "c": 3, "d": 4}


def test_load_config_data_with_specific_file(test_data_path):
    """Test loading config data with a specific file."""
    data = _load_config_data(path=test_data_path, specific_file="dummy_spool.yaml")
    assert data == {"a": 1, "b": 2}
    data = _load_config_data(path=test_data_path, include="dummy_spool.yaml")
    assert data == {"a": 1, "b": 2}
    data = _load_config_data(path=test_data_path, exclude="dummy_spool.yaml")
    assert data == {"c": 3, "d": 4}


def test_load_csv_custom_engine(test_data_path):
    """Tests _load_config_data with custom engine."""
    data_path = test_data_path.parent
    data = _load_config_data(
        path=data_path,
        custom_engine={"csv": read_csv},  # Extend default engine with csv reader
        include="costs",  # Only include filenames containing "costs", costs.csv and costs.toml
    )
    data2 = _load_config_data(
        path=data_path, custom_engine={"csv": read_csv}, specific_file="costs.csv"
    )

    assert_frame_equal(data["costs"], data2["costs"])
    assert isinstance(data, dict) and isinstance(data2, dict)
    # Content from csv and toml, while csv is wrapped in dict
    assert (len(data) == 4) and (len(data2) == 1)
    assert isinstance(data["costs"], DataFrame)
    expected_data = read_csv(data_path / "costs.csv")
    assert_frame_equal(data["costs"], expected_data)
    assert data["city_dict"] == {
        "Cologne": 1000,
        "Berlin": 1250,
        "Munich": 1500,
        "Hamburg": 1210,
        "Frankfurt": 1380,
    }
    assert data["children_dict"] == {"0": 0, "1": 400, "2": 700, "3": 950}
    assert data["subscription_int"] == 45
