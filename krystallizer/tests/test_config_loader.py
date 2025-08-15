from pathlib import Path
import pytest
from krystallizer._decorators.spool import _handle_files_from_iterable, _load_config_data
from krystallizer.tests.data.dummy.dummy import dummy_function


def test_handle_elements_from_iterable():
    """Test the handle_elements_from_iterable function."""
    # Test with a single string pattern
    assert _handle_files_from_iterable(["a", "b", "c"], None) == ["a", "b", "c"]
    assert _handle_files_from_iterable(["a", "b", "c"], "a") == ["a"]
    assert _handle_files_from_iterable(("a","b", "c"), ("a", "b")) == ["a", "b"]
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
    with pytest.raises(FileNotFoundError, match=f"Specified path not found: {test_data_path / "no_dir"}"):
        _load_config_data(path=test_data_path / "no_dir")


def test_load_config_data_no_config_files(test_data_path):
    """Test error when no config files found in directory."""
    with pytest.raises(FileNotFoundError, match=f"No config files found in {test_data_path / "empty_dir"}."):
        _load_config_data(path=test_data_path / "empty_dir")


def test_load_config_data_both_include_exclude_error():
    """Test error when both include and exclude are specified."""
    with pytest.raises(ValueError, match="Cannot specify both 'exclude' and 'include'"):
        _load_config_data(path="/tmp", include=["a"], exclude=["b"])


def test_load_config_data_specific_file_with_include_exclude_error():
    """Test error when specific_file is used with include/exclude."""
    with pytest.raises(ValueError, match="Cannot specify both 'specific_file' and 'exclude/include'"):
        _load_config_data(path="/tmp", specific_file="config.json", include=["a"])


def test_load_config_data_empty_configs(test_data_path):
    """Test error when only empty config files found in directory."""
    with pytest.raises(ValueError, match="Config files found, but no data found in config files."):
        _load_config_data(path=test_data_path, exclude=["dummy_spool"]) # dummy_spool.yaml is not empty, exclude to raise error


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
