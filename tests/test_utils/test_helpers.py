import pytest

from weaveflow._utils import _auto_convert_time_delta


@pytest.mark.parametrize(
    ("input_seconds", "expected_output"),
    [
        (0, "0.0ms"),  # Zero case
        (0.5, "500.0ms"),  # Standard case
        (0.999, "999.0ms"),  # Edge case, just below 1s
        (0.0005, "0.5ms"),  # Test rounding up
        (0.0004, "0.4ms"),  # Test rounding down
    ],
)
def test_converts_to_milliseconds(input_seconds, expected_output):
    """Test that deltas less than 1 second are converted to milliseconds."""
    assert _auto_convert_time_delta(input_seconds) == expected_output


@pytest.mark.parametrize(
    ("input_seconds", "expected_output"),
    [
        (1, "1.0s"),  # Boundary value
        (30.56, "30.6s"),  # Test rounding to one decimal
        (59.9, "59.9s"),  # Edge case, just below 60s
        (59.99, "60.0s"),  # Test rounding at the boundary
    ],
)
def test_converts_to_seconds(input_seconds, expected_output):
    """Test that deltas between 1 and 60 seconds are formatted correctly."""
    assert _auto_convert_time_delta(input_seconds) == expected_output


@pytest.mark.parametrize(
    ("input_seconds", "expected_output"),
    [
        (60, "1.0m"),  # Boundary value
        (125, "2.1m"),  # Test integer division (truncation)
        (3599, "60.0m"),  # Edge case, just below 1h
    ],
)
def test_converts_to_minutes(input_seconds, expected_output):
    """Test that deltas between 60 and 3600 seconds are converted to minutes."""
    assert _auto_convert_time_delta(input_seconds) == expected_output


@pytest.mark.parametrize(
    ("input_seconds", "expected_output"),
    [
        (3600, "1.0h"),  # Boundary value
        (7205, "2.0h"),  # Test integer division (truncation)
        (86400, "24.0h"),  # One full day
    ],
)
def test_converts_to_hours(input_seconds, expected_output):
    """Test that deltas of 3600 seconds or more are converted to hours."""
    assert _auto_convert_time_delta(input_seconds) == expected_output


@pytest.mark.parametrize(
    ("input_seconds", "expected_output"),
    [
        (-0.5, "-500.0ms"),
        (-10, "-10.0s"),
        (-120, "-2.0m"),
        (-3600, "-1.0h"),
    ],
)
def test_handles_negative_values(input_seconds, expected_output):
    """Test that the function handles negative inputs predictably."""
    assert _auto_convert_time_delta(input_seconds) == expected_output
