"""
This module provides utility functions shared across the decorator implementations.
"""
def dump_object_to_dict(obj: object = None) -> dict:
    """
    Convert parameters from 'params_from' object to dict for passing to
    other objects.

    Args:
        params_from (object): Object to load parameters from. Object is expected
            to be a class with a __dict__ attribute.

    Returns:
        dict: Dictionary of parameters.
    """
    if obj is not None:
        params_object = obj()
        params = getattr(params_object, "__dict__", {})
    else:
        params = {}

    return params
