"""Python package wrapper for the native rustwx extension."""

from . import rustwx as _native
from .rustwx import *  # noqa: F401,F403

__doc__ = _native.__doc__
if hasattr(_native, "__all__"):
    __all__ = _native.__all__
