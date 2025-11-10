from __future__ import annotations

from typing import Literal, TypedDict

# TODO: Model all directus responses here with TypedDict
# This will make working with response data much cleaner for
# any end users as it will hint keys and types.

# TypedDict is preferred over dataclass modeling since
# dataclasses are strict with defined parameters while 
# TypedDicts are converted to builtins.dict at runtime 
# meaning desyncronization of Directus schema will only
# cause linter warnings and not runtime errors.