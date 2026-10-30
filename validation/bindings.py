"""Single import point for the generated Python protobuf bindings.

gen/python holds the *_pb2.py modules generated from schemas/market/v1
(make generate-python-protobuf); every other module imports the
messages from here, never from market.v1 directly.
"""

import sys
from pathlib import Path

GEN_PYTHON = Path(__file__).resolve().parent.parent / "gen/python"
sys.path.insert(0, str(GEN_PYTHON))

from market.v1.advisory_pb2 import (  # noqa: E402
    ADVISORY_ACTION_BUY,
    ADVISORY_ACTION_SELL,
    TradeAdvisory,
)
from market.v1.events_pb2 import SECURITY_TYPE_EQUITY, PriceEvent  # noqa: E402
from market.v1.ema_pb2 import EmaWindow  # noqa: E402

__all__ = [
    "PriceEvent",
    "EmaWindow",
    "TradeAdvisory",
    "ADVISORY_ACTION_BUY",
    "ADVISORY_ACTION_SELL",
    "SECURITY_TYPE_EQUITY",
]