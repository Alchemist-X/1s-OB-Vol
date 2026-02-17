"""Default constants. Overridden by CLI arguments at runtime."""

import os

CLOB_REST_URL = "https://clob.polymarket.com"
CLOB_WSS_URL = "wss://ws-subscriptions-clob.polymarket.com"
GAMMA_API_URL = "https://gamma-api.polymarket.com"

DEFAULT_DURATION = 300
DEFAULT_INTERVAL = 5.0
DEFAULT_TOP_LEVELS = 5
DEFAULT_OUTPUT = os.path.join(os.path.dirname(__file__), "data")

DEPTH_CENTS = [1, 5, 10, 15]
