import sys

from .main import entrypoint, parse_args

try:
    sys.exit(entrypoint(**parse_args(sys.argv)))
except KeyboardInterrupt as ex:
    sys.exit(0)
except RuntimeError as ex:
    # Special treatment due to httpx bug
    # https://github.com/encode/httpx/issues/2139
    print(str(ex), file=sys.stderr)
    sys.exit(1)
