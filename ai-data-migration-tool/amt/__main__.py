import sys

from .cli import main

# The guard matters: dashboard worker processes re-import this module on Windows.
if __name__ == "__main__":
    sys.exit(main())
