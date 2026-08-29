from __future__ import annotations

import sys

from milk_harness.run_once import main as run_once_main


def main(argv=None):
    arguments = list(sys.argv[1:] if argv is None else argv)
    if not arguments or arguments.pop(0) != "run-once":
        raise SystemExit("usage: python -m milk_harness run-once --config PATH")
    run_once_main(arguments)


if __name__ == "__main__":
    main()
