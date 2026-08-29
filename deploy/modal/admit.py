#!/usr/bin/env python3
"""Legacy import and CLI wrapper for the provider-neutral winner probe."""

from deploy.winner_admission import qualify


if __name__ == "__main__":
    import runpy

    runpy.run_module("deploy.winner_admission", run_name="__main__")
