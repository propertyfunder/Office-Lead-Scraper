#!/usr/bin/env python3
"""Background worker for the CH office pipeline.

Runs the Companies House SIC-code discovery sweep headlessly,
writing results to office_leads.csv with atomic saves.
Exits cleanly when the sweep completes.
"""

import sys
import os
import argparse
from datetime import datetime

sys.path.insert(0, os.path.dirname(__file__))


def run_office_pipeline_headless():
    print(f"[WORKER] Office pipeline starting at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"[WORKER] PID: {os.getpid()}")

    from main import run_office_pipeline

    args = argparse.Namespace(
        mode="office",
        output="office_leads.csv",
        no_enrich=False,
        fresh=False,
        dry_run=False,
        verbose=False,
        wellness=False,
        require_enrichment=False,
        enrich_existing=False,
        save_interval=1,
    )

    try:
        run_office_pipeline(args)
    except KeyboardInterrupt:
        print("\n[WORKER] Interrupted — progress already saved via checkpoints")
        sys.exit(0)
    except Exception as e:
        print(f"\n[WORKER] Fatal error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

    print(f"[WORKER] Pipeline finished at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")


if __name__ == "__main__":
    run_office_pipeline_headless()
