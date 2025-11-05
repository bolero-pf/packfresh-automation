# jobs/price_update.py
import os, sys, subprocess, threading
from datetime import datetime

RUN_LOG = os.environ.get("PRICE_UPDATE_LOG", "run_output.log")
SCRIPT  = os.environ.get("PRICE_UPDATE_SCRIPT", "dailyrunner.py")

def kickoff_dailyrunner() -> bool:
    """Spawn the price updater in the background; returns True if started."""
    def _launch():
        # append timestamped header to log, then stream the run
        with open(RUN_LOG, "a", buffering=1) as f:
            f.write(f"\n=== RUN {datetime.now().isoformat()} ===\n")
            subprocess.Popen([sys.executable, SCRIPT], stdout=f, stderr=f)
    threading.Thread(target=_launch, daemon=True).start()
    return True
