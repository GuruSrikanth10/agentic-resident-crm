"""
Single source of truth for filesystem paths shared across processes
(API, consumer, and operator CLIs). Defining these in one place prevents the
kind of path drift where two callers independently guess at the same file
and end up pointing at different locations.
"""
import os
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent.parent

# Overridable for the same reason LOCAL_CASESHEETS_DIR is: the default sits
# inside the repo, and a checkout under a synced or indexed directory
# (Windows `Documents`/OneDrive) means an external process holds handles on
# files this one rewrites every few seconds. Point it at plain local disk to
# take those holders out of the picture entirely.
LOCAL_CHECKPOINTS_DIR = Path(
    os.environ.get("LOCAL_CHECKPOINTS_DIR", REPO_ROOT / "local_checkpoints")
)
CHECKPOINT_DB_PATH = LOCAL_CHECKPOINTS_DIR / "checkpoints.db"

# The consumer's liveness stamp. Independently re-derived in kafkaConsumer.py
# and routes.py, which is how the two ended up agreeing only by coincidence
# (G13). Used by the fast consumer (CONSUMER_ROLE=fast, the default); kept
# under its original name for backwards compatibility with existing
# deployments and the tests that assert this exact path.
CONSUMER_HEARTBEAT_PATH = LOCAL_CHECKPOINTS_DIR / "consumer_heartbeat.txt"

# The slow (analysis) consumer's liveness stamp -- a distinct file so the two
# consumers don't clobber each other's heartbeat when co-located under
# start.py on one host.
SLOW_CONSUMER_HEARTBEAT_PATH = LOCAL_CHECKPOINTS_DIR / "slow_consumer_heartbeat.txt"

# Drain3's persisted parse tree and the template catalog. Both were derived
# with their own `dirname(dirname(dirname(...)))` walks in log_pipeline/config.py.
DRAIN3_STATE_DIR = LOCAL_CHECKPOINTS_DIR / "drain3_state"
CATALOG_PATH = LOCAL_CHECKPOINTS_DIR / "template_catalog.json"

# Where casebooks and their log artifacts live. This was independently
# re-derived in storage/local.py, log_pipeline/pipeline.py (twice) and
# log_pipeline/snapshot.py -- four copies of the same `parent.parent.parent`
# walk, which is exactly the path drift this module exists to prevent (F22).
LOCAL_CASESHEETS_DIR = Path(
    os.environ.get("LOCAL_CASESHEETS_DIR", REPO_ROOT / "local_casesheets")
)


def casebook_dir(event_id: str) -> Path:
    """Directory holding one event's casebook and log artifacts.

    Callers that write there must still go through CasebookStorage for the
    casebook itself; this is for the log artifacts (raw_logs.txt,
    reduced_logs.txt, the Kubernetes snapshot) that sit beside it.
    """
    return LOCAL_CASESHEETS_DIR / f"casebook_{event_id}"

# DLT analysis consumers (DLT_PLAN.md Phase 4). Distinct files so all four
# consumer roles can be co-located under start.py without clobbering each
# other's liveness stamp.
DLT_CONSUMER_HEARTBEAT_PATH = LOCAL_CHECKPOINTS_DIR / "dlt_consumer_heartbeat.txt"
DLT_ANALYSIS_HEARTBEAT_PATH = LOCAL_CHECKPOINTS_DIR / "dlt_analysis_heartbeat.txt"
