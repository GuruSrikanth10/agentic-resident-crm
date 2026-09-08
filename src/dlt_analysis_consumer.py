import os
import sys
from dotenv import load_dotenv

load_dotenv()

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Must be set before kafkaConsumer is imported, since its topic/group/
# endpoint/timeout/heartbeat/health-port constants and its message adapter are
# all resolved at import time from CONSUMER_ROLE. Set unconditionally (not
# setdefault) so this entrypoint is always the DLT analysis consumer
# regardless of what the parent environment happens to contain -- see the
# CONSUMER_ROLE block at the top of src/utils/kafkaConsumer.py.
#
# This is the role to scale out: it absorbs LLM latency, and under the reuse
# policy most messages never reach an LLM at all.
os.environ["CONSUMER_ROLE"] = "dlt_analysis"

from src.utils.kafkaConsumer import consume_forever
from src.utils.config_validator import validate_config

if __name__ == "__main__":
    validate_config()

    # Can run in main thread since it's just the consumer
    consume_forever()
