import os
import sys
from dotenv import load_dotenv

load_dotenv()

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Must be set before kafkaConsumer is imported, since its topic/group/
# endpoint/timeout/heartbeat/health-port constants are resolved at import
# time from CONSUMER_ROLE. Set unconditionally (not setdefault) so this
# entrypoint is always the slow consumer regardless of what the parent
# environment happens to contain -- see the CONSUMER_ROLE block at the top
# of src/utils/kafkaConsumer.py.
os.environ["CONSUMER_ROLE"] = "slow"

from src.utils.kafkaConsumer import consume_forever
from src.utils.config_validator import validate_config

if __name__ == "__main__":
    validate_config()

    # Can run in main thread since it's just the consumer
    consume_forever()
