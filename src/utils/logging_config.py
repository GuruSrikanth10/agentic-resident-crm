import logging
import os
import structlog
import sys
import urllib3

# Suppress expected warning when K8S_VERIFY_SSL=false
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


def _resolve_level() -> int:
    """Log level from LOG_LEVEL, defaulting to INFO.

    This was hardcoded to INFO with no override, so raising verbosity to debug
    a production issue meant editing code (F21).
    """
    raw = os.environ.get("LOG_LEVEL", "INFO").strip().upper()
    return getattr(logging, raw, logging.INFO)


def _add_section_separator(logger, method_name, event_dict):
    """Add visual patterns to key orchestration events."""
    event = event_dict.get("event", "")
    if isinstance(event, str):
        lower_event = event.lower()
        if "node started" in lower_event:
            event_dict["event"] = f"\n{'='*70}\n[ >>  {event.upper()}  << ]\n{'='*70}"
        elif "finished" in lower_event or "completed" in lower_event:
            event_dict["event"] = f"--- {event.upper()} ---"
    return event_dict


def setup_logging():
    # Only configure once
    if structlog.is_configured():
        return

    # Route Python warnings through structlog
    logging.captureWarnings(True)

    logging.basicConfig(
        format="%(message)s",
        stream=sys.stdout,
        level=_resolve_level(),
    )
    
    # Silence verbose 3rd-party libraries
    logging.getLogger("kafka").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("httpx").setLevel(logging.WARNING)
    
    log_format = os.environ.get("LOG_FORMAT", "TEXT").strip().upper()
    if log_format == "JSON":
        renderer = structlog.processors.JSONRenderer()
    else:
        renderer = structlog.dev.ConsoleRenderer(colors=True)

    structlog.configure(
        processors=[
            structlog.stdlib.add_log_level,
            structlog.stdlib.PositionalArgumentsFormatter(),
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            _add_section_separator,
            renderer
        ],
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )
    
def get_logger(name=__name__):
    if not structlog.is_configured():
        setup_logging()
    return structlog.get_logger(name)
