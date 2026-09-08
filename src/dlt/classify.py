"""Phase 2 of DLT_PLAN.md -- failure classification.

Deterministic, and it runs *before* any LLM call. Its only job is to decide how
much effort a message is worth (DLT_PLAN.md section 4):

    A  coded BusinessException  -> registry lookup, corroboration, recommendation
    B  code defect (NPE, ...)   -> enrich, group, route to devs. No diagnosis:
                                   there is no source access, so a narrative
                                   about *why* would be invention.
    C  technical / transient    -> "redrive once the dependency recovers"
    U  unclassifiable           -> manual review, trace attached verbatim

An unrecognised exception is **U, never B**. Silently defaulting to B would
route genuinely unknown failures into the cheapest lane and quietly stop
looking at them.

**The reason-code catalog can move a case from A to C.** `BusinessReasonCode
implements IRejectCode`, so all 760 published reject codes can arrive inside a
`BusinessException` -- and 198 of them are declared `TECHNICAL_EXCEPTION` at
source. On the exception type alone, `BusinessException:
[KAFKA_PRODUCER_EXCEPTION]` is indistinguishable from
`BusinessException: [INDEX_MASTER_DATA_NOT_FOUND]`; both read as Class A, go to
the expensive lane, and come back with a business narrative. The first is a
Kafka publish error whose entire treatment is "redrive once the broker
recovers". `code_class` is the hook that tells them apart.

The catalog only ever moves A to C, never the reverse, and never to B: a code
defect is identified by its *exception type*, never by a reject code.

Pure functions, no I/O -- the catalog is read by the caller and passed in as
`code_class`, so this module stays a pure function of its arguments and the
file read stays at the boundary that already does it.
"""
import json
import os
import re
from dataclasses import dataclass
from enum import Enum
from typing import Optional

from src.dlt.stacktrace import ParsedTrace
from src.utils.logging_config import get_logger

logger = get_logger(__name__)


class FailureClass(str, Enum):
    BUSINESS = "A"
    CODE_DEFECT = "B"
    TECHNICAL = "C"
    UNKNOWN = "U"


#: An enumerated error code as it appears in a BusinessException message:
#: `[UID_ORIGIN_TRACKER_DATA_NOT_FOUND] UidOriginTracker data not found ...`
#: Upper-case, digits and underscores, at least three characters, so an
#: incidental `[0]` or `[ok]` in free text is not mistaken for a code.
_CODE_RE = re.compile(r"\[([A-Z][A-Z0-9_]{2,})\]")

#: Exceptions whose FQCN ends with this are business errors regardless of
#: package, which keeps the rule working if another team's exception type
#: shows up on the same topic.
_BUSINESS_SUFFIX = "BusinessException"

DEFAULT_BUSINESS_EXCEPTIONS = ("in.gov.uidai.common.exception.BusinessException",)

#: FQCN prefix -> class. Matched longest-prefix-first, so a specific entry
#: beats a general one. Extend rather than replace via `DLT_CLASS_MAP`.
DEFAULT_CLASS_MAP = {
    # --- B: defects in application code. A developer has to read the source. ---
    "java.lang.NullPointerException": "B",
    "java.lang.IndexOutOfBoundsException": "B",
    "java.lang.ArrayIndexOutOfBoundsException": "B",
    "java.lang.StringIndexOutOfBoundsException": "B",
    "java.lang.ClassCastException": "B",
    "java.lang.NumberFormatException": "B",
    "java.lang.ArithmeticException": "B",
    "java.lang.IllegalArgumentException": "B",
    "java.lang.IllegalStateException": "B",
    "java.lang.UnsupportedOperationException": "B",
    "java.lang.StackOverflowError": "B",
    "java.util.NoSuchElementException": "B",
    "java.util.ConcurrentModificationException": "B",
    "java.lang.ClassNotFoundException": "B",
    "java.lang.NoSuchMethodError": "B",

    # --- C: technical and transient. The recommendation is always a redrive. ---
    "java.net.SocketTimeoutException": "C",
    "java.net.ConnectException": "C",
    "java.net.UnknownHostException": "C",
    "java.net.SocketException": "C",
    "java.util.concurrent.TimeoutException": "C",
    "javax.net.ssl": "C",
    "java.sql.SQLTransientException": "C",
    "java.sql.SQLTimeoutException": "C",
    "java.sql.SQLRecoverableException": "C",
    "org.springframework.dao.QueryTimeoutException": "C",
    "org.springframework.dao.DataAccessResourceFailureException": "C",
    "org.springframework.dao.TransientDataAccessException": "C",
    "org.springframework.dao.CannotAcquireLockException": "C",
    "org.springframework.jdbc.CannotGetJdbcConnectionException": "C",
    "org.springframework.web.client.ResourceAccessException": "C",
    "org.springframework.web.client.HttpServerErrorException": "C",
    "feign.RetryableException": "C",
    "io.github.resilience4j.circuitbreaker.CallNotPermittedException": "C",
    "org.apache.kafka.common.errors.TimeoutException": "C",
    "org.apache.kafka.common.errors.RetriableException": "C",
    "org.apache.kafka.common.errors.NotLeaderOrFollowerException": "C",
    "org.springframework.kafka.support.serializer.DeserializationException": "C",
    "org.apache.kafka.common.errors.SerializationException": "C",
    "com.fasterxml.jackson.core.JsonProcessingException": "C",
    "com.fasterxml.jackson.databind.JsonMappingException": "C",
}


@dataclass(frozen=True)
class Classification:
    failure_class: FailureClass
    root_fqcn: str
    business_code: Optional[str]
    #: Why this class was chosen. Surfaced in the casebook so the decision is
    #: auditable rather than a bare letter.
    reason: str

    @property
    def needs_llm(self) -> bool:
        """Only Class A can produce a narrative worth paying for.

        B has no source to reason about, C has a fixed recommendation, and U
        has nothing parseable. Phase 7's reuse policy layers group-level
        caching on top of this.
        """
        return self.failure_class is FailureClass.BUSINESS


def business_exceptions() -> tuple:
    raw = os.environ.get("DLT_BUSINESS_EXCEPTIONS", "").strip()
    if not raw:
        return DEFAULT_BUSINESS_EXCEPTIONS
    return tuple(v.strip() for v in raw.split(",") if v.strip())


def class_map() -> dict:
    """Built-in map, extended (not replaced) by `DLT_CLASS_MAP`.

    Extending rather than replacing means an operator adding one new exception
    does not silently drop the other 40 entries.
    """
    merged = dict(DEFAULT_CLASS_MAP)
    raw = os.environ.get("DLT_CLASS_MAP", "").strip()
    if not raw:
        return merged
    try:
        overrides = json.loads(raw)
    except (ValueError, TypeError) as exc:
        logger.warning("DLT_CLASS_MAP is not valid JSON; ignoring", error=str(exc))
        return merged
    if not isinstance(overrides, dict):
        logger.warning("DLT_CLASS_MAP must be a JSON object; ignoring")
        return merged

    for key, value in overrides.items():
        text = str(value).strip().upper()
        if text in {c.value for c in FailureClass}:
            merged[str(key)] = text
        else:
            logger.warning("DLT_CLASS_MAP holds an unknown class; ignoring entry",
                           fqcn=key, value=value)
    return merged


def extract_business_code(*texts) -> Optional[str]:
    """First enumerated `[CODE]` found across the given texts, in order.

    Callers pass the root exception message first and the
    `kafka_exception-message` header second: Spring concatenates the root
    business error into that header, so it still carries the code when the
    stacktrace itself is truncated.
    """
    for text in texts:
        if not text:
            continue
        match = _CODE_RE.search(str(text))
        if match:
            return match.group(1)
    return None


def is_business_exception(fqcn: Optional[str]) -> bool:
    if not fqcn:
        return False
    return fqcn in business_exceptions() or fqcn.endswith(_BUSINESS_SUFFIX)


def _lookup_class(fqcn: str) -> Optional[str]:
    """Longest matching prefix wins, so specific entries beat general ones."""
    mapping = class_map()
    if fqcn in mapping:
        return mapping[fqcn]
    best = None
    for prefix, value in mapping.items():
        if fqcn.startswith(prefix) and (best is None or len(prefix) > len(best[0])):
            best = (prefix, value)
    return best[1] if best else None


def reason_code_classification_enabled() -> bool:
    """Whether a catalog category may override the trace-based class."""
    raw = os.environ.get("DLT_REASON_CODE_CLASSIFY", "").strip().lower()
    return raw not in ("false", "0", "no", "off")


def _catalog_class(code: Optional[str], code_class) -> Optional[str]:
    """What the catalog says about `code`, or None.

    None for every failure mode -- no hook, no code, unknown code, catalog
    absent, feature disabled -- so the stacktrace-based classification stands
    exactly as it did before the catalog existed.
    """
    if not code or code_class is None or not reason_code_classification_enabled():
        return None
    try:
        return code_class(code)
    except Exception as exc:
        # A catalog fault must degrade a finding, not lose the message.
        logger.warning("Reason-code lookup failed; classifying from the "
                       "stacktrace alone", code=code, error=str(exc))
        return None


def classify(trace: ParsedTrace,
             exception_message: Optional[str] = None,
             code_class=None) -> Classification:
    """Assign a failure class to a parsed trace.

    Total: every input yields a `Classification`, never an exception.
    `exception_message` is the `kafka_exception-message` header, used as a
    fallback source for the business code.

    `code_class` is an optional callable `code -> Optional[str]` returning the
    failure class the reason-code catalog asserts for a business code --
    `src.dlt.registry.class_for` in production. Omitting it gives exactly the
    pre-catalog behaviour, which is what keeps this a pure function and keeps
    every existing caller working unchanged.
    """
    root = trace.root
    if root is None or not root.fqcn:
        code = extract_business_code(exception_message)
        if code:
            # The trace was unusable but Spring's concatenated message still
            # carries the code -- enough to treat this as a business error,
            # unless the catalog says the code is a technical fault.
            if _catalog_class(code, code_class) == "C":
                return Classification(
                    failure_class=FailureClass.TECHNICAL,
                    root_fqcn="",
                    business_code=code,
                    reason=f"stacktrace unparseable; code {code} recovered from "
                           f"the kafka_exception-message header and declared a "
                           f"technical fault by the reason-code catalog",
                )
            return Classification(
                failure_class=FailureClass.BUSINESS,
                root_fqcn="",
                business_code=code,
                reason="stacktrace unparseable; business code recovered from "
                       "the kafka_exception-message header",
            )
        return Classification(
            failure_class=FailureClass.UNKNOWN,
            root_fqcn="",
            business_code=None,
            reason="stacktrace absent or unparseable",
        )

    fqcn = root.fqcn

    if is_business_exception(fqcn):
        code = extract_business_code(root.message, exception_message)
        if code:
            # The exception type says "business"; the catalog knows better for
            # the 198 codes declared TECHNICAL_EXCEPTION at source. Their
            # treatment is a redrive, and Class C reaches that without an LLM.
            if _catalog_class(code, code_class) == "C":
                return Classification(
                    failure_class=FailureClass.TECHNICAL,
                    root_fqcn=fqcn,
                    business_code=code,
                    reason=f"raised as a business exception, but the reason-code "
                           f"catalog declares {code} a technical fault",
                )
            return Classification(
                failure_class=FailureClass.BUSINESS,
                root_fqcn=fqcn,
                business_code=code,
                reason=f"business exception carrying code {code}",
            )
        # A business exception with no enumerated code is still a business
        # failure -- it just cannot be looked up. Phase 8 caps its confidence
        # via DLT_REGISTRY_MISS_CEILING rather than discarding it as unknown.
        return Classification(
            failure_class=FailureClass.BUSINESS,
            root_fqcn=fqcn,
            business_code=None,
            reason="business exception with no enumerated code in its message",
        )

    mapped = _lookup_class(fqcn)
    if mapped == "B":
        return Classification(FailureClass.CODE_DEFECT, fqcn, None,
                              "code defect; no source access, so it is routed "
                              "to the development team undiagnosed")
    if mapped == "C":
        return Classification(FailureClass.TECHNICAL, fqcn, None,
                              "technical or transient fault")
    if mapped == "A":
        return Classification(FailureClass.BUSINESS, fqcn,
                              extract_business_code(root.message, exception_message),
                              "mapped to business class by DLT_CLASS_MAP")

    return Classification(
        failure_class=FailureClass.UNKNOWN,
        root_fqcn=fqcn,
        business_code=None,
        reason=f"unrecognised root exception {fqcn}; add it to DLT_CLASS_MAP "
               "once its treatment is known",
    )
