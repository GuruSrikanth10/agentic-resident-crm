"""
Timestamps as a casebook writes them and the rejection lane reads them.

Two clocks meet in a casebook, and nothing used to say which was which:

* this system's own stamps (`casebook_metadata`, `packet_status.last_updated`)
  are UTC;
* the packet's `sidDate` -- the sid's trailing yyyyMMddHHmmss -- and the
  older packet event's `eventTimestamp` are the producers' local time, India
  Standard Time, written with no offset.

Side by side without zones, a packet created at 12:14 IST read as created two
and a half hours after its own casebook (09:49 UTC). Every timestamp a
casebook carries is now written with its zone, and a zoneless producer time is
read in `PACKET_LOCAL_UTC_OFFSET` rather than silently as UTC -- which would
put an IST time five and a half hours in the future.
"""
import os
import re
from datetime import datetime, timedelta, timezone
from typing import Optional

#: The UTC offset the packet producers write their zoneless local times in.
ENV_PRODUCER_OFFSET = "PACKET_LOCAL_UTC_OFFSET"
DEFAULT_PRODUCER_TIMEZONE = timezone(timedelta(hours=5, minutes=30))

_OFFSET = re.compile(r"^([+-])(\d{2}):?(\d{2})$")


def producer_timezone() -> timezone:
    """The zone of a producer time written without one, `+05:30` unless
    PACKET_LOCAL_UTC_OFFSET says otherwise. An unreadable setting falls back
    to the default rather than failing the packet."""
    match = _OFFSET.match(os.environ.get(ENV_PRODUCER_OFFSET, "").strip())
    if match is None:
        return DEFAULT_PRODUCER_TIMEZONE
    sign, hours, minutes = match.groups()
    offset = timedelta(hours=int(hours), minutes=int(minutes))
    return timezone(-offset if sign == "-" else offset)


def parse_producer_time(value) -> Optional[datetime]:
    """`value` as an aware datetime, or None when it is not a time.

    ISO-8601 with an offset or `Z` keeps its zone -- the AUDIT contract's
    `eventTimestamp` is UTC. One without is the producers' local time.
    """
    if not isinstance(value, str) or not value.strip():
        return None
    try:
        moment = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
    except ValueError:
        return None
    return moment if moment.tzinfo else moment.replace(tzinfo=producer_timezone())


def zoned(value) -> Optional[str]:
    """A producer timestamp as a casebook writes it.

    Unchanged when it already names its zone, so the producer's own precision
    is kept; given the producers' offset when it does not; and unchanged when
    it cannot be read at all, because the payload's own text is better
    evidence than nothing.
    """
    if not isinstance(value, str) or not value.strip():
        return None
    try:
        moment = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
    except ValueError:
        return value
    if moment.tzinfo:
        return value
    return moment.replace(tzinfo=producer_timezone()).isoformat()


def utc_now() -> str:
    """This system's own timestamp: now, in UTC, to the second, and saying so."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
