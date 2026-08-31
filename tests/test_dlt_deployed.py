"""Phase C1 -- reading the running build off the pods.

The value this has to deliver is narrow but absolute: it must never raise, and
it must never report a version it did not actually observe. Every test below
is one of those two properties, or the rolling-deploy case where reporting a
single version would be a lie.
"""
import json

import pytest

from src.dlt import deployed


@pytest.fixture(autouse=True)
def _clear_cache():
    deployed.reset_cache()
    yield
    deployed.reset_cache()


# ---------------------------------------------------------------------------
# Fakes
# ---------------------------------------------------------------------------

class FakeStatus:
    def __init__(self, name, image):
        self.name = name
        self.image = image
        self.image_id = f"sha256:{name}"


class FakeMeta:
    def __init__(self, name):
        self.name = name


class FakePodStatus:
    def __init__(self, statuses):
        self.container_statuses = statuses


class FakePod:
    def __init__(self, name, images, container="app"):
        self.metadata = FakeMeta(name)
        if isinstance(images, str):
            images = [images]
        self.status = FakePodStatus([FakeStatus(container, i) for i in images])


HARBOR = "mndc-prod.harbor.uidai.net.in/ankalan/enu-biometric"


def _patch_pods(monkeypatch, pods, raises=None):
    """Replace the pod listing the module reaches for, without touching k8s."""
    from src.log_pipeline.sources.k8s import discovery

    def fake(app=None, namespace=None, request_timeout=None):
        if raises is not None:
            raise raises
        return pods

    monkeypatch.setattr(discovery, "list_pods_for_service", fake)


# ---------------------------------------------------------------------------
# Image reference parsing
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("reference,expected", [
    (f"{HARBOR}/1.0.0-release.42", "1.0.0-release.42"),
    (f"{HARBOR}:1.0.0", "1.0.0"),
    # A digest identifies image *content*, not a commit. It is not a version.
    (f"{HARBOR}:1.2.3@sha256:abc123", "1.2.3"),
    # A registry port must not be misread as a tag separator.
    ("registry.local:5000/ankalan/enu-biometric/1.0.0", "1.0.0"),
    ("app:latest", "latest"),
    ("", ""),
    (None, ""),
])
def test_version_is_read_out_of_an_image_reference(reference, expected):
    assert deployed.version_of(reference) == expected


def test_a_bare_digest_reference_yields_no_version():
    """`repo@sha256:...` carries no tag at all -- and the digest is not one."""
    assert deployed.version_of(f"{HARBOR}@sha256:abc123") == "enu-biometric"


# ---------------------------------------------------------------------------
# The happy path
# ---------------------------------------------------------------------------

def test_a_single_version_across_pods_is_reported_as_one(monkeypatch):
    _patch_pods(monkeypatch, [
        FakePod("enu-biometric-1", f"{HARBOR}/1.0.0-release.42"),
        FakePod("enu-biometric-2", f"{HARBOR}/1.0.0-release.42"),
    ])

    result = deployed.running_version()

    assert result.ok
    assert result.versions == ("1.0.0-release.42",)
    assert result.single == "1.0.0-release.42"
    assert result.mixed is False
    assert result.pods == 2


def test_a_rolling_deploy_reports_both_versions_and_no_single(monkeypatch):
    """Reporting one version mid-rollout would be a lie.

    A replay may land on any pod, so the safe reading is the lowest -- but
    ordering is C3's job, and this module deliberately does not guess.
    """
    _patch_pods(monkeypatch, [
        FakePod("enu-biometric-old", f"{HARBOR}/1.0.0-release.42"),
        FakePod("enu-biometric-new", f"{HARBOR}/1.0.0-release.43"),
    ])

    result = deployed.running_version()

    assert result.ok
    assert result.mixed is True
    assert result.single is None
    assert set(result.versions) == {"1.0.0-release.42", "1.0.0-release.43"}


def test_sidecars_on_the_same_pod_contribute_their_own_images(monkeypatch):
    _patch_pods(monkeypatch, [
        FakePod("enu-biometric-1", [f"{HARBOR}/1.0.0", "docker.io/istio/proxyv2:1.20.0"]),
    ])

    result = deployed.running_version()

    assert set(result.versions) == {"1.0.0", "1.20.0"}
    assert result.mixed is True


# ---------------------------------------------------------------------------
# Degradation -- the property that matters most
# ---------------------------------------------------------------------------

def test_no_namespace_resolved_is_a_reason_not_an_exception(monkeypatch):
    _patch_pods(monkeypatch, None)

    result = deployed.running_version()

    assert result.ok is False
    assert result.versions == ()
    assert "namespace" in result.reason


def test_an_api_failure_is_caught_and_reported(monkeypatch):
    _patch_pods(monkeypatch, None, raises=RuntimeError("connection refused"))

    result = deployed.running_version()

    assert result.ok is False
    assert "RuntimeError" in result.reason
    assert "connection refused" in result.reason


def test_pods_with_no_container_status_report_a_reason(monkeypatch):
    pod = FakePod("enu-biometric-1", [])
    pod.status.container_statuses = []
    _patch_pods(monkeypatch, [pod])

    result = deployed.running_version()

    assert result.ok is False
    assert result.pods == 1
    assert "container image" in result.reason


def test_an_empty_pod_list_is_not_an_error_state_with_a_version(monkeypatch):
    _patch_pods(monkeypatch, [])

    result = deployed.running_version()

    assert result.ok is False
    assert result.single is None


# ---------------------------------------------------------------------------
# Caching
# ---------------------------------------------------------------------------

def test_repeat_calls_are_served_from_the_cache(monkeypatch):
    calls = {"n": 0}

    from src.log_pipeline.sources.k8s import discovery

    def counting(app=None, namespace=None, request_timeout=None):
        calls["n"] += 1
        return [FakePod("p1", f"{HARBOR}/1.0.0")]

    monkeypatch.setattr(discovery, "list_pods_for_service", counting)
    monkeypatch.setenv("DLT_DEPLOYED_VERSION_TTL_SECONDS", "60")

    for _ in range(5):
        assert deployed.running_version().single == "1.0.0"

    assert calls["n"] == 1


def test_a_failed_read_is_not_cached(monkeypatch):
    """Caching a blip would hold a whole TTL of cases at UNKNOWN."""
    calls = {"n": 0}

    from src.log_pipeline.sources.k8s import discovery

    def flaky(app=None, namespace=None, request_timeout=None):
        calls["n"] += 1
        if calls["n"] == 1:
            raise RuntimeError("transient")
        return [FakePod("p1", f"{HARBOR}/1.0.0")]

    monkeypatch.setattr(discovery, "list_pods_for_service", flaky)
    monkeypatch.setenv("DLT_DEPLOYED_VERSION_TTL_SECONDS", "60")

    assert deployed.running_version().ok is False
    assert deployed.running_version().single == "1.0.0"
    assert calls["n"] == 2


def test_a_zero_ttl_disables_caching(monkeypatch):
    calls = {"n": 0}

    from src.log_pipeline.sources.k8s import discovery

    def counting(app=None, namespace=None, request_timeout=None):
        calls["n"] += 1
        return [FakePod("p1", f"{HARBOR}/1.0.0")]

    monkeypatch.setattr(discovery, "list_pods_for_service", counting)
    monkeypatch.setenv("DLT_DEPLOYED_VERSION_TTL_SECONDS", "0")

    deployed.running_version()
    deployed.running_version()

    assert calls["n"] == 2


def test_a_malformed_ttl_falls_back_to_the_default(monkeypatch):
    monkeypatch.setenv("DLT_DEPLOYED_VERSION_TTL_SECONDS", "not-a-number")
    assert deployed.ttl_seconds() == deployed.DEFAULT_TTL_SECONDS


# ---------------------------------------------------------------------------
# The record written into the case
# ---------------------------------------------------------------------------

def test_the_record_serialises_to_json(monkeypatch):
    _patch_pods(monkeypatch, [FakePod("enu-biometric-1", f"{HARBOR}/1.0.0")])

    payload = json.loads(json.dumps(deployed.running_version().as_dict()))

    assert payload["ok"] is True
    assert payload["version"] == "1.0.0"
    assert payload["mixed"] is False
    assert payload["images"][0][0] == "enu-biometric-1"


def test_the_recorded_image_list_is_capped(monkeypatch):
    _patch_pods(monkeypatch, [
        FakePod(f"pod-{i}", f"{HARBOR}/1.0.0") for i in range(40)
    ])

    result = deployed.running_version()

    assert result.pods == 40
    assert len(result.images) == deployed.MAX_RECORDED_IMAGES
