"""External prompt template loader for the opencode harness.

Templates live as ``.md`` files under ``src/prompts/harness/``.  Each file
contains free-form instruction text with ``{{variable}}`` placeholders that
are replaced at call time.

Using ``{{var}}`` (double-brace) instead of Python's ``str.format`` syntax
means literal braces in the template -- JSON schemas, code blocks -- never
need escaping.  A regex finds ``{{name}}`` and substitutes the value.

Design notes
------------
* ``render()`` reads from disk on every call so edits take effect without a
  restart (the system prompts in ``agent_orchestrator.py`` are read once at
  build time; harness prompts are per-case and benefit from live reload).
* The backend is intentionally pluggable: a future Langfuse integration
  replaces only the ``_load_text`` function -- callers stay unchanged.
* ``{{> rules/dlt}}`` inlines ``harness/rules/dlt.md``. This is how a template
  gets flow-specific agent rules. The root ``AGENTS.md`` cannot carry them:
  opencode loads it into *every* session whatever the task, so a
  rejection-only rule there is read by the DLT agent too -- which is how the
  DLT agent came to be told it was "the Rejection Investigator Agent" and not
  to read ``reason_codes.csv``, the registry its own flow is built on.
"""
import os
import re

_PLACEHOLDER = re.compile(r"\{\{(\w+)\}\}")
#: Matches every `{{> ...}}` directive, valid or not, so that a malformed one
#: fails loudly in `_include` instead of being left in the prompt as literal
#: text -- where a typo such as `{{> rules/dlt.md}}` would silently drop a
#: flow's rules.
_INCLUDE = re.compile(r"\{\{>\s*([^}]*?)\s*\}\}")
_INCLUDE_NAME = re.compile(r"^[\w-]+(?:/[\w-]+)*$")

_BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_HARNESS_DIR = os.path.join(_BASE_DIR, "prompts", "harness")


def _load_text(name: str) -> str:
    """Read the raw template text for *name* (without extension)."""
    path = os.path.join(_HARNESS_DIR, f"{name}.md")
    with open(path, "r", encoding="utf-8") as fh:
        return fh.read()


def _include(name: str) -> str:
    """Contents of ``harness/<name>.md`` for a ``{{> name}}`` directive.

    Confined to the harness directory: the name admits no ``.`` and no
    leading ``/``, and the resolved path is checked as well, so a template
    cannot include anything outside it.
    """
    if not _INCLUDE_NAME.match(name):
        raise ValueError(f"Invalid include {name!r}: use a path relative to the "
                         f"harness directory, without an extension, e.g. rules/dlt")
    path = os.path.realpath(os.path.join(_HARNESS_DIR, f"{name}.md"))
    root = os.path.realpath(_HARNESS_DIR)
    if os.path.commonpath([path, root]) != root:
        raise ValueError(f"Include '{name}' resolves outside the harness directory")
    with open(path, "r", encoding="utf-8") as fh:
        return fh.read().rstrip("\n")


def render(name: str, **variables: str) -> str:
    """Load a harness template and substitute ``{{var}}`` placeholders.

    Parameters
    ----------
    name:
        Template file name without extension (e.g. ``"RejectionInvestigator"``).
    **variables:
        Values for every ``{{placeholder}}`` in the template.

    Raises
    ------
    KeyError:
        If the template contains a placeholder not present in *variables*.
    """
    # Includes first, so an included file may use placeholders too.
    template = _INCLUDE.sub(lambda m: _include(m.group(1)), _load_text(name))

    def _replace(match: re.Match) -> str:
        key = match.group(1)
        if key not in variables:
            raise KeyError(
                f"Prompt template '{name}' references '{key}' "
                f"but no value was provided"
            )
        return variables[key]

    return _PLACEHOLDER.sub(_replace, template)


def template_names() -> list:
    """Return the list of available harness template names (without extension)."""
    if not os.path.isdir(_HARNESS_DIR):
        return []
    return sorted(
        os.path.splitext(f)[0]
        for f in os.listdir(_HARNESS_DIR)
        if f.endswith(".md")
    )
