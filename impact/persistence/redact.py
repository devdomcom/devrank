"""Secret redaction for raw API data before it hits disk.

Applied by ``FileSystemDumpWriter`` at the persistence boundary so that
every provider's output is scrubbed regardless of adapter.  Only
high-confidence patterns are matched to minimise false positives.
"""

from __future__ import annotations

import re

_PLACEHOLDER = "REDACTED"

# Each entry: (name for logging, compiled regex)
_PATTERNS: list[tuple[str, re.Pattern[str]]] = [
    # --- Provider API tokens ---
    ("Mapbox token", re.compile(r"[ps]k\.eyJ[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+")),
    ("AWS access key", re.compile(r"(?<![A-Z0-9])AKIA[0-9A-Z]{16}(?![A-Z0-9])")),
    ("GitHub token", re.compile(r"(?:ghp|gho|ghu|ghs|ghr)_[A-Za-z0-9_]{36,}")),
    ("GitLab token", re.compile(r"glpat-[A-Za-z0-9_-]{20,}")),
    ("Slack token", re.compile(r"xox[bpras]-[A-Za-z0-9-]+")),
    ("Stripe key", re.compile(r"[sr]k_(live|test)_[A-Za-z0-9]{24,}")),
    ("Google API key", re.compile(r"AIza[A-Za-z0-9_-]{35}")),
    ("Twilio key", re.compile(r"SK[0-9a-fA-F]{32}")),
    ("SendGrid key", re.compile(r"SG\.[A-Za-z0-9_-]{22,}\.[A-Za-z0-9_-]{43,}")),
    ("Heroku API key", re.compile(r"[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}")),
    # --- Private keys ---
    ("Private key", re.compile(r"-----BEGIN (?:RSA |EC |DSA |OPENSSH )?PRIVATE KEY-----")),
    # --- Connection strings with credentials ---
    ("Connection string", re.compile(r"://[^@\s]+:[^@\s]+@[^/\s]+")),
    # --- Generic high-entropy secrets (assignment patterns) ---
    ("Inline secret", re.compile(
        r"""(?:password|passwd|secret|token|api_key|apikey|api-key|access_key|auth_token)"""
        r"""[\s]*[=:]\s*['"][A-Za-z0-9/+=_-]{16,}['"]""",
        re.IGNORECASE,
    )),
]

# Fields in raw API dicts that carry user-generated or code content.
_TEXT_FIELDS: frozenset[str] = frozenset({
    "patch", "content", "body", "message", "description",
})


def redact_secrets(text: str) -> str:
    """Replace all recognised secret patterns in *text* with a placeholder."""
    for _name, pattern in _PATTERNS:
        text = pattern.sub(_PLACEHOLDER, text)
    return text


def scrub_record(record: dict) -> dict:
    """Return a shallow copy of *record* with secret-bearing text fields redacted.

    Only inspects known text-heavy fields (``_TEXT_FIELDS``) to avoid
    scanning every value in large API payloads.
    """
    dirty = False
    for key in _TEXT_FIELDS:
        val = record.get(key)
        if not isinstance(val, str):
            continue
        cleaned = redact_secrets(val)
        if cleaned is not val and cleaned != val:
            if not dirty:
                record = dict(record)  # copy-on-write
                dirty = True
            record[key] = cleaned
    return record
