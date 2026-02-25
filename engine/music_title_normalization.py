from __future__ import annotations

import re
import unicodedata
from typing import Iterable

_BRACKETED_SEGMENT_RE = re.compile(r"[\(\[\{][^)\]\}]*[\)\]\}]")
_WS_RE = re.compile(r"\s+")
_NON_ALNUM_RE = re.compile(r"[^a-z0-9]+")

# Keep this list intentionally narrow to avoid over-aggressive normalization.
_RELAXED_PARENTHESES_PATTERNS: tuple[re.Pattern[str], ...] = (
    re.compile(r"\blive\b", re.IGNORECASE),
    re.compile(r"\bdeluxe(?:\s+edition)?\b", re.IGNORECASE),
    re.compile(r"\bremaster(?:ed)?(?:\s+\d{2,4})?\b", re.IGNORECASE),
)


def _normalize_phrase(value: str) -> str:
    text = unicodedata.normalize("NFKC", str(value or "")).lower().strip()
    text = _NON_ALNUM_RE.sub(" ", text)
    return _WS_RE.sub(" ", text).strip()


def _matches_any_pattern(text: str, patterns: Iterable[re.Pattern[str]]) -> bool:
    return any(pattern.search(text) for pattern in patterns)


def extract_parenthetical_tags(value: str) -> set[str]:
    tags: set[str] = set()
    raw = unicodedata.normalize("NFKC", str(value or ""))
    for match in _BRACKETED_SEGMENT_RE.finditer(raw):
        inner = match.group(0)[1:-1].strip()
        normalized = _normalize_phrase(inner)
        if not normalized:
            continue
        if re.search(r"\blive\b", normalized):
            tags.add("live")
        if re.search(r"\bdeluxe(?:\s+edition)?\b", normalized):
            tags.add("deluxe")
        if re.search(r"\bremaster(?:ed)?(?:\s+\d{2,4})?\b", normalized):
            tags.add("remastered")
    return tags


def relaxed_search_title(value: str) -> str:
    raw = unicodedata.normalize("NFKC", str(value or ""))

    def _replace(match: re.Match[str]) -> str:
        segment = match.group(0)
        inner = segment[1:-1].strip()
        normalized = _normalize_phrase(inner)
        if not normalized:
            return " "
        if _matches_any_pattern(normalized, _RELAXED_PARENTHESES_PATTERNS):
            return " "
        return f" {inner} "

    stripped = _BRACKETED_SEGMENT_RE.sub(_replace, raw)
    return _WS_RE.sub(" ", stripped).strip()


def has_live_intent(*values: str | None) -> bool:
    for value in values:
        normalized = _normalize_phrase(str(value or ""))
        if not normalized:
            continue
        if re.search(r"\blive\b", normalized):
            return True
    return False

