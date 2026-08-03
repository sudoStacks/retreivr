"""Book discovery, acquisition, metadata, and library helpers.

Books deliberately use their own finalization path. Sending PDFs or EPUBs through
the video worker risks container probing/transcoding and loses the book metadata
contract this module preserves.
"""

from __future__ import annotations

import hashlib
import ipaddress
import json
import os
import re
import shutil
import socket
import tempfile
import zipfile
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor
from difflib import SequenceMatcher
from pathlib import Path
from typing import Any
from urllib.parse import quote, urljoin, urlparse

import requests


OPEN_LIBRARY_SEARCH_URL = "https://openlibrary.org/search.json"
OPEN_LIBRARY_BASE_URL = "https://openlibrary.org"
OPEN_LIBRARY_COVERS_URL = "https://covers.openlibrary.org"
OPEN_LIBRARY_WORK_URL = "https://openlibrary.org/works/{work_id}.json"
GUTENBERG_SEARCH_OPDS_URL = "https://www.gutenberg.org/ebooks/search.opds/"
GUTENBERG_BOOK_OPDS_URL = "https://www.gutenberg.org/ebooks/{book_id}.opds"
INTERNET_ARCHIVE_METADATA_URL = "https://archive.org/metadata/{identifier}"
INTERNET_ARCHIVE_DOWNLOAD_URL = "https://archive.org/download/{identifier}/{filename}"
BOOK_EXTENSIONS = {".pdf", ".epub", ".mobi", ".azw", ".azw3", ".txt"}
CONTENT_TYPE_EXTENSIONS = {
    "application/pdf": ".pdf",
    "application/epub+zip": ".epub",
    "application/x-mobipocket-ebook": ".mobi",
    "text/plain": ".txt",
}
DEFAULT_MAX_DOWNLOAD_BYTES = 500 * 1024 * 1024
DEFAULT_TIMEOUT = (8, 45)
_SAFE_COMPONENT = re.compile(r"[^\w\-.()' ]+", re.UNICODE)
_ARCHIVE_IDENTIFIER = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,254}$")
_OPEN_LIBRARY_WORK_ID = re.compile(r"^OL\d+W$", re.IGNORECASE)
_GUTENBERG_ID = re.compile(r"^[1-9]\d{0,8}$")
_ATOM_NS = {"atom": "http://www.w3.org/2005/Atom"}


class BookServiceError(RuntimeError):
    """A user-actionable failure in the Books pipeline."""


def get_books_config(config: dict | None) -> dict[str, Any]:
    root = config if isinstance(config, dict) else {}
    books = root.get("books") if isinstance(root.get("books"), dict) else {}
    setup = root.get("setup") if isinstance(root.get("setup"), dict) else {}
    stack = setup.get("stack") if isinstance(setup.get("stack"), dict) else {}
    library_path = str(books.get("library_path") or stack.get("books_root") or "./media/books").strip()
    return {
        "enabled": bool(books.get("enabled", False)),
        "library_path": library_path or "./media/books",
        "metadata_provider": str(books.get("metadata_provider") or "openlibrary").strip().lower(),
        "allow_direct_urls": bool(books.get("allow_direct_urls", True)),
        "allow_private_source_urls": bool(books.get("allow_private_source_urls", False)),
        "max_download_mb": max(1, min(4096, int(books.get("max_download_mb") or 500))),
    }


def resolve_books_library_path(config: dict | None) -> Path:
    raw = Path(get_books_config(config)["library_path"]).expanduser()
    return raw.resolve() if raw.is_absolute() else (Path.cwd() / raw).resolve()


def _first(values: Any, default: Any = "") -> Any:
    return values[0] if isinstance(values, list) and values else (values if values is not None else default)


def _clean_list(values: Any, *, limit: int = 12) -> list[str]:
    if not isinstance(values, (list, tuple, set)):
        values = [values] if values else []
    result: list[str] = []
    seen: set[str] = set()
    for value in values:
        text = str(value or "").strip()
        key = text.casefold()
        if not text or key in seen:
            continue
        seen.add(key)
        result.append(text)
        if len(result) >= limit:
            break
    return result


def _normalize_open_library_row(row: dict[str, Any]) -> dict[str, Any]:
    work_key = str(row.get("key") or "").strip()
    work_id = work_key.rsplit("/", 1)[-1] if work_key else ""
    cover_id = row.get("cover_i")
    isbn_values = _clean_list(row.get("isbn"), limit=8)
    cover_url = ""
    if cover_id:
        cover_url = f"{OPEN_LIBRARY_COVERS_URL}/b/id/{cover_id}-M.jpg?default=false"
    elif isbn_values:
        cover_url = f"{OPEN_LIBRARY_COVERS_URL}/b/isbn/{isbn_values[0]}-M.jpg?default=false"
    availability = row.get("availability") if isinstance(row.get("availability"), dict) else {}
    ebook_access = str(row.get("ebook_access") or availability.get("status") or "").strip().lower()
    public_readable = ebook_access in {"open", "public"}
    archive_identifiers = _clean_list(row.get("ia"), limit=12)
    availability_identifier = str(availability.get("identifier") or "").strip()
    if availability_identifier and availability_identifier in archive_identifiers:
        archive_identifiers.remove(availability_identifier)
        archive_identifiers.insert(0, availability_identifier)
    public_scan = _truthy_metadata_value(row.get("public_scan_b"))
    download_available = bool(
        public_readable
        and public_scan
        and not _truthy_metadata_value(availability.get("is_restricted"))
        and archive_identifiers
    )
    return {
        "id": work_id or hashlib.sha256(json.dumps(row, sort_keys=True, default=str).encode()).hexdigest()[:16],
        "provider": "openlibrary",
        "work_id": work_id,
        "edition_id": str(_first(row.get("edition_key"), "") or ""),
        "title": str(row.get("title") or "Untitled").strip(),
        "subtitle": str(row.get("subtitle") or "").strip(),
        "authors": _clean_list(row.get("author_name"), limit=8),
        "first_publish_year": row.get("first_publish_year"),
        "publishers": _clean_list(row.get("publisher"), limit=6),
        "subjects": _clean_list(row.get("subject"), limit=8),
        "languages": [value.rsplit("/", 1)[-1] for value in _clean_list(row.get("language"), limit=6)],
        "isbn": isbn_values,
        "edition_count": int(row.get("edition_count") or 0),
        "page_count": row.get("number_of_pages_median"),
        "cover_url": cover_url,
        "details_url": f"{OPEN_LIBRARY_BASE_URL}{work_key}" if work_key.startswith("/") else OPEN_LIBRARY_BASE_URL,
        "read_url": f"{OPEN_LIBRARY_BASE_URL}{work_key}" if work_key.startswith("/") else "",
        "ebook_access": ebook_access,
        "public_readable": public_readable,
        "has_fulltext": bool(row.get("has_fulltext")),
        "public_scan": public_scan,
        "download_available": download_available,
        "download_provider": "internet_archive" if download_available else "",
        "archive_identifier": archive_identifiers[0] if download_available else "",
        "archive_identifiers": archive_identifiers if download_available else [],
        "metadata": {
            "openlibrary_work_id": work_id,
            "openlibrary_edition_id": str(_first(row.get("edition_key"), "") or ""),
            "isbn": isbn_values,
        },
    }


def search_open_library(
    query: str,
    *,
    limit: int = 24,
    page: int = 1,
    downloadable_only: bool = False,
) -> dict[str, Any]:
    text = str(query or "").strip()
    if not text:
        raise BookServiceError("A title, author, subject, or ISBN is required")
    fields = ",".join(
        (
            "key", "title", "subtitle", "author_name", "author_key", "first_publish_year",
            "publisher", "subject", "language", "isbn", "cover_i", "edition_key",
            "edition_count", "number_of_pages_median", "has_fulltext", "ebook_access", "availability",
            "ia", "public_scan_b",
        )
    )
    try:
        requested_limit = max(1, min(50, int(limit)))
        response = requests.get(
            OPEN_LIBRARY_SEARCH_URL,
            params={
                "q": text,
                "fields": fields,
                "limit": 50 if downloadable_only else requested_limit,
                "page": max(1, int(page)),
            },
            headers={
                "Accept": "application/json",
                "User-Agent": "Retreivr/1.0 (+https://github.com/sudostacks/retreivr)",
            },
            timeout=DEFAULT_TIMEOUT,
        )
        response.raise_for_status()
        payload = response.json()
    except (requests.RequestException, ValueError) as exc:
        raise BookServiceError(f"Open Library search failed: {exc}") from exc
    docs = payload.get("docs") if isinstance(payload, dict) else []
    results = [_normalize_open_library_row(row) for row in docs or [] if isinstance(row, dict)]
    for index, row in enumerate(results):
        row["search_rank"] = index
    if downloadable_only:
        results = [row for row in results if row.get("download_available")][:requested_limit]
    results.sort(key=_book_access_sort_key)
    return {
        "provider": "openlibrary",
        "query": text,
        "page": max(1, int(page)),
        "total": int(payload.get("numFound") or payload.get("num_found") or 0),
        "downloadable_only": bool(downloadable_only),
        "results": results,
    }


def _book_access_sort_key(item: dict[str, Any]) -> tuple[int, int, str]:
    if item.get("download_available"):
        tier = 0
    elif item.get("public_readable"):
        tier = 1
    elif item.get("has_fulltext"):
        tier = 2
    else:
        tier = 3
    try:
        search_rank = int(item.get("search_rank") or 0)
    except (TypeError, ValueError):
        search_rank = 0
    return tier, search_rank, str(item.get("title") or "").casefold()


def _gutenberg_entry_id(entry: ET.Element) -> str:
    raw = str(entry.findtext("atom:id", default="", namespaces=_ATOM_NS) or "").strip()
    match = re.search(r"/ebooks/(\d+)\.opds(?:$|\?)", raw)
    return match.group(1) if match else ""


def search_project_gutenberg(query: str, *, limit: int = 12) -> dict[str, Any]:
    text = str(query or "").strip()
    if not text:
        raise BookServiceError("A Project Gutenberg search term is required")
    try:
        response = requests.get(
            GUTENBERG_SEARCH_OPDS_URL,
            params={"query": text},
            headers={
                "Accept": "application/atom+xml;profile=opds-catalog, application/atom+xml",
                "User-Agent": "Retreivr/1.0 (+https://github.com/sudostacks/retreivr)",
            },
            timeout=(8, 35),
        )
        response.raise_for_status()
        root = ET.fromstring(response.content)
    except (requests.RequestException, ET.ParseError) as exc:
        raise BookServiceError(f"Project Gutenberg search failed: {exc}") from exc
    results = []
    for entry in root.findall("atom:entry", _ATOM_NS):
        book_id = _gutenberg_entry_id(entry)
        if not book_id:
            continue
        title = str(entry.findtext("atom:title", default="", namespaces=_ATOM_NS) or "").strip() or "Untitled"
        author_text = str(entry.findtext("atom:content", default="", namespaces=_ATOM_NS) or "").strip()
        authors = [] if re.search(r"\bdownloads?$", author_text, re.IGNORECASE) else _clean_list(author_text, limit=4)
        results.append(
            {
                "id": f"gutenberg:{book_id}",
                "provider": "project_gutenberg",
                "title": title,
                "subtitle": "",
                "authors": authors,
                "first_publish_year": None,
                "publishers": ["Project Gutenberg"],
                "subjects": ["Public domain"],
                "languages": [],
                "isbn": [],
                "edition_count": 1,
                "page_count": None,
                "cover_url": "",
                "details_url": f"https://www.gutenberg.org/ebooks/{book_id}",
                "read_url": f"https://www.gutenberg.org/ebooks/{book_id}",
                "ebook_access": "public",
                "public_readable": True,
                "has_fulltext": True,
                "public_scan": True,
                "download_available": True,
                "download_provider": "project_gutenberg",
                "gutenberg_id": book_id,
                "archive_identifier": "",
                "archive_identifiers": [],
                "source_labels": ["Project Gutenberg"],
                "search_rank": len(results),
            }
        )
        if len(results) >= max(1, min(25, int(limit))):
            break
    return {"provider": "project_gutenberg", "query": text, "results": results}


def _book_result_key(item: dict[str, Any]) -> str:
    title = re.sub(r"[^a-z0-9]+", " ", str(item.get("title") or "").casefold()).strip()
    authors = item.get("authors") if isinstance(item.get("authors"), list) else []
    author_text = re.sub(r"[^a-z0-9]+", " ", str(authors[0] if authors else "").casefold()).strip()
    # Catalogs disagree on "First Last" versus "Last, First". Token ordering
    # gives exact title/author duplicates a stable cross-provider identity.
    author = " ".join(sorted(author_text.split()))
    return f"{title}|{author}"


def _merge_book_results(open_library: list[dict[str, Any]], gutenberg: list[dict[str, Any]]) -> list[dict[str, Any]]:
    merged: list[dict[str, Any]] = []
    by_key: dict[str, dict[str, Any]] = {}
    for item in open_library:
        key = _book_result_key(item)
        existing = by_key.get(key)
        if not existing:
            row = dict(item)
            merged.append(row)
            by_key[key] = row
            continue
        archive_ids = _clean_list(
            list(existing.get("archive_identifiers") or []) + list(item.get("archive_identifiers") or []),
            limit=12,
        )
        existing["archive_identifiers"] = archive_ids
        if not existing.get("download_available") and item.get("download_available"):
            existing["download_available"] = True
            existing["download_provider"] = "internet_archive"
            existing["archive_identifier"] = item.get("archive_identifier") or (archive_ids[0] if archive_ids else "")
        existing["search_rank"] = min(
            int(existing.get("search_rank") or 0),
            int(item.get("search_rank") or 0),
        )
    for item in gutenberg:
        key = _book_result_key(item)
        existing = by_key.get(key)
        if not existing:
            row = dict(item)
            merged.append(row)
            by_key[key] = row
            continue
        existing["download_available"] = True
        existing["download_provider"] = "project_gutenberg"
        if not existing.get("gutenberg_id"):
            existing["gutenberg_id"] = item.get("gutenberg_id")
        existing["search_rank"] = min(
            int(existing.get("search_rank") or 0),
            int(item.get("search_rank") or 0),
        )
        existing["source_labels"] = ["Project Gutenberg", "Open Library"] + (
            ["Internet Archive"] if existing.get("archive_identifiers") else []
        )
    merged.sort(key=_book_access_sort_key)
    return merged


def search_book_catalogs(
    query: str,
    *,
    limit: int = 24,
    page: int = 1,
    downloadable_only: bool = False,
) -> dict[str, Any]:
    requested_limit = max(1, min(50, int(limit)))
    with ThreadPoolExecutor(max_workers=2) as pool:
        open_future = pool.submit(
            search_open_library,
            query,
            limit=requested_limit,
            page=page,
            downloadable_only=downloadable_only,
        )
        gutenberg_future = pool.submit(search_project_gutenberg, query, limit=min(12, requested_limit))
        provider_errors = {}
        try:
            open_payload = open_future.result()
        except BookServiceError as exc:
            open_payload = {"provider": "openlibrary", "results": []}
            provider_errors["openlibrary"] = str(exc)
        try:
            gutenberg_payload = gutenberg_future.result()
        except BookServiceError as exc:
            gutenberg_payload = {"provider": "project_gutenberg", "results": []}
            provider_errors["project_gutenberg"] = str(exc)
    results = _merge_book_results(
        list(open_payload.get("results") or []),
        list(gutenberg_payload.get("results") or []),
    )[:requested_limit]
    if not results and provider_errors:
        raise BookServiceError("; ".join(provider_errors.values()))
    return {
        "provider": "multi_source",
        "providers": ["project_gutenberg", "openlibrary", "internet_archive"],
        "provider_errors": provider_errors,
        "query": str(query or "").strip(),
        "page": max(1, int(page)),
        "downloadable_only": bool(downloadable_only),
        "results": results,
    }


def get_open_library_work(work_id: str) -> dict[str, Any]:
    normalized = str(work_id or "").strip().upper()
    if not _OPEN_LIBRARY_WORK_ID.fullmatch(normalized):
        raise BookServiceError("A valid Open Library work identifier is required")
    try:
        response = requests.get(
            OPEN_LIBRARY_WORK_URL.format(work_id=normalized),
            headers={
                "Accept": "application/json",
                "User-Agent": "Retreivr/1.0 (+https://github.com/sudostacks/retreivr)",
            },
            timeout=DEFAULT_TIMEOUT,
        )
        response.raise_for_status()
        payload = response.json()
    except (requests.RequestException, ValueError) as exc:
        raise BookServiceError(f"Open Library details failed: {exc}") from exc
    if not isinstance(payload, dict):
        raise BookServiceError("Open Library returned invalid book details")
    raw_description = payload.get("description")
    description = str(raw_description.get("value") or "").strip() if isinstance(raw_description, dict) else str(raw_description or "").strip()
    links = []
    for row in payload.get("links") or []:
        if not isinstance(row, dict):
            continue
        url = str(row.get("url") or "").strip()
        if url:
            links.append({"title": str(row.get("title") or "Reference").strip(), "url": url})
        if len(links) >= 8:
            break
    return {
        "provider": "openlibrary",
        "work_id": normalized,
        "title": str(payload.get("title") or "").strip(),
        "description": description,
        "subjects": _clean_list(payload.get("subjects"), limit=24),
        "subject_places": _clean_list(payload.get("subject_places"), limit=12),
        "subject_people": _clean_list(payload.get("subject_people"), limit=12),
        "subject_times": _clean_list(payload.get("subject_times"), limit=12),
        "first_publish_date": str(payload.get("first_publish_date") or "").strip(),
        "links": links,
        "details_url": f"{OPEN_LIBRARY_BASE_URL}/works/{normalized}",
    }


def _safe_component(value: Any, fallback: str) -> str:
    text = _SAFE_COMPONENT.sub(" ", str(value or "")).strip(" .")
    text = re.sub(r"\s+", " ", text)
    return (text[:160].strip() or fallback)


def _canonical_metadata(metadata: dict[str, Any] | None) -> dict[str, Any]:
    source = metadata if isinstance(metadata, dict) else {}
    authors = _clean_list(source.get("authors") or source.get("author"), limit=12)
    identifiers = source.get("identifiers") if isinstance(source.get("identifiers"), dict) else {}
    isbn = _clean_list(source.get("isbn") or identifiers.get("isbn"), limit=8)
    subjects = _clean_list(source.get("subjects") or source.get("subject") or source.get("tags"), limit=20)
    return {
        "schema_version": 1,
        "media_type": "book",
        "title": str(source.get("title") or "Untitled").strip() or "Untitled",
        "subtitle": str(source.get("subtitle") or "").strip(),
        "authors": authors,
        "publisher": str(source.get("publisher") or _first(source.get("publishers"), "") or "").strip(),
        "published_date": str(
            source.get("published_date")
            or source.get("release_date")
            or source.get("first_publish_year")
            or ""
        ).strip(),
        "description": str(source.get("description") or "").strip(),
        "subjects": subjects,
        "languages": _clean_list(source.get("languages") or source.get("language"), limit=8),
        "isbn": isbn,
        "openlibrary_work_id": str(source.get("openlibrary_work_id") or source.get("work_id") or "").strip(),
        "openlibrary_edition_id": str(source.get("openlibrary_edition_id") or source.get("edition_id") or "").strip(),
        "cover_url": str(source.get("cover_url") or "").strip(),
        "source_url": str(source.get("source_url") or "").strip(),
        "source_provider": str(source.get("source_provider") or source.get("provider") or "manual").strip(),
        "archive_identifier": str(source.get("archive_identifier") or "").strip(),
        "gutenberg_id": str(source.get("gutenberg_id") or "").strip(),
        "license_url": str(source.get("license_url") or "").strip(),
        "rights": str(source.get("rights") or "").strip(),
    }


def _truthy_metadata_value(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return str(value or "").strip().lower() in {"1", "true", "yes", "on"}


def _title_match_score(expected: str, actual: str) -> float:
    expected_text = re.sub(r"[^a-z0-9]+", " ", str(expected or "").casefold()).strip()
    actual_text = re.sub(r"[^a-z0-9]+", " ", str(actual or "").casefold()).strip()
    if not expected_text or not actual_text:
        return 1.0
    stop_words = {"a", "an", "and", "of", "the"}
    expected_tokens = {token for token in expected_text.split() if token not in stop_words}
    actual_tokens = {token for token in actual_text.split() if token not in stop_words}
    token_score = len(expected_tokens & actual_tokens) / max(1, len(expected_tokens))
    sequence_score = SequenceMatcher(None, expected_text, actual_text).ratio()
    return max(token_score, sequence_score)


def _archive_download_file(payload: dict[str, Any], *, preferred_format: str = "") -> dict[str, Any] | None:
    metadata = payload.get("metadata") if isinstance(payload.get("metadata"), dict) else {}
    if _truthy_metadata_value(metadata.get("access-restricted-item")):
        return None
    requested = str(preferred_format or "").strip().lower()
    format_priority = {
        "epub": ("epub", "pdf", "mobi"),
        "pdf": ("pdf", "epub", "mobi"),
        "mobi": ("mobi", "epub", "pdf"),
    }.get(requested, ("epub", "pdf", "mobi"))
    candidates: list[tuple[int, int, dict[str, Any]]] = []
    for file_row in payload.get("files") or []:
        if not isinstance(file_row, dict) or _truthy_metadata_value(file_row.get("private")):
            continue
        name = str(file_row.get("name") or "").strip()
        extension = Path(name).suffix.lower().lstrip(".")
        if extension not in format_priority or name.casefold().endswith("_bw.pdf"):
            continue
        format_name = str(file_row.get("format") or "").strip().casefold()
        if extension == "pdf" and format_name not in {"text pdf", "pdf"}:
            continue
        try:
            size = int(file_row.get("size") or 0)
        except (TypeError, ValueError):
            size = 0
        candidates.append((format_priority.index(extension), size, file_row))
    if not candidates:
        return None
    candidates.sort(key=lambda entry: (entry[0], entry[1] or 2**63))
    return candidates[0][2]


def acquire_open_library_book(
    config: dict | None,
    archive_identifiers: list[str] | tuple[str, ...],
    metadata: dict[str, Any] | None,
    *,
    preferred_format: str = "",
) -> dict[str, Any]:
    """Resolve a public Open Library scan and finalize it in one request."""

    identifiers: list[str] = []
    for value in archive_identifiers or []:
        identifier = str(value or "").strip()
        if identifier and _ARCHIVE_IDENTIFIER.fullmatch(identifier) and identifier not in identifiers:
            identifiers.append(identifier)
        if len(identifiers) >= 6:
            break
    if not identifiers:
        raise BookServiceError("This result does not expose a public downloadable artifact")

    expected_title = str((metadata or {}).get("title") or "").strip()
    last_error = "No matching public EPUB or PDF was found"
    for identifier in identifiers:
        try:
            response = requests.get(
                INTERNET_ARCHIVE_METADATA_URL.format(identifier=quote(identifier, safe="")),
                headers={"Accept": "application/json", "User-Agent": "Retreivr/1.0 (+https://github.com/sudostacks/retreivr)"},
                timeout=(6, 20),
            )
            response.raise_for_status()
            payload = response.json()
        except (requests.RequestException, ValueError) as exc:
            last_error = f"Internet Archive metadata lookup failed: {exc}"
            continue
        if not isinstance(payload, dict):
            continue
        archive_metadata = payload.get("metadata") if isinstance(payload.get("metadata"), dict) else {}
        if _title_match_score(expected_title, str(archive_metadata.get("title") or "")) < 0.5:
            last_error = "The public scan did not match the selected title"
            continue
        file_row = _archive_download_file(payload, preferred_format=preferred_format)
        if not file_row:
            last_error = "The matching archive item has no public EPUB or PDF file"
            continue
        name = str(file_row.get("name") or "").strip()
        source_url = INTERNET_ARCHIVE_DOWNLOAD_URL.format(
            identifier=quote(identifier, safe=""),
            filename=quote(name, safe=""),
        )
        enriched = dict(metadata or {})
        enriched.update(
            {
                "archive_identifier": identifier,
                "source_provider": "internet_archive_openlibrary",
                "license_url": str(archive_metadata.get("licenseurl") or "").strip(),
                "rights": str(archive_metadata.get("rights") or archive_metadata.get("usage") or "").strip(),
            }
        )
        return acquire_book_url(config, source_url, enriched)
    raise BookServiceError(last_error)


def acquire_project_gutenberg_book(
    config: dict | None,
    book_id: str,
    metadata: dict[str, Any] | None,
    *,
    preferred_format: str = "",
) -> dict[str, Any]:
    normalized_id = str(book_id or "").strip()
    if not _GUTENBERG_ID.fullmatch(normalized_id):
        raise BookServiceError("A valid Project Gutenberg book identifier is required")
    try:
        response = requests.get(
            GUTENBERG_BOOK_OPDS_URL.format(book_id=normalized_id),
            headers={
                "Accept": "application/atom+xml;profile=opds-catalog, application/atom+xml",
                "User-Agent": "Retreivr/1.0 (+https://github.com/sudostacks/retreivr)",
            },
            timeout=(8, 35),
        )
        response.raise_for_status()
        root = ET.fromstring(response.content)
    except (requests.RequestException, ET.ParseError) as exc:
        raise BookServiceError(f"Project Gutenberg download lookup failed: {exc}") from exc
    requested = str(preferred_format or "").strip().lower()
    format_priority = {
        "epub": ("epub", "mobi"),
        "mobi": ("mobi", "epub"),
    }.get(requested, ("epub", "mobi"))
    mime_format = {
        "application/epub+zip": "epub",
        "application/x-mobipocket-ebook": "mobi",
    }
    candidates = []
    for link in root.findall(".//atom:link", _ATOM_NS):
        if str(link.attrib.get("rel") or "") != "http://opds-spec.org/acquisition":
            continue
        kind = mime_format.get(str(link.attrib.get("type") or "").strip().lower())
        href = str(link.attrib.get("href") or "").strip()
        parsed = urlparse(href)
        if kind not in format_priority or parsed.scheme != "https" or not str(parsed.hostname or "").endswith("gutenberg.org"):
            continue
        try:
            length = int(link.attrib.get("length") or 0)
        except (TypeError, ValueError):
            length = 0
        candidates.append((format_priority.index(kind), length or 2**63, href))
    if not candidates:
        raise BookServiceError("Project Gutenberg did not expose a supported EPUB or Kindle file")
    candidates.sort()
    enriched = dict(metadata or {})
    enriched.update(
        {
            "gutenberg_id": normalized_id,
            "source_provider": "project_gutenberg",
            "license_url": "https://www.gutenberg.org/policy/license.html",
            "rights": "Project Gutenberg license and applicable public-domain terms",
        }
    )
    return acquire_book_url(config, candidates[0][2], enriched)


def _write_sidecar(path: Path, metadata: dict[str, Any]) -> Path:
    sidecar = path.with_suffix(path.suffix + ".metadata.json")
    temp = sidecar.with_name(f".{sidecar.name}.tmp")
    temp.write_text(json.dumps(metadata, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    os.replace(temp, sidecar)
    return sidecar


def _embed_pdf_metadata(path: Path, metadata: dict[str, Any]) -> bool:
    try:
        from pypdf import PdfReader, PdfWriter
    except ImportError:
        return False
    reader = PdfReader(str(path))
    if reader.is_encrypted:
        return False
    writer = PdfWriter()
    writer.clone_document_from_reader(reader)
    writer.add_metadata(
        {
            "/Title": metadata["title"],
            "/Author": "; ".join(metadata["authors"]),
            "/Subject": metadata["description"] or "; ".join(metadata["subjects"]),
            "/Keywords": ", ".join(metadata["subjects"]),
            "/RetreivrSchema": "book/v1",
            "/RetreivrSource": metadata["source_url"],
            "/ISBN": ", ".join(metadata["isbn"]),
            "/OpenLibraryWorkID": metadata["openlibrary_work_id"],
        }
    )
    fd, temp_name = tempfile.mkstemp(prefix=".retreivr-book-", suffix=".pdf", dir=str(path.parent))
    os.close(fd)
    try:
        with open(temp_name, "wb") as handle:
            writer.write(handle)
        os.replace(temp_name, path)
    finally:
        if os.path.exists(temp_name):
            os.remove(temp_name)
    return True


def _epub_package_path(archive: zipfile.ZipFile) -> str:
    import xml.etree.ElementTree as ET

    container = ET.fromstring(archive.read("META-INF/container.xml"))
    for element in container.iter():
        if element.tag.rsplit("}", 1)[-1] == "rootfile" and element.attrib.get("full-path"):
            return element.attrib["full-path"]
    raise BookServiceError("EPUB package document is missing")


def _embed_epub_metadata(path: Path, metadata: dict[str, Any]) -> bool:
    import xml.etree.ElementTree as ET

    with zipfile.ZipFile(path, "r") as source:
        package_path = _epub_package_path(source)
        root = ET.fromstring(source.read(package_path))
        metadata_node = next((node for node in root.iter() if node.tag.rsplit("}", 1)[-1] == "metadata"), None)
        if metadata_node is None:
            return False
        ns = "http://purl.org/dc/elements/1.1/"

        def replace(local_name: str, values: list[str]) -> None:
            for child in list(metadata_node):
                if child.tag == f"{{{ns}}}{local_name}":
                    metadata_node.remove(child)
            for value in values:
                node = ET.SubElement(metadata_node, f"{{{ns}}}{local_name}")
                node.text = value

        replace("title", [metadata["title"]])
        replace("creator", metadata["authors"])
        replace("publisher", [metadata["publisher"]] if metadata["publisher"] else [])
        replace("date", [metadata["published_date"]] if metadata["published_date"] else [])
        replace("subject", metadata["subjects"])
        replace("language", metadata["languages"])
        identifiers = [f"isbn:{value}" for value in metadata["isbn"]]
        if metadata["openlibrary_work_id"]:
            identifiers.append(f"openlibrary:{metadata['openlibrary_work_id']}")
        existing_identifiers = {
            str(child.text or "").strip()
            for child in metadata_node
            if child.tag == f"{{{ns}}}identifier"
        }
        for value in identifiers:
            if value in existing_identifiers:
                continue
            node = ET.SubElement(metadata_node, f"{{{ns}}}identifier")
            node.text = value
        package_bytes = ET.tostring(root, encoding="utf-8", xml_declaration=True)
        fd, temp_name = tempfile.mkstemp(prefix=".retreivr-book-", suffix=".epub", dir=str(path.parent))
        os.close(fd)
        try:
            with zipfile.ZipFile(temp_name, "w") as target:
                names = source.namelist()
                if "mimetype" in names:
                    target.writestr("mimetype", source.read("mimetype"), compress_type=zipfile.ZIP_STORED)
                for info in source.infolist():
                    if info.filename == "mimetype":
                        continue
                    target.writestr(
                        info,
                        package_bytes if info.filename == package_path else source.read(info.filename),
                    )
            os.replace(temp_name, path)
        finally:
            if os.path.exists(temp_name):
                os.remove(temp_name)
    return True


def finalize_book_artifact(
    source_path: str | Path,
    destination_dir: str | Path,
    metadata: dict[str, Any] | None,
) -> dict[str, Any]:
    source = Path(source_path)
    extension = source.suffix.lower()
    if extension not in BOOK_EXTENSIONS:
        raise BookServiceError(f"Unsupported book format: {extension or 'unknown'}")
    canonical = _canonical_metadata(metadata)
    author = canonical["authors"][0] if canonical["authors"] else "Unknown Author"
    target_dir = Path(destination_dir) / _safe_component(author, "Unknown Author")
    target_dir.mkdir(parents=True, exist_ok=True)
    target = target_dir / f"{_safe_component(canonical['title'], 'Untitled')}{extension}"
    counter = 2
    while target.exists():
        target = target_dir / f"{_safe_component(canonical['title'], 'Untitled')} ({counter}){extension}"
        counter += 1
    shutil.move(str(source), str(target))
    embedded = False
    try:
        if extension == ".pdf":
            embedded = _embed_pdf_metadata(target, canonical)
        elif extension == ".epub":
            embedded = _embed_epub_metadata(target, canonical)
    except Exception:
        embedded = False
    canonical["file_name"] = target.name
    canonical["file_format"] = extension.lstrip(".")
    canonical["file_size"] = target.stat().st_size
    canonical["metadata_embedded"] = embedded
    sidecar = _write_sidecar(target, canonical)
    return {"path": str(target), "sidecar": str(sidecar), "metadata": canonical}


def _validate_public_url(url: str, *, allow_private: bool) -> None:
    parsed = urlparse(url)
    if parsed.scheme not in {"http", "https"} or not parsed.hostname:
        raise BookServiceError("Book source must be an HTTP or HTTPS URL")
    if allow_private:
        return
    try:
        port = parsed.port or (443 if parsed.scheme == "https" else 80)
        addresses = {entry[4][0] for entry in socket.getaddrinfo(parsed.hostname, port)}
    except socket.gaierror as exc:
        raise BookServiceError("Book source host could not be resolved") from exc
    for address in addresses:
        ip = ipaddress.ip_address(address)
        if not ip.is_global:
            raise BookServiceError("Private, loopback, and link-local source URLs are disabled")


def acquire_book_url(config: dict | None, url: str, metadata: dict[str, Any] | None) -> dict[str, Any]:
    books = get_books_config(config)
    if not books["allow_direct_urls"]:
        raise BookServiceError("Direct book URLs are disabled in Settings")
    source_url = str(url or "").strip()
    _validate_public_url(source_url, allow_private=books["allow_private_source_urls"])
    maximum = int(books["max_download_mb"]) * 1024 * 1024
    try:
        response = None
        current_url = source_url
        for _redirect in range(6):
            _validate_public_url(current_url, allow_private=books["allow_private_source_urls"])
            response = requests.get(
                current_url,
                stream=True,
                allow_redirects=False,
                timeout=DEFAULT_TIMEOUT,
                headers={"User-Agent": "Retreivr/1.0 (+https://github.com/sudostacks/retreivr)"},
            )
            if response.status_code not in {301, 302, 303, 307, 308}:
                break
            location = str(response.headers.get("Location") or "").strip()
            response.close()
            if not location:
                raise BookServiceError("Book source returned an invalid redirect")
            current_url = urljoin(current_url, location)
        if response is None or response.status_code in {301, 302, 303, 307, 308}:
            raise BookServiceError("Book source redirected too many times")
        response.raise_for_status()
        _validate_public_url(response.url, allow_private=books["allow_private_source_urls"])
        content_length = int(response.headers.get("Content-Length") or 0)
        if content_length > maximum:
            raise BookServiceError(f"Book exceeds the configured {books['max_download_mb']} MB limit")
        parsed_path = Path(urlparse(response.url).path)
        extension = parsed_path.suffix.lower()
        content_type = response.headers.get("Content-Type", "").split(";", 1)[0].lower()
        if extension not in BOOK_EXTENSIONS:
            extension = CONTENT_TYPE_EXTENSIONS.get(content_type, "")
        if extension not in BOOK_EXTENSIONS:
            raise BookServiceError("The URL did not return a supported PDF or ebook format")
        library = resolve_books_library_path(config)
        library.mkdir(parents=True, exist_ok=True)
        fd, temp_name = tempfile.mkstemp(prefix=".retreivr-book-download-", suffix=extension, dir=str(library))
        downloaded = 0
        try:
            with os.fdopen(fd, "wb") as handle:
                for chunk in response.iter_content(chunk_size=1024 * 256):
                    if not chunk:
                        continue
                    downloaded += len(chunk)
                    if downloaded > maximum:
                        raise BookServiceError(f"Book exceeds the configured {books['max_download_mb']} MB limit")
                    handle.write(chunk)
            enriched = dict(metadata or {})
            enriched.update(
                {
                    "source_url": response.url,
                    "source_provider": enriched.get("source_provider") or enriched.get("provider") or "direct_url",
                }
            )
            return finalize_book_artifact(temp_name, library, enriched)
        except Exception:
            if os.path.exists(temp_name):
                os.remove(temp_name)
            raise
    except requests.RequestException as exc:
        raise BookServiceError(f"Book download failed: {exc}") from exc


def import_book_file(config: dict | None, source_path: str | Path, metadata: dict[str, Any] | None) -> dict[str, Any]:
    library = resolve_books_library_path(config)
    library.mkdir(parents=True, exist_ok=True)
    enriched = dict(metadata or {})
    enriched.setdefault("source_provider", "local_import")
    return finalize_book_artifact(source_path, library, enriched)


def _metadata_for_file(path: Path) -> dict[str, Any]:
    sidecar = path.with_suffix(path.suffix + ".metadata.json")
    if sidecar.is_file():
        try:
            payload = json.loads(sidecar.read_text(encoding="utf-8"))
            if isinstance(payload, dict):
                return _canonical_metadata(payload) | {
                    "metadata_embedded": bool(payload.get("metadata_embedded")),
                }
        except (OSError, ValueError):
            pass
    return _canonical_metadata({"title": path.stem, "source_provider": "library_scan"})


def list_book_library(config: dict | None) -> dict[str, Any]:
    root = resolve_books_library_path(config)
    results: list[dict[str, Any]] = []
    if root.is_dir():
        for path in sorted(root.rglob("*")):
            if not path.is_file() or path.suffix.lower() not in BOOK_EXTENSIONS:
                continue
            metadata = _metadata_for_file(path)
            relative = path.relative_to(root).as_posix()
            book_id = hashlib.sha256(relative.encode("utf-8")).hexdigest()[:24]
            results.append(
                {
                    "id": book_id,
                    "title": metadata["title"] or path.stem,
                    "authors": metadata["authors"],
                    "publisher": metadata["publisher"],
                    "published_date": metadata["published_date"],
                    "subjects": metadata["subjects"],
                    "languages": metadata["languages"],
                    "isbn": metadata["isbn"],
                    "cover_url": metadata["cover_url"],
                    "format": path.suffix.lstrip(".").upper(),
                    "size": path.stat().st_size,
                    "relative_path": relative,
                    "metadata_embedded": bool(metadata.get("metadata_embedded")),
                }
            )
    return {"library_path": str(root), "count": len(results), "results": results}


def resolve_library_book(config: dict | None, book_id: str) -> Path:
    root = resolve_books_library_path(config)
    for path in root.rglob("*") if root.is_dir() else []:
        if path.is_file() and path.suffix.lower() in BOOK_EXTENSIONS:
            relative = path.relative_to(root).as_posix()
            if hashlib.sha256(relative.encode("utf-8")).hexdigest()[:24] == book_id:
                return path
    raise BookServiceError("Book was not found in the library")
