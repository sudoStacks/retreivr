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
from pathlib import Path
from typing import Any
from urllib.parse import urljoin, urlparse

import requests


OPEN_LIBRARY_SEARCH_URL = "https://openlibrary.org/search.json"
OPEN_LIBRARY_BASE_URL = "https://openlibrary.org"
OPEN_LIBRARY_COVERS_URL = "https://covers.openlibrary.org"
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
        "public_readable": ebook_access in {"open", "public"},
        "has_fulltext": bool(row.get("has_fulltext")),
        "metadata": {
            "openlibrary_work_id": work_id,
            "openlibrary_edition_id": str(_first(row.get("edition_key"), "") or ""),
            "isbn": isbn_values,
        },
    }


def search_open_library(query: str, *, limit: int = 24, page: int = 1) -> dict[str, Any]:
    text = str(query or "").strip()
    if not text:
        raise BookServiceError("A title, author, subject, or ISBN is required")
    fields = ",".join(
        (
            "key", "title", "subtitle", "author_name", "author_key", "first_publish_year",
            "publisher", "subject", "language", "isbn", "cover_i", "edition_key",
            "edition_count", "number_of_pages_median", "has_fulltext", "ebook_access", "availability",
        )
    )
    try:
        response = requests.get(
            OPEN_LIBRARY_SEARCH_URL,
            params={"q": text, "fields": fields, "limit": max(1, min(50, int(limit))), "page": max(1, int(page))},
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
    return {
        "provider": "openlibrary",
        "query": text,
        "page": max(1, int(page)),
        "total": int(payload.get("numFound") or payload.get("num_found") or 0),
        "results": [_normalize_open_library_row(row) for row in docs or [] if isinstance(row, dict)],
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
    }


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
            enriched.update({"source_url": response.url, "source_provider": enriched.get("provider") or "direct_url"})
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
