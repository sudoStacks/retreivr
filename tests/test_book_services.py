from __future__ import annotations

import json
import zipfile
from pathlib import Path

import pytest

from engine import book_services


def _config(tmp_path, **overrides):
    books = {
        "enabled": True,
        "library_path": str(tmp_path / "books"),
        "metadata_provider": "openlibrary",
        "allow_direct_urls": True,
        "allow_private_source_urls": True,
        "max_download_mb": 10,
    }
    books.update(overrides)
    return {"books": books}


def test_search_open_library_normalizes_rich_metadata(monkeypatch):
    class Response:
        def raise_for_status(self):
            return None

        def json(self):
            return {
                "numFound": 1,
                "docs": [
                    {
                        "key": "/works/OL1W",
                        "title": "Example Book",
                        "author_name": ["A. Writer"],
                        "first_publish_year": 2024,
                        "publisher": ["Example Press"],
                        "subject": ["Testing", "Software"],
                        "language": ["eng"],
                        "isbn": ["9780000000001"],
                        "cover_i": 42,
                        "edition_key": ["OL1M"],
                        "ebook_access": "public",
                    }
                ],
            }

    monkeypatch.setattr(book_services.requests, "get", lambda *args, **kwargs: Response())
    result = book_services.search_open_library("Example", limit=10)

    assert result["total"] == 1
    row = result["results"][0]
    assert row["work_id"] == "OL1W"
    assert row["authors"] == ["A. Writer"]
    assert row["public_readable"] is True
    assert row["cover_url"].endswith("/b/id/42-M.jpg?default=false")


def test_import_book_writes_deterministic_sidecar_and_library_record(tmp_path):
    source = tmp_path / "incoming.txt"
    source.write_text("book body", encoding="utf-8")
    result = book_services.import_book_file(
        _config(tmp_path),
        source,
        {
            "title": "A Useful Book",
            "authors": ["Ada Example"],
            "isbn": ["9780000000002"],
            "subjects": ["Reference"],
            "work_id": "OL2W",
        },
    )

    final_path = tmp_path / "books" / "Ada Example" / "A Useful Book.txt"
    assert result["path"] == str(final_path)
    sidecar = json.loads((final_path.with_suffix(".txt.metadata.json")).read_text(encoding="utf-8"))
    assert sidecar["schema_version"] == 1
    assert sidecar["openlibrary_work_id"] == "OL2W"
    assert sidecar["isbn"] == ["9780000000002"]
    library = book_services.list_book_library(_config(tmp_path))
    assert library["count"] == 1
    assert library["results"][0]["title"] == "A Useful Book"


def test_epub_metadata_embed_preserves_package_unique_identifier(tmp_path):
    source = tmp_path / "incoming.epub"
    with zipfile.ZipFile(source, "w") as archive:
        archive.writestr("mimetype", "application/epub+zip", compress_type=zipfile.ZIP_STORED)
        archive.writestr(
            "META-INF/container.xml",
            '<?xml version="1.0"?>'
            '<container xmlns="urn:oasis:names:tc:opendocument:xmlns:container">'
            '<rootfiles><rootfile full-path="OEBPS/content.opf" '
            'media-type="application/oebps-package+xml"/></rootfiles></container>',
        )
        archive.writestr(
            "OEBPS/content.opf",
            '<?xml version="1.0"?>'
            '<package xmlns="http://www.idpf.org/2007/opf" unique-identifier="book-id">'
            '<metadata xmlns:dc="http://purl.org/dc/elements/1.1/">'
            '<dc:identifier id="book-id">original-id</dc:identifier>'
            '<dc:title>Old</dc:title></metadata></package>',
        )

    result = book_services.import_book_file(
        _config(tmp_path), source, {"title": "New Title", "authors": ["E. Pub"], "isbn": ["123"]}
    )
    with zipfile.ZipFile(result["path"], "r") as archive:
        package = archive.read("OEBPS/content.opf").decode("utf-8")
    assert "original-id" in package
    assert "New Title" in package
    assert "isbn:123" in package


def test_pdf_metadata_is_embedded_and_sidecar_remains_authoritative(tmp_path):
    pypdf = pytest.importorskip("pypdf")
    source = tmp_path / "incoming.pdf"
    writer = pypdf.PdfWriter()
    writer.add_blank_page(width=300, height=400)
    with source.open("wb") as handle:
        writer.write(handle)

    result = book_services.import_book_file(
        _config(tmp_path),
        source,
        {"title": "PDF Title", "authors": ["P. Writer"], "isbn": ["978123"]},
    )

    reader = pypdf.PdfReader(result["path"])
    assert reader.metadata.title == "PDF Title"
    assert reader.metadata.author == "P. Writer"
    assert reader.metadata.get("/ISBN") == "978123"
    sidecar = json.loads(Path(result["sidecar"]).read_text(encoding="utf-8"))
    assert sidecar["metadata_embedded"] is True


def test_direct_url_blocks_private_hosts_by_default(tmp_path):
    with pytest.raises(book_services.BookServiceError, match="Private, loopback"):
        book_services.acquire_book_url(
            _config(tmp_path, allow_private_source_urls=False),
            "http://127.0.0.1/book.pdf",
            {"title": "Blocked"},
        )


def test_direct_url_rejects_unsupported_content(monkeypatch, tmp_path):
    class Response:
        status_code = 200
        url = "https://example.test/file.bin"
        headers = {"Content-Type": "application/octet-stream"}

        def raise_for_status(self):
            return None

        def iter_content(self, chunk_size):
            yield b"data"

    monkeypatch.setattr(book_services.requests, "get", lambda *args, **kwargs: Response())
    with pytest.raises(book_services.BookServiceError, match="supported PDF or ebook"):
        book_services.acquire_book_url(_config(tmp_path), "https://example.test/file.bin", {"title": "Nope"})
