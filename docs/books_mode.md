# Books Mode Contract

Books is an optional, first-class Retreivr media mode. It is hidden until
`books.enabled` is turned on in Settings. Enabling the UI is independent from
configuring Readarr or any future acquisition provider.

## Product flow

1. **Discover** — Search Open Library by title, author, subject, or ISBN.
2. **Inspect** — Cards show cover, title, authors, first publication year,
   subjects, edition count, and provider access state.
3. **Acquire** — Download a verified public Open Library/Internet Archive scan
   in one click, import a local DRM-free file, or provide a direct downloadable
   URL that the user is authorized to access.
4. **Finalize** — Store the book under `Author/Title.ext`, embed metadata into
   PDF/EPUB when the format permits it, and always write `Title.ext.metadata.json`.
5. **Browse** — Rescan the configured books root and display a local library
   using the same card vocabulary as Movies & TV.
6. **Read/export** — Open the local file in the browser/OS. Apple Books and
   Kindle are reader destinations for compatible user-owned files, not
   acquisition or DRM-removal providers.

## Source policy

Discovery and acquisition are separate contracts:

- **Open Library** is the default discovery/cover provider. Public scan results
  expose their Internet Archive identifiers. Retreivr verifies the selected
  archive item is unrestricted, title-matched, and has a public EPUB/PDF before
  showing and executing the one-click download path. Other availability hints
  remain `Read / Preview` actions and are not treated as authorization.
- **Local import** accepts PDF, EPUB, MOBI, AZW, AZW3, and TXT.
- **Direct URL** accepts only HTTP(S) responses with a supported file extension
  or content type. Redirect targets are revalidated, private/link-local hosts
  are blocked, and streamed size is capped by `books.max_download_mb`.
- **Readarr** is a legacy optional adapter. It cannot be the required metadata
  or acquisition path because the upstream project is retired.
- **Project Gutenberg / OPDS** is the preferred next public-domain adapter. It
  should use the official catalog/OPDS feed, respect request guidance, retain
  the ebook license, and never scrape Gutenberg HTML pages.
- Retreivr does not scrape Kindle or Apple Books stores and does not bypass DRM.

## Canonical metadata schema (`book/v1`)

Every finalized file has a deterministic sidecar containing:

- title and subtitle
- ordered authors
- publisher and published date
- description and subjects
- languages
- ISBN values
- Open Library work and edition identifiers
- cover URL
- source URL and source provider
- final format, filename, byte size, and whether in-file embedding succeeded

PDF embedding uses document information keys. EPUB embedding updates Dublin
Core title, creator, publisher, date, subject, language, and Retreivr identifiers
without removing the package's original unique identifier. MOBI/AZW/TXT retain
the full sidecar even when safe in-file rewriting is unavailable.

## API

- `GET /api/books/status`
- `GET /api/books/search?q=...&limit=24&page=1`
- `GET /api/books/library`
- `GET /api/books/library/{book_id}/file`
- `POST /api/books/acquire/url`
- `POST /api/books/acquire/openlibrary`
- `POST /api/books/import` (multipart file plus `metadata_json`)

All search, library, and acquisition endpoints except status fail closed while
Books is disabled.

## Next adapters

Provider adapters should return the same normalized discovery record and one of
three explicit actions: `preview`, `download`, or `send_to_manager`. A result
must never infer `download` from `has_fulltext` alone. Suggested order:

1. Project Gutenberg OPDS for lawful public-domain EPUB/Kindle downloads.
2. Calibre/Calibre-Web library sync and OPDS export.
3. A maintained `send_to_manager` adapter if a viable Readarr successor emerges.
4. Optional Send to Kindle / Apple Books export helpers operating only on local,
   user-owned files.
