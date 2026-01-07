# Contributing
Thanks for taking the time to contribute.

## Quick guidelines
- Keep changes focused and minimal; avoid large refactors unless discussed first.
- Do not commit secrets, tokens, logs, or local config files.
- Keep config.json semantics stable unless explicitly agreed.
- Use Python 3.11 for local development.

## Ways to help
- Bug reports with clear reproduction steps and logs.
- Small fixes (UI polish, docs clarity, error handling).
- Improvements to Docker docs and deployment examples.

## Development notes
- Install deps: `pip install -r requirements.txt`
- Run the API: `uvicorn api.main:app --host 127.0.0.1 --port 8000`
- Open the UI at `http://127.0.0.1:8000`

## Submitting a PR
- Describe the problem and why the change is needed.
- Keep diffs readable and avoid unnecessary formatting changes.
- Update docs if behavior or UX changes.

## Code style
- Prefer explicit and readable code over cleverness.
- Keep comments short and only where they add clarity.
