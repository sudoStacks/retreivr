from fastapi.testclient import TestClient


def test_video_search_preview_is_bounded_and_queue_free(monkeypatch):
    import api.main as api_module
    monkeypatch.setattr(api_module, "youtube_fast_search", lambda query, limit: [
        {"video_id": "abc123", "title": "Moon files", "channel": "Example", "url": "https://youtube.com/watch?v=abc123", "thumbnail_url": "https://img.example/abc.jpg", "posted_label": "2 days ago"},
    ])
    response = TestClient(api_module.app).post("/api/video/search/preview", json={"query": "why files moon", "limit": 99})
    assert response.status_code == 200
    body = response.json()
    assert body["query"] == "why files moon"
    assert body["candidates"][0]["video_id"] == "abc123"
    assert "request_id" not in body


def test_video_search_preview_rejects_blank_query():
    import api.main as api_module
    assert TestClient(api_module.app).post("/api/video/search/preview", json={"query": " "}).status_code == 400
