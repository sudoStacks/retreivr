from db.downloaded_tracks import has_downloaded_isrc, record_downloaded_track


def test_record_downloaded_track_and_lookup(tmp_path, monkeypatch) -> None:
    db_path = tmp_path / "downloaded_tracks.sqlite"
    monkeypatch.setenv("RETREIVR_DB_PATH", str(db_path))

    record_downloaded_track(
        playlist_id="playlist-a",
        isrc="USABC1234567",
        file_path="/music/playlist-a/01 - Track.mp3",
    )

    assert has_downloaded_isrc("playlist-a", "USABC1234567") is True


def test_has_downloaded_isrc_false_for_other_playlist_or_isrc(tmp_path, monkeypatch) -> None:
    db_path = tmp_path / "downloaded_tracks.sqlite"
    monkeypatch.setenv("RETREIVR_DB_PATH", str(db_path))

    record_downloaded_track(
        playlist_id="playlist-a",
        isrc="USABC1234567",
        file_path="/music/playlist-a/01 - Track.mp3",
    )

    assert has_downloaded_isrc("playlist-b", "USABC1234567") is False
    assert has_downloaded_isrc("playlist-a", "USZZZ9999999") is False

