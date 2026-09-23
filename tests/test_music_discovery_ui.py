"""Static DOM wiring checks; live visual validation is documented separately."""
from html.parser import HTMLParser
from pathlib import Path
import re

ROOT = Path(__file__).resolve().parents[1]


class Elements(HTMLParser):
    def __init__(self):
        super().__init__()
        self.ids = []
        self.scripts = []
        self.nav = []

    def handle_starttag(self, tag, attrs):
        attrs = dict(attrs)
        if attrs.get('id'):
            self.ids.append(attrs['id'])
        if tag == 'script':
            self.scripts.append(attrs.get('src', ''))
        if 'data-music-section' in attrs:
            self.nav.append(attrs['data-music-section'])


def test_discovery_controls_have_unique_dom_targets():
    dom = Elements()
    dom.feed((ROOT/'webUI/index.html').read_text())
    script = (ROOT/'webUI/discovery.js').read_text()
    ids = [item for item in dom.ids if item.startswith('discovery-')]
    assert len(ids) == len(set(ids))
    for target in re.findall(r"\$\('(discovery-[\w-]+)'\)", script):
        assert target in ids
    assert 'discovery' in dom.nav
    assert any(s.startswith('discovery.js') for s in dom.scripts)
    assert 'innerHTML' not in script
    assert "node.textContent = text" in script
    assert 'aria-live="polite"' in (ROOT/'webUI/index.html').read_text()


def test_jellyfin_owner_library_fields_load_and_save():
    dom = Elements()
    dom.feed((ROOT/'webUI/index.html').read_text())
    source = (ROOT/'webUI/app.js').read_text()
    for name in ('user', 'library'):
        assert f'cfg-arr-jellyfin-{name}-id' in dom.ids
        assert f'arrJellyfin.{name}_id' in source
        assert f'arr.jellyfin.{name}_id =' in source
