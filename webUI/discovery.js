/* Discovery is an additive Music view. All remote text is inserted as textContent. */
(() => {
  'use strict';
  const $ = (id) => document.getElementById(id);
  let selected = '', playlist = null, refreshing = false, playlistRequest = 0;
  const base = '/api/music/discovery';
  function element(tag, text, cls) {
    const node = document.createElement(tag);
    if (text !== undefined) node.textContent = text;
    if (cls) node.className = cls;
    return node;
  }
  function message(text) { $('discovery-message').textContent = text; }
  async function api(path, payload, method = 'POST') {
    const options = {method};
    if (payload instanceof FormData) options.body = payload;
    else if (payload !== undefined) { options.headers = {'Content-Type': 'application/json'}; options.body = JSON.stringify(payload); }
    const response = await fetch(base + path, options);
    const data = await response.json();
    if (!response.ok) throw new Error(typeof data.detail === 'string' ? data.detail : 'Request failed');
    return data;
  }
  function button(text, action) {
    const node = element('button', text, 'button ghost small'); node.type = 'button';
    node.addEventListener('click', async () => {
      node.disabled = true;
      try { await action(); } catch (error) { message(error.message); }
      finally { node.disabled = false; }
    });
    return node;
  }
  const displayState = (value) => ({resolved: 'High confidence', probable: 'Probable — review required', ambiguous: 'Ambiguous — choose a recording', unresolved: 'Unresolved', existing: 'Existing locally', pending: 'Awaiting resolution'}[value] || value);
  const summaryText = (counts) => Object.entries(counts).map(([name, count]) => `${name.replaceAll('_', ' ')}: ${count}`).join(' · ');
  async function openPlaylist(key) {
    const request = ++playlistRequest;
    selected = key; $('discovery-playlists').value = key;
    if (!key) { playlist = null; $('discovery-playlist-detail').replaceChildren(); return; }
    const loaded = await api('/playlists/' + encodeURIComponent(key), undefined, 'GET');
    if (request !== playlistRequest) return;
    playlist = loaded;
    const host = $('discovery-playlist-detail'); host.replaceChildren();
    host.append(element('h4', playlist.name), element('p', `${playlist.tracks.length} tracks · ${summaryText(playlist.summary)}`, 'meta'));
    const actions = element('div', undefined, 'row compact');
    const selectedPositions = new Set();
    const endpoint = '/playlists/' + encodeURIComponent(key);
    actions.append(button('Resolve identities', async () => { await api(endpoint + '/resolve', {}); message('Resolution queued. Progress is saved as tracks are matched.'); await refresh(); }),
      button('Acquire resolved missing tracks', async () => { const result = await api(endpoint + '/acquire', {}); message(summaryText(result)); await openPlaylist(key); }),
      button('Acquire selected tracks', async () => { if (!selectedPositions.size) throw new Error('Select tracks first.'); const result = await api(endpoint + '/acquire', {positions: [...selectedPositions]}); message(summaryText(result)); await openPlaylist(key); }),
      button('Test Jellyfin', async () => { const result = await api('/jellyfin/test', {}); message(`Connected to Jellyfin ${result.version || ''}`); }),
      button('Preview Jellyfin matches', async () => { await jellyfin(endpoint, true, host); }),
      button('Create / refresh Jellyfin playlist', async () => { await jellyfin(endpoint, false, host); }),
      button('Resolve sources without downloading', async () => { const result = await api(endpoint + '/builder', {}); message(`${result.seeded} track identities submitted to the cache builder.`); await refresh(); }));
    host.append(actions);
    const list = element('div', undefined, 'discovery-track-list');
    // Render bounded pages; large imports should never create thousands of interactive controls.
    let offset = 0;
    function page() {
      const rows = playlist.tracks.slice(offset, offset + 100); offset += rows.length;
      rows.forEach((track) => {
        const row = element('div', undefined, 'discovery-row');
        if (track.identity && track.state !== 'existing' && !track.job_id) {
          const choice = element('input'); choice.type = 'checkbox'; choice.setAttribute('aria-label', `Select track ${track.position + 1}`);
          choice.addEventListener('change', () => { if (choice.checked) selectedPositions.add(track.position); else selectedPositions.delete(track.position); });
          row.append(choice);
        }
        row.append(element('span', `${track.position + 1}. ${track.intent.artist || 'Unknown artist'} — ${track.intent.title || 'Untitled'}`),
          element('span', track.job_id ? 'Submitted to acquisition' : displayState(track.state), 'meta'));
        if (!track.identity && track.candidates?.length) {
          const select = element('select'); select.setAttribute('aria-label', `Recording for track ${track.position + 1}`);
          select.append(element('option', 'Choose a recording after review'));
          select.options[0].value = '';
          track.candidates.forEach((candidate) => {
            const option = element('option', `${candidate.artist} — ${candidate.title} · ${candidate.album || 'Unknown album'} · ${Math.round(candidate.score * 100)}% · ${candidate.recording_mbid}`);
            option.value = candidate.recording_mbid; select.append(option);
          });
          row.append(select, button('Use selected recording', async () => {
            if (!select.value) return;
            await api(endpoint + '/choose', {position: track.position, recording_mbid: select.value}); await openPlaylist(key);
          }));
        }
        list.append(row);
      });
      more.hidden = offset >= playlist.tracks.length;
    }
    const more = button('Show next 100 tracks', async () => page());
    host.append(list, more); page();
  }
  async function jellyfin(endpoint, preview, host) {
    const result = await api(endpoint + '/jellyfin', {preview});
    message(`${preview ? 'Jellyfin preview' : 'Jellyfin synchronized'}: ${result.matched} matched · ${result.unmatched} unmatched · ${result.ambiguous} ambiguous${result.source_repetitions ? ' · ' + result.source_repetitions + ' source repetitions retained only in Retreivr (Jellyfin deduplicates)' : ''}`);
    host.querySelector('.discovery-jellyfin-report')?.remove();
    const report = element('details', undefined, 'discovery-jellyfin-report');
    report.append(element('summary', 'Jellyfin track report'));
    result.tracks.forEach((row) => report.append(element('div', `${row.position + 1}. ${row.title || 'Untitled'} — ${row.state}`, 'meta')));
    host.append(report);
  }
  function policy() {
    return {types: [['albums', 'Album'], ['eps', 'EP'], ['singles', 'Single']].filter(([id]) => $('discovery-' + id).checked).map(([, value]) => value),
      secondary_types: [['live', 'Live'], ['compilations', 'Compilation']].filter(([id]) => $('discovery-' + id).checked).map(([, value]) => value), max_releases: 25};
  }
  async function refresh() {
    if (refreshing) return;
    refreshing = true;
    try {
      const data = await api('/state', undefined, 'GET');
      const select = $('discovery-playlists'); select.replaceChildren(element('option', 'Choose a playlist')); select.options[0].value = '';
      data.playlists.forEach((row) => { const option = element('option', row.name); option.value = row.id; select.append(option); }); select.value = selected;
      const subscriptions = $('discovery-subscriptions'); subscriptions.replaceChildren();
      if (!data.subscriptions.length) subscriptions.append(element('p', 'No artist subscriptions yet.', 'meta'));
      data.subscriptions.forEach((artist) => {
        const row = element('div', undefined, 'discovery-row');
        row.append(element('strong', artist.name), element('span', `${artist.enabled ? 'Enabled' : 'Disabled'} · Last checked: ${artist.last_checked ? new Date(artist.last_checked * 1000).toLocaleString() : 'Never'}${artist.error ? ' · ' + artist.error : ''}`, 'meta'));
        const url = '/subscriptions/' + artist.artist_mbid;
        row.append(button('Refresh releases', async () => { await api(url + '/refresh', {}); message('Artist refresh queued.'); await refresh(); }),
          button(artist.enabled ? 'Disable' : 'Enable', async () => { await api(url, {enabled: !artist.enabled}); await refresh(); }),
          button('Remove subscription', async () => { await api(url, undefined, 'DELETE'); await refresh(); }));
        const releases = element('details'); releases.append(element('summary', `${artist.releases.length} discovered releases`));
        const choices = [];
        const previewSelected = async (all) => {
          const groups = choices.filter((choice) => all || choice.input.checked).map((choice) => choice.id);
          if (!groups.length) throw new Error('Select at least one release.');
          await api(url + '/preview', {release_group_mbids: groups});
          message('Release preview queued. Open the completed preview in the activity list below, then acquire selected tracks or all missing tracks.');
          await refresh();
        };
        releases.append(button('Preview selected releases', async () => previewSelected(false)), button('Preview all releases (up to 25)', async () => previewSelected(true)));
        artist.releases.forEach((release) => {
          const line = element('div', undefined, 'row compact');
          const choice = element('input'); choice.type = 'checkbox'; choice.setAttribute('aria-label', 'Select ' + release.payload.title);
          choices.push({id: release.release_group_mbid, input: choice});
          line.append(choice, element('span', `${release.payload.title} · ${release.payload['first-release-date'] || ''}`), button('Preview tracks', async () => {
            const result = await api(url + '/releases/' + release.release_group_mbid, {}); selected = result.id; await refresh(); await openPlaylist(result.id);
          })); releases.append(line);
        }); row.append(releases); subscriptions.append(row);
      });
      $('discovery-stats').textContent = [...data.metrics.map((r) => `${r.name.replaceAll('_', ' ')}: ${r.value}`),
        ...data.cache.map((r) => `${r.count} ${r.status} sources · mean confidence ${Math.round(r.confidence * 100)}%`),
        ...data.job_counts.map((r) => `${r.kind} ${r.state}: ${r.count}`)].join(' · ') || 'No cache-builder activity yet.';
      const jobs = $('discovery-jobs'); jobs.replaceChildren();
      data.jobs.slice(0, 20).forEach((job) => {
        const row = element('div', undefined, 'row compact');
        row.append(element('span', `${job.kind} · ${job.state} · attempts ${job.attempts}${job.error ? ' · ' + job.error : ''} · ${new Date(job.updated_at * 1000).toLocaleString()}`, 'meta'));
        const result = job.result ? JSON.parse(job.result) : {};
        if (result.playlist_id) row.append(button('Open release preview', async () => { await openPlaylist(result.playlist_id); $('discovery-playlist-detail').scrollIntoView({behavior: 'smooth'}); }));
        jobs.append(row);
      });
    } catch (error) { message(error.message); } finally { refreshing = false; }
  }
  document.addEventListener('DOMContentLoaded', () => {
    const bind = (id, action) => $(id)?.addEventListener('click', async (event) => {
      const target = event.currentTarget; target.disabled = true;
      try { await action(); } catch (error) { message(error.message); } finally { target.disabled = false; }
    });
    bind('discovery-upload', async () => {
      const file = $('discovery-file').files[0]; if (!file) throw new Error('Choose a playlist file first.');
      const form = new FormData(); form.append('file', file);
      const result = await api('/playlists', form); selected = result.playlists[0]?.id || ''; await refresh(); await openPlaylist(selected);
      message(`${result.playlists.length} playlists saved for review. Resolve identities when ready.`);
    });
    bind('discovery-refresh', async () => { await refresh(); if (selected) await openPlaylist(selected); });
    $('discovery-playlists')?.addEventListener('change', (event) => openPlaylist(event.target.value).catch((error) => message(error.message)));
    bind('discovery-preview', async () => {
      const source = $('discovery-provider').value, query = $('discovery-query').value.trim();
      const result = await api(source === 'artist' ? '/artists/search' : '/preview', {provider: source, query});
      const host = $('discovery-artists'); host.replaceChildren();
      result.artists.forEach((artist) => {
        const row = element('div', undefined, 'discovery-row');
        const id = artist.artist_mbid || artist.id;
        row.append(element('span', `${artist.name}${artist.disambiguation ? ' — ' + artist.disambiguation : ''}`), element('span', id, 'meta'),
          button('Subscribe', async () => { await api('/subscriptions', {artist_mbid: id, name: artist.name, policy: policy()}); message(`Subscribed to ${artist.name}. Refresh releases to preview the discography.`); await refresh(); })); host.append(row);
      });
      if (!result.artists.length) host.append(element('p', 'No artists found.', 'meta'));
    });
    // Poll only this visible view; do not rebuild focused review controls in the background.
    setInterval(() => { if (!document.hidden && !$('music-discovery-view')?.classList.contains('hidden') && !$('discovery-subscriptions')?.querySelector('details[open]')) refresh(); }, 15000);
  });
  window.retreivrDiscovery = {refresh};
})();
