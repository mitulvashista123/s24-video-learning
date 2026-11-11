// S24 Learning Console: a dependency-free UI over the content-delivery-api.
// Served by the API itself at /ui, so every call is same-origin (no CORS, no base URL to configure).
'use strict';

const $ = (sel, root = document) => root.querySelector(sel);
const $$ = (sel, root = document) => [...root.querySelectorAll(sel)];

// ------------------------------------------------------------------ storage (per browser, best effort)
const store = {
  get(key, fallback) {
    try { const v = localStorage.getItem(key); return v === null ? fallback : JSON.parse(v); } catch { return fallback; }
  },
  set(key, value) { try { localStorage.setItem(key, JSON.stringify(value)); } catch { /* private mode */ } },
};

const state = {
  users: store.get('s24.users', []),          // [{id, name, role, email}]
  userId: store.get('s24.userId', null),
  courseId: null,
  course: null,
  lesson: null,                                // last GET /api/lessons/{id} response
  positions: store.get('s24.positions', {}),  // lessonId -> seconds watched (for progress events)
};

// ------------------------------------------------------------------ DOM helper
function h(tag, attrs = {}, ...children) {
  const el = document.createElement(tag);
  for (const [k, v] of Object.entries(attrs || {})) {
    if (v === null || v === undefined || v === false) continue;
    if (k === 'class') el.className = v;
    else if (k.startsWith('on')) el.addEventListener(k.slice(2), v);
    else el.setAttribute(k, v === true ? '' : v);
  }
  for (const c of children.flat()) {
    if (c === null || c === undefined || c === false) continue;
    el.append(c instanceof Node ? c : document.createTextNode(String(c)));
  }
  return el;
}
const fmtSec = s => { s = Math.round(Number(s) || 0); return s >= 60 ? `${Math.floor(s / 60)} min ${s % 60 ? (s % 60) + ' s' : ''}`.trim() : `${s} s`; };
const pct = v => `${(Number(v) * 100).toFixed(1)}%`;

// ------------------------------------------------------------------ toast
let toastTimer;
function toast(msg, isError = false) {
  const t = $('#toast');
  t.textContent = msg;
  t.className = 'toast' + (isError ? ' error' : '');
  t.hidden = false;
  clearTimeout(toastTimer);
  toastTimer = setTimeout(() => { t.hidden = true; }, isError ? 6000 : 3000);
}

// ------------------------------------------------------------------ API client + call log
class ApiError extends Error {
  constructor(message, status, data) { super(message); this.status = status; this.data = data; }
}
function errorText(status, data) {
  const d = data && data.detail;
  if (Array.isArray(d)) return d.map(x => `${(x.loc || []).slice(1).join('.') || 'request'}: ${x.msg}`).join('; ');
  if (typeof d === 'string') return `${status}: ${d}`;
  return `HTTP ${status}`;
}
function logCall(method, path, status, ms) {
  const ok = typeof status === 'number' && status < 400;
  const li = h('li', {},
    h('span', { class: 'st ' + (ok ? 'ok' : 'bad') }, String(status)),
    h('span', { class: 'path' }, `${method} ${path}`),
    h('span', { class: 'muted' }, `${ms} ms`));
  const log = $('#log');
  log.prepend(li);
  while (log.children.length > 60) log.lastChild.remove();
}
async function api(method, path, { body, form } = {}) {
  const headers = {};
  if (state.userId) headers['X-User-Id'] = state.userId;
  let payload;
  if (form) payload = form;
  else if (body !== undefined) { headers['Content-Type'] = 'application/json'; payload = JSON.stringify(body); }
  const t0 = performance.now();
  let res;
  try {
    res = await fetch(path, { method, headers, body: payload });
  } catch (e) {
    logCall(method, path, 'ERR', Math.round(performance.now() - t0));
    throw new ApiError(`Network error: ${e.message}`, 0, null);
  }
  const ms = Math.round(performance.now() - t0);
  const text = await res.text();
  let data = null;
  try { data = text ? JSON.parse(text) : null; } catch { data = text; }
  logCall(method, path, res.status, ms);
  if (!res.ok) throw new ApiError(errorText(res.status, data), res.status, data);
  return data;
}
// Runs an async action with its button disabled; reports failures as a toast.
async function run(btn, fn) {
  if (btn) btn.disabled = true;
  try { return await fn(); }
  catch (e) {
    const hint = e.status === 403 ? ' (this user’s role is not allowed to do that)' : e.status === 401 ? ' (pick a user first)' : '';
    toast(e.message + hint, true);
  } finally { if (btn) btn.disabled = false; }
}

// ------------------------------------------------------------------ cache badges
function badge(meta) {
  if (!meta) return h('span', { class: 'pill' }, 'n/a');
  const c = String(meta.cache || 'N/A').toUpperCase();
  return h('span', { class: 'pill ' + (c === 'HIT' ? 'hit' : c === 'MISS' ? 'miss' : '') }, c);
}
function cacheCell(name, meta) {
  const parts = [];
  if (meta && meta.owner) parts.push(h('div', {}, 'owner ', h('code', {}, meta.owner)));
  if (meta && meta.served_by) parts.push(h('div', {}, 'served by ', h('code', {}, meta.served_by)));
  if (meta && meta.age_ms !== undefined) parts.push(h('div', {}, `age ${meta.age_ms} ms`));
  if (meta && meta.reason && !meta.owner) parts.push(h('div', { class: 'muted' }, meta.reason));
  return h('div', { class: 'cache-cell' }, h('b', {}, name, ' ', badge(meta)), parts);
}

// ------------------------------------------------------------------ users
function currentUser() { return state.users.find(u => u.id === state.userId) || null; }
function saveUsers() { store.set('s24.users', state.users); store.set('s24.userId', state.userId); }
function rememberUser(u) {
  const i = state.users.findIndex(x => x.id === u.id);
  const entry = { id: u.id, name: u.name, role: u.role, email: u.email };
  if (i >= 0) state.users[i] = entry; else state.users.push(entry);
  state.userId = u.id;
  saveUsers();
  renderUsers();
}
function renderUsers() {
  const sel = $('#user-select');
  sel.replaceChildren(h('option', { value: '' }, 'anonymous (no X-User-Id)'),
    ...state.users.map(u => h('option', { value: u.id, selected: u.id === state.userId }, `${u.name} · ${u.role}`)));
  const list = $('#user-list');
  list.replaceChildren(...(state.users.length ? state.users.map(u => h('li', { class: u.id === state.userId ? 'selected' : '' },
    h('div', {}, h('div', {}, `${u.name} · ${u.role}`), h('div', { class: 'meta' }, u.id)),
    h('div', { class: 'row' },
      h('button', { class: 'btn ghost small', onclick: () => { state.userId = u.id; saveUsers(); renderUsers(); refreshForUser(); } }, 'Act as'),
      h('button', { class: 'btn ghost small', onclick: () => { state.users = state.users.filter(x => x.id !== u.id); if (state.userId === u.id) state.userId = null; saveUsers(); renderUsers(); } }, 'Forget'))))
    : [h('li', { class: 'empty' }, 'No users yet: create one on the left.')]));
  const u = currentUser();
  const instructor = u && (u.role === 'instructor' || u.role === 'admin');
  $('#course-create-box').hidden = !instructor;
  $('#lesson-create-box').hidden = !instructor || !state.courseId;
  $$('#lesson-card details.sub').forEach(d => { d.hidden = !instructor; });
}
$('#user-select').addEventListener('change', e => { state.userId = e.target.value || null; saveUsers(); renderUsers(); refreshForUser(); });
$('#user-form').addEventListener('submit', e => {
  e.preventDefault();
  const f = new FormData(e.target);
  run(e.submitter, async () => {
    const u = await api('POST', '/api/users', { body: { name: f.get('name'), email: f.get('email'), role: f.get('role') } });
    rememberUser(u);
    e.target.reset();
    toast(`Created ${u.name} (${u.role}); now acting as them.`);
  });
});
$('#user-add-form').addEventListener('submit', e => {
  e.preventDefault();
  const id = new FormData(e.target).get('id').trim();
  run(e.submitter, async () => {
    const prev = state.userId;
    state.userId = id;                      // read your own record as yourself
    try { rememberUser(await api('GET', `/api/users/${encodeURIComponent(id)}`)); e.target.reset(); }
    catch (err) { state.userId = prev; throw err; }
  });
});
function refreshForUser() {
  if (state.lesson) loadLesson(state.lesson.lesson.id);
  if (!$('#tab-learning').hidden) loadLearning();
}

// ------------------------------------------------------------------ courses
async function loadCourses() {
  const data = await api('GET', '/api/courses');
  $('#courses-cache').replaceChildren(badge(data.cache));
  const items = data.items || [];
  $('#course-list').replaceChildren(...(items.length ? items.map(c => h('li', { class: c.id === state.courseId ? 'selected' : '' },
    h('div', {}, h('button', { class: 'link', onclick: () => run(null, () => openCourse(c.id)) }, c.title),
      h('div', { class: 'meta' }, `${c.lesson_count} lessons · ${fmtSec(c.total_duration_seconds)} · ${c.level}`))))
    : [h('li', { class: 'empty' }, 'No courses yet.')]));
}
async function openCourse(id) {
  const data = await api('GET', `/api/courses/${id}`);
  state.courseId = id;
  state.course = data.course;
  $('#course-title').textContent = data.course.title;
  $('#course-desc').textContent = data.course.description || '';
  $('#course-cache').replaceChildren(badge(data.cache));
  const lessons = data.course.lessons || [];
  $('#lesson-list').replaceChildren(...(lessons.length ? lessons.map(l => h('li', { class: state.lesson && state.lesson.lesson.id === l.id ? 'selected' : '' },
    h('div', {}, h('button', { class: 'link', onclick: () => run(null, () => loadLesson(l.id)) }, l.title),
      h('div', { class: 'meta' }, fmtSec(l.duration_seconds)))))
    : [h('li', { class: 'empty' }, 'No lessons yet.')]));
  $$('#course-list li').forEach(li => li.classList.toggle('selected', li.textContent.startsWith(data.course.title)));
  renderUsers();
}
$('#courses-refresh').addEventListener('click', e => run(e.currentTarget, loadCourses));
$('#course-form').addEventListener('submit', e => {
  e.preventDefault();
  const f = new FormData(e.target);
  run(e.submitter, async () => {
    const c = await api('POST', '/api/courses', { body: Object.fromEntries(f) });
    e.target.reset();
    toast(`Course “${c.title}” created.`);
    await loadCourses();
    await openCourse(c.id);
  });
});
$('#lesson-form').addEventListener('submit', e => {
  e.preventDefault();
  const f = new FormData(e.target);
  run(e.submitter, async () => {
    const l = await api('POST', `/api/courses/${state.courseId}/lessons`, {
      body: { title: f.get('title'), summary: f.get('summary'), duration_seconds: Number(f.get('duration_seconds')) || 0 },
    });
    e.target.reset();
    toast(`Lesson “${l.title}” added.`);
    await openCourse(state.courseId);
    await loadLesson(l.id);
  });
});

// ------------------------------------------------------------------ lesson
async function loadLesson(id) {
  const data = await api('GET', `/api/lessons/${id}`);
  state.lesson = data;
  const l = data.lesson;
  $('#lesson-card').hidden = false;
  $('#lesson-title').textContent = l.title;
  $('#lesson-summary').textContent = `${l.summary ? l.summary + ' · ' : ''}${fmtSec(l.duration_seconds)}`;
  $('#lesson-cache').replaceChildren(cacheCell('Lesson', data.cache.lesson), cacheCell('Video metadata', data.cache.video), cacheCell('Stream URL', data.cache.stream));
  $('#lesson-lookup-out').hidden = true;

  const v = data.video;
  const box = $('#video-box');
  if (data.stream && data.stream.video_url) {
    box.replaceChildren(h('video', { controls: true, preload: 'metadata', src: data.stream.video_url }),
      h('p', { class: 'small muted' }, `${v.title} · presigned S3 URL, valid ${fmtSec(data.stream.expires_in)}`));
  } else {
    box.replaceChildren(h('div', { class: 'note' }, v ? `“${v.title}”: status ${v.status}. Upload a file to make it playable.` : 'No video yet. Uploading creates the video record from the lesson.'));
  }

  const p = data.progress;
  $('#progress-box').replaceChildren(p
    ? h('p', {}, `${Math.round(Number(p.percent_complete) || 0)}% · watched ${fmtSec(p.watch_seconds)} · ${p.event_count} events${p.completed ? ' · completed' : ''}`)
    : h('p', { class: 'muted' }, state.userId ? 'No progress yet for this user.' : 'Pick a user to track progress.'));

  const ef = $('#lesson-edit-form').elements;
  ef.namedItem('title').value = l.title;
  ef.namedItem('summary').value = l.summary || '';
  const vf = $('#video-form').elements;
  vf.namedItem('title').value = v ? v.title : l.title;
  vf.namedItem('duration_seconds').value = v ? v.duration_seconds : l.duration_seconds || 600;
  $('#analytics-out').replaceChildren();
  $$('#lesson-list li').forEach(li => li.classList.toggle('selected', li.textContent.startsWith(l.title)));
  renderUsers();
}
$('#lesson-read').addEventListener('click', e => run(e.currentTarget, () => loadLesson(state.lesson.lesson.id)));
$('#lesson-lookup').addEventListener('click', e => run(e.currentTarget, async () => {
  const key = `lesson:${state.lesson.lesson.id}`;
  const r = await api('GET', `/api/cache/lookup?key=${encodeURIComponent(key)}`);
  const out = $('#lesson-lookup-out');
  out.replaceChildren(lookupText(r));
  out.hidden = false;
}));
function lookupText(r) {
  return h('span', {}, 'Key ', h('code', {}, r.key), ` hashes to ${Number(r.key_position_pct).toFixed(2)}% around the ring; the next virtual node clockwise is `,
    h('code', {}, r.vnode), ', so ', h('b', {}, r.owner), ' owns it. Replicas in order: ', (r.replicas || []).join(' → '), '.');
}

$('#lesson-edit-form').addEventListener('submit', e => {
  e.preventDefault();
  const f = new FormData(e.target);
  run(e.submitter, async () => {
    await api('PUT', `/api/lessons/${state.lesson.lesson.id}`, { body: { title: f.get('title'), summary: f.get('summary') } });
    toast('Saved. The catalog published a cache invalidation; the next read is a MISS.');
    await new Promise(r => setTimeout(r, 1500));
    await loadLesson(state.lesson.lesson.id);
    if (state.courseId) await openCourse(state.courseId);
  });
});

function videoBody(form) {
  const f = new FormData(form);
  return {
    title: f.get('title'), description: f.get('description') || '', language: f.get('language') || 'en',
    duration_seconds: Number(f.get('duration_seconds')) || 0,
    resolutions: String(f.get('resolutions') || '').split(',').map(s => s.trim()).filter(Boolean),
  };
}
$('#video-form').addEventListener('submit', e => {
  e.preventDefault();
  run(e.submitter, async () => {
    await api('PUT', `/api/videos/${state.lesson.lesson.id}`, { body: videoBody(e.target) });
    toast('Video details saved.');
    await loadLesson(state.lesson.lesson.id);
  });
});
$('#upload-form').addEventListener('submit', e => {
  e.preventDefault();
  const file = e.target.elements.namedItem('file').files[0];
  if (!file) return;
  const status = $('#upload-status');
  run(e.submitter, async () => {
    const lid = state.lesson.lesson.id;
    if (!state.lesson.video) {               // the metadata service needs a video record before an upload
      status.textContent = 'Creating the video record…';
      await api('PUT', `/api/videos/${lid}`, { body: videoBody($('#video-form')) });
    }
    status.textContent = `Uploading ${file.name} (${(file.size / 1048576).toFixed(1)} MB)…`;
    const form = new FormData();
    form.append('file', file, file.name);
    await api('POST', `/api/videos/${lid}/upload`, { form });
    status.textContent = 'Uploaded to S3. The upload event triggers the recommendation Lambda.';
    e.target.reset();
    await loadLesson(lid);
  }).finally(() => { if (status.textContent.endsWith('…')) status.textContent = ''; });
});

// progress events
function progressBody(event, positionSeconds, delta, ts) {
  const l = state.lesson.lesson;
  const duration = Number(l.duration_seconds) || 600;
  return {
    lesson_id: l.id, course_id: l.course_id, event,
    position_seconds: Math.min(positionSeconds, duration), duration_seconds: duration,
    watched_delta_seconds: delta, client_ts: ts || Date.now(),
  };
}
$$('#lesson-card [data-event]').forEach(btn => btn.addEventListener('click', e => {
  const event = e.currentTarget.dataset.event;
  run(e.currentTarget, async () => {
    if (!state.userId) throw new ApiError('Pick a user first', 401);
    const l = state.lesson.lesson;
    const duration = Number(l.duration_seconds) || 600;
    let pos = state.positions[l.id] || 0;
    if (event === 'heartbeat') pos = Math.min(pos + 10, duration);
    if (event === 'complete') pos = duration;
    state.positions[l.id] = pos;
    store.set('s24.positions', state.positions);
    await api('POST', '/api/progress', { body: progressBody(event, pos, event === 'heartbeat' ? 10 : 0) });
    toast(`${event} sent at ${fmtSec(pos)}.`);
    await loadLesson(l.id);
  });
}));
$('#watch-2min').addEventListener('click', e => run(e.currentTarget, async () => {
  if (!state.userId) throw new ApiError('Pick a user first', 401);
  const l = state.lesson.lesson;
  const start = state.positions[l.id] || 0;
  const now = Date.now();
  // 12 heartbeats, 10 s apart in event time, ending now: spans two Flink windows.
  const events = Array.from({ length: 12 }, (_, i) => progressBody(i === 0 ? 'play' : 'heartbeat', start + i * 10, i === 0 ? 0 : 10, now - (11 - i) * 10000));
  await api('POST', '/api/progress/batch', { body: events });
  state.positions[l.id] = start + 110;
  store.set('s24.positions', state.positions);
  toast('Sent 12 events covering 2 minutes. Window results appear in analytics about a minute later.');
  await loadLesson(l.id);
}));

// analytics (instructor)
$('#analytics-load').addEventListener('click', e => run(e.currentTarget, async () => {
  const d = await api('GET', `/api/analytics/items/${state.lesson.lesson.id}?limit=20`);
  const t = d.totals;
  const out = $('#analytics-out');
  if (!d.windows.length) { out.replaceChildren(h('p', { class: 'note' }, 'No windows yet. Send progress events, then wait about a minute after the window closes.')); return; }
  out.replaceChildren(
    h('p', { class: 'small' }, `Last ${d.windows.length} windows: ${t.progress_events} progress events, ${t.cache_hits} hits / ${t.cache_misses} misses`,
      t.cache_hit_ratio !== null ? ` (hit ratio ${pct(t.cache_hit_ratio)})` : '', '.'),
    h('div', { class: 'table-wrap' }, h('table', {},
      h('tr', {}, h('th', {}, 'Window (UTC)'), ...['Unique users', 'Progress', 'Completions', 'Hits', 'Misses', 'Watch s'].map(x => h('th', { class: 'num' }, x))),
      ...d.windows.map(w => h('tr', {},
        h('td', {}, String(w.WindowStart || '').replace('T', ' ').replace('Z', '')),
        ...[w.unique_users, w.progress_events, w.completions, w.cache_hits, w.cache_misses, Math.round(w.watch_seconds || 0)].map(x => h('td', { class: 'num' }, x ?? 0)))))));
}));

// ------------------------------------------------------------------ my learning
async function loadLearning() {
  const prog = $('#my-progress'), recs = $('#recs');
  if (!state.userId) {
    prog.replaceChildren(h('p', { class: 'muted' }, 'Pick a user first.'));
    recs.replaceChildren(h('li', { class: 'empty' }, 'Pick a user first.'));
    return;
  }
  const [p, r] = await Promise.all([
    api('GET', `/api/users/${state.userId}/progress`),
    api('GET', `/api/users/${state.userId}/recommendations`),
  ]);
  prog.replaceChildren(
    h('div', { class: 'stats' },
      h('div', { class: 'stat' }, h('strong', {}, p.lessons_started ?? p.items.length), h('span', { class: 'muted small' }, 'lessons started')),
      h('div', { class: 'stat' }, h('strong', {}, fmtSec(p.total_watch_seconds || 0)), h('span', { class: 'muted small' }, 'watched'))),
    p.items.length ? h('div', { class: 'table-wrap' }, h('table', {},
      h('tr', {}, h('th', {}, 'Lesson'), h('th', { class: 'num' }, 'Progress'), h('th', { class: 'num' }, 'Watched'), h('th', {}, '')),
      ...p.items.map(i => h('tr', {},
        h('td', {}, h('button', { class: 'link', onclick: () => openLessonFromAnywhere(i.lesson_id) }, i.lesson_id.slice(0, 8) + '…')),
        h('td', { class: 'num' }, `${Math.round(Number(i.percent_complete) || 0)}%`),
        h('td', { class: 'num' }, fmtSec(i.watch_seconds)),
        h('td', {}, i.completed ? h('span', { class: 'pill ok' }, 'completed') : ''))))) : h('p', { class: 'muted' }, 'Nothing watched yet.'));
  recs.replaceChildren(...(r.items.length ? r.items.map(i => h('li', {},
    h('div', {}, h('button', { class: 'link', onclick: () => openLessonFromAnywhere(i.lesson_id) }, i.title),
      h('div', { class: 'meta' }, `score ${Number(i.score).toFixed(2)} · ${i.unique_users ?? 0} learners · ${i.views ?? 0} views`)),
    i.is_new ? h('span', { class: 'pill hit' }, 'new') : ''))
    : [h('li', { class: 'empty' }, 'No recommendations yet. Upload a video to a lesson to seed them.')]));
}
function openLessonFromAnywhere(id) {
  selectTab('catalog');
  run(null, async () => {
    await loadLesson(id);
    const cid = state.lesson.lesson.course_id;
    if (cid && cid !== state.courseId) await openCourse(cid);
    $('#lesson-card').scrollIntoView({ behavior: 'smooth' });
  });
}
$('#progress-refresh').addEventListener('click', e => run(e.currentTarget, loadLearning));
$('#recs-refresh').addEventListener('click', e => run(e.currentTarget, loadLearning));

// ------------------------------------------------------------------ consistent hashing
function bars(container, entries, total, fmt, quietKey) {
  container.replaceChildren(...entries.map(([name, v]) => h('div', { class: 'bar' },
    h('span', {}, name),
    h('span', { class: 'track' }, h('span', { class: 'fill' + (name === quietKey ? ' quiet' : ''), style: `width:${Math.max(1, (v / total) * 100).toFixed(1)}%` })),
    h('span', {}, fmt(v)))));
}
async function loadRing() {
  const r = await api('GET', '/api/cache/ring');
  $('#ring-meta').textContent = `${r.nodes.length} nodes × ${r.vnodes_per_node} virtual nodes = ${r.total_vnodes} points · ${r.hash_function}`;
  const entries = Object.entries(r.ownership).sort();
  bars($('#ring-bars'), entries, Math.max(...entries.map(e => e[1])), v => pct(v));
}
$('#ring-refresh').addEventListener('click', e => run(e.currentTarget, loadRing));
$('#lookup-form').addEventListener('submit', e => {
  e.preventDefault();
  const key = new FormData(e.target).get('key').trim();
  run(e.submitter, async () => {
    const r = await api('GET', `/api/cache/lookup?key=${encodeURIComponent(key)}`);
    const out = $('#lookup-out');
    out.replaceChildren(lookupText(r));
    out.hidden = false;
  });
});
$('#sim-form').addEventListener('submit', e => {
  e.preventDefault();
  const f = new FormData(e.target);
  const q = new URLSearchParams({ add: f.get('add') || 'cache-manager-3' });
  if (f.get('vnodes')) q.set('vnodes', f.get('vnodes'));
  run(e.submitter, async () => {
    const r = await api('GET', `/api/cache/simulate?${q}`);
    const ch = r.consistent_hashing || {}, mh = r.modulo_hashing || {};
    const after = Object.entries(r.keys_per_node_after || {}).sort();
    const total = after.reduce((s, [, v]) => s + v, 0);
    const box = h('div', {});
    bars(box, after, Math.max(...after.map(e => e[1]), 1), v => v.toLocaleString(), f.get('add'));
    $('#sim-out').replaceChildren(
      h('div', { class: 'stats' },
        h('div', { class: 'stat accent' }, h('strong', {}, `${Number(ch.moved_pct).toFixed(1)}%`), h('span', { class: 'muted small' }, `keys moved, consistent hashing (ideal ${ch.ideal_moved_pct}%)`)),
        h('div', { class: 'stat' }, h('strong', {}, `${Number(mh.moved_pct).toFixed(1)}%`), h('span', { class: 'muted small' }, 'keys moved, hash(key) mod N'))),
      h('h3', {}, `Keys per node afterwards (${total.toLocaleString()} keys)`), box);
  });
});

// ------------------------------------------------------------------ tabs, log, start-up
function selectTab(name) {
  $$('.tab').forEach(t => { const on = t.dataset.tab === name; t.classList.toggle('active', on); t.setAttribute('aria-selected', String(on)); });
  $$('.tab-panel').forEach(p => { p.hidden = p.id !== `tab-${name}`; });
  if (name === 'learning') run(null, loadLearning);
  if (name === 'hashing') run(null, loadRing);
}
$$('.tab').forEach(t => t.addEventListener('click', () => selectTab(t.dataset.tab)));
$('#log-clear').addEventListener('click', () => $('#log').replaceChildren());

(async function start() {
  renderUsers();
  try {
    await api('GET', '/healthz');
    $('#health').textContent = 'API healthy';
    $('#health').className = 'pill ok';
  } catch {
    $('#health').textContent = 'API unreachable';
    $('#health').className = 'pill err';
  }
  if (!state.users.length) selectTab('users');
  run(null, loadCourses);
})();
