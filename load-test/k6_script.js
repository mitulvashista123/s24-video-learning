// S24 Online Video Learning – k6 load test
//
// Goal: sustained traffic on content-delivery-api (cached lesson/course reads) and
// progress-tracker (heartbeat writes through the API) that pushes average CPU above the
// 50 % HPA target so both Deployments scale out, then ramps down so they scale back in.
//
//   k6 run -e BASE_URL=http://<ingress-nlb-hostname> load-test/k6_script.js
//   optional: -e PEAK_RPS=80 -e SUSTAIN=2m -e LEARNERS=60 -e LESSONS=12
//
// Watch it live:   kubectl -n s24 get hpa -w     (and the Grafana "S24 … Overview" dashboard)

import http from 'k6/http';
import { check, group, sleep } from 'k6';
import { Counter, Rate, Trend } from 'k6/metrics';
import { randomIntBetween, randomItem } from 'https://jslib.k6.io/k6-utils/1.4.0/index.js';

const BASE_URL = (__ENV.BASE_URL || 'http://localhost:8080').replace(/\/$/, '');
// 80 req/s fits the 2 x t3.large cluster: both HPAs reach 6 replicas while p95 stays < 500 ms.
// 250 req/s saturates it (p95 ~19 s). Raise PEAK_RPS after scaling the node group up.
const PEAK_RPS = parseInt(__ENV.PEAK_RPS || '80', 10);
const SUSTAIN = __ENV.SUSTAIN || '2m';
const LEARNERS = parseInt(__ENV.LEARNERS || '60', 10);
const LESSONS = parseInt(__ENV.LESSONS || '12', 10);
const RUN_ID = `${Date.now()}`;

const cacheHits = new Counter('s24_cache_hits');
const cacheMisses = new Counter('s24_cache_misses');
const progressAccepted = new Rate('s24_progress_accepted');
const lessonLatency = new Trend('s24_lesson_read_ms', true);
const progressLatency = new Trend('s24_progress_write_ms', true);

const JSON_HEADERS = { 'Content-Type': 'application/json' };

function stages(peak) {
  return [
    { duration: '1m', target: Math.round(peak * 0.2) }, // warm-up
    { duration: '2m', target: peak },                    // ramp to peak  -> HPA scale-out
    { duration: SUSTAIN, target: peak },                 // sustained load
    { duration: '2m', target: Math.round(peak * 0.1) }, // ramp down     -> HPA scale-in
    { duration: '1m', target: 0 },
  ];
}

export const options = {
  setupTimeout: '5m',
  discardResponseBodies: false,
  scenarios: {
    browse_lessons: {
      executor: 'ramping-arrival-rate',
      exec: 'browseLessons',
      startRate: 5,
      timeUnit: '1s',
      preAllocatedVUs: 100,
      maxVUs: 600,
      stages: stages(Math.round(PEAK_RPS * 0.6)),
    },
    playback_heartbeats: {
      executor: 'ramping-arrival-rate',
      exec: 'playbackHeartbeats',
      startRate: 5,
      timeUnit: '1s',
      preAllocatedVUs: 100,
      maxVUs: 600,
      stages: stages(Math.round(PEAK_RPS * 0.4)),
    },
    recommendations_and_ring: {
      executor: 'constant-arrival-rate',
      exec: 'recommendationsAndRing',
      rate: 3,
      timeUnit: '1s',
      duration: '8m',  // = the ramping scenarios' total with SUSTAIN=2m
      preAllocatedVUs: 10,
      maxVUs: 40,
    },
  },
  thresholds: {
    http_req_failed: ['rate<0.02'],
    'http_req_duration{scenario:browse_lessons}': ['p(95)<800'],
    'http_req_duration{scenario:playback_heartbeats}': ['p(95)<800'],
    s24_progress_accepted: ['rate>0.98'],
  },
};

function post(path, body, userId) {
  const headers = Object.assign({}, JSON_HEADERS, userId ? { 'X-User-Id': userId } : {});
  return http.post(`${BASE_URL}${path}`, JSON.stringify(body), { headers });
}

function must(res, what) {
  if (res.status < 200 || res.status >= 300) {
    throw new Error(`${what} failed: HTTP ${res.status} ${res.body}`);
  }
  return res.json();
}

// ---------------------------------------------------------------------------- seed data
export function setup() {
  const health = http.get(`${BASE_URL}/healthz`);
  if (health.status !== 200) throw new Error(`API not healthy at ${BASE_URL}: ${health.status}`);

  const instructor = must(post('/api/users', {
    name: `k6 instructor ${RUN_ID}`, email: `instructor-${RUN_ID}@loadtest.s24.dev`, role: 'instructor',
  }), 'create instructor');

  const courses = [];
  for (let c = 0; c < 3; c++) {
    const course = must(post('/api/courses', {
      title: `Distributed Systems ${c + 1} (${RUN_ID})`,
      description: 'Load-test course: consistent hashing, replication, stream processing.',
      category: 'cloud-computing', level: 'intermediate',
    }, instructor.id), 'create course');
    const lessons = [];
    for (let l = 0; l < Math.ceil(LESSONS / 3); l++) {
      const duration = randomIntBetween(300, 1200);
      const lesson = must(post(`/api/courses/${course.id}/lessons`, {
        title: `Lesson ${l + 1}: topic ${c + 1}.${l + 1}`, summary: 'k6 seeded lesson', duration_seconds: duration,
      }, instructor.id), 'create lesson');
      const res = http.put(`${BASE_URL}/api/videos/${lesson.id}`, JSON.stringify({
        title: `Lecture video ${c + 1}.${l + 1}`, description: 'seeded by k6', language: 'en',
        duration_seconds: duration, resolutions: ['480p', '720p', '1080p'], tags: ['k6', 'distributed-systems'],
      }), { headers: Object.assign({ 'X-User-Id': instructor.id }, JSON_HEADERS) });
      must(res, 'put video metadata');
      lessons.push({ id: lesson.id, duration });
    }
    courses.push({ id: course.id, lessons });
  }

  const learners = [];
  for (let i = 0; i < LEARNERS; i++) {
    const u = must(post('/api/users', {
      name: `k6 learner ${i}`, email: `learner-${i}-${RUN_ID}@loadtest.s24.dev`, role: 'learner',
    }), 'create learner');
    learners.push(u.id);
  }
  console.log(`seeded ${courses.length} courses, ${courses.length * Math.ceil(LESSONS / 3)} lessons, ${learners.length} learners`);
  return { instructor: instructor.id, courses, learners };
}

function track(res, part) {
  const meta = (res && res.cache) ? res.cache[part] || res.cache : null;
  if (!meta) return;
  if (meta.cache === 'HIT') cacheHits.add(1);
  else if (meta.cache === 'MISS') cacheMisses.add(1);
}

// ---------------------------------------------------------------------------- scenarios
export function browseLessons(data) {
  const user = randomItem(data.learners);
  const course = randomItem(data.courses);
  const lesson = randomItem(course.lessons);
  const params = { headers: { 'X-User-Id': user }, tags: { name: 'lesson' } };

  group('browse', () => {
    if (Math.random() < 0.25) {
      const r = http.get(`${BASE_URL}/api/courses/${course.id}`, { headers: { 'X-User-Id': user }, tags: { name: 'course' } });
      check(r, { 'course 200': (x) => x.status === 200 });
      if (r.status === 200) track(r.json(), 'course');
    }
    const res = http.get(`${BASE_URL}/api/lessons/${lesson.id}`, params);
    lessonLatency.add(res.timings.duration);
    const ok = check(res, {
      'lesson 200': (r) => r.status === 200,
      'lesson has metadata': (r) => r.status === 200 && r.json('lesson.id') === lesson.id,
    });
    if (ok) {
      const body = res.json();
      track(body, 'lesson');
      track(body, 'video');
    }
  });
  if (Math.random() < 0.05) http.get(`${BASE_URL}/api/courses`, { tags: { name: 'course-list' } });
}

export function playbackHeartbeats(data) {
  const user = randomItem(data.learners);
  const course = randomItem(data.courses);
  const lesson = randomItem(course.lessons);
  const position = randomIntBetween(0, lesson.duration);
  const roll = Math.random();
  const event = roll < 0.05 ? 'play' : roll < 0.08 ? 'complete' : roll < 0.12 ? 'pause' : roll < 0.15 ? 'seek' : 'heartbeat';
  const res = post('/api/progress', {
    lesson_id: lesson.id,
    course_id: course.id,
    event,
    position_seconds: event === 'complete' ? lesson.duration : position,
    duration_seconds: lesson.duration,
    watched_delta_seconds: event === 'heartbeat' ? 10 : 0,
    client_ts: Date.now(),
  }, user);
  progressLatency.add(res.timings.duration);
  const accepted = check(res, { 'progress 202': (r) => r.status === 202 });
  progressAccepted.add(accepted);
}

export function recommendationsAndRing(data) {
  const user = randomItem(data.learners);
  const r1 = http.get(`${BASE_URL}/api/users/${user}/recommendations`, { headers: { 'X-User-Id': user }, tags: { name: 'recommendations' } });
  check(r1, { 'recommendations 200': (r) => r.status === 200 });
  const r2 = http.get(`${BASE_URL}/api/users/${user}/progress`, { headers: { 'X-User-Id': user }, tags: { name: 'user-progress' } });
  check(r2, { 'progress list 200': (r) => r.status === 200 });
  if (Math.random() < 0.1) {
    const r3 = http.get(`${BASE_URL}/api/cache/ring`, { tags: { name: 'ring' } });
    check(r3, { 'ring 200': (r) => r.status === 200 });
  }
  sleep(0.1);
}

export function teardown(data) {
  const ring = http.get(`${BASE_URL}/api/cache/ring`);
  if (ring.status === 200) console.log(`ring ownership after test: ${JSON.stringify(ring.json('ownership'))}`);
  console.log(`load test ${RUN_ID} finished; instructor ${data.instructor}`);
}
