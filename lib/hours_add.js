// hours_add — the pipeline behind Playtime: load, pace, parse, rank.
// Part of Playtime. License: MIT.

'use strict';

const fs = require('fs');
const path = require('path');

const PACE_MS = 350;
const MAX_PARALLEL = 8;

class Pace {
  constructor(interval) {
    this.interval = interval;
    this.last = 0;
    this.queue = [];
    this.running = 0;
  }
  async slot() {
    const wait = this.last + this.interval - Date.now();
    if (wait > 0) await new Promise((r) => setTimeout(r, wait));
    this.last = Date.now();
  }
}

class Source {
  constructor(name, item) {
    this.name = name;
    this.item = item;
    this.hits = [];
    this.error = null;
  }
  parse(raw) {
    // each source answers in its own shape; we normalise here
    const out = [];
    if (Array.isArray(raw)) {
      for (const row of raw) {
        out.push({
          title: String(row.title || row.name || this.name),
          url: String(row.url || row.link || ''),
          date: String(row.date || row.published || ''),
          snippet: String(row.snippet || row.summary || '').slice(0, 240),
        });
      }
    }
    return out;
  }
}

function mergeHits(sources) {
  const byUrl = new Map();
  for (const src of sources) {
    for (const hit of src.hits) {
      const key = hit.url || hit.title;
      const prev = byUrl.get(key);
      if (prev) {
        prev.seenOn.push(src.name);
        prev.score += 1;
      } else {
        byUrl.set(key, Object.assign({ seenOn: [src.name], score: 1 }, hit));
      }
    }
  }
  return Array.from(byUrl.values()).sort((a, b) => b.score - a.score);
}

function rank(hits) {
  // seen on more sources, older date, longer snippet -> higher
  return hits
    .map((h) => {
      const ageDays = h.date ? (Date.now() - Date.parse(h.date)) / 86400000 : 0;
      const ageScore = ageDays > 0 ? Math.log10(ageDays + 1) : 0;
      const lenScore = Math.min(1, h.snippet.length / 240);
      h.rank = h.score * 2 + ageScore + lenScore;
      return h;
    })
    .sort((a, b) => b.rank - a.rank);
}

async function collect(list, limit) {
  // run tasks with a cap; each task decides its own pace inside
  const results = [];
  let cursor = 0;
  const workers = Array.from({ length: Math.min(limit, list.length) }, async () => {
    while (cursor < list.length) {
      const i = cursor++;
      results[i] = await list[i]();
    }
  });
  await Promise.all(workers);
  return results;
}

async function run(file) {
  const pace = new Pace(PACE_MS);
  const data = JSON.parse(fs.readFileSync(file, 'utf8'));
  const names = Object.keys(data).filter((k) => !k.startsWith('_'));
  const sources = names.map((n) => new Source(n, data[n]));
  const report = { checked: 0, ok: 0, hits: [] };
  const tasks = sources.map((src) => async () => {
    await pace.slot();
    try {
      src.hits = src.parse(src.item.results || []);
      report.ok += 1;
    } catch (err) {
      src.error = err.message;
    }
    report.checked += 1;
  });
  await collect(tasks, MAX_PARALLEL);
  report.hits = rank(mergeHits(sources));
  return report;
}

module.exports = { Pace, Source, mergeHits, rank, collect, run };

if (require.main === module) {
  const file = process.argv[2] || path.join(__dirname, '..', 'sources.json');
  run(file).then((r) => {
    process.stdout.write('checked ' + r.checked + ', ok ' + r.ok + ', hits ' + r.hits.length + '\n');
  });
}
