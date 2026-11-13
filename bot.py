#!/usr/bin/env python3
"""
Drips Stellar Wave 11 — hourly tracker bot.

Runs end-to-end every hour (via GitHub Actions cron) and tracks, for the
current / latest Stellar Wave:

  - new issues opened since the last run
  - total open issues + expected max Wave points (base × 4 across all open issues)
  - points distribution (small / medium / large)
  - unique closed-and-rewarded issues whose gitHubClosedAt falls inside the
    Wave 11 window — these are the issues a contributor was actually paid for
  - per-contributor points earned during Wave 11 (and the running total)
  - estimated score "scored so far" in Wave 11 = sum of pointsEarned across
    those closed-and-rewarded issues

Outputs (per run, all written under runs/<UTC-timestamp>/):
  - summary.json   — structured snapshot
  - summary.md     — human-readable summary
  - open_issues_sample.json — first 50 open issues (for diffing/debugging)
  - closed_in_wave.json     — full list of rewarded issues closed in Wave 11 window
  - contributors.json       — per-contributor points table

Persistent state (committed to repo so each hourly run sees the previous state):
  - state/last_run.json   — last run's compact state (for delta detection)
  - state/history.jsonl   — append-only time-series (one JSON-line per run)

Logs are written to stdout so GitHub Actions captures them in the workflow log.
"""
from __future__ import annotations

import json
import os
import sys
import time
import urllib.request
import urllib.parse
import urllib.error
import datetime as dt
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

# ──────────────────────────── CONFIG ────────────────────────────
PID        = 'fdc01c95-806f-4b6a-998b-a6ed37e0d81b'   # Stellar Wave program
WAVE_NUM   = 11
ACTIVE_MULT = 4                                        # ×4 multiplier during an active Wave

ROOT       = Path(__file__).resolve().parent
STATE_DIR  = ROOT / 'state'
RUNS_DIR   = ROOT / 'runs'
LAST_RUN   = STATE_DIR / 'last_run.json'
HISTORY    = STATE_DIR / 'history.jsonl'

HEADERS = {
    'User-Agent': 'drips-wave-bot/1.0 (+https://github.com/)',
    'Origin':     'https://www.drips.network',
    'Referer':    'https://www.drips.network/wave/stellar/issues',
    'Accept':     'application/json',
}
BASE_ISSUES = 'https://wave-api.drips.network/api/issues'
BASE_WAVES  = f'https://wave-api.drips.network/api/wave-programs/{PID}/waves'

# ──────────────────────────── HTTP ────────────────────────────
def _get(url: str, timeout: int = 30) -> dict[str, Any]:
    req = urllib.request.Request(url, headers=HEADERS)
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode('utf-8'))

def _fetch_all(url_base: str, extra_params: dict[str, str] | None = None,
               limit: int = 100, max_pages: int = 500,
               early_stop=None) -> list[dict]:
    """Paginate through `url_base`. If `early_stop(items, latest) -> bool` returns
    True, stop fetching further pages (used to stop scanning closed issues once
    we reach ones older than Wave 11 start)."""
    items: list[dict] = []
    page = 1
    while True:
        params = {'page': str(page), 'limit': str(limit)}
        if extra_params:
            params.update(extra_params)
        url = f'{url_base}?{urllib.parse.urlencode(params)}'
        d = _get(url)
        page_items = d.get('data', []) or []
        items.extend(page_items)
        pag = d.get('pagination', {}) or {}
        if page == 1 or page % 20 == 0 or not pag.get('hasNextPage'):
            print(f'  page {page}: got {len(page_items)}, cumulative={len(items)}, '
                  f'total={pag.get("total")}, hasNext={pag.get("hasNextPage")}', flush=True)
        if early_stop and page_items and early_stop(items, page_items):
            print(f'  early-stop triggered at page {page} (cumulative={len(items)})', flush=True)
            break
        if not pag.get('hasNextPage'):
            break
        page += 1
        if page > max_pages:
            print('  safety break (max_pages reached)', flush=True)
            break
        time.sleep(0.04)   # be gentle on the API
    return items

# ──────────────────────────── PIPELINE ────────────────────────────
def fetch_wave_meta() -> dict[str, Any] | None:
    d = _get(f'{BASE_WAVES}?&limit=20')
    for w in d.get('data', []):
        if w.get('waveNumber') == WAVE_NUM:
            return w
    return None

def parse_iso(s: str | None) -> dt.datetime | None:
    if not s: return None
    return dt.datetime.fromisoformat(s.replace('Z', '+00:00'))

def fetch_open_issues() -> list[dict]:
    print('[1/4] Fetching all open Wave issues (state=open only)...', flush=True)
    return _fetch_all(BASE_ISSUES, extra_params={'state': 'open', 'waveProgramId': PID})

def fetch_closed_rewarded_in_wave(wave_start: dt.datetime,
                                  wave_end: dt.datetime) -> list[dict]:
    """Fetch closed issues with linked PRs (potential reward), sorted by
    updatedAt desc, and stop paginating once we cross below wave_start
    (i.e., the oldest issue we see was closed before the Wave started)."""
    print(f'[2/4] Fetching closed+PR issues closed within Wave 11 window '
          f'({wave_start.isoformat()} → {wave_end.isoformat()})...', flush=True)

    def stop_fn(all_items, latest_page):
        # If every item in the latest page was closed before wave_start, stop.
        for i in latest_page:
            ca = parse_iso(i.get('gitHubClosedAt'))
            if ca and ca >= wave_start:
                return False
        # also stop if we've gone well past the wave end and there's nothing new
        return True

    items = _fetch_all(
        BASE_ISSUES,
        extra_params={'state': 'closed', 'hasPr': 'true', 'waveProgramId': PID,
                      'sortBy': 'updatedAt', 'sortOrder': 'desc'},
        max_pages=400,
        early_stop=stop_fn,
    )
    # filter to within wave window and rewarded
    in_window = []
    for i in items:
        ca = parse_iso(i.get('gitHubClosedAt'))
        if not ca: continue
        if wave_start <= ca <= wave_end and i.get('pointsEarned'):
            in_window.append(i)
    # dedupe by issue id (an issue might appear once but be safest)
    seen = set(); deduped = []
    for i in in_window:
        if i.get('id') in seen: continue
        seen.add(i.get('id')); deduped.append(i)
    print(f'  → {len(deduped)} unique rewarded issues closed within Wave 11 window', flush=True)
    return deduped

def compute_distribution(open_issues: list[dict]) -> dict[str, Any]:
    base_counter = Counter(i.get('points') for i in open_issues)
    buckets = []
    for label, base, w11_pts in [('small', 100, 400), ('medium', 150, 600), ('large', 200, 800)]:
        c = base_counter.get(base, 0)
        buckets.append({
            'complexity': label, 'basePoints': base,
            'wave11PointsPerIssue': w11_pts, 'count': c,
            'sharePct': round(c / max(len(open_issues), 1) * 100, 2),
            'wave11Contribution': c * w11_pts,
        })
    other = sum(v for k, v in base_counter.items() if k not in (100, 150, 200))
    if other:
        buckets.append({'complexity': 'other', 'basePoints': None,
                        'wave11PointsPerIssue': None, 'count': other,
                        'sharePct': round(other / max(len(open_issues), 1) * 100, 2),
                        'wave11Contribution': 0})
    return {
        'totalOpenIssues': len(open_issues),
        'byComplexity': buckets,
        'maxTotalWave11Points': sum((i.get('points') or 0) * ACTIVE_MULT for i in open_issues),
    }

def compute_contributor_scores(closed_rewarded: list[dict]) -> list[dict]:
    """Aggregate pointsEarned by contributor (assignedApplicant.gitHubUsername)."""
    scores: dict[str, dict] = defaultdict(lambda: {'points': 0, 'issuesClosed': 0, 'repos': set()})
    for i in closed_rewarded:
        applicant = i.get('assignedApplicant') or {}
        u = applicant.get('gitHubUsername')
        if not u:
            continue
        pe = i.get('pointsEarned') or 0
        scores[u]['points'] += pe
        scores[u]['issuesClosed'] += 1
        repo = ((i.get('repo') or {}).get('gitHubRepoFullName'))
        if repo: scores[u]['repos'].add(repo)
    rows = []
    for u, s in scores.items():
        rows.append({
            'contributor': u,
            'profileUrl': f'https://github.com/{u}',
            'pointsEarned': s['points'],
            'issuesClosed': s['issuesClosed'],
            'reposContributedTo': sorted(s['repos']),
        })
    rows.sort(key=lambda r: r['pointsEarned'], reverse=True)
    return rows

def detect_new_issues(open_issues: list[dict], last_state: dict | None) -> dict[str, Any]:
    """New issues opened since last run = issue IDs in current set but not in last."""
    cur_ids = {i.get('id') for i in open_issues}
    prev_ids = set(last_state.get('openIssueIds', [])) if last_state else set()
    new_ids = cur_ids - prev_ids
    # Keep only issues actually opened since last run timestamp (additional sanity)
    since_ts = parse_iso(last_state.get('runAt')) if last_state else None
    new_items = []
    for i in open_issues:
        if i.get('id') in new_ids:
            ca = parse_iso(i.get('gitHubCreatedAt'))
            if since_ts is None or (ca and ca >= since_ts):
                new_items.append(i)
    return {
        'newSinceLastRun': len(new_items),
        'prevOpenCount': len(prev_ids),
        'currentOpenCount': len(cur_ids),
        'newIssuesSample': [
            {'number': i.get('gitHubIssueNumber'),
             'title': i.get('title'),
             'repo': ((i.get('repo') or {}).get('gitHubRepoFullName')),
             'url': f"https://github.com/{((i.get('repo') or {}).get('gitHubRepoFullName'))}/issues/{i.get('gitHubIssueNumber')}",
             'complexity': i.get('complexity'),
             'points': i.get('points'),
             'createdAt': i.get('gitHubCreatedAt')}
            for i in new_items[:20]   # sample of up to 20
        ],
    }

# ──────────────────────────── OUTPUT ────────────────────────────
def write_summary(run_ts: dt.datetime, wave11: dict | None,
                  open_issues: list[dict], closed_rewarded: list[dict],
                  distribution: dict, contributors: list[dict],
                  new_issues: dict, last_state: dict | None) -> Path:
    run_dir = RUNS_DIR / run_ts.strftime('%Y-%m-%dT%H-%MZ')
    run_dir.mkdir(parents=True, exist_ok=True)

    wave_start = parse_iso(wave11.get('startDate')) if wave11 else None
    wave_end   = parse_iso(wave11.get('endDate'))   if wave11 else None
    now_utc    = dt.datetime.now(dt.timezone.utc)
    in_wave    = wave_start and wave_end and wave_start <= now_utc <= wave_end

    total_scored = sum(i.get('pointsEarned') or 0 for i in closed_rewarded)

    snapshot = {
        'runAt': run_ts.isoformat(),
        'wave': {
            'waveNumber': WAVE_NUM,
            'startDate': wave11.get('startDate') if wave11 else None,
            'endDate':   wave11.get('endDate')   if wave11 else None,
            'status':    wave11.get('status')    if wave11 else None,
            'budgetUSD': wave11.get('budgetUSD') if wave11 else None,
            'inWaveWindowNow': in_wave,
        },
        'openIssues': {
            'total': distribution['totalOpenIssues'],
            'maxTotalWave11Points': distribution['maxTotalWave11Points'],
            'distribution': distribution['byComplexity'],
        },
        'closedRewardedInWave11': {
            'uniqueIssues': len(closed_rewarded),
            'totalPointsScored': total_scored,
        },
        'newIssuesSinceLastRun': new_issues,
        'topContributors': [
            {'contributor': c['contributor'], 'pointsEarned': c['pointsEarned'],
             'issuesClosed': c['issuesClosed']}
            for c in contributors[:20]
        ],
        'contributorsCount': len(contributors),
    }

    # summary.json
    (run_dir / 'summary.json').write_text(json.dumps(snapshot, indent=2))

    # full per-contributor table
    (run_dir / 'contributors.json').write_text(json.dumps(contributors, indent=2))

    # closed-in-wave dump
    (run_dir / 'closed_in_wave.json').write_text(json.dumps(closed_rewarded, indent=2))

    # open issues sample (first 50 — for debugging, not full dump to keep repo small)
    (run_dir / 'open_issues_sample.json').write_text(
        json.dumps(open_issues[:50], indent=2))

    # human-readable markdown
    md = []
    md.append(f'# Drips Stellar Wave {WAVE_NUM} — hourly snapshot\n')
    md.append(f'**Run at (UTC):** {run_ts.isoformat()}  ')
    if wave11:
        md.append(f'**Wave {WAVE_NUM} window:** {wave11["startDate"]} → {wave11["endDate"]}  ')
        md.append(f'**Status:** {wave11["status"]}  ')
        md.append(f'**Prize budget:** ${float(wave11["budgetUSD"]):,.0f}  ')
        md.append(f'**In Wave window right now?** {"YES" if in_wave else "NO"}\n')
    md.append('## Open issues (state=open only)\n')
    md.append(f'- **Total open issues:** {distribution["totalOpenIssues"]:,}')
    md.append(f'- **Max total Wave {WAVE_NUM} points (Σ base × 4):** **{distribution["maxTotalWave11Points"]:,} pts**\n')
    md.append('### Distribution by complexity\n')
    md.append('| Complexity | Base pts | Per-issue pts in Wave 11 (×4) | # open issues | Share | Max contribution |')
    md.append('|:--|--:|--:|--:|--:|--:|')
    for b in distribution['byComplexity']:
        md.append(f'| {b["complexity"]} | {b["basePoints"] if b["basePoints"] is not None else "—"} | '
                  f'{b["wave11PointsPerIssue"] if b["wave11PointsPerIssue"] is not None else "—"} | '
                  f'{b["count"]:,} | {b["sharePct"]}% | {b["wave11Contribution"]:,} pts |')
    md.append('')
    md.append(f'## New issues opened since last run\n')
    md.append(f'- **New since last run:** {new_issues["newSinceLastRun"]:,}')
    md.append(f'- **Previous open count:** {new_issues["prevOpenCount"]:,}')
    md.append(f'- **Current open count:** {new_issues["currentOpenCount"]:,}\n')
    if new_issues['newIssuesSample']:
        md.append('Sample of new issues (up to 20):\n')
        md.append('| # | Repo | Title | Complexity | Base pts |')
        md.append('|--:|---|---|:--:|--:|')
        for idx, n in enumerate(new_issues['newIssuesSample'], 1):
            md.append(f'| {idx} | `{n["repo"]}` | {n["title"][:80]} | {n["complexity"] or "—"} | {n["points"]} |')
    md.append('')
    md.append(f'## Closed-and-rewarded issues inside Wave {WAVE_NUM} window\n')
    md.append(f'- **Unique rewarded issues closed in window:** {len(closed_rewarded):,}')
    md.append(f'- **Total points scored so far in Wave {WAVE_NUM}:** **{total_scored:,} pts**\n')
    md.append(f'## Top contributors (by points earned in Wave {WAVE_NUM})\n')
    if contributors:
        md.append('| Rank | Contributor | Points earned | Issues closed | # Repos |')
        md.append('|--:|---|--:|--:|--:|')
        for i, c in enumerate(contributors[:20], 1):
            md.append(f'| {i} | [{c["contributor"]}]({c["profileUrl"]}) | **{c["pointsEarned"]:,}** | '
                      f'{c["issuesClosed"]} | {len(c["reposContributedTo"])} |')
        md.append(f'\n_Full contributor table: `contributors.json`_')
    else:
        md.append(f'_No contributor rewards recorded yet in Wave {WAVE_NUM} (either before Wave start, or no issues closed-and-rewarded yet)._')
    (run_dir / 'summary.md').write_text('\n'.join(md))
    return run_dir

def append_history(snapshot: dict) -> None:
    HISTORY.parent.mkdir(parents=True, exist_ok=True)
    row = {
        'runAt': snapshot['runAt'],
        'waveStatus': snapshot['wave']['status'],
        'inWaveWindowNow': snapshot['wave']['inWaveWindowNow'],
        'openIssuesTotal': snapshot['openIssues']['total'],
        'maxTotalWave11Points': snapshot['openIssues']['maxTotalWave11Points'],
        'closedRewardedInWave11': snapshot['closedRewardedInWave11']['uniqueIssues'],
        'totalPointsScored': snapshot['closedRewardedInWave11']['totalPointsScored'],
        'newIssuesSinceLastRun': snapshot['newIssuesSinceLastRun']['newSinceLastRun'],
        'contributorsCount': snapshot['contributorsCount'],
        'topContributor': (snapshot['topContributors'][0] if snapshot['topContributors'] else None),
    }
    with HISTORY.open('a') as f:
        f.write(json.dumps(row) + '\n')

def write_last_run(snapshot: dict, open_issues: list[dict]) -> None:
    LAST_RUN.parent.mkdir(parents=True, exist_ok=True)
    # store only compact state + a sample of open issue ids (to keep file small,
    # we store ALL ids — at ~9,639 ids × ~40 bytes ≈ 400KB which is fine)
    state = {
        'runAt': snapshot['runAt'],
        'openIssueIds': [i.get('id') for i in open_issues],
        'openIssuesTotal': snapshot['openIssues']['total'],
        'maxTotalWave11Points': snapshot['openIssues']['maxTotalWave11Points'],
        'closedRewardedInWave11': snapshot['closedRewardedInWave11']['uniqueIssues'],
        'totalPointsScored': snapshot['closedRewardedInWave11']['totalPointsScored'],
    }
    LAST_RUN.write_text(json.dumps(state))

# ──────────────────────────── MAIN ────────────────────────────
def main() -> int:
    print(f'=== Drips Wave {WAVE_NUM} hourly bot — run at '
          f'{dt.datetime.now(dt.timezone.utc).isoformat()} ===\n', flush=True)

    # 0. Load last run state
    last_state = None
    if LAST_RUN.exists():
        try:
            last_state = json.loads(LAST_RUN.read_text())
            print(f'[0/4] Loaded last run state from {LAST_RUN} '
                  f'(run at {last_state.get("runAt")}, '
                  f'openIssues={last_state.get("openIssuesTotal")})', flush=True)
        except Exception as e:
            print(f'[0/4] WARN: could not load last run state: {e}', flush=True)
    else:
        print(f'[0/4] No previous run state found — first run.', flush=True)

    # 1. Wave 11 metadata
    wave11 = fetch_wave_meta()
    if not wave11:
        print(f'ERROR: Wave {WAVE_NUM} not found in program {PID}', flush=True)
        return 1
    print(f'[1/4] Wave {WAVE_NUM}: start={wave11.get("startDate")} end={wave11.get("endDate")} '
          f'status={wave11.get("status")} budget=${wave11.get("budgetUSD")}', flush=True)

    wave_start = parse_iso(wave11['startDate'])
    wave_end   = parse_iso(wave11['endDate'])

    # 2. Fetch open issues
    open_issues = fetch_open_issues()
    print(f'[1/4] open issues total: {len(open_issues)}\n', flush=True)

    # 3. Fetch closed+rewarded in Wave 11 window
    closed_rewarded = fetch_closed_rewarded_in_wave(wave_start, wave_end)
    print(f'[2/4] closed+rewarded in Wave {WAVE_NUM} window: {len(closed_rewarded)}\n', flush=True)

    # 4. Compute distributions + contributor scores + new issues delta
    print(f'[3/4] Computing distributions + contributor scores + deltas...', flush=True)
    distribution = compute_distribution(open_issues)
    contributors = compute_contributor_scores(closed_rewarded)
    new_issues   = detect_new_issues(open_issues, last_state)
    print(f'  maxTotalWave11Points = {distribution["maxTotalWave11Points"]:,}', flush=True)
    print(f'  totalPointsScored    = {sum(i.get("pointsEarned") or 0 for i in closed_rewarded):,}', flush=True)
    print(f'  contributors         = {len(contributors)}', flush=True)
    print(f'  newSinceLastRun      = {new_issues["newSinceLastRun"]}\n', flush=True)

    # 5. Write snapshot
    run_ts = dt.datetime.now(dt.timezone.utc).replace(microsecond=0)
    print(f'[4/4] Writing snapshot to runs/{run_ts.strftime("%Y-%m-%dT%H-%MZ")}/...', flush=True)
    run_dir = write_summary(run_ts, wave11, open_issues, closed_rewarded,
                            distribution, contributors, new_issues, last_state)

    # Load the snapshot we just wrote (consistent source of truth)
    snapshot = json.loads((run_dir / 'summary.json').read_text())
    append_history(snapshot)
    write_last_run(snapshot, open_issues)
    print(f'  → {run_dir}/summary.json', flush=True)
    print(f'  → {run_dir}/summary.md', flush=True)
    print(f'  → {run_dir}/contributors.json', flush=True)
    print(f'  → {run_dir}/closed_in_wave.json', flush=True)
    print(f'  appended row to {HISTORY}', flush=True)
    print(f'  updated {LAST_RUN}', flush=True)

    # 6. Print a concise log summary for the GitHub Actions log
    print('\n=== HOURLY RUN SUMMARY ===', flush=True)
    print(f'Run at (UTC)        : {snapshot["runAt"]}', flush=True)
    print(f'Wave {WAVE_NUM} status      : {wave11.get("status")}  '
          f'({wave11["startDate"]} → {wave11["endDate"]})', flush=True)
    print(f'In Wave window now? : {snapshot["wave"]["inWaveWindowNow"]}', flush=True)
    print(f'Open issues total   : {snapshot["openIssues"]["total"]:,}', flush=True)
    print(f'Max W{WAVE_NUM} points (Σ base × 4): {snapshot["openIssues"]["maxTotalWave11Points"]:,} pts', flush=True)
    print(f'Distribution        : ' + ', '.join(
        f'{b["complexity"]}={b["count"]}' for b in snapshot["openIssues"]["distribution"]), flush=True)
    print(f'New since last run  : {snapshot["newIssuesSinceLastRun"]["newSinceLastRun"]:,}', flush=True)
    print(f'Rewarded issues closed in Wave {WAVE_NUM} window: '
          f'{snapshot["closedRewardedInWave11"]["uniqueIssues"]:,}', flush=True)
    print(f'Points scored so far in Wave {WAVE_NUM}: '
          f'{snapshot["closedRewardedInWave11"]["totalPointsScored"]:,} pts', flush=True)
    print(f'Contributors paid in Wave {WAVE_NUM}: {snapshot["contributorsCount"]}', flush=True)
    if snapshot['topContributors']:
        tc = snapshot['topContributors'][0]
        print(f'Top contributor     : {tc["contributor"]} — {tc["pointsEarned"]:,} pts '
              f'across {tc["issuesClosed"]} issues', flush=True)
    print('=== END ===\n', flush=True)
    return 0

if __name__ == '__main__':
    sys.exit(main())
