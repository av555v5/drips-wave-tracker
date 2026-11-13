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
    """Precise distribution. Uses each issue's actual `points × pointsMultiplier`
    from the JSON (no synthetic ×4 assumption)."""
    buckets = []
    for label, base in [('small', 100), ('medium', 150), ('large', 200)]:
        issues_in_bucket = [i for i in open_issues if (i.get('points') or 0) == base]
        c = len(issues_in_bucket)
        # precise current contribution = Σ (points × pointsMultiplier) per issue
        cur_contrib = sum((i.get('points') or 0) * (i.get('pointsMultiplier') or 0) for i in issues_in_bucket)
        mult_breakdown = Counter(i.get('pointsMultiplier') for i in issues_in_bucket)
        pts_breakdown = Counter(
            (i.get('points') or 0) * (i.get('pointsMultiplier') or 0)
            for i in issues_in_bucket
        )
        buckets.append({
            'complexity': label,
            'basePoints': base,
            'count': c,
            'sharePct': round(c / max(len(open_issues), 1) * 100, 2),
            'currentContribution': cur_contrib,
            'multiplierBreakdown': {str(k): v for k, v in sorted(mult_breakdown.items())},
            'actualPointsBreakdown': {str(k): v for k, v in sorted(pts_breakdown.items())},
        })
    other = [i for i in open_issues if (i.get('points') or 0) not in (100, 150, 200)]
    if other:
        cur_other = sum((i.get('points') or 0) * (i.get('pointsMultiplier') or 0) for i in other)
        buckets.append({
            'complexity': 'other', 'basePoints': None,
            'count': len(other),
            'sharePct': round(len(other) / max(len(open_issues), 1) * 100, 2),
            'currentContribution': cur_other,
            'multiplierBreakdown': {},
            'actualPointsBreakdown': {},
        })
    return {
        'totalOpenIssues': len(open_issues),
        'byComplexity': buckets,
        # PRECISE: sum of actual per-issue (points × pointsMultiplier) from JSON
        'currentTotalPoints': sum(
            (i.get('points') or 0) * (i.get('pointsMultiplier') or 0)
            for i in open_issues
        ),
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
             'pointsMultiplier': i.get('pointsMultiplier'),
             'totalPoints': (i.get('points') or 0) * (i.get('pointsMultiplier') or 0),
             'createdAt': i.get('gitHubCreatedAt')}
            for i in new_items[:20]   # sample of up to 20
        ],
    }

def compute_closed_in_wave_metrics(closed_rewarded: list[dict]) -> dict[str, Any]:
    """Precise metrics for closed-and-PR'd issues closed within the Wave 11
    window. Uses each issue's actual `points × pointsMultiplier` (NOT ×4)."""
    closed_total_pts = sum(
        (i.get('points') or 0) * (i.get('pointsMultiplier') or 0)
        for i in closed_rewarded
    )
    scored_pts = sum(i.get('pointsEarned') or 0 for i in closed_rewarded)
    return {
        'uniqueIssues': len(closed_rewarded),
        'closedTotalPoints': closed_total_pts,           # Σ points × multiplier (precise)
        'scoredPoints': scored_pts,                       # Σ pointsEarned (already paid)
        'remainingClosedPoints': closed_total_pts - scored_pts,
    }

# ──────────────────────────── OUTPUT ────────────────────────────
def write_summary(run_ts: dt.datetime, wave11: dict | None,
                  open_issues: list[dict], closed_rewarded: list[dict],
                  distribution: dict, closed_metrics: dict,
                  contributors: list[dict],
                  new_issues: dict, last_state: dict | None) -> Path:
    run_dir = RUNS_DIR / run_ts.strftime('%Y-%m-%dT%H-%MZ')
    run_dir.mkdir(parents=True, exist_ok=True)

    wave_start = parse_iso(wave11.get('startDate')) if wave11 else None
    wave_end   = parse_iso(wave11.get('endDate'))   if wave11 else None
    now_utc    = dt.datetime.now(dt.timezone.utc)
    in_wave    = wave_start and wave_end and wave_start <= now_utc <= wave_end

    # Max points (PRECISE) = open total points + closed-in-wave total points
    # Using actual per-issue `points × pointsMultiplier` — NOT ×4.
    open_total_pts   = distribution['currentTotalPoints']
    closed_total_pts = closed_metrics['closedTotalPoints']
    max_total_pts    = open_total_pts + closed_total_pts
    scored_pts       = closed_metrics['scoredPoints']
    remaining_pts    = max_total_pts - scored_pts

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
            'totalPoints': open_total_pts,   # PRECISE — Σ points × multiplier
            'distribution': distribution['byComplexity'],
        },
        'closedInWave11': closed_metrics,    # {uniqueIssues, closedTotalPoints, scoredPoints, remainingClosedPoints}
        'maxTotalPoints': max_total_pts,     # PRECISE — open + closed (in wave)
        'scoredPoints':   scored_pts,
        'remainingPoints': remaining_pts,
        'newIssuesSinceLastRun': new_issues,
        'topContributors': [
            {'contributor': c['contributor'], 'pointsEarned': c['pointsEarned'],
             'issuesClosed': c['issuesClosed']}
            for c in contributors[:20]   # short list used in the log summary
        ],
        # ALL contributors — used by the GitHub Pages site to render the full
        # leaderboard. The bot writes this into latest.json so the site can
        # fetch('latest.json') once and have the complete table.
        'allContributors': [
            {'contributor': c['contributor'],
             'profileUrl':  c['profileUrl'],
             'pointsEarned': c['pointsEarned'],
             'issuesClosed': c['issuesClosed'],
             'reposContributedTo': c['reposContributedTo']}
            for c in contributors
        ],
        'contributorsCount': len(contributors),
    }

    # summary.json
    (run_dir / 'summary.json').write_text(json.dumps(snapshot, indent=2))

    # full per-contributor table
    (run_dir / 'contributors.json').write_text(json.dumps(contributors, indent=2))

    # NOTE: `closed_in_wave.json` and `open_issues_sample.json` were removed by
    # request — only the aggregated metrics in summary.json + the contributor
    # table are persisted per run. The closed-issue raw dump is no longer saved.

    # human-readable markdown
    md = []
    md.append(f'# Drips Stellar Wave {WAVE_NUM} — snapshot\n')
    md.append(f'**Run at (UTC):** {run_ts.isoformat()}  ')
    if wave11:
        md.append(f'**Wave {WAVE_NUM} window:** {wave11["startDate"]} → {wave11["endDate"]}  ')
        md.append(f'**Status:** {wave11["status"]}  ')
        md.append(f'**Prize budget:** ${float(wave11["budgetUSD"]):,.0f}  ')
        md.append(f'**In Wave window right now?** {"YES" if in_wave else "NO"}\n')
    md.append('## Max points (PRECISE — `Σ (points × pointsMultiplier)` per issue, NOT ×4)\n')
    md.append(f'- **Open issues total points:** {open_total_pts:,} pts (across {distribution["totalOpenIssues"]:,} open issues)')
    md.append(f'- **Closed-in-Wave-{WAVE_NUM} total points:** {closed_total_pts:,} pts (across {closed_metrics["uniqueIssues"]:,} closed+PR issues in Wave window)')
    md.append(f'- **Max total points that can be scored:** **{max_total_pts:,} pts**  _(= open + closed-in-wave)_')
    md.append(f'- **Scored so far (Σ `pointsEarned`):** **{scored_pts:,} pts**')
    md.append(f'- **Remaining claimable points:** **{remaining_pts:,} pts**\n')
    md.append('### Distribution by complexity (precise per-issue `points × pointsMultiplier`)\n')
    md.append('| Complexity | Base pts | # open issues | Share | Multiplier breakdown | Actual-pts breakdown | Current contribution (precise) |')
    md.append('|:--|--:|--:|--:|---|---|--:|')
    for b in distribution['byComplexity']:
        mb = b.get('multiplierBreakdown') or {}
        mb_s = ', '.join(f'×{k}={v}' for k, v in sorted(mb.items())) or '—'
        pb = b.get('actualPointsBreakdown') or {}
        pb_s = ', '.join(f'{k}pts={v}' for k, v in sorted(pb.items())) or '—'
        md.append(f'| {b["complexity"]} | {b["basePoints"] if b["basePoints"] is not None else "—"} | '
                  f'{b["count"]:,} | {b["sharePct"]}% | {mb_s} | {pb_s} | '
                  f'{b["currentContribution"]:,} |')
    md.append('')
    md.append(f'## New issues opened since last run\n')
    md.append(f'- **New since last run:** {new_issues["newSinceLastRun"]:,}')
    md.append(f'- **Previous open count:** {new_issues["prevOpenCount"]:,}')
    md.append(f'- **Current open count:** {new_issues["currentOpenCount"]:,}\n')
    if new_issues['newIssuesSample']:
        md.append('Sample of new issues (up to 20):\n')
        md.append('| # | Repo | Title | Complexity | Base pts | Mult | Total pts (precise) |')
        md.append('|--:|---|---|:--:|--:|--:|--:|')
        for idx, n in enumerate(new_issues['newIssuesSample'], 1):
            md.append(f'| {idx} | `{n["repo"]}` | {n["title"][:80]} | {n["complexity"] or "—"} | '
                      f'{n["points"]} | ×{n["pointsMultiplier"]} | {n["totalPoints"]} |')
    md.append('')
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
        'openTotalPoints': snapshot['openIssues']['totalPoints'],
        'closedInWave11': snapshot['closedInWave11']['uniqueIssues'],
        'closedTotalPoints': snapshot['closedInWave11']['closedTotalPoints'],
        'maxTotalPoints': snapshot['maxTotalPoints'],
        'scoredPoints': snapshot['scoredPoints'],
        'remainingPoints': snapshot['remainingPoints'],
        'newIssuesSinceLastRun': snapshot['newIssuesSinceLastRun']['newSinceLastRun'],
        'contributorsCount': snapshot['contributorsCount'],
        'topContributor': (snapshot['topContributors'][0] if snapshot['topContributors'] else None),
    }
    with HISTORY.open('a') as f:
        f.write(json.dumps(row) + '\n')

def write_last_run(snapshot: dict, open_issues: list[dict]) -> None:
    LAST_RUN.parent.mkdir(parents=True, exist_ok=True)
    # store only compact state + open issue ids (for delta detection next run)
    state = {
        'runAt': snapshot['runAt'],
        'openIssueIds': [i.get('id') for i in open_issues],
        'openIssuesTotal': snapshot['openIssues']['total'],
        'openTotalPoints': snapshot['openIssues']['totalPoints'],
        'closedInWave11': snapshot['closedInWave11']['uniqueIssues'],
        'maxTotalPoints': snapshot['maxTotalPoints'],
        'scoredPoints': snapshot['scoredPoints'],
    }
    LAST_RUN.write_text(json.dumps(state))

def write_latest_for_site(snapshot: dict) -> None:
    """Write a stable /latest.json at repo root that the GitHub Pages site
    fetches on every load. Also writes /state/history.json as a JSON array
    (mirror of history.jsonl) for the site's time-series chart."""
    latest_path = ROOT / 'latest.json'
    latest_path.write_text(json.dumps(snapshot, indent=2))
    # mirror history.jsonl -> history.json (array) for easy fetch() on the site
    if HISTORY.exists():
        rows = [json.loads(line) for line in HISTORY.read_text().splitlines() if line.strip()]
        (ROOT / 'history.json').write_text(json.dumps(rows, indent=2))

def prune_old_runs(keep: int = 10) -> None:
    """Keep only the most recent `keep` run directories under runs/. Older ones
    are removed so the repo doesn't grow unboundedly."""
    if not RUNS_DIR.exists():
        return
    runs = sorted([p for p in RUNS_DIR.iterdir() if p.is_dir()],
                  key=lambda p: p.name, reverse=True)
    if len(runs) <= keep:
        return
    for old in runs[keep:]:
        # remove all files in the old run dir
        for f in old.iterdir():
            try: f.unlink()
            except Exception: pass
        try: old.rmdir()
        except Exception: pass
        print(f'  pruned old run: {old.name}', flush=True)

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

    # 4. Compute distributions + closed-in-wave metrics + contributor scores + deltas
    print(f'[3/5] Computing precise metrics (per-issue points × multiplier)...', flush=True)
    distribution    = compute_distribution(open_issues)
    closed_metrics  = compute_closed_in_wave_metrics(closed_rewarded)
    contributors    = compute_contributor_scores(closed_rewarded)
    new_issues      = detect_new_issues(open_issues, last_state)
    print(f'  openTotalPoints      = {distribution["currentTotalPoints"]:,}', flush=True)
    print(f'  closedInWave points  = {closed_metrics["closedTotalPoints"]:,}  '
          f'(scored={closed_metrics["scoredPoints"]:,})', flush=True)
    print(f'  maxTotalPoints       = {distribution["currentTotalPoints"] + closed_metrics["closedTotalPoints"]:,}  '
          f'(= open + closed-in-wave, PRECISE)', flush=True)
    print(f'  contributors         = {len(contributors)}', flush=True)
    print(f'  newSinceLastRun      = {new_issues["newSinceLastRun"]}\n', flush=True)

    # 5. Write snapshot + persistent state
    run_ts = dt.datetime.now(dt.timezone.utc).replace(microsecond=0)
    print(f'[4/5] Writing snapshot to runs/{run_ts.strftime("%Y-%m-%dT%H-%MZ")}/...', flush=True)
    run_dir = write_summary(run_ts, wave11, open_issues, closed_rewarded,
                            distribution, closed_metrics, contributors,
                            new_issues, last_state)

    # Load the snapshot we just wrote (consistent source of truth)
    snapshot = json.loads((run_dir / 'summary.json').read_text())
    append_history(snapshot)
    write_last_run(snapshot, open_issues)
    write_latest_for_site(snapshot)
    print(f'  → {run_dir}/summary.json', flush=True)
    print(f'  → {run_dir}/summary.md', flush=True)
    print(f'  → {run_dir}/contributors.json', flush=True)
    print(f'  appended row to {HISTORY}', flush=True)
    print(f'  updated {LAST_RUN}', flush=True)
    print(f'  wrote {ROOT / "latest.json"} + {ROOT / "history.json"} (for GitHub Pages site)', flush=True)

    # 5b. Prune old runs (keep last 10)
    print(f'[5/5] Pruning runs/ to keep last 10...', flush=True)
    prune_old_runs(keep=10)

    # 6. Print a concise log summary for the GitHub Actions log
    print('\n=== RUN SUMMARY ===', flush=True)
    print(f'Run at (UTC)        : {snapshot["runAt"]}', flush=True)
    print(f'Wave {WAVE_NUM} status      : {wave11.get("status")}  '
          f'({wave11["startDate"]} → {wave11["endDate"]})', flush=True)
    print(f'In Wave window now? : {snapshot["wave"]["inWaveWindowNow"]}', flush=True)
    print(f'Open issues total   : {snapshot["openIssues"]["total"]:,}', flush=True)
    print(f'Open total pts      : {snapshot["openIssues"]["totalPoints"]:,}  '
          f'(PRECISE — Σ points × pointsMultiplier)', flush=True)
    print(f'Closed-in-W11 pts   : {snapshot["closedInWave11"]["closedTotalPoints"]:,}  '
          f'(across {snapshot["closedInWave11"]["uniqueIssues"]:,} issues)', flush=True)
    print(f'Max total pts       : {snapshot["maxTotalPoints"]:,}  '
          f'(= open + closed-in-wave, PRECISE — NOT ×4)', flush=True)
    print(f'Scored so far       : {snapshot["scoredPoints"]:,}  '
          f'(Σ pointsEarned)', flush=True)
    print(f'Remaining claimable : {snapshot["remainingPoints"]:,}', flush=True)
    print('Distribution        :', flush=True)
    for b in snapshot['openIssues']['distribution']:
        mb = b.get('multiplierBreakdown') or {}
        mb_s = ','.join(f'×{k}={v}' for k,v in sorted(mb.items())) or '—'
        print(f'  {b["complexity"]:6s} base={b["basePoints"]!s:>4} count={b["count"]:>5} '
              f'cur={b["currentContribution"]:>8,}  mults=[{mb_s}]', flush=True)
    print(f'New since last run  : {snapshot["newIssuesSinceLastRun"]["newSinceLastRun"]:,}', flush=True)
    print(f'Contributors paid in Wave {WAVE_NUM}: {snapshot["contributorsCount"]}', flush=True)
    if snapshot['topContributors']:
        tc = snapshot['topContributors'][0]
        print(f'Top contributor     : {tc["contributor"]} — {tc["pointsEarned"]:,} pts '
              f'across {tc["issuesClosed"]} issues', flush=True)
    print('=== END ===\n', flush=True)
    return 0

if __name__ == '__main__':
    sys.exit(main())
