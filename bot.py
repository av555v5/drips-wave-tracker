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
    print('[1/6] Fetching all open Wave issues (state=open only)...', flush=True)
    return _fetch_all(BASE_ISSUES, extra_params={'state': 'open', 'waveProgramId': PID})

def fetch_counts(extra_params: dict[str, str]) -> int:
    """Fetch just the pagination.total for a given filter combo (1 API call)."""
    url = f'{BASE_ISSUES}?limit=1&' + urllib.parse.urlencode(extra_params)
    try:
        d = _get(url)
        return d.get('pagination', {}).get('total', 0) or 0
    except Exception as e:
        print(f'  WARN fetch_counts failed: {e}', flush=True)
        return 0

def fetch_closed_rewarded_in_wave(wave_start: dt.datetime,
                                  wave_end: dt.datetime) -> list[dict]:
    """Fetch closed issues with linked PRs (potential reward), sorted by
    updatedAt desc, and stop paginating once we cross below wave_start
    (i.e., the oldest issue we see was closed before the Wave started)."""
    print(f'[2/6] Fetching closed+PR issues closed within Wave {WAVE_NUM} window '
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
    print(f'  → {len(deduped)} unique rewarded issues closed within Wave {WAVE_NUM} window', flush=True)
    return deduped

# ════════════════════════════ ENROLLMENTS ════════════════════════════
ENROLLMENTS_URL = f'https://wave-api.drips.network/api/wave-programs/{PID}/repos'

def fetch_enrollments() -> list[dict]:
    """Fetch all wave-program-repo enrollments (gives pointsBudget, pointsUsed,
    pointsMultiplier, isFeatured, tags, appliedAt, etc. per repo)."""
    print(f'  fetching enrollments...', flush=True)
    items = []
    page = 1
    while True:
        url = f'{ENROLLMENTS_URL}?page={page}&limit=100'
        d = _get(url)
        items.extend(d.get('data', []))
        pag = d.get('pagination', {})
        if not pag.get('hasNextPage'):
            break
        page += 1
        if page > 50:
            break
        time.sleep(0.05)
    print(f'  → {len(items)} enrollments', flush=True)
    return items

# ════════════════════════════ PER-REPO ANALYTICS ════════════════════════════
def compute_per_repo_analytics(open_issues: list[dict],
                               closed_rewarded: list[dict],
                               enrollments: list[dict]) -> list[dict]:
    """Per-repo rollup across 252 Wave-11 repos."""
    enr_by_fn = {e['repo']['gitHubRepoFullName']: e
                 for e in enrollments if e.get('repo')}
    repos = defaultdict(lambda: {
        'org': None, 'openIssues': 0, 'unassignedOpen': 0,
        'closedInWave': 0, 'pointsScored': 0,
        'contributors': set(), 'timeToCloseSecs': [],
    })
    # open issues
    for i in open_issues:
        fn = (i.get('repo') or {}).get('gitHubRepoFullName')
        if not fn: continue
        repos[fn]['org'] = ((i.get('repo') or {}).get('org') or {}).get('gitHubOrgLogin')
        repos[fn]['openIssues'] += 1
        if not i.get('assignedApplicant'):
            repos[fn]['unassignedOpen'] += 1
        u = (i.get('assignedApplicant') or {}).get('gitHubUsername')
        if u: repos[fn]['contributors'].add(u)
    # closed-in-wave
    for i in closed_rewarded:
        fn = (i.get('repo') or {}).get('gitHubRepoFullName')
        if not fn: continue
        repos[fn]['closedInWave'] += 1
        repos[fn]['pointsScored'] += i.get('pointsEarned') or 0
        u = (i.get('assignedApplicant') or {}).get('gitHubUsername')
        if u: repos[fn]['contributors'].add(u)
        ca = parse_iso(i.get('gitHubClosedAt'))
        cr = parse_iso(i.get('gitHubCreatedAt'))
        if ca and cr:
            repos[fn]['timeToCloseSecs'].append((ca - cr).total_seconds())

    rows = []
    for fn, s in repos.items():
        e = enr_by_fn.get(fn) or {}
        ttcs = s['timeToCloseSecs']
        avg_ttc_hours = round(sum(ttcs) / len(ttcs) / 3600, 2) if ttcs else None
        rows.append({
            'repo': fn,
            'repoUrl': f'https://github.com/{fn}',
            'org': s['org'],
            'openIssues': s['openIssues'],
            'unassignedOpen': s['unassignedOpen'],
            'closedInWave': s['closedInWave'],
            'pointsScored': s['pointsScored'],
            'distinctContributors': len(s['contributors']),
            'avgTimeToCloseHours': avg_ttc_hours,
            'pointsBudget': e.get('pointsBudget'),
            'pointsUsed': e.get('pointsUsed'),
            'pointsRemaining': e.get('pointsRemaining'),
            'pointsMultiplier': e.get('pointsMultiplier'),
            'isFeatured': e.get('isFeatured') or False,
            'tags': [t.get('name') for t in (e.get('repo', {}).get('tags') or [])],
        })
    rows.sort(key=lambda r: (r['pointsScored'], r['openIssues']), reverse=True)
    return rows

# ════════════════════════════ WAVE ECONOMICS ════════════════════════════
def compute_wave_economics(budget_usd: float, max_total_points: int,
                           scored_points: int, contributors: list[dict],
                           wave_start, wave_end, now_utc):
    """Wave economics + payout modeling."""
    usd_per_point = budget_usd / max_total_points if max_total_points else 0
    # contributor payouts (precise, points × usd_per_point)
    payouts = []
    for c in contributors:
        payouts.append({
            'contributor': c['contributor'],
            'pointsEarned': c['pointsEarned'],
            'estimatedUSD': round(c['pointsEarned'] * usd_per_point, 2),
        })
    payouts.sort(key=lambda x: x['estimatedUSD'], reverse=True)

    # concentration
    concentration = {}
    if contributors and scored_points > 0:
        n = len(contributors)
        sorted_pts = sorted([c['pointsEarned'] for c in contributors], reverse=True)
        def top_share(frac):
            k = max(1, int(n * frac))
            return round(sum(sorted_pts[:k]) / scored_points * 100, 2)
        concentration = {
            'top1pctShare':  top_share(0.01),
            'top10pctShare': top_share(0.10),
            'top3Share':     top_share(3 / n) if n >= 3 else 100.0,
            'top10Share':    top_share(10 / n) if n >= 10 else 100.0,
            'medianPoints':  sorted_pts[n // 2] if n else 0,
            'meanPoints':    round(scored_points / n, 2) if n else 0,
            'longTailUnder100pts': sum(1 for p in sorted_pts if p < 100),
        }

    # USD remaining
    usd_paid = scored_points * usd_per_point
    usd_remaining = budget_usd - usd_paid

    # Projected final scored = current_scored × (wave_duration / elapsed)
    wave_dur_h = (wave_end - wave_start).total_seconds() / 3600 if wave_start and wave_end else 0
    elapsed_h = (now_utc - wave_start).total_seconds() / 3600 if wave_start and now_utc >= wave_start else 0
    if elapsed_h > 0 and now_utc <= wave_end:
        projected_final = int(scored_points * (wave_dur_h / elapsed_h))
    else:
        projected_final = scored_points  # pre-wave or post-wave → no projection

    return {
        'budgetUSD':         round(budget_usd, 2),
        'usdPerPoint':       round(usd_per_point, 6),
        'usdPaid':           round(usd_paid, 2),
        'usdRemaining':      round(usd_remaining, 2),
        'projectedFinalScoredPts': projected_final,
        'waveDurationHours': round(wave_dur_h, 1),
        'elapsedHours':      round(max(elapsed_h, 0), 1),
        'concentration':     concentration,
        'topPayouts':        payouts[:20],
        'allPayoutsCount':   len(payouts),
    }

# ════════════════════════════ ISSUE-FLOW VELOCITY ════════════════════════════
def compute_issue_velocity(open_issues: list[dict], closed_rewarded: list[dict],
                           wave_start, wave_end) -> dict:
    """Hourly buckets of activity within the Wave window."""
    def hr_key(ts):
        if not ts: return None
        return ts.strftime('%Y-%m-%dT%H:00Z')

    # filter to within wave window
    def in_wave(ts):
        return ts and wave_start <= ts <= wave_end

    opened_per_hr = Counter()
    closed_per_hr = Counter()
    scored_per_hr = Counter()
    new_contributors_per_hr = Counter()
    prs_per_hr = Counter()
    seen_contributors = set()
    for i in open_issues:
        ca = parse_iso(i.get('gitHubCreatedAt'))
        if in_wave(ca):
            opened_per_hr[hr_key(ca)] += 1
    for i in closed_rewarded:
        cl = parse_iso(i.get('gitHubClosedAt'))
        if in_wave(cl):
            closed_per_hr[hr_key(cl)] += 1
            scored_per_hr[hr_key(cl)] += i.get('pointsEarned') or 0
            u = (i.get('assignedApplicant') or {}).get('gitHubUsername')
            if u and u not in seen_contributors:
                seen_contributors.add(u)
                new_contributors_per_hr[hr_key(cl)] += 1
            if i.get('prLink'):
                prs_per_hr[hr_key(cl)] += 1

    # build a sorted hourly time-series
    keys = sorted(set(opened_per_hr) | set(closed_per_hr) | set(scored_per_hr) |
                  set(new_contributors_per_hr) | set(prs_per_hr))
    series = [{
        'hour': k,
        'opened': opened_per_hr.get(k, 0),
        'closed': closed_per_hr.get(k, 0),
        'scoredPts': scored_per_hr.get(k, 0),
        'newContributors': new_contributors_per_hr.get(k, 0),
        'prs': prs_per_hr.get(k, 0),
    } for k in keys]
    return {
        'series': series,
        'totals': {
            'openedInWave':  sum(opened_per_hr.values()),
            'closedInWave':  sum(closed_per_hr.values()),
            'scoredInWave':  sum(scored_per_hr.values()),
            'newContributorsInWave': len(seen_contributors),
            'prsInWave':     sum(prs_per_hr.values()),
        },
    }

# ════════════════════════════ APPLICATION PIPELINE ════════════════════════════
def compute_application_pipeline(open_issues: list[dict], now_utc) -> dict:
    open_assigned = 0
    open_with_apps_no_assign = 0
    total_apps = 0
    competition = []
    stale = []
    for i in open_issues:
        has_applicant = bool(i.get('assignedApplicant'))
        apps = i.get('pendingApplicationsCount') or 0
        total_apps += apps
        if has_applicant:
            open_assigned += 1
            due = parse_iso((i.get('assignedApplicant') or {}).get('dueDate'))
            if due and due < now_utc:
                stale.append({
                    'issueNumber': i.get('gitHubIssueNumber'),
                    'repo': (i.get('repo') or {}).get('gitHubRepoFullName'),
                    'assignee': (i.get('assignedApplicant') or {}).get('gitHubUsername'),
                    'dueDate': due.isoformat(),
                })
        elif apps > 0:
            open_with_apps_no_assign += 1
        if apps > 0:
            competition.append({
                'issueNumber': i.get('gitHubIssueNumber'),
                'repo': (i.get('repo') or {}).get('gitHubRepoFullName'),
                'title': i.get('title'),
                'apps': apps,
                'url': f"https://github.com/{(i.get('repo') or {}).get('gitHubRepoFullName')}/issues/{i.get('gitHubIssueNumber')}",
            })
    competition.sort(key=lambda x: x['apps'], reverse=True)
    return {
        'openAssigned':             open_assigned,
        'openWithAppsNoAssignment': open_with_apps_no_assign,
        'avgApplicationsPerOpen':   round(total_apps / max(len(open_issues), 1), 3),
        'mostCompetitionIssues':    competition[:10],
        'staleAssignments':         stale[:20],
        'staleAssignmentsCount':    len(stale),
    }

# ════════════════════════════ ISSUE QUALITY ════════════════════════════
def compute_issue_quality(open_issues: list[dict],
                          closed_with_pr_count: int,
                          closed_without_pr_count: int,
                          now_utc, stale_days: int = 7) -> dict:
    total_closed = closed_with_pr_count + closed_without_pr_count
    pr_ratio = round(closed_with_pr_count / total_closed * 100, 2) if total_closed else 0
    bounce = round(closed_without_pr_count / total_closed * 100, 2) if total_closed else 0
    stale_threshold = now_utc - dt.timedelta(days=stale_days)
    recent_threshold = now_utc - dt.timedelta(hours=1)
    stale_open = 0
    recently_active = 0
    for i in open_issues:
        u = parse_iso(i.get('gitHubUpdatedAt'))
        if u and u < stale_threshold: stale_open += 1
        if u and u >= recent_threshold: recently_active += 1
    return {
        'closedWithPR':         closed_with_pr_count,
        'closedWithoutPR':      closed_without_pr_count,
        'prToIssueRatio':       pr_ratio,
        'bounceRate':           bounce,
        'staleOpenIssues':      stale_open,
        'staleThresholdDays':   stale_days,
        'recentlyActiveOpen':   recently_active,
    }

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
                  new_issues: dict, last_state: dict | None,
                  per_repo: list[dict],
                  wave_econ: dict,
                  velocity: dict,
                  app_pipeline: dict,
                  issue_quality: dict) -> Path:
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

        # ─── NEW: per-repo analytics (top 50 in snapshot, full table in repos.json) ───
        'perRepo': {
            'totalRepos': len(per_repo),
            'topRepos':   per_repo[:50],
        },

        # ─── NEW: wave economics / payout modeling ───
        'waveEconomics': wave_econ,

        # ─── NEW: issue-flow velocity (hourly time-series in Wave window) ───
        'velocity': velocity,

        # ─── NEW: application / assignment pipeline ───
        'applicationPipeline': app_pipeline,

        # ─── NEW: issue quality / engagement ───
        'issueQuality': issue_quality,
    }

    # summary.json
    (run_dir / 'summary.json').write_text(json.dumps(snapshot, indent=2))

    # full per-contributor table
    (run_dir / 'contributors.json').write_text(json.dumps(contributors, indent=2))

    # full per-repo table (top 50 in snapshot, full list here for downloads/digging)
    (run_dir / 'repos.json').write_text(json.dumps(per_repo, indent=2))

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
            print(f'[0/7] Loaded last run state from {LAST_RUN} '
                  f'(run at {last_state.get("runAt")}, '
                  f'openIssues={last_state.get("openIssuesTotal")})', flush=True)
        except Exception as e:
            print(f'[0/7] WARN: could not load last run state: {e}', flush=True)
    else:
        print(f'[0/7] No previous run state found — first run.', flush=True)

    # 1. Wave 11 metadata
    wave11 = fetch_wave_meta()
    if not wave11:
        print(f'ERROR: Wave {WAVE_NUM} not found in program {PID}', flush=True)
        return 1
    print(f'[1/7] Wave {WAVE_NUM}: start={wave11.get("startDate")} end={wave11.get("endDate")} '
          f'status={wave11.get("status")} budget=${wave11.get("budgetUSD")}', flush=True)

    wave_start = parse_iso(wave11['startDate'])
    wave_end   = parse_iso(wave11['endDate'])
    now_utc    = dt.datetime.now(dt.timezone.utc)

    # 2. Fetch open issues + closed+rewarded in wave + enrollments + counts
    open_issues = fetch_open_issues()
    print(f'  open issues total: {len(open_issues)}\n', flush=True)

    closed_rewarded = fetch_closed_rewarded_in_wave(wave_start, wave_end)
    print(f'[3/7] closed+rewarded in Wave {WAVE_NUM} window: {len(closed_rewarded)}\n', flush=True)

    print('[4/7] Fetching enrollments + filter counts...', flush=True)
    enrollments = fetch_enrollments()
    closed_with_pr_count = fetch_counts({'state':'closed','hasPr':'true','waveProgramId':PID})
    closed_without_pr_count = fetch_counts({'state':'closed','hasPr':'false','waveProgramId':PID})
    print(f'  enrollments={len(enrollments)}, closed+PR={closed_with_pr_count:,}, '
          f'closed−PR={closed_without_pr_count:,}\n', flush=True)

    # 5. Compute all metrics
    print(f'[5/7] Computing all metrics (per-repo, economics, velocity, pipeline, quality)...', flush=True)
    distribution    = compute_distribution(open_issues)
    closed_metrics  = compute_closed_in_wave_metrics(closed_rewarded)
    contributors    = compute_contributor_scores(closed_rewarded)
    new_issues      = detect_new_issues(open_issues, last_state)
    per_repo        = compute_per_repo_analytics(open_issues, closed_rewarded, enrollments)
    wave_econ       = compute_wave_economics(
        float(wave11['budgetUSD']),
        distribution['currentTotalPoints'] + closed_metrics['closedTotalPoints'],
        closed_metrics['scoredPoints'],
        contributors, wave_start, wave_end, now_utc,
    )
    velocity        = compute_issue_velocity(open_issues, closed_rewarded, wave_start, wave_end)
    app_pipeline    = compute_application_pipeline(open_issues, now_utc)
    issue_quality   = compute_issue_quality(open_issues, closed_with_pr_count,
                                            closed_without_pr_count, now_utc)

    print(f'  openTotalPoints      = {distribution["currentTotalPoints"]:,}', flush=True)
    print(f'  closedInWave points  = {closed_metrics["closedTotalPoints"]:,}  '
          f'(scored={closed_metrics["scoredPoints"]:,})', flush=True)
    print(f'  maxTotalPoints       = {wave_econ and (distribution["currentTotalPoints"] + closed_metrics["closedTotalPoints"]):,}  '
          f'(PRECISE)', flush=True)
    print(f'  usdPerPoint          = ${wave_econ["usdPerPoint"]:.6f}  '
          f'(budget=${wave_econ["budgetUSD"]:,.0f} ÷ max)', flush=True)
    print(f'  per-repo rollup      = {len(per_repo)} repos', flush=True)
    print(f'  velocity (in-wave)   = opened {velocity["totals"]["openedInWave"]}, '
          f'closed {velocity["totals"]["closedInWave"]}, '
          f'new contribs {velocity["totals"]["newContributorsInWave"]}', flush=True)
    print(f'  app pipeline         = assigned {app_pipeline["openAssigned"]}, '
          f'apps+no-assign {app_pipeline["openWithAppsNoAssignment"]}, '
          f'stale {app_pipeline["staleAssignmentsCount"]}', flush=True)
    print(f'  issue quality        = PR ratio {issue_quality["prToIssueRatio"]}%, '
          f'bounce {issue_quality["bounceRate"]}%, '
          f'recently-active {issue_quality["recentlyActiveOpen"]}', flush=True)
    print(f'  contributors         = {len(contributors)}', flush=True)
    print(f'  newSinceLastRun      = {new_issues["newSinceLastRun"]}\n', flush=True)

    # 6. Write snapshot + persistent state
    run_ts = dt.datetime.now(dt.timezone.utc).replace(microsecond=0)
    print(f'[6/7] Writing snapshot to runs/{run_ts.strftime("%Y-%m-%dT%H-%MZ")}/...', flush=True)
    run_dir = write_summary(run_ts, wave11, open_issues, closed_rewarded,
                            distribution, closed_metrics, contributors,
                            new_issues, last_state,
                            per_repo, wave_econ, velocity, app_pipeline, issue_quality)

    # Load the snapshot we just wrote (consistent source of truth)
    snapshot = json.loads((run_dir / 'summary.json').read_text())
    append_history(snapshot)
    write_last_run(snapshot, open_issues)
    write_latest_for_site(snapshot)
    print(f'  → {run_dir}/summary.json', flush=True)
    print(f'  → {run_dir}/summary.md', flush=True)
    print(f'  → {run_dir}/contributors.json', flush=True)
    print(f'  → {run_dir}/repos.json', flush=True)
    print(f'  appended row to {HISTORY}', flush=True)
    print(f'  updated {LAST_RUN}', flush=True)
    print(f'  wrote {ROOT / "latest.json"} + {ROOT / "history.json"} (for GitHub Pages site)', flush=True)

    # 7. Prune old runs (keep last 10)
    print(f'[7/7] Pruning runs/ to keep last 10...', flush=True)
    prune_old_runs(keep=10)

    # ─── Final log summary ───
    print('\n=== RUN SUMMARY ===', flush=True)
    print(f'Run at (UTC)        : {snapshot["runAt"]}', flush=True)
    print(f'Wave {WAVE_NUM} status      : {wave11.get("status")}  '
          f'({wave11["startDate"]} → {wave11["endDate"]})', flush=True)
    print(f'In Wave window now? : {snapshot["wave"]["inWaveWindowNow"]}', flush=True)
    print(f'Open issues total   : {snapshot["openIssues"]["total"]:,}', flush=True)
    print(f'Open total pts      : {snapshot["openIssues"]["totalPoints"]:,}', flush=True)
    print(f'Closed-in-W11 pts   : {snapshot["closedInWave11"]["closedTotalPoints"]:,}  '
          f'(across {snapshot["closedInWave11"]["uniqueIssues"]:,} issues)', flush=True)
    print(f'Max total pts       : {snapshot["maxTotalPoints"]:,}  '
          f'(PRECISE — NOT ×4)', flush=True)
    print(f'Scored so far       : {snapshot["scoredPoints"]:,}', flush=True)
    print(f'Remaining claimable : {snapshot["remainingPoints"]:,}', flush=True)
    we = snapshot.get('waveEconomics', {})
    print(f'USD per point       : ${we.get("usdPerPoint",0):.6f}  '
          f'(budget ${we.get("budgetUSD",0):,.0f}, '
          f'paid ${we.get("usdPaid",0):,.2f}, remaining ${we.get("usdRemaining",0):,.2f})', flush=True)
    iq = snapshot.get('issueQuality', {})
    print(f'Issue quality       : PR ratio {iq.get("prToIssueRatio",0)}%, '
          f'bounce {iq.get("bounceRate",0)}%, '
          f'recently-active {iq.get("recentlyActiveOpen",0):,}, '
          f'stale>{iq.get("staleThresholdDays",7)}d {iq.get("staleOpenIssues",0):,}', flush=True)
    ap = snapshot.get('applicationPipeline', {})
    print(f'App pipeline        : assigned {ap.get("openAssigned",0):,}, '
          f'apps+no-assign {ap.get("openWithAppsNoAssignment",0):,}, '
          f'avg apps/open {ap.get("avgApplicationsPerOpen",0)}, '
          f'stale {ap.get("staleAssignmentsCount",0)}', flush=True)
    pr = snapshot.get('perRepo', {})
    print(f'Per-repo            : {pr.get("totalRepos",0)} repos active in Wave 11', flush=True)
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
