#!/usr/bin/env python3
"""
Drips Stellar Wave 11 tracker bot (runs on a schedule via GitHub Actions).

Per run it produces, under runs/<UTC-timestamp>/ : summary.json, summary.md,
contributors.json, repos.json. It also maintains:
  state/last_run.json   compact state for delta detection
  state/history.jsonl   append-only time series (one row per run)
  latest.json           full snapshot read by the dashboard (index.html)
  history.json          history.jsonl as a JSON array (sparkline)

Accuracy rules this script follows
  * every API call retries with backoff; a failed call never turns into a
    silent 0 (counts fall back to the last known value and are flagged)
  * paginated lists are de-duplicated by id and checked against the API's
    own reported total; mismatches are retried and recorded in dataQuality
  * the closed-issue scan stops only when it is provably past the Wave start
  * all times are timezone-aware UTC and every metric uses one `now`
  * output files are written atomically, and a clearly broken fetch aborts
    the run instead of overwriting good data
"""
from __future__ import annotations

import datetime as dt
import json
import math
import os
import statistics
import sys
import time
import traceback
import urllib.error
import urllib.parse
import urllib.request
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

# ──────────────────────────── CONFIG ────────────────────────────
PID        = 'fdc01c95-806f-4b6a-998b-a6ed37e0d81b'   # Stellar Wave program
WAVE_NUM   = 11

ROOT       = Path(__file__).resolve().parent
STATE_DIR  = ROOT / 'state'
RUNS_DIR   = ROOT / 'runs'
LAST_RUN   = STATE_DIR / 'last_run.json'
HISTORY    = STATE_DIR / 'history.jsonl'

STALE_DAYS          = 7
# "recently active" window; match it to the schedule interval (2 h) so no
# update between two runs is missed.
RECENT_ACTIVE_HOURS = int(os.environ.get('WAVE_RECENT_HOURS', '2'))
SKIP_THRESHOLD_MIN  = int(os.environ.get('WAVE_BOT_SKIP_MIN', '15'))
KEEP_RUNS           = 10

HEADERS = {
    'User-Agent': 'drips-wave-bot/2.0 (+https://github.com/)',
    'Origin':     'https://www.drips.network',
    'Referer':    'https://www.drips.network/wave/stellar/issues',
    'Accept':     'application/json',
}
BASE_ISSUES     = 'https://wave-api.drips.network/api/issues'
BASE_WAVES      = f'https://wave-api.drips.network/api/wave-programs/{PID}/waves'
ENROLLMENTS_URL = f'https://wave-api.drips.network/api/wave-programs/{PID}/repos'

RETRY_STATUS = {429, 500, 502, 503, 504}


def log(msg: str) -> None:
    print(msg, flush=True)


# ──────────────────────────── HTTP ────────────────────────────
def _get(url: str, timeout: int = 30, retries: int = 5) -> dict[str, Any]:
    delay = 1.0
    for attempt in range(1, retries + 1):
        wait = delay
        try:
            req = urllib.request.Request(url, headers=HEADERS)
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                return json.loads(resp.read().decode('utf-8'))
        except urllib.error.HTTPError as e:
            if e.code not in RETRY_STATUS or attempt == retries:
                raise
            ra = e.headers.get('Retry-After') if e.headers else None
            if ra and ra.isdigit():
                wait = min(float(ra), 60)
        except (urllib.error.URLError, TimeoutError, ConnectionError, ValueError):
            if attempt == retries:
                raise
        log(f'  retry {attempt}/{retries - 1} in {wait:.0f}s  ({url[:100]})')
        time.sleep(wait)
        delay = min(delay * 2, 30)
    raise RuntimeError('unreachable')


def _fetch_all(url_base: str, extra_params: dict[str, str] | None = None,
               limit: int = 100, max_pages: int = 500,
               early_stop=None) -> tuple[list[dict], dict[str, Any]]:
    """Paginate `url_base`. Items are de-duplicated by `id` (offset pagination
    over live data can repeat rows). Returns (items, meta)."""
    items: list[dict] = []
    seen: set = set()
    dups = pages = 0
    reported = None
    stopped = truncated = False
    page = 1
    while True:
        params = {'page': str(page), 'limit': str(limit)}
        params.update(extra_params or {})
        d = _get(f'{url_base}?{urllib.parse.urlencode(params)}')
        page_items = d.get('data') or []
        pag = d.get('pagination') or {}
        if pag.get('total') is not None:
            reported = pag['total']
        pages += 1
        for it in page_items:
            k = it.get('id')
            if k is not None:
                if k in seen:
                    dups += 1
                    continue
                seen.add(k)
            items.append(it)
        if page == 1 or page % 20 == 0 or not pag.get('hasNextPage'):
            log(f'  page {page}: got {len(page_items)}, unique so far={len(items)}, '
                f'reported total={reported}, hasNext={pag.get("hasNextPage")}')
        if early_stop and page_items and early_stop(page_items):
            stopped = True
            log(f'  early stop at page {page} (unique={len(items)})')
            break
        if not page_items or not pag.get('hasNextPage'):
            break
        page += 1
        if page > max_pages:
            truncated = True
            log('  WARN: max_pages reached, list may be truncated')
            break
        time.sleep(0.04)
    return items, {'reportedTotal': reported, 'pages': pages, 'duplicatesRemoved': dups,
                   'stoppedEarly': stopped, 'truncated': truncated}


# ──────────────────────────── HELPERS ────────────────────────────
def parse_iso(s: Any) -> dt.datetime | None:
    if not s:
        return None
    try:
        d = dt.datetime.fromisoformat(str(s).replace('Z', '+00:00'))
    except ValueError:
        return None
    return d if d.tzinfo else d.replace(tzinfo=dt.timezone.utc)


def utcnow() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)


def write_atomic(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + '.tmp')
    tmp.write_text(text, encoding='utf-8')
    os.replace(tmp, path)


def points_of(i: dict) -> int:
    """Precise per-issue value: points × pointsMultiplier."""
    return (i.get('points') or 0) * (i.get('pointsMultiplier') or 0)


def complexity_of(i: dict) -> str:
    c = i.get('complexity')
    if c:
        return str(c).lower()
    return {100: 'small', 150: 'medium', 200: 'large'}.get(i.get('points'), 'other')


def repo_name(i: dict) -> str | None:
    return (i.get('repo') or {}).get('gitHubRepoFullName')


def user_of(i: dict) -> str | None:
    return (i.get('assignedApplicant') or {}).get('gitHubUsername')


# ──────────────────────────── FETCHERS ────────────────────────────
def fetch_wave_meta() -> dict[str, Any] | None:
    d = _get(f'{BASE_WAVES}?limit=50')
    for w in d.get('data', []):
        if w.get('waveNumber') == WAVE_NUM:
            return w
    return None


def fetch_open_issues() -> tuple[list[dict], dict]:
    """Fetch all open issues; retry up to 3 times if the unique count does not
    match the API's reported total, and keep the closest attempt."""
    log('[1/6] Fetching all open Wave issues...')
    best = None
    for attempt in range(1, 4):
        items, meta = _fetch_all(BASE_ISSUES, {'state': 'open', 'waveProgramId': PID})
        rep = meta['reportedTotal']
        diff = abs(len(items) - rep) if rep is not None else 0
        if best is None or diff < best[2]:
            best = (items, meta, diff)
        if diff == 0:
            break
        log(f'  WARN: fetched {len(items)} but API reports {rep}; retrying ({attempt}/3)')
        time.sleep(5)
    items, meta, diff = best
    meta['countMismatch'] = diff
    return items, meta


def fetch_counts(params: dict[str, str]) -> int | None:
    """pagination.total for a filter. None on failure (never a silent 0)."""
    try:
        d = _get(f'{BASE_ISSUES}?limit=1&' + urllib.parse.urlencode(params))
        t = (d.get('pagination') or {}).get('total')
        return int(t) if t is not None else None
    except Exception as e:
        log(f'  WARN fetch_counts failed: {e}')
        return None


def fetch_closed_rewarded_in_wave(wave_start: dt.datetime,
                                  wave_end: dt.datetime) -> tuple[list[dict], dict]:
    """Closed issues with a linked PR, newest-updated first. Scanning stops only
    after two consecutive pages in which EVERY issue was last touched before
    the Wave start: such issues cannot have been closed inside the window, and
    sorting by updatedAt means later pages are older still. (Judging by
    closedAt alone, as before, could stop too early because an issue closed
    before the Wave can be updated after it.)"""
    log(f'[2/6] Fetching closed+PR issues closed in Wave {WAVE_NUM} window '
        f'({wave_start.isoformat()} → {wave_end.isoformat()})...')
    streak = 0

    def newest_ts(i: dict) -> dt.datetime | None:
        vals = [parse_iso(i.get(k)) for k in ('gitHubUpdatedAt', 'updatedAt', 'gitHubClosedAt')]
        vals = [v for v in vals if v]
        return max(vals) if vals else None

    def stop_fn(page_items: list[dict]) -> bool:
        nonlocal streak
        old = all((t := newest_ts(i)) is not None and t < wave_start for i in page_items)
        streak = streak + 1 if old else 0
        return streak >= 2

    items, meta = _fetch_all(
        BASE_ISSUES,
        {'state': 'closed', 'hasPr': 'true', 'waveProgramId': PID,
         'sortBy': 'updatedAt', 'sortOrder': 'desc'},
        max_pages=600, early_stop=stop_fn)

    in_window = [i for i in items
                 if (ca := parse_iso(i.get('gitHubClosedAt'))) and wave_start <= ca <= wave_end]
    rewarded = [i for i in in_window if i.get('pointsEarned')]
    rewarded.sort(key=lambda i: parse_iso(i.get('gitHubClosedAt')))      # chronological
    log(f'  → {len(in_window)} closed+PR in window, {len(rewarded)} rewarded')
    meta.update({'closedPrInWindow': len(in_window), 'scanned': len(items)})
    return rewarded, meta


def fetch_enrollments() -> list[dict]:
    log('  fetching enrollments...')
    items, _ = _fetch_all(ENROLLMENTS_URL, max_pages=50)
    log(f'  → {len(items)} enrollments')
    return items


# ──────────────────────────── METRICS ────────────────────────────
def compute_distribution(open_issues: list[dict]) -> dict[str, Any]:
    n = max(len(open_issues), 1)
    buckets = []
    for label, base in [('small', 100), ('medium', 150), ('large', 200)]:
        grp = [i for i in open_issues if (i.get('points') or 0) == base]
        buckets.append({
            'complexity': label, 'basePoints': base, 'count': len(grp),
            'sharePct': round(len(grp) / n * 100, 2),
            'currentContribution': sum(points_of(i) for i in grp),
            'multiplierBreakdown': {str(k): v for k, v in sorted(Counter(i.get('pointsMultiplier') for i in grp).items(), key=lambda kv: str(kv[0]))},
            'actualPointsBreakdown': {str(k): v for k, v in sorted(Counter(points_of(i) for i in grp).items())},
        })
    other = [i for i in open_issues if (i.get('points') or 0) not in (100, 150, 200)]
    if other:
        buckets.append({
            'complexity': 'other', 'basePoints': None, 'count': len(other),
            'sharePct': round(len(other) / n * 100, 2),
            'currentContribution': sum(points_of(i) for i in other),
            'multiplierBreakdown': {}, 'actualPointsBreakdown': {},
        })
    return {'totalOpenIssues': len(open_issues), 'byComplexity': buckets,
            'currentTotalPoints': sum(points_of(i) for i in open_issues)}


def compute_closed_in_wave_metrics(closed: list[dict]) -> dict[str, Any]:
    closed_total = sum(points_of(i) for i in closed)
    scored = sum(i.get('pointsEarned') or 0 for i in closed)
    return {'uniqueIssues': len(closed), 'closedTotalPoints': closed_total,
            'scoredPoints': scored, 'remainingClosedPoints': closed_total - scored}


def compute_contributor_scores(closed: list[dict]) -> tuple[list[dict], dict]:
    sc: dict[str, dict] = defaultdict(lambda: {'pts': 0, 'n': 0, 'repos': set(), 'cx': Counter(), 'first': None, 'last': None})
    un_pts = un_n = 0
    for i in closed:                                  # already chronological
        pe = i.get('pointsEarned') or 0
        u = user_of(i)
        if not u:
            un_pts += pe; un_n += 1
            continue
        s = sc[u]
        s['pts'] += pe; s['n'] += 1; s['cx'][complexity_of(i)] += 1
        if repo_name(i):
            s['repos'].add(repo_name(i))
        ca = i.get('gitHubClosedAt')
        s['first'] = s['first'] or ca
        s['last'] = ca
    rows = [{
        'contributor': u, 'profileUrl': f'https://github.com/{u}',
        'pointsEarned': s['pts'], 'issuesClosed': s['n'],
        'avgPointsPerIssue': round(s['pts'] / s['n'], 1) if s['n'] else 0,
        'reposCount': len(s['repos']), 'reposContributedTo': sorted(s['repos']),
        'complexityBreakdown': dict(s['cx']),
        'firstClosedAt': s['first'], 'lastClosedAt': s['last'],
    } for u, s in sc.items()]
    rows.sort(key=lambda r: (-r['pointsEarned'], r['contributor'].lower()))
    return rows, {'unattributedPoints': un_pts, 'unattributedIssues': un_n}


def compute_per_repo_analytics(open_issues, closed, enrollments) -> list[dict]:
    enr = {e['repo']['gitHubRepoFullName']: e for e in enrollments
           if e.get('repo') and e['repo'].get('gitHubRepoFullName')}
    repos = defaultdict(lambda: {'org': None, 'open': 0, 'unassigned': 0, 'closed': 0,
                                 'pts': 0, 'people': set(), 'ttc': []})
    for i in open_issues:
        fn = repo_name(i)
        if not fn:
            continue
        r = repos[fn]
        r['org'] = r['org'] or ((i.get('repo') or {}).get('org') or {}).get('gitHubOrgLogin')
        r['open'] += 1
        if not i.get('assignedApplicant'):
            r['unassigned'] += 1
        if user_of(i):
            r['people'].add(user_of(i))
    for i in closed:
        fn = repo_name(i)
        if not fn:
            continue
        r = repos[fn]
        r['org'] = r['org'] or ((i.get('repo') or {}).get('org') or {}).get('gitHubOrgLogin')
        r['closed'] += 1
        r['pts'] += i.get('pointsEarned') or 0
        if user_of(i):
            r['people'].add(user_of(i))
        ca, cr = parse_iso(i.get('gitHubClosedAt')), parse_iso(i.get('gitHubCreatedAt'))
        if ca and cr:
            r['ttc'].append((ca - cr).total_seconds())
    rows = []
    for fn, r in repos.items():
        e = enr.get(fn) or {}
        t = r['ttc']
        rows.append({
            'repo': fn, 'repoUrl': f'https://github.com/{fn}', 'org': r['org'],
            'openIssues': r['open'], 'unassignedOpen': r['unassigned'],
            'closedInWave': r['closed'], 'pointsScored': r['pts'],
            'distinctContributors': len(r['people']),
            'avgTimeToCloseHours': round(sum(t) / len(t) / 3600, 2) if t else None,
            'medianTimeToCloseHours': round(statistics.median(t) / 3600, 2) if t else None,
            'pointsBudget': e.get('pointsBudget'), 'pointsUsed': e.get('pointsUsed'),
            'pointsRemaining': e.get('pointsRemaining'), 'pointsMultiplier': e.get('pointsMultiplier'),
            'isFeatured': bool(e.get('isFeatured')),
            'tags': [t_.get('name') for t_ in ((e.get('repo') or {}).get('tags') or [])],
        })
    rows.sort(key=lambda r: (-r['pointsScored'], -r['openIssues'], r['repo'].lower()))
    return rows


def compute_wave_economics(budget: float, max_pts: int, scored: int, contributors: list[dict],
                           unattr: dict, wave_start, wave_end, now) -> dict[str, Any]:
    per_pt = budget / max_pts if max_pts else 0
    attributed = sum(c['pointsEarned'] for c in contributors)
    payouts = [{
        'contributor': c['contributor'], 'pointsEarned': c['pointsEarned'],
        'estimatedUSD': round(c['pointsEarned'] * per_pt, 2),                       # budget ÷ max points
        'shareOfScoredPct': round(c['pointsEarned'] / attributed * 100, 3) if attributed else 0,
        'estimatedUSDIfEndedNow': round(budget * c['pointsEarned'] / attributed, 2) if attributed else 0,  # budget split by points scored
    } for c in contributors]
    payouts.sort(key=lambda p: (-p['estimatedUSD'], p['contributor'].lower()))

    conc: dict[str, Any] = {}
    n = len(contributors)
    if n and attributed > 0:
        pts = sorted((c['pointsEarned'] for c in contributors), reverse=True)
        share = lambda k: round(sum(pts[:max(1, min(k, n))]) / attributed * 100, 2)
        conc = {
            'top1pctShare': share(math.ceil(n * 0.01)), 'top10pctShare': share(math.ceil(n * 0.10)),
            'top3Share': share(3), 'top10Share': share(10),
            'medianPoints': statistics.median(pts), 'meanPoints': round(attributed / n, 2),
            'longTailUnder100pts': sum(1 for p in pts if p < 100),
            'attributedPoints': attributed, **unattr,
        }

    dur_h = (wave_end - wave_start).total_seconds() / 3600
    el_h = max((now - wave_start).total_seconds() / 3600, 0)
    projected = int(scored * dur_h / el_h) if 0 < el_h and now <= wave_end else scored
    paid = scored * per_pt
    return {
        'budgetUSD': round(budget, 2), 'usdPerPoint': round(per_pt, 6),
        'usdPerScoredPoint': round(budget / scored, 6) if scored else 0,
        'usdPaid': round(paid, 2), 'usdRemaining': round(budget - paid, 2),
        'projectedFinalScoredPts': projected,
        'waveDurationHours': round(dur_h, 1), 'elapsedHours': round(min(el_h, dur_h), 1),
        'concentration': conc, 'topPayouts': payouts[:20], 'allPayoutsCount': len(payouts),
    }


def compute_issue_velocity(open_issues, closed, wave_start, wave_end, now) -> dict[str, Any]:
    """Hourly series across the whole elapsed Wave window, zero-filled so the
    time axis is true (hours with no activity are present, not skipped)."""
    hk = lambda ts: ts.strftime('%Y-%m-%dT%H:00Z')
    in_wave = lambda ts: ts is not None and wave_start <= ts <= wave_end
    opened, closed_c, scored, newc, prs = Counter(), Counter(), Counter(), Counter(), Counter()
    still_open = 0
    seen: set[str] = set()
    for i in open_issues:
        ca = parse_iso(i.get('gitHubCreatedAt'))
        if in_wave(ca):
            opened[hk(ca)] += 1; still_open += 1
    for i in sorted(closed, key=lambda x: parse_iso(x.get('gitHubClosedAt'))):   # first-reward hour needs chronology
        ca, cl = parse_iso(i.get('gitHubCreatedAt')), parse_iso(i.get('gitHubClosedAt'))
        if in_wave(ca):
            opened[hk(ca)] += 1
        if in_wave(cl):
            k = hk(cl)
            closed_c[k] += 1
            scored[k] += i.get('pointsEarned') or 0
            if bool(i.get('prLink')) or i.get('hasPr') is not False:   # list is fetched with hasPr=true
                prs[k] += 1
            u = user_of(i)
            if u and u not in seen:
                seen.add(u); newc[k] += 1
    series = []
    if now >= wave_start:
        h = wave_start.replace(minute=0, second=0, microsecond=0)
        end = min(now, wave_end).replace(minute=0, second=0, microsecond=0)
        while h <= end:
            k = hk(h)
            series.append({'hour': k, 'opened': opened.get(k, 0), 'closed': closed_c.get(k, 0),
                           'scoredPts': scored.get(k, 0), 'newContributors': newc.get(k, 0), 'prs': prs.get(k, 0)})
            h += dt.timedelta(hours=1)
    return {'series': series, 'totals': {
        'openedInWave': sum(opened.values()),          # still-open + rewarded-closed issues created in the window
        'openedInWaveStillOpen': still_open,
        'closedInWave': sum(closed_c.values()), 'scoredInWave': sum(scored.values()),
        'newContributorsInWave': len(seen), 'prsInWave': sum(prs.values())}}


def compute_application_pipeline(open_issues, now) -> dict[str, Any]:
    assigned = no_assign_apps = total_apps = 0
    competition, stale = [], []
    for i in open_issues:
        apps = i.get('pendingApplicationsCount') or 0
        total_apps += apps
        fn, num = repo_name(i), i.get('gitHubIssueNumber')
        if i.get('assignedApplicant'):
            assigned += 1
            due = parse_iso((i['assignedApplicant']).get('dueDate'))
            if due and due < now:
                stale.append({'issueNumber': num, 'repo': fn, 'assignee': user_of(i), 'dueDate': due.isoformat(),
                              'overdueHours': round((now - due).total_seconds() / 3600, 1),
                              'url': f'https://github.com/{fn}/issues/{num}'})
        elif apps > 0:
            no_assign_apps += 1
        if apps > 0:
            competition.append({'issueNumber': num, 'repo': fn, 'title': i.get('title'), 'apps': apps,
                                'url': f'https://github.com/{fn}/issues/{num}'})
    competition.sort(key=lambda x: (-x['apps'], str(x['repo']), x['issueNumber'] or 0))
    stale.sort(key=lambda x: -x['overdueHours'])
    return {'openAssigned': assigned, 'openWithAppsNoAssignment': no_assign_apps,
            'avgApplicationsPerOpen': round(total_apps / max(len(open_issues), 1), 3),
            'mostCompetitionIssues': competition[:10],
            'staleAssignments': stale[:50], 'staleAssignmentsCount': len(stale)}


def compute_issue_quality(open_issues, with_pr, without_pr, closed_pr_in_window, rewarded_in_window,
                          counts_stale, now) -> dict[str, Any]:
    total = (with_pr or 0) + (without_pr or 0)
    stale_cut = now - dt.timedelta(days=STALE_DAYS)
    recent_cut = now - dt.timedelta(hours=RECENT_ACTIVE_HOURS)
    stale_open = recent = 0
    for i in open_issues:
        u = parse_iso(i.get('gitHubUpdatedAt'))
        if u and u < stale_cut:
            stale_open += 1
        if u and u >= recent_cut:
            recent += 1
    return {
        'closedWithPR': with_pr or 0, 'closedWithoutPR': without_pr or 0,
        'prToIssueRatio': round((with_pr or 0) / total * 100, 2) if total else 0,
        'bounceRate': round((without_pr or 0) / total * 100, 2) if total else 0,
        'countsScope': 'program, all time',
        'countsStale': counts_stale,
        'closedPrInWave': closed_pr_in_window, 'rewardedInWave': rewarded_in_window,
        'staleOpenIssues': stale_open, 'staleThresholdDays': STALE_DAYS,
        'recentlyActiveOpen': recent, 'recentWindowHours': RECENT_ACTIVE_HOURS,
    }


def detect_new_issues(open_issues, last_state) -> dict[str, Any]:
    """Issues present now, absent last run, and created after the last run's
    fetch STARTED (not when it finished, which would miss issues created
    during the previous fetch)."""
    cur = {i.get('id') for i in open_issues}
    prev = set(last_state.get('openIssueIds', [])) if last_state else set()
    since = parse_iso((last_state or {}).get('fetchStartedAt') or (last_state or {}).get('runAt'))
    new = []
    for i in open_issues:
        if i.get('id') in prev:
            continue
        ca = parse_iso(i.get('gitHubCreatedAt'))
        if since is None or (ca and ca >= since):
            new.append(i)
    new.sort(key=lambda i: i.get('gitHubCreatedAt') or '', reverse=True)
    return {
        'newSinceLastRun': len(new), 'prevOpenCount': len(prev), 'currentOpenCount': len(cur),
        'newIssuesSample': [{
            'number': i.get('gitHubIssueNumber'), 'title': i.get('title'), 'repo': repo_name(i),
            'url': f'https://github.com/{repo_name(i)}/issues/{i.get("gitHubIssueNumber")}',
            'complexity': complexity_of(i), 'points': i.get('points'),
            'pointsMultiplier': i.get('pointsMultiplier'), 'totalPoints': points_of(i),
            'createdAt': i.get('gitHubCreatedAt')} for i in new[:20]],
    }


# ──────────────────────────── OUTPUT ────────────────────────────
def build_snapshot(started, wave11, open_issues, dist, closed_metrics, contributors, new_issues,
                   per_repo, enrollments_n, wave_econ, velocity, pipeline, quality, dq) -> dict:
    ws, we = parse_iso(wave11['startDate']), parse_iso(wave11['endDate'])
    open_pts, closed_pts = dist['currentTotalPoints'], closed_metrics['closedTotalPoints']
    max_pts, scored = open_pts + closed_pts, closed_metrics['scoredPoints']
    return {
        'runAt': started.isoformat(),
        'wave': {'waveNumber': WAVE_NUM, 'startDate': wave11.get('startDate'), 'endDate': wave11.get('endDate'),
                 'status': wave11.get('status'), 'budgetUSD': wave11.get('budgetUSD'),
                 'inWaveWindowNow': ws <= started <= we},
        'openIssues': {'total': dist['totalOpenIssues'], 'totalPoints': open_pts, 'distribution': dist['byComplexity']},
        'closedInWave11': closed_metrics,
        'maxTotalPoints': max_pts,                    # open + closed-in-wave, Σ points × multiplier
        'scoredPoints': scored,                       # Σ pointsEarned
        'remainingPoints': open_pts,                  # claimable: still-open issues only
        'unawardedOnClosedPoints': closed_metrics['remainingClosedPoints'],   # closed, but paid less than points × mult
        'newIssuesSinceLastRun': new_issues,
        'topContributors': [{'contributor': c['contributor'], 'pointsEarned': c['pointsEarned'],
                             'issuesClosed': c['issuesClosed']} for c in contributors[:20]],
        'allContributors': contributors,
        'contributorsCount': len(contributors),
        'perRepo': {'totalRepos': len(per_repo), 'enrolledRepos': enrollments_n, 'topRepos': per_repo},
        'waveEconomics': wave_econ, 'velocity': velocity,
        'applicationPipeline': pipeline, 'issueQuality': quality,
        'dataQuality': dq,
    }


def write_markdown(run_dir: Path, snap: dict) -> None:
    w, oi, cw, dq = snap['wave'], snap['openIssues'], snap['closedInWave11'], snap['dataQuality']
    esc = lambda s: str(s or '').replace('|', '\\|').replace('\n', ' ')
    md = [f'# Drips Stellar Wave {WAVE_NUM} — snapshot\n',
          f'**Run at (UTC):** {snap["runAt"]}  ',
          f'**Window:** {w["startDate"]} → {w["endDate"]}  ',
          f'**Status:** {w["status"]}  **Budget:** ${float(w["budgetUSD"] or 0):,.0f}  ',
          f'**In window now?** {"YES" if w["inWaveWindowNow"] else "NO"}\n',
          '## Points (Σ points × pointsMultiplier per issue)\n',
          f'- Open issues: {oi["totalPoints"]:,} pts across {oi["total"]:,} issues',
          f'- Closed in Wave {WAVE_NUM}: {cw["closedTotalPoints"]:,} pts across {cw["uniqueIssues"]:,} rewarded issues',
          f'- **Max total:** {snap["maxTotalPoints"]:,}   **Scored:** {snap["scoredPoints"]:,}   '
          f'**Remaining claimable:** {snap["remainingPoints"]:,}',
          f'- Closed but paid below full value: {snap["unawardedOnClosedPoints"]:,}\n',
          '## Distribution\n', '| Complexity | Base | Open | Share | Multipliers | Contribution |', '|:--|--:|--:|--:|---|--:|']
    for b in oi['distribution']:
        mb = ', '.join(f'×{k}={v}' for k, v in (b.get('multiplierBreakdown') or {}).items()) or '—'
        md.append(f'| {b["complexity"]} | {b["basePoints"] or "—"} | {b["count"]:,} | {b["sharePct"]}% | {mb} | {b["currentContribution"]:,} |')
    ni = snap['newIssuesSinceLastRun']
    md += ['', f'## New issues since last run: {ni["newSinceLastRun"]:,}\n']
    if ni['newIssuesSample']:
        md += ['| Repo | Title | Complexity | Total pts |', '|---|---|:--:|--:|']
        md += [f'| `{n["repo"]}` | {esc(n["title"])[:80]} | {n["complexity"]} | {n["totalPoints"]} |' for n in ni['newIssuesSample']]
    md += ['', f'## Top contributors (Wave {WAVE_NUM})\n']
    if snap['topContributors']:
        md += ['| # | Contributor | Points | Issues |', '|--:|---|--:|--:|']
        md += [f'| {i} | {c["contributor"]} | {c["pointsEarned"]:,} | {c["issuesClosed"]} |' for i, c in enumerate(snap['topContributors'], 1)]
    else:
        md.append('_No rewards recorded yet._')
    md += ['', '## Data quality\n']
    md += [f'- {k}: {v}' for k, v in dq.items() if k != 'warnings']
    md += [f'- ⚠ {x}' for x in dq.get('warnings', [])]
    write_atomic(run_dir / 'summary.md', '\n'.join(md))


def append_history(snap: dict) -> None:
    HISTORY.parent.mkdir(parents=True, exist_ok=True)
    if HISTORY.exists():
        lines = HISTORY.read_text(encoding='utf-8').splitlines()
        if lines and snap['runAt'] in lines[-1]:
            return                                    # idempotent on re-run
    row = {'runAt': snap['runAt'], 'waveStatus': snap['wave']['status'],
           'inWaveWindowNow': snap['wave']['inWaveWindowNow'],
           'openIssuesTotal': snap['openIssues']['total'], 'openTotalPoints': snap['openIssues']['totalPoints'],
           'closedInWave11': snap['closedInWave11']['uniqueIssues'],
           'closedTotalPoints': snap['closedInWave11']['closedTotalPoints'],
           'maxTotalPoints': snap['maxTotalPoints'], 'scoredPoints': snap['scoredPoints'],
           'remainingPoints': snap['remainingPoints'],
           'newIssuesSinceLastRun': snap['newIssuesSinceLastRun']['newSinceLastRun'],
           'contributorsCount': snap['contributorsCount'],
           'topContributor': snap['topContributors'][0] if snap['topContributors'] else None}
    with HISTORY.open('a', encoding='utf-8') as f:
        f.write(json.dumps(row) + '\n')


def write_latest_for_site(snap: dict) -> None:
    write_atomic(ROOT / 'latest.json', json.dumps(snap, indent=2))
    rows = []
    if HISTORY.exists():
        for line in HISTORY.read_text(encoding='utf-8').splitlines():
            try:
                if line.strip():
                    rows.append(json.loads(line))
            except json.JSONDecodeError:
                log('  WARN: skipped a malformed history line')
    write_atomic(ROOT / 'history.json', json.dumps(rows, indent=2))


def prune_old_runs(keep: int = KEEP_RUNS) -> None:
    if not RUNS_DIR.exists():
        return
    runs = sorted((p for p in RUNS_DIR.iterdir() if p.is_dir()), key=lambda p: p.name, reverse=True)
    for old in runs[keep:]:
        for f in old.iterdir():
            try: f.unlink()
            except OSError: pass
        try: old.rmdir()
        except OSError: pass
        log(f'  pruned old run: {old.name}')


# ──────────────────────────── MAIN ────────────────────────────
def main() -> int:
    started = utcnow().replace(microsecond=0)         # one `now` for every time-based metric
    log(f'=== Drips Wave {WAVE_NUM} bot — run at {started.isoformat()} ===\n')

    last_state = None
    if LAST_RUN.exists():
        try:
            last_state = json.loads(LAST_RUN.read_text(encoding='utf-8'))
            log(f'[0] Loaded last run state (run at {last_state.get("runAt")}, open={last_state.get("openIssuesTotal")})')
        except Exception as e:
            log(f'[0] WARN: could not load last run state: {e}')
    else:
        log('[0] No previous state: first run.')

    if last_state and last_state.get('runAt'):
        last_ts = parse_iso(last_state['runAt'])
        if last_ts:
            mins = (started - last_ts).total_seconds() / 60
            if 0 <= mins < SKIP_THRESHOLD_MIN:
                log(f'[0] SKIP: last run was {mins:.1f} min ago (< {SKIP_THRESHOLD_MIN}). Exiting cleanly.')
                return 0

    wave11 = fetch_wave_meta()
    if not wave11:
        log(f'ERROR: Wave {WAVE_NUM} not found in program {PID}')
        return 1
    ws, we = parse_iso(wave11['startDate']), parse_iso(wave11['endDate'])
    budget = float(wave11.get('budgetUSD') or 0)
    log(f'Wave {WAVE_NUM}: {wave11["startDate"]} → {wave11["endDate"]}  status={wave11.get("status")}  budget=${budget:,.0f}\n')

    open_issues, open_meta = fetch_open_issues()
    log(f'  open issues: {len(open_issues)}\n')
    if not open_issues and last_state and (last_state.get('openIssuesTotal') or 0) > 0 and wave11.get('status') != 'ended':
        log('ERROR: fetched 0 open issues but the previous run had some. Aborting so good data is not overwritten.')
        return 1

    closed, closed_meta = fetch_closed_rewarded_in_wave(ws, we)

    log('[4/6] Fetching enrollments + filter counts...')
    enrollments = fetch_enrollments()
    with_pr = fetch_counts({'state': 'closed', 'hasPr': 'true', 'waveProgramId': PID})
    without_pr = fetch_counts({'state': 'closed', 'hasPr': 'false', 'waveProgramId': PID})
    counts_stale = False
    if with_pr is None or without_pr is None:
        counts_stale = True
        with_pr = with_pr if with_pr is not None else (last_state or {}).get('closedWithPRCount')
        without_pr = without_pr if without_pr is not None else (last_state or {}).get('closedWithoutPRCount')
        log('  WARN: counts unavailable, using last known values')

    log('[5/6] Computing metrics...')
    dist = compute_distribution(open_issues)
    closed_metrics = compute_closed_in_wave_metrics(closed)
    contributors, unattr = compute_contributor_scores(closed)
    new_issues = detect_new_issues(open_issues, last_state)
    per_repo = compute_per_repo_analytics(open_issues, closed, enrollments)
    max_pts = dist['currentTotalPoints'] + closed_metrics['closedTotalPoints']
    wave_econ = compute_wave_economics(budget, max_pts, closed_metrics['scoredPoints'], contributors, unattr, ws, we, started)
    velocity = compute_issue_velocity(open_issues, closed, ws, we, started)
    pipeline = compute_application_pipeline(open_issues, started)
    quality = compute_issue_quality(open_issues, with_pr, without_pr, closed_meta['closedPrInWindow'],
                                    len(closed), counts_stale, started)

    warnings = []
    if open_meta['countMismatch']:
        warnings.append(f'open issues: fetched {len(open_issues)} but API reported {open_meta["reportedTotal"]}')
    if closed_meta['truncated']:
        warnings.append('closed scan hit the page limit; closed-in-wave totals may be incomplete')
    if not closed_meta['stoppedEarly'] and not closed_meta['truncated']:
        pass                                          # scanned to the end of the list: complete
    missing = sum(1 for i in open_issues if not i.get('points') or not i.get('pointsMultiplier'))
    if missing:
        warnings.append(f'{missing} open issues have no points or no multiplier and count as 0')
    if unattr['unattributedIssues']:
        warnings.append(f'{unattr["unattributedIssues"]} rewarded issues ({unattr["unattributedPoints"]:,} pts) have no assignee and are excluded from contributor tables')
    if counts_stale:
        warnings.append('closed with/without PR counts are from the previous run')
    if last_state and last_state.get('openIssuesTotal') and len(open_issues) < 0.5 * last_state['openIssuesTotal']:
        warnings.append(f'open issues fell from {last_state["openIssuesTotal"]:,} to {len(open_issues):,}')
    dq = {'openFetched': len(open_issues), 'openReportedTotal': open_meta['reportedTotal'],
          'openDuplicatesRemoved': open_meta['duplicatesRemoved'], 'closedScanned': closed_meta['scanned'],
          'closedPagesScanned': closed_meta['pages'], 'closedScanStoppedEarly': closed_meta['stoppedEarly'],
          'enrollments': len(enrollments), 'countsStale': counts_stale, 'warnings': warnings}
    for w_ in warnings:
        log(f'  WARN: {w_}')

    snap = build_snapshot(started, wave11, open_issues, dist, closed_metrics, contributors, new_issues,
                          per_repo, len(enrollments), wave_econ, velocity, pipeline, quality, dq)
    snap['finishedAt'] = utcnow().replace(microsecond=0).isoformat()

    log('[6/6] Writing outputs...')
    run_dir = RUNS_DIR / started.strftime('%Y-%m-%dT%H-%MZ')
    write_atomic(run_dir / 'summary.json', json.dumps(snap, indent=2))
    write_atomic(run_dir / 'contributors.json', json.dumps(contributors, indent=2))
    write_atomic(run_dir / 'repos.json', json.dumps(per_repo, indent=2))
    write_markdown(run_dir, snap)
    append_history(snap)
    write_atomic(LAST_RUN, json.dumps({
        'runAt': snap['runAt'], 'fetchStartedAt': started.isoformat(),
        'openIssueIds': [i.get('id') for i in open_issues], 'openIssuesTotal': len(open_issues),
        'openTotalPoints': dist['currentTotalPoints'], 'closedInWave11': len(closed),
        'maxTotalPoints': snap['maxTotalPoints'], 'scoredPoints': snap['scoredPoints'],
        'closedWithPRCount': with_pr, 'closedWithoutPRCount': without_pr}))
    write_latest_for_site(snap)
    prune_old_runs()

    log('\n=== RUN SUMMARY ===')
    log(f'Run at (UTC)      : {snap["runAt"]}   in window now: {snap["wave"]["inWaveWindowNow"]}')
    log(f'Open issues       : {snap["openIssues"]["total"]:,}  ({dist["currentTotalPoints"]:,} pts)')
    log(f'Closed in wave    : {closed_metrics["uniqueIssues"]:,}  ({closed_metrics["closedTotalPoints"]:,} pts)')
    log(f'Max / scored / left: {snap["maxTotalPoints"]:,} / {snap["scoredPoints"]:,} / {snap["remainingPoints"]:,}')
    log(f'USD per point     : ${wave_econ["usdPerPoint"]:.6f}   paid ${wave_econ["usdPaid"]:,.2f}   remaining ${wave_econ["usdRemaining"]:,.2f}')
    log(f'Contributors      : {len(contributors)}   new issues: {new_issues["newSinceLastRun"]:,}   repos: {len(per_repo)}')
    log(f'Warnings          : {len(warnings)}')
    log('=== END ===')
    return 0


if __name__ == '__main__':
    try:
        sys.exit(main())
    except Exception:
        traceback.print_exc()
        sys.exit(1)
