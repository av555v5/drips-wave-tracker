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
import csv
import io
import json
import math
import os
import re
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
SKIP_THRESHOLD_MIN  = int(os.environ.get('WAVE_BOT_SKIP_MIN', '100'))   # scheduled + dispatch runs only
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
    """Paginate `url_base` and de-duplicate by `id`.

    The API returns `pagination.nextCursor`. Cursor paging is stable while rows
    are being created or updated; page offsets are not (rows shift, so items get
    skipped or repeated). The cursor is used when the server honours a `cursor`
    parameter, and the code falls back to page offsets automatically when it
    does not (an error, or the same page coming back again)."""
    items: list[dict] = []
    seen: set = set()
    dups = pages = cursor_pages = 0
    reported = None
    stopped = truncated = False
    page = 1
    cursor = None
    cursor_ok = True
    while True:
        params = {'limit': str(limit)}
        params.update(extra_params or {})
        use_cursor = cursor_ok and cursor is not None
        if use_cursor:
            params['cursor'] = cursor
        else:
            params['page'] = str(page)
        try:
            d = _get(f'{url_base}?{urllib.parse.urlencode(params)}')
        except urllib.error.HTTPError as e:
            if use_cursor and e.code in (400, 422):
                log('  cursor rejected by the API: falling back to page offsets')
                cursor_ok = False
                continue
            raise
        page_items = d.get('data') or []
        pag = d.get('pagination') or {}
        if use_cursor and page_items and all(it.get('id') in seen for it in page_items):
            log('  cursor ignored by the API (page repeated): falling back to page offsets')
            cursor_ok = False
            continue
        if use_cursor:
            cursor_pages += 1
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
        cursor = pag.get('nextCursor')
        page += 1
        if page > max_pages:
            truncated = True
            log('  WARN: max_pages reached, list may be truncated')
            break
        time.sleep(0.04)
    return items, {'reportedTotal': reported, 'pages': pages, 'cursorPages': cursor_pages,
                   'duplicatesRemoved': dups, 'stoppedEarly': stopped, 'truncated': truncated}


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


def newest_ts(i: dict) -> dt.datetime | None:
    vals = [parse_iso(i.get(k)) for k in ('gitHubUpdatedAt', 'updatedAt', 'gitHubClosedAt', 'completedAt')]
    vals = [v for v in vals if v]
    return max(vals) if vals else None


def fetch_closed_scan(wave_start: dt.datetime, wave_end: dt.datetime) -> tuple[list[dict], dict]:
    """Scan closed issues with a linked PR, newest-updated first. Scanning stops
    only after two consecutive pages in which EVERY issue was last touched before
    the Wave start (such issues cannot belong to the Wave). Classification into
    "closed in Wave 11" happens afterwards, in select_closed_in_wave()."""
    log(f'[2/6] Scanning closed+PR issues for Wave {WAVE_NUM} '
        f'({wave_start.isoformat()} → {wave_end.isoformat()})...')
    streak = 0

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
    meta['scanned'] = len(items)
    return items, meta


def resolved_in_wave(i: dict, wave: dict) -> bool | None:
    """The API's own `resolvedInWave` field. True/False when it names a wave,
    None when empty. Accepts a wave id, a wave number, or an object holding
    either, because the exact shape is not documented."""
    v = i.get('resolvedInWave')
    if v in (None, '', False):
        return None
    wid, wnum = wave.get('id'), wave.get('waveNumber')
    if isinstance(v, dict):
        return (wid is not None and wid in v.values()) or \
               any(v.get(k) == wnum for k in ('waveNumber', 'number', 'wave'))
    return v == wid or v == wnum or str(v) == str(wnum)


def select_closed_in_wave(items: list[dict], wave: dict, ws, we) -> tuple[list[dict], dict]:
    """Decide which scanned issues count as "closed in Wave 11" and report the
    result under every plausible definition, so the numbers can be compared with
    the Drips site and the right definition pinned (env WAVE_CLOSED_BASIS =
    resolved | closed | completed | auto)."""
    inwin = lambda ts: ts is not None and ws <= ts <= we
    bases = {
        'resolved':  [i for i in items if resolved_in_wave(i, wave) is True],            # API field resolvedInWave
        'closed':    [i for i in items if inwin(parse_iso(i.get('gitHubClosedAt')))],    # GitHub close time in window
        'completed': [i for i in items if inwin(parse_iso(i.get('completedAt')))],       # Drips completedAt in window
    }

    def stats(lst: list[dict]) -> dict:
        rw = [i for i in lst if i.get('pointsEarned')]
        return {'issues': len(lst), 'rewarded': len(rw),
                'scoredPoints': sum(i['pointsEarned'] for i in rw),
                'totalPoints': sum(points_of(i) for i in rw)}

    st = {k: stats(v) for k, v in bases.items()}
    requested = os.environ.get('WAVE_CLOSED_BASIS', 'auto').lower()
    if requested not in bases:
        has_field = any(i.get('resolvedInWave') is not None for i in items)
        used = 'resolved' if has_field and st['resolved']['rewarded'] else 'closed'
    else:
        used = requested
    chosen = [i for i in bases[used] if i.get('pointsEarned')]
    far = dt.datetime.max.replace(tzinfo=dt.timezone.utc)
    chosen.sort(key=lambda i: parse_iso(i.get('gitHubClosedAt')) or parse_iso(i.get('completedAt')) or far)
    vals = Counter(repr(i.get('resolvedInWave'))[:80] for i in items if i.get('resolvedInWave') is not None)
    sample = next((i for i in chosen), None) or {}
    recon = {
        'basisUsed': used, 'basisRequested': requested, 'bases': st,
        'resolvedInWaveValues': vals.most_common(6),
        'schemaProbe': {'resolvedInWave': repr(sample.get('resolvedInWave'))[:80],
                        'completedAt': sample.get('completedAt'),
                        'assignedApplicantKeys': sorted((sample.get('assignedApplicant') or {}).keys())},
    }
    log(f'  closed-in-wave by definition (rewarded issues / points scored):')
    for k, v in st.items():
        log(f'    {k:9s}: {v["rewarded"]:>6,} issues  {v["scoredPoints"]:>10,} pts{"   <-- used" if k == used else ""}')
    return chosen, recon


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
    per_pt = budget / scored if scored else 0          # earnings are valued on points actually SCORED
    floor_pt = budget / max_pts if max_pts else 0      # reference only: value per point if every open point were scored too
    attributed = sum(c['pointsEarned'] for c in contributors)
    payouts = [{
        'contributor': c['contributor'], 'pointsEarned': c['pointsEarned'],
        'estimatedUSD': round(c['pointsEarned'] * per_pt, 2),                       # points × (budget ÷ total scored points)
        'shareOfScoredPct': round(c['pointsEarned'] / attributed * 100, 3) if attributed else 0,
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
    return {
        'budgetUSD': round(budget, 2),
        'usdPerPoint': round(per_pt, 6),                 # budget ÷ total scored points (earnings basis)
        'usdPerPointAtMax': round(floor_pt, 6),          # budget ÷ (open + closed) points: the lowest it can be
        'projectedFinalScoredPts': projected,
        'projectedUsdPerPoint': round(budget / projected, 6) if projected else 0,
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
    rc = dq.get('reconciliation') or {}
    if rc:
        md += [f'Closed-in-wave basis used: **{rc["basisUsed"]}** (requested: {rc["basisRequested"]})\n',
               '| Definition | Issues | Rewarded | Points scored | Total points |', '|:--|--:|--:|--:|--:|']
        md += [f'| {k} | {v["issues"]:,} | {v["rewarded"]:,} | {v["scoredPoints"]:,} | {v["totalPoints"]:,} |' for k, v in rc['bases'].items()]
        md += ['', f'resolvedInWave values seen: {rc["resolvedInWaveValues"]}', '']
    md += [f'- {k}: {v}' for k, v in dq.items() if k not in ('warnings', 'reconciliation')]
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


# ──────────────────────────── PR REVIEW FLAGS ────────────────────────────
# For every rewarded issue the credited contributor's PR is read from GitHub
# (public data) and checked against simple, explainable rules. The flags are
# review hints for a human, NOT proof of misconduct: a tiny PR can be a genuine
# one-line fix, and a missing "closes #N" is often just a style choice.
GRAPHQL_URL    = 'https://api.github.com/graphql'
GH_TOKEN       = os.environ.get('GH_TOKEN') or os.environ.get('GITHUB_TOKEN') or ''
PR_CACHE       = STATE_DIR / 'pr_cache.json'
PR_MAX_PER_RUN = int(os.environ.get('WAVE_PR_MAX', '600'))
PR_BATCH       = 20
PR_RECHECK_H   = 12          # unmerged PRs are re-read after this long; merged PRs never change
PR_MIN_RATE    = 150         # stop fetching when fewer GraphQL points than this remain
PR_URL_RE      = re.compile(r'github\.com/([^/\s]+)/([^/\s]+)/pull/(\d+)', re.I)
GENERATED_RE   = re.compile(r'(^|/)(package-lock\.json|yarn\.lock|pnpm-lock\.yaml|Cargo\.lock|go\.sum|poetry\.lock|Gemfile\.lock|composer\.lock)$'
                            r'|(^|/)(dist|build|vendor|node_modules)/|\.min\.(js|css)$', re.I)
DOC_RE         = re.compile(r'\.(md|mdx|txt|rst|adoc)$|(^|/)(LICENSE|CHANGELOG|CODEOWNERS|README)[^/]*$', re.I)
GENERIC_TITLE  = re.compile(r'^(update|create|delete|rename)\s+\S+\.\w{1,6}$|^add files via upload$|^patch-?\d*$', re.I)
PR_FIELDS = """number state merged isDraft title bodyText additions deletions changedFiles createdAt closedAt mergedAt
  author{login} mergedBy{login}
  commitCount: commits{totalCount} reviewCount: reviews{totalCount} commentCount: comments{totalCount}
  closingIssuesReferences(first:5){nodes{number repository{nameWithOwner}}}
  files(first:30){nodes{path additions deletions}}
  lastCommit: commits(last:1){nodes{commit{statusCheckRollup{state}}}}"""


def parse_pr_url(url: Any) -> tuple[str, str, int] | None:
    m = PR_URL_RE.search(url or '')
    return (m.group(1), m.group(2), int(m.group(3))) if m else None


def pr_key(o: str, r: str, n: int) -> str:
    return f'{o}/{r}#{n}'.lower()


def gh_graphql(query: str) -> dict[str, Any]:
    if not GH_TOKEN:
        raise RuntimeError('no GitHub token (set GH_TOKEN or GITHUB_TOKEN)')
    body = json.dumps({'query': query}).encode('utf-8')
    delay = 2.0
    for attempt in range(1, 5):
        try:
            req = urllib.request.Request(GRAPHQL_URL, data=body, method='POST', headers={
                'Authorization': f'Bearer {GH_TOKEN}', 'Content-Type': 'application/json',
                'User-Agent': HEADERS['User-Agent']})
            with urllib.request.urlopen(req, timeout=60) as resp:
                return json.loads(resp.read().decode('utf-8'))
        except urllib.error.HTTPError as e:
            if e.code not in RETRY_STATUS or attempt == 4:
                raise
        except (urllib.error.URLError, TimeoutError, ConnectionError, ValueError):
            if attempt == 4:
                raise
        time.sleep(delay)
        delay *= 2
    raise RuntimeError('unreachable')


def compact_pr(p: dict) -> dict[str, Any]:
    body, title = p.get('bodyText') or '', p.get('title') or ''
    refs = sorted({int(x) for x in re.findall(r'#(\d{1,7})', title + ' ' + body)})[:20]
    files = [{'p': f.get('path'), 'a': f.get('additions') or 0, 'd': f.get('deletions') or 0}
             for f in ((p.get('files') or {}).get('nodes') or [])]
    closes = [f'{n["repository"]["nameWithOwner"]}#{n["number"]}'.lower()
              for n in ((p.get('closingIssuesReferences') or {}).get('nodes') or []) if n and n.get('repository')]
    last = ((((p.get('lastCommit') or {}).get('nodes') or [{}])[0] or {}).get('commit') or {}).get('statusCheckRollup') or {}
    return {
        'number': p.get('number'), 'state': (p.get('state') or '').lower(), 'merged': bool(p.get('merged')),
        'draft': bool(p.get('isDraft')), 'title': title, 'bodyLen': len(body), 'refs': refs, 'closes': closes,
        'additions': p.get('additions') or 0, 'deletions': p.get('deletions') or 0,
        'changedFiles': p.get('changedFiles') or 0, 'files': files,
        'author': (p.get('author') or {}).get('login'), 'mergedBy': (p.get('mergedBy') or {}).get('login'),
        'createdAt': p.get('createdAt'), 'closedAt': p.get('closedAt'), 'mergedAt': p.get('mergedAt'),
        'commits': (p.get('commitCount') or {}).get('totalCount', 0),
        'reviews': (p.get('reviewCount') or {}).get('totalCount', 0),
        'comments': (p.get('commentCount') or {}).get('totalCount', 0),
        'ci': last.get('state'),
    }


def fetch_pr_batch(batch: list[tuple[str, str, int]]) -> tuple[dict[str, dict], int | None]:
    parts = [f'p{i}: repository(owner:{json.dumps(o)}, name:{json.dumps(r)}) {{ pullRequest(number:{n}) {{ {PR_FIELDS} }} }}'
             for i, (o, r, n) in enumerate(batch)]
    d = gh_graphql('query { rateLimit { remaining } ' + ' '.join(parts) + ' }')
    data = d.get('data')
    if not data:
        raise RuntimeError('GraphQL error: ' + str((d.get('errors') or [{}])[0].get('message')))
    out = {}
    for i, (o, r, n) in enumerate(batch):
        node = (data.get(f'p{i}') or {}).get('pullRequest')
        out[pr_key(o, r, n)] = compact_pr(node) if node else {'missing': True}
    return out, (data.get('rateLimit') or {}).get('remaining')


def refresh_pr_cache(closed: list[dict], now: dt.datetime) -> tuple[dict, dict]:
    """Read each rewarded issue's PR once (merged PRs are final) and keep them in
    state/pr_cache.json. At most PR_MAX_PER_RUN per run, newest first, so the
    backlog clears over a few runs without exhausting the API rate limit."""
    cache: dict[str, dict] = {}
    if PR_CACHE.exists():
        try:
            cache = json.loads(PR_CACHE.read_text(encoding='utf-8'))
        except (OSError, json.JSONDecodeError):
            log('  WARN: pr_cache.json unreadable, starting empty')
    want: dict[str, tuple] = {}
    for i in closed:
        p = parse_pr_url(i.get('prLink'))
        if p:
            want.setdefault(pr_key(*p), (*p, i.get('gitHubClosedAt') or ''))

    def stale(rec: dict | None) -> bool:
        if not rec:
            return True
        if (rec.get('pr') or {}).get('merged'):
            return False
        at = parse_iso(rec.get('at'))
        return at is None or now - at > dt.timedelta(hours=PR_RECHECK_H)

    todo = sorted((v for k, v in want.items() if stale(cache.get(k))), key=lambda v: v[3], reverse=True)
    stats = {'prsReferenced': len(want), 'fetchedThisRun': 0, 'pending': 0, 'skipped': None, 'rateRemaining': None}
    if todo and not GH_TOKEN:
        stats['skipped'] = 'no GitHub token: PR checks not run'
    elif todo:
        log(f'  PR review: {len(todo)} PRs to read (max {PR_MAX_PER_RUN} this run)')
        done = 0
        try:
            for s in range(0, min(len(todo), PR_MAX_PER_RUN), PR_BATCH):
                batch = [t[:3] for t in todo[s:s + PR_BATCH]]
                res, remaining = fetch_pr_batch(batch)
                for k, rec in res.items():
                    cache[k] = {'at': now.isoformat(), 'pr': rec}
                done += len(batch)
                stats['rateRemaining'] = remaining
                if remaining is not None and remaining < PR_MIN_RATE:
                    log(f'  PR review: GraphQL budget low ({remaining}); stopping for this run')
                    break
                time.sleep(0.2)
        except Exception as e:
            stats['skipped'] = f'PR fetch stopped early: {e}'
            log(f'  WARN: {stats["skipped"]}')
        stats['fetchedThisRun'] = done
    stats['pending'] = sum(1 for k in want if stale(cache.get(k)))
    cache = {k: v for k, v in cache.items() if k in want}          # bound the file size
    write_atomic(PR_CACHE, json.dumps(cache, separators=(',', ':')))
    return cache, stats


def analyze_pr(it: dict, pr: dict | None, title_count: int) -> list[dict]:
    """Rule-based flags {c: code, w: weight, t: text} for one contribution."""
    flags: list[dict] = []
    add = lambda c, w, t: flags.append({'c': c, 'w': w, 't': t})
    if not it['prUrl']:
        add('no_pr', 3, 'No PR is linked to this rewarded issue')
        return flags
    if pr is None:
        return flags                                      # not read yet: unchecked, no flags
    if pr.get('missing'):
        add('pr_unavailable', 2, 'PR could not be loaded (deleted, private or wrong link)')
        return flags
    o, r, _ = parse_pr_url(it['prUrl'])
    if f'{o}/{r}'.lower() != it['repo'].lower():
        add('pr_other_repo', 5, f'PR is in {o}/{r}, not in the issue\'s repo {it["repo"]}')
    author = (pr.get('author') or '')
    if not author:
        add('author_unknown', 1, 'PR author account is deleted or unknown')
    elif author.lower() != it['assignee'].lower():
        add('author_mismatch', 5, f'PR was written by {author}, not by the credited contributor')
    if not pr.get('merged'):
        add('not_merged', 2, f'PR is {pr.get("state") or "unknown"}, not merged')
    lines = (pr.get('additions') or 0) + (pr.get('deletions') or 0)
    nfiles = pr.get('changedFiles') or 0
    if nfiles == 0 or lines == 0:
        add('empty_diff', 5, 'PR changes no lines of code')
    elif lines <= 5 and nfiles <= 1:
        add('tiny_change', 2, f'Very small change ({lines} line{"s" if lines != 1 else ""} in {nfiles} file)')
    paths = [f['p'] for f in pr.get('files') or [] if f.get('p')]
    if paths and len(paths) == nfiles:
        if all(GENERATED_RE.search(p) for p in paths):
            add('generated_only', 2, 'Only lock files or generated/build output changed')
        elif all(DOC_RE.search(p) for p in paths):
            add('docs_only', 2 if it['points'] >= 150 else 1, 'Only documentation files changed')
    n = it['number']
    if f'{it["repo"]}#{n}'.lower() not in (pr.get('closes') or []) and n not in (pr.get('refs') or []):
        add('no_issue_link', 2, 'PR neither closes nor mentions the issue')
    if pr.get('mergedBy') and author and pr['mergedBy'].lower() == author.lower() and not pr.get('reviews'):
        add('self_merged', 1, 'Merged by its own author with no reviews')
    ca, ma = parse_iso(pr.get('createdAt')), parse_iso(pr.get('mergedAt'))
    if ca and ma and (ma - ca).total_seconds() < 300:
        add('fast_merge', 1, 'Merged less than 5 minutes after it was opened')
    if str(pr.get('ci') or '').upper() in ('FAILURE', 'ERROR'):
        add('ci_failed', 1, 'CI checks were failing on the last commit')
    if GENERIC_TITLE.match((pr.get('title') or '').strip()) and lines <= 10:
        add('low_effort_title', 1, 'Default or generic title on a very small change')
    ic = parse_iso(it.get('issueCreatedAt'))
    if ca and ic and ca < ic - dt.timedelta(minutes=1):
        add('pr_before_issue', 1, 'PR was opened before the issue existed')
    if title_count >= 3:
        add('duplicate_title', 1, f'Same PR title used {title_count} times by this contributor')
    return flags


def level_of(risk: int) -> str:
    return 'high' if risk >= 5 else 'medium' if risk >= 3 else 'low' if risk >= 1 else 'clean'


def build_contributions(closed: list[dict], cache: dict, contributors: list[dict], stats: dict, now) -> dict[str, Any]:
    """Per-contributor list of rewarded issues with their PR and review flags.
    Also writes summary counts (flagsHigh, flagsMedium, ...) into each row of
    `contributors` so the contributors table can show them."""
    def rec_of(i: dict):
        p = parse_pr_url(i.get('prLink'))
        return p, ((cache.get(pr_key(*p)) or {}).get('pr') if p else None)

    titles: Counter = Counter()
    for i in closed:
        _, pr = rec_of(i)
        if user_of(i) and pr and not pr.get('missing'):
            titles[(user_of(i).lower(), (pr.get('title') or '').strip().lower())] += 1

    by_user: dict[str, list[dict]] = defaultdict(list)
    for i in closed:
        u = user_of(i)
        if not u:
            continue
        p, pr = rec_of(i)
        it = {'repo': repo_name(i) or '', 'number': i.get('gitHubIssueNumber'), 'title': i.get('title'),
              'complexity': complexity_of(i), 'points': points_of(i), 'pointsEarned': i.get('pointsEarned') or 0,
              'closedAt': i.get('gitHubClosedAt'), 'issueCreatedAt': i.get('gitHubCreatedAt'),
              'prUrl': (f'https://github.com/{p[0]}/{p[1]}/pull/{p[2]}' if p else None), 'assignee': u}
        tc = titles[(u.lower(), (pr.get('title') or '').strip().lower())] if pr and not pr.get('missing') else 0
        flags = analyze_pr(it, pr, tc)
        risk = sum(f['w'] for f in flags)
        analyzed = bool(pr) or not it['prUrl']
        it.update({
            'issueUrl': f'https://github.com/{it["repo"]}/issues/{it["number"]}',
            'analyzed': analyzed, 'flags': flags, 'risk': risk, 'level': level_of(risk) if analyzed else 'unchecked',
            'pr': None if not pr or pr.get('missing') else {
                'number': pr['number'], 'state': 'merged' if pr['merged'] else pr['state'], 'author': pr['author'],
                'mergedBy': pr['mergedBy'], 'additions': pr['additions'], 'deletions': pr['deletions'],
                'changedFiles': pr['changedFiles'], 'files': [f['p'] for f in pr['files'][:8]], 'title': pr['title'],
                'createdAt': pr['createdAt'], 'mergedAt': pr['mergedAt'], 'commits': pr['commits'],
                'reviews': pr['reviews'], 'comments': pr['comments'], 'ci': pr['ci']}})
        del it['assignee'], it['issueCreatedAt']
        by_user[u].append(it)

    rows = {c['contributor']: c for c in contributors}
    out: dict[str, Any] = {}
    for u, items in by_user.items():
        items.sort(key=lambda x: (-x['risk'], x['closedAt'] or ''), reverse=False)
        lv = Counter(x['level'] for x in items)
        at_risk = sum(x['pointsEarned'] for x in items if x['level'] in ('high', 'medium'))
        summary = {'issues': len(items), 'high': lv['high'], 'medium': lv['medium'], 'low': lv['low'],
                   'clean': lv['clean'], 'unchecked': lv['unchecked'], 'pointsAtRisk': at_risk,
                   'riskScore': sum(x['risk'] for x in items)}
        out[u] = {'summary': summary, 'items': items}
        if u in rows:
            rows[u].update({'flagsHigh': lv['high'], 'flagsMedium': lv['medium'], 'flagsLow': lv['low'],
                            'prsUnchecked': lv['unchecked'], 'pointsAtRisk': at_risk})
    return {'generatedAt': now.replace(microsecond=0).isoformat(), 'stats': stats,
            'rules': 'Automated heuristics on public PR data. Flags are review hints, not proof of misconduct.',
            'contributors': out}


def write_flagged_csv(contributions: dict) -> int:
    """flagged_prs.csv: every PR that carries at least one review flag, highest
    risk first. One row per rewarded issue: who it was credited to, who wrote
    the PR, who merged it, in which repo, and why it was flagged. Cells that
    start with = + - @ are prefixed with ' so a spreadsheet cannot run them as
    formulas (PR titles are written by strangers)."""
    cols = ['risk_level', 'risk_score', 'repo', 'issue_number', 'issue_url', 'issue_title', 'credited_contributor',
            'pr_url', 'pr_number', 'pr_author', 'merged_by', 'pr_state', 'additions', 'deletions', 'changed_files',
            'points_earned', 'flags', 'reasons', 'closed_at']
    safe = lambda v: ("'" + str(v)) if isinstance(v, str) and v[:1] in ('=', '+', '-', '@', '\t', '\r') else ('' if v is None else v)
    rows = []
    for user, d in (contributions.get('contributors') or {}).items():
        for x in d['items']:
            if x['risk'] <= 0:
                continue
            pr = x.get('pr') or {}
            rows.append((-x['risk'], x['repo'].lower(), [
                x['level'], x['risk'], x['repo'], x['number'], x['issueUrl'], x['title'], user,
                x['prUrl'], pr.get('number'), pr.get('author'), pr.get('mergedBy'),
                pr.get('state') or ('unavailable' if x['prUrl'] else 'no PR'),
                pr.get('additions'), pr.get('deletions'), pr.get('changedFiles'), x['pointsEarned'],
                '; '.join(f['c'] for f in x['flags']), '; '.join(f['t'] for f in x['flags']), x['closedAt']]))
    rows.sort(key=lambda r: r[:2])
    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(cols)
    for _, _, r in rows:
        w.writerow([safe(v) for v in r])
    write_atomic(ROOT / 'flagged_prs.csv', '\ufeff' + buf.getvalue())       # BOM: Excel reads UTF-8 correctly
    return len(rows)


def build_runs_index() -> None:
    """runs.json: one compact row per stored run (the last KEEP_RUNS), read from
    each run's summary.json, so the dashboard can show and compare them with a
    single fetch. GitHub Pages cannot list a directory, so this index is what
    tells the page which runs exist. Tolerates summaries written by older
    versions of this script (missing keys become null)."""
    def dig(o, *keys):
        for k in keys:
            if not isinstance(o, dict):
                return None
            o = o.get(k)
        return o

    rows = []
    for d in sorted(p for p in RUNS_DIR.iterdir() if p.is_dir()) if RUNS_DIR.exists() else []:
        f = d / 'summary.json'
        if not f.exists():
            continue
        try:
            s = json.loads(f.read_text(encoding='utf-8'))
        except (OSError, json.JSONDecodeError) as e:
            log(f'  WARN: skipped {d.name} in runs.json: {e}')
            continue
        dist = {b.get('complexity'): {'count': b.get('count'), 'points': b.get('currentContribution')}
                for b in (dig(s, 'openIssues', 'distribution') or [])}
        rows.append({
            'dir': d.name, 'runAt': s.get('runAt'),
            'status': dig(s, 'wave', 'status'), 'inWaveWindowNow': dig(s, 'wave', 'inWaveWindowNow'),
            'openIssues': dig(s, 'openIssues', 'total'), 'openPoints': dig(s, 'openIssues', 'totalPoints'),
            'closedIssues': dig(s, 'closedInWave11', 'uniqueIssues'), 'closedPoints': dig(s, 'closedInWave11', 'closedTotalPoints'),
            'maxPoints': s.get('maxTotalPoints'), 'scoredPoints': s.get('scoredPoints'),
            'remainingPoints': s.get('remainingPoints'),
            'newIssues': dig(s, 'newIssuesSinceLastRun', 'newSinceLastRun'),
            'contributors': s.get('contributorsCount'), 'repos': dig(s, 'perRepo', 'totalRepos'),
            'usdPerPoint': dig(s, 'waveEconomics', 'usdPerPoint'),
            'projectedFinal': dig(s, 'waveEconomics', 'projectedFinalScoredPts'),
            'assigned': dig(s, 'applicationPipeline', 'openAssigned'),
            'appsNoAssign': dig(s, 'applicationPipeline', 'openWithAppsNoAssignment'),
            'stale': dig(s, 'applicationPipeline', 'staleAssignmentsCount'),
            'prRatio': dig(s, 'issueQuality', 'prToIssueRatio'), 'bounceRate': dig(s, 'issueQuality', 'bounceRate'),
            'warnings': len(dig(s, 'dataQuality', 'warnings') or []),
            'basis': dig(s, 'dataQuality', 'reconciliation', 'basisUsed'),
            'distribution': dist,
            'top': (s.get('topContributors') or [])[:3],
        })
    write_atomic(ROOT / 'runs.json', json.dumps(
        {'generatedAt': utcnow().replace(microsecond=0).isoformat(), 'keep': KEEP_RUNS, 'runs': rows}, indent=1))
    log(f'  wrote runs.json ({len(rows)} runs)')


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

    # Manual runs (workflow_dispatch) and local runs ALWAYS execute. Scheduled runs
    # skip when the previous run is recent: the workflow fires at :07 and :37 of
    # every even hour so a late or dropped first fire is caught up by the second,
    # and the second must not run twice.
    if os.environ.get('GITHUB_EVENT_NAME') in ('schedule', 'repository_dispatch') and last_state and last_state.get('runAt'):
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

    closed_items, closed_meta = fetch_closed_scan(ws, we)
    closed, recon = select_closed_in_wave(closed_items, wave11, ws, we)
    closed_meta['closedPrInWindow'] = recon['bases']['closed']['issues']

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
    log('  Reviewing PRs of rewarded issues...')
    pr_cache, pr_stats = refresh_pr_cache(closed, started)
    contributions = build_contributions(closed, pr_cache, contributors, pr_stats, started)
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
    if pr_stats.get('skipped'):
        warnings.append('PR review: ' + str(pr_stats['skipped']))
    if pr_stats.get('pending'):
        warnings.append(f'PR review: {pr_stats["pending"]} PRs still waiting to be read (cleared over the next runs)')
    if last_state and last_state.get('openIssuesTotal') and len(open_issues) < 0.5 * last_state['openIssuesTotal']:
        warnings.append(f'open issues fell from {last_state["openIssuesTotal"]:,} to {len(open_issues):,}')
    sc = {k: v['scoredPoints'] for k, v in recon['bases'].items() if v['issues']}
    if len(set(sc.values())) > 1:
        warnings.append('scored points differ by definition of "closed in wave": '
                        + ', '.join(f'{k}={v:,}' for k, v in sc.items()) + f'; using {recon["basisUsed"]}')
    dq = {'reconciliation': recon, 'prReview': pr_stats, 'paginationCursorPages': closed_meta.get('cursorPages', 0),
          'openFetched': len(open_issues), 'openReportedTotal': open_meta['reportedTotal'],
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
    write_atomic(ROOT / 'contributions.json', json.dumps(contributions, separators=(',', ':')))
    log(f'  wrote flagged_prs.csv ({write_flagged_csv(contributions)} flagged PRs)')
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
    build_runs_index()

    log('\n=== RUN SUMMARY ===')
    log(f'Run at (UTC)      : {snap["runAt"]}   in window now: {snap["wave"]["inWaveWindowNow"]}')
    log(f'Open issues       : {snap["openIssues"]["total"]:,}  ({dist["currentTotalPoints"]:,} pts)')
    log(f'Closed in wave    : {closed_metrics["uniqueIssues"]:,}  ({closed_metrics["closedTotalPoints"]:,} pts)')
    log(f'Max / scored / left: {snap["maxTotalPoints"]:,} / {snap["scoredPoints"]:,} / {snap["remainingPoints"]:,}')
    log(f'USD per point     : ${wave_econ["usdPerPoint"]:.6f} (budget ÷ scored points)   floor at max ${wave_econ["usdPerPointAtMax"]:.6f}')
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
