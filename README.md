# Drips Stellar Wave 11 — Hourly Tracker Bot

A self-contained Python bot that scrapes the [Drips Wave API](https://www.drips.network/wave/stellar/issues)
once an hour via a GitHub Actions cron, and tracks the **Wave 11** lifecycle:

- New issues opened since the last hourly run
- Total open issues + **expected max Wave 11 points** (`Σ base × 4`)
- Points distribution (small / medium / large)
- **Unique closed-and-rewarded issues** whose `gitHubClosedAt` falls inside the
  Wave 11 time window — i.e. issues a contributor was actually paid for
- Per-contributor points earned inside Wave 11 (running total)
- Estimated total **score scored so far** in Wave 11

> Not a maintainer-leaderboard bot — it's a focused Wave 11 issue/rewards tracker.

---

## File layout

```
drips-wave-bot/
├── bot.py                        # main script (stdlib only — no pip deps)
├── requirements.txt              # empty (stdlib only)
├── README.md                     # this file
├── .github/workflows/hourly.yml  # GitHub Actions cron — runs every hour
├── state/                        # committed by the bot each run
│   ├── last_run.json             # compact state of last run (for delta detection)
│   └── history.jsonl             # append-only time-series (one JSON line per run)
└── runs/                         # committed by the bot each run
    └── <YYYY-MM-DDTHH-MMZ>/
        ├── summary.json          # structured snapshot of this run
        ├── summary.md            # human-readable summary
        ├── contributors.json     # full per-contributor table
        ├── closed_in_wave.json   # full list of rewarded issues closed in Wave 11 window
        └── open_issues_sample.json   # first 50 open issues (for debugging)
```

## How it works

Each hourly run:

1. **Loads `state/last_run.json`** (if present) — used for delta detection (new issues opened since the previous run).
2. **Fetches Wave 11 metadata** — start/end dates, status, budget. `GET /api/wave-programs/{pid}/waves`.
3. **Fetches all open Wave issues** — paginates `GET /api/issues?state=open&waveProgramId=...&page=N&limit=100` until exhausted. Today: ~9,639 issues across ~97 pages.
4. **Fetches closed-and-PR issues sorted by `updatedAt` desc**, paginating with an **early-stop**: once every item in a page has `gitHubClosedAt < wave11_start`, we stop fetching. This keeps the closed-issue scan bounded — during Wave 11 it's typically a few hundred items, not the full 360k+.
5. **Computes**:
   - distribution by base points (`small=100`, `medium=150`, `large=200`) → multiplied by 4 (active Wave multiplier) → contribution per bucket
   - **Max total Wave 11 points** = `Σ base × 4` across all open issues
   - per-contributor aggregation: `Σ pointsEarned` by `assignedApplicant.gitHubUsername` over the closed-and-rewarded-in-window set
   - new issues opened since last run (set-diff by issue ID, then filtered by `gitHubCreatedAt >= last_run_ts`)
6. **Writes timestamped snapshot** to `runs/<UTC-timestamp>/`.
7. **Appends a compact row** to `state/history.jsonl` (one JSON line per run — easy to chart over time).
8. **Overwrites `state/last_run.json`** with the current compact state.
9. **Prints a concise summary** to stdout (which GitHub Actions captures in the workflow log).

## Pre-wave vs in-wave vs post-wave behavior

The bot runs every hour regardless of Wave status:

- **Pre-wave (status=`upcoming`):** Tracks open-issue growth and new issues being added to the Wave pool. `closedRewardedInWave11` will be 0.
- **In-wave (status=`active`, `now ∈ [start, end]`):** Same + tracks real-time rewards — every hour you'll see how many points have been scored so far in Wave 11 and who the current top contributors are.
- **Post-wave (status=`ended`):** Final tally is preserved. The bot keeps running and recording open-issue counts (since closed issues from Wave 11 stay in the window), but `totalPointsScored` will plateau.

To switch the bot to a future Wave (e.g. Wave 12 after Wave 11 ends), change `WAVE_NUM` at the top of `bot.py`.

## Setup

1. **Create a new GitHub repo** (e.g. `your-user/drips-wave-bot`) and push the contents of this `drips-wave-bot/` directory to its default branch:

   ```bash
   cd drips-wave-bot
   git init
   git add .
   git commit -m "init: drips wave 11 hourly bot"
   git branch -M main
   git remote add origin git@github.com:your-user/drips-wave-bot.git
   git push -u origin main
   ```

2. **Enable Actions** — on GitHub, go to *Settings → Actions → General*, ensure "Allow all actions and reusable workflows" is selected.

3. **(Optional) Set webhook secret** — if you want each hourly run to push a Slack/Discord/Telegram summary:
   - Go to *Settings → Secrets and variables → Actions → New repository secret*
   - Name: `WAVE_WEBHOOK_URL`
   - Value: your incoming webhook URL (Slack-compatible — Discord needs a Slack-bridge URL or a different payload format)

4. **(Optional) Set `PAT_TOKEN`** — only needed if you want the bot's commits to trigger other workflows. Generate a Personal Access Token with `repo` scope at <https://github.com/settings/tokens>, save it as the repo secret `PAT_TOKEN`. If unset, the workflow falls back to the default `GITHUB_TOKEN` (which is fine for the bot itself).

5. **First run** — trigger manually: GitHub Actions tab → "Hourly Wave 11 Tracker" → *Run workflow*. After that the cron schedule (`0 * * * *`) takes over and runs at the top of every hour UTC.

## Local run

```bash
python bot.py
```

(No install step needed — uses only the Python standard library.)

## Reading the data

- **Latest snapshot:** `runs/<latest-timestamp>/summary.md`
- **Time-series chart fodder:** `state/history.jsonl` — one JSON line per hourly run, with `runAt`, `openIssuesTotal`, `maxTotalWave11Points`, `totalPointsScored`, `newIssuesSinceLastRun`, `topContributor`, etc. Load with `pandas.read_json('state/history.jsonl', lines=True)` to plot.
- **Workflow log:** GitHub Actions → "Hourly Wave 11 Tracker" → latest run → job log. The bot prints a clean summary block at the end of every run.

## Caveats

- **API courtesy:** The bot fetches ~100 pages of open issues + up to a few hundred closed-issue pages per run. At 0.04s sleep between pages, a full run takes ~30–60 seconds. The Wave API is hosted behind Cloudflare and is generous but not infinite — please don't drop the cron interval below 1 hour.
- **Repo size growth:** Each run commits ~50–500 KB of summary files. Over a year of hourly runs (~8,760 runs) that's roughly 1–4 GB. Either prune old `runs/` periodically (e.g. `git rm -r runs/2025-*`) or rely on the artifact retention (90 days) for full dumps and only keep `summary.json` + `summary.md` in git.
- **Wave window filter:** The bot considers an issue "rewarded in Wave 11" iff `wave11_start ≤ gitHubClosedAt ≤ wave11_end` AND `pointsEarned != null`. If the Wave team backdates or postdates points, this filter may miss/over-count edge cases.
