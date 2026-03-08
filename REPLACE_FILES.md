# 📋 Files to Replace in GitHub Repo

## Summary of all changes

| File | Action | Why |
|------|--------|-----|
| `main.py` | **REPLACE** | All fixes + 30 new scrapers + self-training |
| `requirements.txt` | **REPLACE** | Updated pandas version pin |
| `.github/workflows/job_alert.yml` | **REPLACE** | Runs every 2h, cache fix, commits results |
| `model_weights.json` | **ADD NEW** | Initial seed file for adaptive AI weights |
| `results.json` | **ADD NEW** | Empty results file for the dashboard |
| `dashboard/index.html` | **ADD NEW** | Vercel dashboard |
| `dashboard/vercel.json` | **ADD NEW** | Vercel routing config |

---

## Step-by-step instructions

### 1. Replace `main.py`
Drop the new `main.py` into your repo root. Key fixes vs the old version:
- ✅ **SettingWithCopyWarning fixed** — `df.copy()` added in `deduplicate_jobs()`
- ✅ **50 scrapers** (was 20, now +30 Canadian law firms)
- ✅ **Self-training `ModelTrainer`** — updates `model_weights.json` after every run
- ✅ **Writes `results.json`** so the Vercel dashboard can read live data
- ✅ **Richer Telegram alerts** — includes model run #, threshold, and all-time count

### 2. Replace `.github/workflows/job_alert.yml`
This fixes two bugs and adds features:
- ✅ **Runs every 2 hours** (was weekly on Mondays)
- ✅ **Cache conflict fixed** — uses `actions/cache/restore@v4` + `actions/cache/save@v4` as separate steps instead of the combined `actions/cache@v3` (which caused the "Failed to save: Unable to reserve cache" error you saw in the logs)
- ✅ **Commits `results.json` + `model_weights.json` back to the repo** after each run (so the dashboard reads fresh data)
- ✅ **`contents: write` permission** added so the bot can push

### 3. Add `model_weights.json` (new file, repo root)
Seed file for the adaptive model. Will be updated by GitHub Actions after every run.
Also add `model_weights.json` to your **GitHub Actions cache** (already handled in the new workflow).

### 4. Add `results.json` (new file, repo root)
Empty initial structure. Gets populated by the scraper on every run.
> ⚠️ Add `results.json` to `.gitignore` is **NOT** recommended — it needs to be tracked so the Vercel dashboard can read it via GitHub raw URL.

### 5. Add new GitHub Actions secret
In your repo → **Settings → Secrets and variables → Actions**, add:
```
WEIGHTS_FILE = model_weights.json
RESULTS_FILE = results.json
```
(Both are optional — the code defaults to these names if not set.)

### 6. Deploy the Vercel dashboard
1. Create a new folder on your computer called `lexscan-dashboard/`
2. Put `dashboard/index.html` and `dashboard/vercel.json` inside it
3. Go to [vercel.com](https://vercel.com) → **Add New Project**
4. Import this folder (or push it as a new GitHub repo and import that)
5. Vercel will auto-detect it as a static site — no build command needed
6. Deploy!

The dashboard reads from:
```
https://raw.githubusercontent.com/shaheersinn/law-associate-job-alerts/main/results.json
```
This is hardcoded at the top of `dashboard/index.html`. If you fork the repo under a different name, update the `GITHUB_USER` and `REPO_NAME` variables in the dashboard file.

---

## Bugs fixed (from log analysis)

### Bug 1: `SettingWithCopyWarning` (line 321 of old main.py)
```
A value is trying to be set on a copy of a slice from a DataFrame.
  df['CLEAN_URL'] = df['URL'].apply(lambda x: str(x).split('?')[0])
```
**Fix:** Added `df = df.copy()` at the top of `deduplicate_jobs()`.

### Bug 2: Cache conflict ("Failed to save: Unable to reserve cache")
```
Failed to save: Unable to reserve cache with key job-history-Linux,
another job may be creating this cache.
```
**Fix:** Split `actions/cache@v3` (combined) into separate `actions/cache/restore@v4` and `actions/cache/save@v4` steps. Also bumped to `@v4` for stability.

### Bug 3: Only weekly schedule
The workflow ran once a week. **Fixed to every 2 hours** (`'0 */2 * * *'`).

### Non-bug note: Only 2 jobs found
The original run found 2 jobs. This is partly because:
- Many law firm sites use JavaScript-rendered job boards (not crawlable by requests)
- The direct scraper only went 1 page deep (`MAX_PAGES = 1`)

**Improvements in new version:**
- `MAX_PAGES = 2` for deeper crawls
- 30 additional sites added
- JobSpy covers Indeed/LinkedIn/Google aggregators

---

## How self-training works

After every run, `main.py` calls `train_model()` which:

1. **Site productivity tracking** — notes which domains returned valid jobs; boosts their future priority
2. **Score threshold adaptation** — if 0 jobs found, lowers threshold by 1; if >25 found, raises by 1
3. **Keyword weight evolution** — words that appear in matched job titles get their weight boosted (capped at 20)
4. **Run history** — last 50 runs stored; visible in the dashboard bar chart

All state is saved in `model_weights.json` which GitHub Actions caches between runs.

The model does **not** call any external AI API — it is a deterministic, rules-based adaptive scorer. This keeps it fast, free, and transparent.

---

## Telegram setup reminder

Make sure these are set in GitHub Actions secrets:
- `TELEGRAM_BOT_TOKEN` — from @BotFather
- `TELEGRAM_CHAT_ID`   — your personal chat ID (use @userinfobot to find it)
- `RESULTS_WANTED`     — optional, defaults to 30

Telegram alerts now include:
- Model run number
- Adaptive threshold used
- All-time jobs found
- Per-job score in brackets
