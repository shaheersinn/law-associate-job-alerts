#!/usr/bin/env python3
"""
Law Associate Job Scraper — Enhanced Edition
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Fixes applied vs original:
  ✓ SettingWithCopyWarning — use df.copy() before mutations in deduplicate_jobs()
  ✓ Cache conflict — workflow now uses separate restore/save steps
  ✓ Results exported to results.json for the Vercel dashboard
  ✓ model_weights.json — adaptive self-training every run (updates weights,
    threshold, and site productivity automatically)
  ✓ 30 new Canadian law-firm scrapers (50 total direct-scrape targets)
  ✓ Richer Telegram alerts with per-run stats
  ✓ Auto-retry on transient HTTP errors
"""

from __future__ import annotations

import os
import re
import json
import time
import logging
import concurrent.futures
from urllib.parse import urljoin, urlparse
from datetime import datetime, timezone
from typing import List, Dict, Any

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup

try:
    from jobspy import scrape_jobs
except ImportError:
    scrape_jobs = None

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
log = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
# 1. CONFIGURATION
# ─────────────────────────────────────────────────────────────────────────────

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept":          "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-CA,en-US;q=0.9,en;q=0.8",
    "Connection":      "keep-alive",
}

HISTORY_FILE      = os.environ.get("HISTORY_FILE",       "job_history.json")
WEIGHTS_FILE      = os.environ.get("WEIGHTS_FILE",        "model_weights.json")
RESULTS_FILE      = os.environ.get("RESULTS_FILE",        "results.json")
RESULTS_WANTED    = int(os.environ.get("RESULTS_WANTED") or "30")

# ─────────────────────────────────────────────────────────────────────────────
# 2. COMPILED REGEX
# ─────────────────────────────────────────────────────────────────────────────

RE_ASSOCIATE = re.compile(
    r"\b(associate|lawyer|counsel|juriste|avocat|attorney|solicitor)\b",
    re.IGNORECASE,
)
RE_STUDENT = re.compile(
    r"\b(articling|student|summer|clerk|stagiaire)\b",
    re.IGNORECASE,
)
RE_BLOCKED_TITLE = re.compile(
    r"\b(senior|partner|director|manager|vp|president|chair|head|principal|"
    r"c-suite|executive|paralegal|assistant|clerk\s+typist|technician|driver|"
    r"warehouse|sales|marketing|receptionist|mid-?level|intermediate|law\s+clerk)\b",
    re.IGNORECASE,
)
# FIX: no anchors — re.search() catches partial matches like "Join Us careers..."
RE_NAV_LINKS = re.compile(
    r"\b(our\s+team|profiles?|meet\s+our|join\s+us|careers?|student\s+programs?|"
    r"summer\s+programs?|articling\s+programs?|recruitment|who\s+we\s+are|about\s+us|"
    r"attorney\s+advertising|terms|privacy|search|menu|home|summer\s+recruitment|"
    r"our\s+summer\s+students|articling)\b",
    re.IGNORECASE,
)
RE_BAD_URLS = re.compile(
    r"(/who-we-are/|/our-team/|/profiles/|/people/|/attorney-advertising|"
    r"students\.cassels\.com|/student-programs/|/articling-program|/summer-program)",
    re.IGNORECASE,
)
RE_EXP_KILLER = re.compile(
    r"\b((?:minimum|at least|over)\s+)?(3|4|5|6|7|8|9|10)(\+|\s*(-|to)\s*\d+)?\s*years",
    re.IGNORECASE,
)
RE_SENIOR_ROLE = re.compile(r"\b(senior|mid-level|intermediate)\s+associate", re.IGNORECASE)

_LOCATIONS = [
    r"\bOntario\b", r"\bAlberta\b", r"\bAB\b", r"\bON\b",
    r"\bToronto\b", r"\bOttawa\b", r"\bMississauga\b", r"\bBrampton\b", r"\bHamilton\b",
    r"\bLondon\b", r"\bMarkham\b", r"\bVaughan\b", r"\bKitchener\b", r"\bWindsor\b",
    r"\bCalgary\b", r"\bEdmonton\b", r"\bRed Deer\b", r"\bLethbridge\b", r"\bSt\.? Albert\b",
    r"\bMississauga\b", r"\bNorth York\b", r"\bEtobicoke\b", r"\bScarborough\b",
]
RE_LOCATIONS     = re.compile("|".join(_LOCATIONS), re.IGNORECASE)
RE_BAD_LOCATIONS = re.compile(
    r"\b(Vancouver|British Columbia|BC|Montreal|Quebec|QC|Halifax|Nova Scotia|"
    r"Winnipeg|Manitoba|MB|Saskatchewan|SK|New Brunswick|NB|PEI|Newfoundland|NL)\b",
    re.IGNORECASE,
)


# ─────────────────────────────────────────────────────────────────────────────
# 3. ADAPTIVE MODEL WEIGHTS (SELF-TRAINING)
# ─────────────────────────────────────────────────────────────────────────────

DEFAULT_WEIGHTS: Dict[str, Any] = {
    "version":           2,
    "training_runs":     0,
    "last_trained":      None,
    "score_threshold":   5,
    "keyword_weights": {
        "associate": 10, "lawyer": 8, "counsel": 7,
        "attorney": 8,  "solicitor": 6, "juriste": 6,
        "articling": 5, "student": 4,  "summer": 3,
    },
    "site_productivity": {},   # domain → {hits, runs, rate, last_hit}
    "run_history": [],         # last 50 run summaries
    "total_jobs_found":  0,
}


def load_weights() -> Dict[str, Any]:
    if os.path.exists(WEIGHTS_FILE):
        try:
            with open(WEIGHTS_FILE) as f:
                data = json.load(f)
            # Merge any missing keys from DEFAULT_WEIGHTS
            for k, v in DEFAULT_WEIGHTS.items():
                if k not in data:
                    data[k] = v
            return data
        except Exception as e:
            log.warning(f"Could not load weights: {e}. Using defaults.")
    return dict(DEFAULT_WEIGHTS)


def save_weights(weights: Dict[str, Any]) -> None:
    try:
        with open(WEIGHTS_FILE, "w") as f:
            json.dump(weights, f, indent=2, default=str)
    except Exception as e:
        log.warning(f"Could not save weights: {e}")


def train_model(
    weights: Dict[str, Any],
    jobs_found: List[dict],
    sites_scanned: List[str],
    run_timestamp: str,
) -> Dict[str, Any]:
    """
    Adaptive self-training:
    1. Update site productivity scores based on which domains produced hits.
    2. Dynamically adjust score_threshold (up if too many, down if zero results).
    3. Update keyword weights from matching job titles.
    4. Append run summary to run_history.
    """
    hit_domains = set()
    for job in jobs_found:
        try:
            domain = urlparse(job.get("URL", "")).netloc.replace("www.", "")
            hit_domains.add(domain.split(".")[0].lower())
        except Exception:
            pass

    for site_url in sites_scanned:
        try:
            domain = urlparse(site_url).netloc.replace("www.", "")
            key    = domain.split(".")[0].lower()
            entry  = weights["site_productivity"].setdefault(
                key, {"hits": 0, "runs": 0, "rate": 0.0, "last_hit": None}
            )
            entry["runs"] += 1
            if key in hit_domains:
                entry["hits"]     += 1
                entry["last_hit"]  = run_timestamp
            entry["rate"] = round(entry["hits"] / max(entry["runs"], 1), 3)
        except Exception:
            pass

    # Adjust threshold
    n = len(jobs_found)
    threshold = weights["score_threshold"]
    if n == 0 and threshold > 3:
        threshold -= 1
        log.info(f"[Train] No results → lowering score threshold to {threshold}")
    elif n > 25 and threshold < 10:
        threshold += 1
        log.info(f"[Train] Many results → raising score threshold to {threshold}")
    weights["score_threshold"] = threshold

    # Update keyword weights from job titles
    kw = weights["keyword_weights"]
    for job in jobs_found:
        title = str(job.get("TITLE", "")).lower()
        for word in kw:
            if word in title:
                kw[word] = min(kw[word] + 1, 20)

    # Append run summary
    summary = {
        "timestamp":   run_timestamp,
        "jobs_found":  n,
        "sites_hit":   list(hit_domains),
        "threshold":   weights["score_threshold"],
    }
    weights["run_history"] = (weights["run_history"] + [summary])[-50:]  # keep last 50
    weights["training_runs"]  += 1
    weights["total_jobs_found"] = weights.get("total_jobs_found", 0) + n
    weights["last_trained"]    = run_timestamp

    log.info(
        f"[Train] Run #{weights['training_runs']} complete. "
        f"Jobs: {n}. Threshold: {weights['score_threshold']}. "
        f"Total all-time: {weights['total_jobs_found']}"
    )
    return weights


# ─────────────────────────────────────────────────────────────────────────────
# 4. JOB SCORER (uses adaptive threshold from weights)
# ─────────────────────────────────────────────────────────────────────────────

class JobScorer:
    def __init__(self, weights: Dict[str, Any]):
        self.threshold = weights.get("score_threshold", 5)
        self.kw        = weights.get("keyword_weights", DEFAULT_WEIGHTS["keyword_weights"])

    def score_job(self, title: str, description: str, url: str) -> tuple[bool, str, int]:
        title     = str(title).strip()
        desc      = str(description).strip()
        url_l     = str(url).lower()
        full_text = (title + " " + desc).lower()

        # A. Fast-fail checks
        if RE_NAV_LINKS.search(title):
            return False, "Nav Link", -100
        if RE_BAD_URLS.search(url_l):
            return False, "Bad URL", -100
        if RE_BLOCKED_TITLE.search(title):
            return False, "Blocked Title", -100
        if RE_EXP_KILLER.search(full_text) or RE_SENIOR_ROLE.search(full_text):
            return False, "Too Senior", -100

        # B. Categorise
        category = "Unknown"
        score    = 0
        if RE_ASSOCIATE.search(title):
            category = "Associate"
            score   += self.kw.get("associate", 10)
        elif RE_STUDENT.search(title):
            category = "Student"
            score   += self.kw.get("articling", 5)
        else:
            return False, "Not Legal", -100

        # C. Location
        if not RE_LOCATIONS.search(full_text):
            if RE_BAD_LOCATIONS.search(full_text):
                return False, "Wrong Location", -100
            score -= 5

        # D. Content quality
        if desc and "apply" not in full_text and "resume" not in full_text and "contact" not in full_text:
            score -= 3

        return (score >= self.threshold), category, score


# ─────────────────────────────────────────────────────────────────────────────
# 5. HTTP HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def get_session() -> requests.Session:
    s = requests.Session()
    retry = Retry(total=3, backoff_factor=1.5, status_forcelist=[429, 500, 502, 503, 504])
    s.mount("https://", HTTPAdapter(max_retries=retry))
    s.mount("http://",  HTTPAdapter(max_retries=retry))
    s.headers.update(_HEADERS)
    return s


def clean_html(html: str) -> str:
    soup = BeautifulSoup(html, "html.parser")
    for tag in soup(["script", "style", "nav", "footer", "header", "noscript", "aside"]):
        tag.extract()
    return " ".join(soup.get_text(separator=" ").split())


# ─────────────────────────────────────────────────────────────────────────────
# 6. DIRECT SITE SCRAPER
# ─────────────────────────────────────────────────────────────────────────────

def scrape_site(url: str, scorer: JobScorer) -> List[dict]:
    session    = get_session()
    found_jobs: List[dict] = []
    visited    = set()
    queue      = [url]
    domain     = urlparse(url).netloc.replace("www.", "")
    MAX_PAGES  = 2  # slightly deeper than before

    while queue and MAX_PAGES > 0:
        curr = queue.pop(0)
        if curr in visited:
            continue
        visited.add(curr)
        MAX_PAGES -= 1

        try:
            resp = session.get(curr, timeout=12)
            if resp.status_code != 200:
                continue
            soup = BeautifulSoup(resp.text, "html.parser")

            for a in soup.find_all("a", href=True):
                text = a.get_text(" ", strip=True)
                href = urljoin(curr, a["href"])

                if len(text) < 4 or RE_NAV_LINKS.search(text) or RE_BAD_URLS.search(href):
                    continue

                if (RE_ASSOCIATE.search(text) or RE_STUDENT.search(text)) \
                        and not RE_BLOCKED_TITLE.search(text):
                    if href not in visited:
                        try:
                            job_resp = session.get(href, timeout=10)
                            if job_resp.status_code == 200:
                                desc        = clean_html(job_resp.text)
                                is_fit, cat, score = scorer.score_job(text, desc, href)
                                if is_fit:
                                    found_jobs.append({
                                        "TITLE":    text,
                                        "COMPANY":  domain.split(".")[0].title(),
                                        "URL":      href,
                                        "CATEGORY": cat,
                                        "SCORE":    score,
                                        "SOURCE":   "direct",
                                    })
                                    visited.add(href)
                        except Exception:
                            pass
        except Exception:
            pass

    return found_jobs


def run_direct_scrape(urls: List[str], scorer: JobScorer) -> pd.DataFrame:
    all_jobs: List[dict] = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
        futures = {executor.submit(scrape_site, u, scorer): u for u in urls}
        for future in concurrent.futures.as_completed(futures):
            try:
                data = future.result()
                if data:
                    all_jobs.extend(data)
            except Exception:
                pass
    return pd.DataFrame(all_jobs) if all_jobs else pd.DataFrame()


# ─────────────────────────────────────────────────────────────────────────────
# 7. AGGREGATOR SCRAPER (JobSpy)
# ─────────────────────────────────────────────────────────────────────────────

def scrape_jobspy_wrapper(scorer: JobScorer) -> pd.DataFrame:
    if scrape_jobs is None:
        log.warning("python-jobspy not installed; skipping aggregators.")
        return pd.DataFrame()

    log.info("  Scraping Aggregators (ON & AB)...")
    search_term = "lawyer associate"
    locations   = ["Ontario, Canada", "Alberta, Canada"]
    all_rows: List[dict] = []

    for loc in locations:
        try:
            jobs = scrape_jobs(
                site_name=["indeed", "linkedin", "google"],
                search_term=search_term,
                google_search_term=(
                    f'{search_term} "0-2 years" -senior -warehouse -driver in {loc}'
                ),
                location=loc,
                results_wanted=RESULTS_WANTED,
                hours_old=168,
                country_indeed="Canada",
                linkedin_fetch_description=True,
                verbose=0,
            )
            if jobs is not None and not jobs.empty:
                jobs.columns = [c.upper() for c in jobs.columns]
                for _, row in jobs.iterrows():
                    title   = str(row.get("TITLE",       ""))
                    desc    = str(row.get("DESCRIPTION", ""))
                    url     = str(row.get("JOB_URL",     ""))
                    company = str(row.get("COMPANY",     ""))
                    is_fit, cat, score = scorer.score_job(title, desc, url)
                    if is_fit:
                        all_rows.append({
                            "TITLE":    title,
                            "COMPANY":  company,
                            "URL":      url,
                            "CATEGORY": cat,
                            "SCORE":    score,
                            "SOURCE":   "jobspy",
                        })
            time.sleep(2)
        except Exception as e:
            log.warning(f"JobSpy error for {loc}: {e}")

    return pd.DataFrame(all_rows) if all_rows else pd.DataFrame()


# ─────────────────────────────────────────────────────────────────────────────
# 8. TARGET URLS — 50 DIRECT SCRAPERS (original 20 + 30 new)
# ─────────────────────────────────────────────────────────────────────────────

def get_target_urls() -> List[str]:
    return [
        # ── Original 20 ──────────────────────────────────────────────────────
        "https://www.joinblakes.com/careers/associates/",
        "https://www.bennettjones.com/en/Careers/Legal-Professionals",
        "https://www.fasken.com/en/careers/lawyers",
        "https://gowlingwlg.com/en/careers/current-opportunities/",
        "https://www.stikeman.com/en/careers/legal",
        "https://www.dwpv.com/en/Careers/Lawyers",
        "https://www.mccarthy.ca/en/careers/lawyers",
        "https://www.torys.com/careers/lawyers",
        "https://www.goodmans.ca/careers/associates",
        "https://www.blg.com/en/careers/current-opportunities",
        "https://www.millerthomson.com/en/careers/lawyers/",
        "https://cassels.com/join-us/career-opportunities-lawyers/",
        "https://www.airdberlis.com/join-us/current-opportunities",
        "https://www.lerners.ca/careers/lawyers/",
        "https://www.litigate.com/careers/lawyers",
        "https://www.wildlaw.ca/careers/lawyers/",
        "https://www.osler.com/en/careers/opportunities",
        "https://www.zsa.ca/job-board/",
        "https://thecounselnetwork.com/job-search/",
        "https://legaljobs.ca/jobs/",

        # ── 30 NEW ────────────────────────────────────────────────────────────
        # Big national/international firms with Canadian offices
        "https://www.dentons.com/en/careers/find-a-job",
        "https://www.nortonrosefulbright.com/en-ca/careers/job-listings",
        "https://www.dlapiper.com/en/canada/careers/",
        "https://www.bakermckenzie.com/en/careers/search",
        "https://www.hklaw.com/en/careers/",                         # Holland & Knight (CDN)

        # Ontario-focused firms
        "https://www.weirfoulds.com/careers/lawyers-and-law-students/",
        "https://www.tgf.ca/join-us/opportunities/",
        "https://www.blaney.com/en/careers/",
        "https://www.foglers.com/career-opportunities/",
        "https://www.singleton.com/careers/",
        "https://www.paliare.com/careers/",
        "https://www.skylaw.ca/careers/",
        "https://www.ravenlaw.com/careers/",
        "https://www.cohenhighley.com/about/careers/",
        "https://www.nelligan.ca/careers/",
        "https://www.hicksmorley.com/careers/",
        "https://www.glaholt.com/careers/",
        "https://www.shibleyrighton.com/firm/careers/",
        "https://www.carters.ca/careers/",
        "https://www.emond.ca/careers/",
        "https://www.paliareroland.com/careers/",

        # Alberta-focused firms
        "https://www.fieldlaw.com/careers/associate-opportunities/",
        "https://www.bdplaw.com/careers/",
        "https://www.parlee.com/careers/",
        "https://www.mross.com/about-us/careers/",
        "https://www.jssh.ca/careers/",
        "https://www.hendersonheinrichs.com/careers/",

        # Job boards & aggregators (direct scrape of listing pages)
        "https://www.oba.org/Careers",
        "https://www.cba.org/For-Lawyers/Employment",
        "https://www.legaljobsboard.com/jobs?location=canada&q=associate+lawyer",
        "https://ca.talent.com/jobs?k=associate+lawyer&l=ontario",
    ]


# ─────────────────────────────────────────────────────────────────────────────
# 9. DEDUPLICATION  (BUG FIX: df.copy() prevents SettingWithCopyWarning)
# ─────────────────────────────────────────────────────────────────────────────

def deduplicate_jobs(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    df = df.copy()  # ← FIX: prevents SettingWithCopyWarning

    def make_sig(row: pd.Series) -> str:
        t = re.sub(r"\W+", "", str(row["TITLE"]).lower())
        c = re.sub(r"\W+", "", str(row["COMPANY"]).lower())
        return f"{t}_{c}"

    df["SIG"]       = df.apply(make_sig, axis=1)
    df              = df.drop_duplicates(subset=["SIG"])
    df["CLEAN_URL"] = df["URL"].apply(lambda x: str(x).split("?")[0])
    df              = df.drop_duplicates(subset=["CLEAN_URL"])
    return df.drop(columns=["SIG", "CLEAN_URL"])


# ─────────────────────────────────────────────────────────────────────────────
# 10. RESULTS JSON (persists all runs for the Vercel dashboard)
# ─────────────────────────────────────────────────────────────────────────────

def append_results(jobs: List[dict], run_ts: str, weights: Dict[str, Any]) -> None:
    if os.path.exists(RESULTS_FILE):
        try:
            with open(RESULTS_FILE) as f:
                data = json.load(f)
        except Exception:
            data = {"runs": []}
    else:
        data = {"runs": []}

    run_entry = {
        "run_id":       run_ts.replace(":", "").replace("-", "").replace(" ", "_"),
        "timestamp":    run_ts,
        "jobs_found":   len(jobs),
        "training_run": weights.get("training_runs", 0),
        "threshold":    weights.get("score_threshold", 5),
        "jobs":         jobs,
    }

    data["runs"] = (data["runs"] + [run_entry])[-200:]  # keep last 200 runs
    data["last_updated"]    = run_ts
    data["total_all_time"]  = weights.get("total_jobs_found", 0)
    data["training_runs"]   = weights.get("training_runs", 0)
    data["score_threshold"] = weights.get("score_threshold", 5)

    try:
        with open(RESULTS_FILE, "w") as f:
            json.dump(data, f, indent=2, default=str)
        log.info(f"✓ Results saved to {RESULTS_FILE}")
    except Exception as e:
        log.warning(f"Could not save results: {e}")


# ─────────────────────────────────────────────────────────────────────────────
# 11. TELEGRAM ALERT
# ─────────────────────────────────────────────────────────────────────────────

def send_telegram(df: pd.DataFrame, weights: Dict[str, Any]) -> None:
    bot_token = os.environ.get("TELEGRAM_BOT_TOKEN")
    chat_id   = os.environ.get("TELEGRAM_CHAT_ID")

    if not (bot_token and chat_id):
        print("\n[!] TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID missing. No alert sent.")
        return

    associates = df[df["CATEGORY"] == "Associate"]
    students   = df[df["CATEGORY"] == "Student"]
    now_str    = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    lines = [
        f"<b>⚖️ Law Jobs — ON/AB | {now_str}</b>",
        f"<b>{len(df)} new listing(s) found</b>",
        f"<i>Model run #{weights.get('training_runs', '?')} · "
        f"Threshold: {weights.get('score_threshold', 5)} · "
        f"All-time: {weights.get('total_jobs_found', 0)} jobs</i>",
    ]

    if not associates.empty:
        lines.append("\n<b>🏛 ASSOCIATES / LAWYERS (0-2 Yrs)</b>")
        for _, row in associates.iterrows():
            score_str = f" [score:{row.get('SCORE','')}]" if row.get("SCORE") else ""
            lines.append(
                f"• <a href=\"{row['URL']}\">{row['TITLE']}</a> "
                f"— {row['COMPANY']}{score_str}"
            )

    if not students.empty:
        lines.append("\n<b>🎓 STUDENTS / ARTICLING</b>")
        for _, row in students.iterrows():
            lines.append(
                f"• <a href=\"{row['URL']}\">{row['TITLE']}</a> — {row['COMPANY']}"
            )

    if df.empty:
        lines.append("\n<i>No new positions this run. The model will lower its threshold next cycle.</i>")

    full_message = "\n".join(lines)
    chunks  = [full_message[i : i + 4096] for i in range(0, len(full_message), 4096)]
    api_url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    success = True

    for chunk in chunks:
        payload = {
            "chat_id":                  chat_id,
            "text":                     chunk,
            "parse_mode":               "HTML",
            "disable_web_page_preview": True,
        }
        try:
            resp = requests.post(api_url, json=payload, timeout=10)
            if not resp.ok:
                print(f"✗ Telegram error: {resp.status_code} — {resp.text}")
                success = False
        except Exception as e:
            print(f"✗ Telegram request failed: {e}")
            success = False

    if success:
        print("✓ Telegram alert sent.")


# ─────────────────────────────────────────────────────────────────────────────
# 12. MAIN
# ─────────────────────────────────────────────────────────────────────────────

def main() -> None:
    run_ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    print("=" * 60)
    print("Law Associate Job Scraper — Enhanced Edition")
    print(f"Run timestamp : {run_ts}")
    print("=" * 60)

    # Load adaptive model weights
    weights = load_weights()
    scorer  = JobScorer(weights)
    target_urls = get_target_urls()

    print(f"\n[Scraper] Direct targets : {len(target_urls)} sites")
    print(f"[Model]   Score threshold: {scorer.threshold}")
    print(f"[Model]   Training run #  : {weights['training_runs']}")
    print()

    # 1. Scrape
    print("── Direct scrape ─────────────────────────────────────────")
    for u in target_urls:
        print(f"  Scanning: {urlparse(u).netloc.replace('www.', '')} ...")
    df_direct = run_direct_scrape(target_urls, scorer)

    print("\n── Aggregators (JobSpy) ─────────────────────────────────")
    df_agg = scrape_jobspy_wrapper(scorer)

    # 2. Combine & deduplicate
    combined    = pd.concat([df_direct, df_agg], ignore_index=True, sort=False)
    unique_jobs = deduplicate_jobs(combined)

    # 3. History filter
    if os.path.exists(HISTORY_FILE):
        with open(HISTORY_FILE) as f:
            history_ids: set = set(json.load(f))
    else:
        history_ids = set()

    final_jobs: List[dict] = []
    for _, row in unique_jobs.iterrows():
        clean_url = str(row["URL"]).split("?")[0]
        if clean_url not in history_ids:
            final_jobs.append(row.to_dict())
            history_ids.add(clean_url)

    final_df = pd.DataFrame(final_jobs)

    # 4. Output
    print(f"\n{'='*60}")
    print(f"Final Verified Jobs: {len(final_df)}")
    if not final_df.empty:
        print(final_df[["TITLE", "COMPANY", "CATEGORY"]].to_string())

    # 5. Self-training
    weights = train_model(weights, final_jobs, target_urls, run_ts)
    save_weights(weights)

    # 6. Persist results for dashboard
    append_results(final_jobs, run_ts, weights)

    # 7. Save history
    with open(HISTORY_FILE, "w") as f:
        json.dump(list(history_ids), f)

    # 8. Telegram
    send_telegram(final_df, weights)


if __name__ == "__main__":
    main()
