#!/usr/bin/env python3
"""
Weekly Law Associate Job Scraper (Strict Clean Edition)

Fixes applied:
- RE_NAV_LINKS now uses re.search() without anchors, so partial matches like
  "Join Us career and student opportunities" are correctly blocked.
- history_file path set via HISTORY_FILE env var so GitHub Actions cache
  can restore/save it between runs.
- RESULTS_WANTED env var is actually read and passed to jobspy (was ignored).
- Email replaced with Telegram Bot API alerts.
- Dead 'openai' import removed.
"""

from __future__ import annotations

import os
import re
import json
import time
import concurrent.futures
from urllib.parse import urljoin, urlparse
from datetime import datetime
from typing import List

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup

try:
    from jobspy import scrape_jobs
except ImportError:
    scrape_jobs = None

# ─────────────────────────────────────────────────────────────────────────────
# 1. CONFIGURATION & REGEX
# ─────────────────────────────────────────────────────────────────────────────

_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-CA,en-US;q=0.9,en;q=0.8",
    "Connection": "keep-alive",
}

# --- 1. ROLE PATTERNS ---
RE_ASSOCIATE = re.compile(r"\b(associate|lawyer|counsel|juriste|avocat|attorney|solicitor)\b", re.IGNORECASE)
RE_STUDENT   = re.compile(r"\b(articling|student|summer|clerk|stagiaire)\b", re.IGNORECASE)

# --- 2. INSTANT REJECT (TITLES) ---
RE_BLOCKED_TITLE = re.compile(
    r"\b(senior|partner|director|manager|vp|president|chair|head|principal|"
    r"c-suite|executive|paralegal|assistant|clerk\s+typist|technician|driver|"
    r"warehouse|sales|marketing|receptionist|mid-?level|intermediate)\b",
    re.IGNORECASE
)

# --- 3. "INFO PAGE" BLOCKER ---
# FIX: removed ^...$ anchors. Now re.search() catches titles that merely
#      *contain* these phrases, e.g. "Join Us career and student opportunities".
RE_NAV_LINKS = re.compile(
    r"\b(our\s+team|profiles?|meet\s+our|join\s+us|careers?|student\s+programs?|"
    r"summer\s+programs?|articling\s+programs?|recruitment|who\s+we\s+are|about\s+us|"
    r"attorney\s+advertising|terms|privacy|search|menu|home|summer\s+recruitment|"
    r"our\s+summer\s+students|articling)\b",
    re.IGNORECASE
)

# --- 4. URL BLOCKLIST ---
RE_BAD_URLS = re.compile(
    r"(/who-we-are/|/our-team/|/profiles/|/people/|/attorney-advertising|"
    r"students\.cassels\.com|/student-programs/|/articling-program|/summer-program)",
    re.IGNORECASE
)

# --- 5. EXPERIENCE FILTER ---
RE_EXP_KILLER  = re.compile(r"\b((?:minimum|at least|over)\s+)?(3|4|5|6|7|8|9|10)(\+|\s*(-|to)\s*\d+)?\s*years", re.IGNORECASE)
RE_SENIOR_ROLE = re.compile(r"\b(senior|mid-level|intermediate)\s+associate", re.IGNORECASE)

# --- 6. LOCATION FILTER (Ontario & Alberta) ---
_LOCATIONS = [
    r"\bOntario\b", r"\bAlberta\b", r"\bAB\b", r"\bON\b",
    r"\bToronto\b", r"\bOttawa\b", r"\bMississauga\b", r"\bBrampton\b", r"\bHamilton\b",
    r"\bLondon\b", r"\bMarkham\b", r"\bVaughan\b", r"\bKitchener\b", r"\bWindsor\b",
    r"\bCalgary\b", r"\bEdmonton\b", r"\bRed Deer\b", r"\bLethbridge\b", r"\bSt\.? Albert\b"
]
RE_LOCATIONS     = re.compile("|".join(_LOCATIONS), re.IGNORECASE)
RE_BAD_LOCATIONS = re.compile(r"\b(Vancouver|British Columbia|BC|Montreal|Quebec|QC|Halifax|Nova Scotia)\b", re.IGNORECASE)


# ─────────────────────────────────────────────────────────────────────────────
# 2. HELPER CLASSES
# ─────────────────────────────────────────────────────────────────────────────

class JobScorer:
    @staticmethod
    def score_job(title: str, description: str, url: str) -> tuple[bool, str, int]:
        title     = str(title).strip()
        desc      = str(description).strip()
        url       = str(url).lower()
        full_text = (title + " " + desc).lower()

        # --- A. SANITY CHECKS (FAIL FAST) ---
        if RE_NAV_LINKS.search(title):
            return False, "Nav Link", -100

        if RE_BAD_URLS.search(url):
            return False, "Bad URL", -100

        if RE_BLOCKED_TITLE.search(title):
            return False, "Blocked Title", -100

        if RE_EXP_KILLER.search(full_text) or RE_SENIOR_ROLE.search(full_text):
            return False, "Too Senior", -100

        # --- B. CATEGORIZATION ---
        category = "Unknown"
        score    = 0

        if RE_ASSOCIATE.search(title):
            category = "Associate"
            score += 10
        elif RE_STUDENT.search(title):
            category = "Student"
            score += 5
        else:
            return False, "Not Legal", -100

        # --- C. LOCATION CHECK ---
        if not RE_LOCATIONS.search(full_text):
            if RE_BAD_LOCATIONS.search(full_text):
                return False, "Wrong Location", -100
            score -= 5

        # --- D. CONTENT CHECK ---
        if desc and "apply" not in full_text and "resume" not in full_text and "contact" not in full_text:
            score -= 5

        return (score >= 5), category, score


def get_session():
    s = requests.Session()
    retry = Retry(total=2, backoff_factor=1, status_forcelist=[500, 502, 503, 504])
    s.mount("https://", HTTPAdapter(max_retries=retry))
    s.headers.update(_HEADERS)
    return s

def clean_html(html: str) -> str:
    soup = BeautifulSoup(html, "html.parser")
    for tag in soup(["script", "style", "nav", "footer", "header", "noscript", "aside"]):
        tag.extract()
    return soup.get_text(separator=' ').replace('\n', ' ').strip()


# ─────────────────────────────────────────────────────────────────────────────
# 3. DIRECT SITE SCRAPER
# ─────────────────────────────────────────────────────────────────────────────

def scrape_site(url: str) -> List[dict]:
    session    = get_session()
    found_jobs = []
    visited    = set()
    queue      = [url]
    domain     = urlparse(url).netloc.replace("www.", "")

    print(f"  Scanning: {domain} ...")
    MAX_PAGES = 1  # Keep shallow to avoid wandering into blog posts

    while queue and MAX_PAGES > 0:
        curr = queue.pop(0)
        if curr in visited:
            continue
        visited.add(curr)
        MAX_PAGES -= 1

        try:
            resp = session.get(curr, timeout=10)
            if resp.status_code != 200:
                continue

            soup = BeautifulSoup(resp.text, "html.parser")

            for a in soup.find_all("a", href=True):
                text = a.get_text(" ", strip=True)
                href = urljoin(curr, a["href"])

                if len(text) < 4 or RE_NAV_LINKS.search(text) or RE_BAD_URLS.search(href):
                    continue

                if (RE_ASSOCIATE.search(text) or RE_STUDENT.search(text)) and not RE_BLOCKED_TITLE.search(text):
                    if href not in visited:
                        try:
                            job_resp = session.get(href, timeout=8)
                            if job_resp.status_code == 200:
                                desc = clean_html(job_resp.text)
                                is_fit, category, score = JobScorer.score_job(text, desc, href)

                                if is_fit:
                                    found_jobs.append({
                                        "TITLE":    text,
                                        "COMPANY":  domain.split('.')[0].title(),
                                        "URL":      href,
                                        "CATEGORY": category
                                    })
                                    visited.add(href)
                        except Exception:
                            pass
        except Exception:
            pass

    return found_jobs

def run_direct_scrape(urls: List[str]) -> pd.DataFrame:
    all_jobs = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        future_to_url = {executor.submit(scrape_site, u): u for u in urls}
        for future in concurrent.futures.as_completed(future_to_url):
            try:
                data = future.result()
                if data:
                    all_jobs.extend(data)
            except Exception:
                pass
    return pd.DataFrame(all_jobs)


# ─────────────────────────────────────────────────────────────────────────────
# 4. AGGREGATOR SCRAPER
# ─────────────────────────────────────────────────────────────────────────────

def scrape_jobspy_wrapper() -> pd.DataFrame:
    if scrape_jobs is None:
        return pd.DataFrame()

    print("  Scraping Aggregators (ON & AB)...")
    search_term = "lawyer associate"
    locations   = ["Ontario, Canada", "Alberta, Canada"]
    # FIX: was hardcoded to 30; now reads the env var as originally intended
    results_wanted = int(os.environ.get("RESULTS_WANTED", "30"))
    all_rows = []

    for loc in locations:
        try:
            jobs = scrape_jobs(
                site_name=["indeed", "linkedin", "google"],
                search_term=search_term,
                google_search_term=f'{search_term} "0-2 years" -senior -warehouse -driver in {loc}',
                location=loc,
                results_wanted=results_wanted,
                hours_old=168,
                country_indeed="Canada",
                linkedin_fetch_description=True,
                verbose=0
            )

            if jobs is not None and not jobs.empty:
                jobs.columns = [col.upper() for col in jobs.columns]
                for _, row in jobs.iterrows():
                    title   = str(row.get("TITLE", ""))
                    desc    = str(row.get("DESCRIPTION", ""))
                    url     = str(row.get("JOB_URL", ""))
                    company = str(row.get("COMPANY", ""))

                    is_fit, category, _ = JobScorer.score_job(title, desc, url)
                    if is_fit:
                        all_rows.append({
                            "TITLE":    title,
                            "COMPANY":  company,
                            "URL":      url,
                            "CATEGORY": category
                        })
            time.sleep(2)
        except Exception:
            pass

    return pd.DataFrame(all_rows)


# ─────────────────────────────────────────────────────────────────────────────
# 5. UTILS
# ─────────────────────────────────────────────────────────────────────────────

def get_target_urls():
    return [
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
    ]

def deduplicate_jobs(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    def make_sig(row):
        t = re.sub(r'\W+', '', str(row['TITLE']).lower())
        c = re.sub(r'\W+', '', str(row['COMPANY']).lower())
        return f"{t}_{c}"

    df['SIG']       = df.apply(make_sig, axis=1)
    df              = df.drop_duplicates(subset=['SIG'])
    df['CLEAN_URL'] = df['URL'].apply(lambda x: str(x).split('?')[0])
    df              = df.drop_duplicates(subset=['CLEAN_URL'])
    return df.drop(columns=['SIG', 'CLEAN_URL'])


# ─────────────────────────────────────────────────────────────────────────────
# 6. TELEGRAM ALERT  (replaces email)
# ─────────────────────────────────────────────────────────────────────────────

def send_telegram(df: pd.DataFrame):
    bot_token = os.environ.get("TELEGRAM_BOT_TOKEN")
    chat_id   = os.environ.get("TELEGRAM_CHAT_ID")

    if not (bot_token and chat_id):
        print("\n[!] TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID missing. No alert sent.")
        return

    associates = df[df["CATEGORY"] == "Associate"]
    students   = df[df["CATEGORY"] == "Student"]

    lines = [
        f"<b>⚖️ Law Jobs — ON/AB ({datetime.now().strftime('%Y-%m-%d')})</b>",
        f"<b>{len(df)} new listing(s) found</b>",
    ]

    if not associates.empty:
        lines.append("\n<b>🏛 ASSOCIATES / LAWYERS (0-2 Yrs)</b>")
        for _, row in associates.iterrows():
            lines.append(f"• <a href=\"{row['URL']}\">{row['TITLE']}</a> — {row['COMPANY']}")

    if not students.empty:
        lines.append("\n<b>🎓 STUDENTS / ARTICLING</b>")
        for _, row in students.iterrows():
            lines.append(f"• <a href=\"{row['URL']}\">{row['TITLE']}</a> — {row['COMPANY']}")

    full_message = "\n".join(lines)

    # Telegram hard limit is 4096 chars per message
    chunks  = [full_message[i:i+4096] for i in range(0, len(full_message), 4096)]
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
# 7. MAIN
# ─────────────────────────────────────────────────────────────────────────────

def main():
    print("=" * 60)
    print("Strict Law Scraper (ON/AB | Clean Output)")
    print("=" * 60)

    # 1. SCRAPE
    df_direct = run_direct_scrape(get_target_urls())
    df_agg    = scrape_jobspy_wrapper()

    # 2. COMBINE & DEDUPLICATE
    combined    = pd.concat([df_direct, df_agg], ignore_index=True, sort=False)
    unique_jobs = deduplicate_jobs(combined)

    # 3. HISTORY CHECK
    # FIX: path is now configurable via HISTORY_FILE env var.
    #      The workflow caches this file so history actually persists across runs.
    history_file = os.environ.get("HISTORY_FILE", "job_history.json")
    if os.path.exists(history_file):
        with open(history_file, 'r') as f:
            history_ids = set(json.load(f))
    else:
        history_ids = set()

    final_jobs = []
    for _, row in unique_jobs.iterrows():
        clean_url = row['URL'].split('?')[0]
        if clean_url not in history_ids:
            final_jobs.append(row)
            history_ids.add(clean_url)

    final_df = pd.DataFrame(final_jobs)

    # 4. OUTPUT
    print(f"\nFinal Verified Jobs: {len(final_df)}")
    if not final_df.empty:
        print(final_df[["TITLE", "COMPANY", "CATEGORY"]].to_string())
        send_telegram(final_df)

        with open(history_file, 'w') as f:
            json.dump(list(history_ids), f)
    else:
        print("No new jobs found this run.")


if __name__ == "__main__":
    main()
