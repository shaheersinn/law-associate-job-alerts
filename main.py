#!/usr/bin/env python3
"""
Weekly Law Associate Job Scraper (ON & AB Edition)

Target:
- Roles: Associates (0-2 Yrs) & Articling/Students.
- Locations: STRICTLY Ontario & Alberta.
- Features: Smart Scoring, Deep Crawl, Location Filtering.
"""

from __future__ import annotations

import os
import re
import smtplib
import json
import time
import concurrent.futures
from email.message import EmailMessage
from urllib.parse import urljoin, urlparse
from datetime import datetime
from typing import List, Dict, Optional

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

# --- 2. NEGATIVE TITLES ---
RE_BLOCKED_TITLE = re.compile(r"\b(senior|partner|director|manager|vp|president|chair|head|principal|c-suite|executive|paralegal|assistant|clerk\s+typist|technician|driver|warehouse|sales|marketing|receptionist)\b", re.IGNORECASE)

# --- 3. EXPERIENCE FILTER ("Killers") ---
# Rejects "5+ years", "3-5 years", "Senior Associate"
RE_EXP_KILLER = re.compile(r"\b((?:minimum|at least|over)\s+)?(3|4|5|6|7|8|9|10)(\+|\s*(-|to)\s*\d+)?\s*years", re.IGNORECASE)
RE_SENIOR_ROLE = re.compile(r"\b(senior|mid-level|intermediate)\s+associate", re.IGNORECASE)

# --- 4. POSITIVE SCORING ---
RE_JUNIOR = re.compile(r"\b(0-2|0\s*to\s*2|1-2|1\s*to\s*2|first|second)\s*years?", re.IGNORECASE)
RE_ENTRY  = re.compile(r"\b(entry\s*level|junior|newly\s*called|recent\s*call|202[4-6]\s*call)", re.IGNORECASE)

# --- 5. LOCATION FILTER (Ontario & Alberta) ---
# Must match at least one of these cities/provinces in the text
_LOCATIONS = [
    # Provinces
    r"\bOntario\b", r"\bAlberta\b", r"\bAB\b", r"\bON\b",
    # ON Cities
    r"\bToronto\b", r"\bOttawa\b", r"\bMississauga\b", r"\bBrampton\b", r"\bHamilton\b", 
    r"\bLondon\b", r"\bMarkham\b", r"\bVaughan\b", r"\bKitchener\b", r"\bWindsor\b", 
    r"\bBurlington\b", r"\bSudbury\b", r"\bOshawa\b", r"\bBarrie\b", r"\bKingston\b", 
    r"\bGuelph\b", r"\bWaterloo\b", r"\bThunder Bay\b", r"\bOakville\b", r"\bRichmond Hill\b",
    # AB Cities
    r"\bCalgary\b", r"\bEdmonton\b", r"\bRed Deer\b", r"\bLethbridge\b", r"\bSt\.? Albert\b",
    r"\bMedicine Hat\b", r"\bGrande Prairie\b", r"\bAirdrie\b", r"\bFort McMurray\b"
]
RE_LOCATIONS = re.compile("|".join(_LOCATIONS), re.IGNORECASE)

# Negative Locations (Stop "Vancouver" jobs from sneaking in via national firms)
RE_BAD_LOCATIONS = re.compile(r"\b(Vancouver|British Columbia|BC|Montreal|Quebec|QC|Halifax|Nova Scotia)\b", re.IGNORECASE)


# ─────────────────────────────────────────────────────────────────────────────
# 2. HELPER CLASSES
# ─────────────────────────────────────────────────────────────────────────────

class JobScorer:
    """Analyzes job text to determine fit and location."""
    
    @staticmethod
    def score_job(title: str, description: str) -> tuple[bool, str, int]:
        title = str(title).strip()
        desc  = str(description).strip()
        full_text = (title + " " + desc).lower()
        
        score = 0
        category = "Unknown"

        # A. TITLE CHECK
        if RE_BLOCKED_TITLE.search(title):
            return False, "Blocked", -100

        if RE_ASSOCIATE.search(title):
            category = "Associate"
            score += 10
        elif RE_STUDENT.search(title):
            category = "Student"
            score += 5
        else:
            return False, "Not Legal", -100

        # B. LOCATION CHECK
        # 1. Must contain an ON/AB keyword
        if not RE_LOCATIONS.search(full_text):
            return False, "Wrong Location", -100
        
        # 2. If it explicitly mentions bad locations (e.g. "Vancouver") AND NOT good ones nearby
        # (This is tricky, so we rely on the positive check mostly. But if title says "Associate - Vancouver", kill it.)
        if RE_BAD_LOCATIONS.search(title):
            return False, "Wrong City in Title", -100

        # C. EXPERIENCE CHECK ("The Killer")
        if RE_EXP_KILLER.search(full_text) or RE_SENIOR_ROLE.search(full_text):
            return False, "Too Senior", -100

        # D. SCORING
        if RE_JUNIOR.search(full_text): score += 20
        if RE_ENTRY.search(full_text):  score += 15
        
        # Content Valid check
        if desc and "apply" not in full_text and "resume" not in full_text and "contact" not in full_text:
            score -= 5

        return (score >= 5), category, score


def get_session():
    s = requests.Session()
    retry = Retry(total=3, backoff_factor=1, status_forcelist=[500, 502, 503, 504])
    s.mount("https://", HTTPAdapter(max_retries=retry))
    s.headers.update(_HEADERS)
    return s

def clean_html(html: str) -> str:
    soup = BeautifulSoup(html, "html.parser")
    for tag in soup(["script", "style", "nav", "footer", "header", "noscript", "aside"]):
        tag.extract()
    return soup.get_text(separator=' ').replace('\n', ' ').strip()


# ─────────────────────────────────────────────────────────────────────────────
# 3. DIRECT SITE SCRAPER (Parallel)
# ─────────────────────────────────────────────────────────────────────────────

def scrape_site(url: str) -> List[dict]:
    session = get_session()
    found_jobs = []
    visited = set()
    queue = [url]
    domain = urlparse(url).netloc.replace("www.", "")
    
    print(f"  Scanning: {domain} ...")
    pages_scraped = 0
    MAX_PAGES = 2 

    while queue and pages_scraped < MAX_PAGES:
        curr = queue.pop(0)
        if curr in visited: continue
        visited.add(curr)
        pages_scraped += 1

        try:
            resp = session.get(curr, timeout=10)
            if resp.status_code != 200: continue
            
            soup = BeautifulSoup(resp.text, "html.parser")
            
            # Extract Links
            for a in soup.find_all("a", href=True):
                text = a.get_text(" ", strip=True)
                href = urljoin(curr, a["href"])
                
                # Basic Pre-filter
                if len(text) < 4 or "javascript" in href: continue
                
                # Check Title Match
                if (RE_ASSOCIATE.search(text) or RE_STUDENT.search(text)) and not RE_BLOCKED_TITLE.search(text):
                    if href not in visited:
                        # DEEP DIVE: Visit the link to check Location & Description
                        try:
                            job_resp = session.get(href, timeout=8)
                            if job_resp.status_code == 200:
                                desc = clean_html(job_resp.text)
                                is_fit, category, score = JobScorer.score_job(text, desc)
                                
                                if is_fit:
                                    found_jobs.append({
                                        "TITLE": text,
                                        "COMPANY": domain.split('.')[0].title(),
                                        "URL": href,
                                        "CATEGORY": category,
                                        "SCORE": score
                                    })
                                    visited.add(href)
                        except: pass

        except Exception: pass

    return found_jobs

def run_direct_scrape(urls: List[str]) -> pd.DataFrame:
    all_jobs = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        future_to_url = {executor.submit(scrape_site, u): u for u in urls}
        for future in concurrent.futures.as_completed(future_to_url):
            try:
                data = future.result()
                if data: all_jobs.extend(data)
            except: pass
    return pd.DataFrame(all_jobs)


# ─────────────────────────────────────────────────────────────────────────────
# 4. AGGREGATOR SCRAPER (Specific Locations)
# ─────────────────────────────────────────────────────────────────────────────

def scrape_jobspy_wrapper() -> pd.DataFrame:
    if scrape_jobs is None: return pd.DataFrame()
    
    print("  Scraping Aggregators (Indeed/LinkedIn/Google) for ON & AB...")
    search_term = "lawyer associate"
    
    # We run two separate scrapes to force the engine to filter by province
    locations = ["Ontario, Canada", "Alberta, Canada"]
    all_rows = []

    for loc in locations:
        print(f"    -> Querying {loc}...")
        try:
            jobs = scrape_jobs(
                site_name=["indeed", "linkedin", "google"],
                search_term=search_term,
                google_search_term=f"{search_term} \"0-2 years\" -senior -warehouse -driver in {loc}",
                location=loc,
                results_wanted=30,
                hours_old=168,
                country_indeed="Canada",
                linkedin_fetch_description=True,
                verbose=0
            )
            
            if jobs is not None and not jobs.empty:
                jobs.columns = [col.upper() for col in jobs.columns]
                for _, row in jobs.iterrows():
                    title = str(row.get("TITLE", ""))
                    desc  = str(row.get("DESCRIPTION", ""))
                    url   = str(row.get("JOB_URL", ""))
                    
                    is_fit, category, score = JobScorer.score_job(title, desc)
                    if is_fit:
                        all_rows.append({
                            "TITLE": title,
                            "COMPANY": row.get("COMPANY"),
                            "URL": url,
                            "CATEGORY": category,
                            "SCORE": score
                        })
            time.sleep(2) # Polite delay between location queries
        except Exception as e:
            print(f"    Error scraping {loc}: {e}")

    return pd.DataFrame(all_rows)


# ─────────────────────────────────────────────────────────────────────────────
# 5. TARGET URLS
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


# ─────────────────────────────────────────────────────────────────────────────
# 6. MAIN & EMAIL
# ─────────────────────────────────────────────────────────────────────────────

def send_email(df: pd.DataFrame):
    sender = os.environ.get("EMAIL_USER")
    password = os.environ.get("EMAIL_PASS")
    recipients = os.environ.get("EMAIL_TO", "").split(",")

    if not (sender and password and recipients):
        print("\n[!] Credentials missing. No email sent.")
        return

    associates = df[df["CATEGORY"] == "Associate"]
    students   = df[df["CATEGORY"] == "Student"]

    body = [f"Law Job Report (ON/AB Only) - {datetime.now().strftime('%Y-%m-%d')}\n"]
    
    if not associates.empty:
        body.append("\n=== 🏛 ASSOCIATES / LAWYERS (0-2 Yrs) ===\n")
        for _, row in associates.iterrows():
            body.append(f"• {row['TITLE']}\n  {row['COMPANY']}\n  {row['URL']}\n")
            
    if not students.empty:
        body.append("\n=== 🎓 STUDENTS / ARTICLING ===\n")
        for _, row in students.iterrows():
            body.append(f"• {row['TITLE']}\n  {row['COMPANY']}\n  {row['URL']}\n")

    msg = EmailMessage()
    msg.set_content("\n".join(body))
    msg["Subject"] = f"Legal Jobs ({len(df)} Found) - ON & AB"
    msg["From"] = sender
    msg["To"] = ", ".join(recipients)

    try:
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as s:
            s.login(sender, password)
            s.send_message(msg)
        print("✓ Email sent.")
    except Exception as e:
        print(f"✗ Email failed: {e}")


def main():
    print("="*60)
    print("Law Scraper (ON & AB Only | 0-2 Years)")
    print("="*60)

    # 1. SCRAPE
    df_direct = run_direct_scrape(get_target_urls())
    df_agg    = scrape_jobspy_wrapper()

    # 2. COMBINE
    combined = pd.concat([df_direct, df_agg], ignore_index=True, sort=False)
    
    if combined.empty:
        print("No jobs found.")
        return

    # 3. DEDUPLICATE
    combined["CLEAN_URL"] = combined["URL"].apply(lambda x: str(x).split('?')[0])
    combined = combined.drop_duplicates(subset=["CLEAN_URL"])

    # 4. HISTORY CHECK
    history_file = "job_history.json"
    if os.path.exists(history_file):
        with open(history_file, 'r') as f:
            history_ids = set(json.load(f))
    else:
        history_ids = set()

    new_jobs = combined[~combined["CLEAN_URL"].isin(history_ids)].copy()

    # 5. OUTPUT
    print(f"\nFinal Verified Jobs: {len(new_jobs)}")
    if not new_jobs.empty:
        print(new_jobs[["TITLE", "COMPANY", "CATEGORY"]].to_string())
        send_email(new_jobs)
        
        history_ids.update(new_jobs["CLEAN_URL"].tolist())
        with open(history_file, 'w') as f:
            json.dump(list(history_ids), f)
    else:
        print("No new jobs.")

if __name__ == "__main__":
    main()
