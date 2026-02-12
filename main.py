#!/usr/bin/env python3
"""
Weekly Law Associate Job Scraper (Strict Clean Edition)

Fixes:
- Removes generic "Info" pages (e.g., "Our Student Program", "Join Us").
- Hard blocks "Mid-Level" and "Intermediate" roles.
- Blocks "Our Team", "Profiles", and "Attorney Advertising" links.
- Deduplicates jobs based on Title + Company (prevents Indeed/LinkedIn doubles).
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
from typing import List, Set

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
# Removes Senior, Mid-Level, and non-legal roles
RE_BLOCKED_TITLE = re.compile(r"\b(senior|partner|director|manager|vp|president|chair|head|principal|c-suite|executive|paralegal|assistant|clerk\s+typist|technician|driver|warehouse|sales|marketing|receptionist|mid-?level|intermediate)\b", re.IGNORECASE)

# --- 3. "INFO PAGE" BLOCKER (NEW) ---
# Removes navigation links that look like jobs but aren't.
RE_NAV_LINKS = re.compile(r"^(our\s+team|profiles?|meet\s+our|join\s+us|careers?|student\s+programs?|summer\s+programs?|articling\s+programs?|recruitment|who\s+we\s+are|about\s+us|attorney\s+advertising|terms|privacy|search|menu|home|summer\s+recruitment|our\s+summer\s+students|articling)$", re.IGNORECASE)

# --- 4. URL BLOCKLIST (NEW) ---
# Blocks specific subfolders known to contain generic info
RE_BAD_URLS = re.compile(r"(/who-we-are/|/our-team/|/profiles/|/people/|/attorney-advertising|students\.cassels\.com|/student-programs/|/articling-program|/summer-program)", re.IGNORECASE)

# --- 5. EXPERIENCE FILTER ("Killers") ---
RE_EXP_KILLER = re.compile(r"\b((?:minimum|at least|over)\s+)?(3|4|5|6|7|8|9|10)(\+|\s*(-|to)\s*\d+)?\s*years", re.IGNORECASE)
RE_SENIOR_ROLE = re.compile(r"\b(senior|mid-level|intermediate)\s+associate", re.IGNORECASE)

# --- 6. LOCATION FILTER (Ontario & Alberta) ---
_LOCATIONS = [
    r"\bOntario\b", r"\bAlberta\b", r"\bAB\b", r"\bON\b",
    r"\bToronto\b", r"\bOttawa\b", r"\bMississauga\b", r"\bBrampton\b", r"\bHamilton\b", 
    r"\bLondon\b", r"\bMarkham\b", r"\bVaughan\b", r"\bKitchener\b", r"\bWindsor\b", 
    r"\bCalgary\b", r"\bEdmonton\b", r"\bRed Deer\b", r"\bLethbridge\b", r"\bSt\.? Albert\b"
]
RE_LOCATIONS = re.compile("|".join(_LOCATIONS), re.IGNORECASE)
RE_BAD_LOCATIONS = re.compile(r"\b(Vancouver|British Columbia|BC|Montreal|Quebec|QC|Halifax|Nova Scotia)\b", re.IGNORECASE)


# ─────────────────────────────────────────────────────────────────────────────
# 2. HELPER CLASSES
# ─────────────────────────────────────────────────────────────────────────────

class JobScorer:
    @staticmethod
    def score_job(title: str, description: str, url: str) -> tuple[bool, str, int]:
        title = str(title).strip()
        desc  = str(description).strip()
        url   = str(url).lower()
        full_text = (title + " " + desc).lower()
        
        # --- A. SANITY CHECKS (FAIL FAST) ---
        
        # 1. Check for "Nav Link" titles (e.g. "Join Us", "Our Team")
        if RE_NAV_LINKS.search(title):
            return False, "Nav Link", -100
            
        # 2. Check for Blocked URLs (e.g. /who-we-are/)
        if RE_BAD_URLS.search(url):
            return False, "Bad URL", -100

        # 3. Check for Blocked Roles (Senior, Mid-Level, Admin)
        if RE_BLOCKED_TITLE.search(title):
            return False, "Blocked Title", -100
            
        # 4. Check for Experience Killers (3-5 years, Mid-Level in desc)
        if RE_EXP_KILLER.search(full_text) or RE_SENIOR_ROLE.search(full_text):
            return False, "Too Senior", -100

        # --- B. CATEGORIZATION ---
        category = "Unknown"
        score = 0
        
        if RE_ASSOCIATE.search(title):
            category = "Associate"
            score += 10
        elif RE_STUDENT.search(title):
            category = "Student"
            score += 5
        else:
            return False, "Not Legal", -100

        # --- C. LOCATION CHECK ---
        # Must have ON/AB keyword OR be a remote/unspecified role on a firm site
        # We are strict: must find location in text OR title
        if not RE_LOCATIONS.search(full_text):
            # If it explicitly says Vancouver/Montreal, kill it.
            if RE_BAD_LOCATIONS.search(full_text):
                return False, "Wrong Location", -100
            # If no location found, we punish score but don't kill (might be inferred)
            score -= 5

        # --- D. CONTENT CHECK ---
        # Real jobs usually have "Apply" or "Resume"
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
    session = get_session()
    found_jobs = []
    visited = set()
    queue = [url]
    domain = urlparse(url).netloc.replace("www.", "")
    
    print(f"  Scanning: {domain} ...")
    MAX_PAGES = 1 # Keep shallow to avoid wandering into blog posts

    while queue and MAX_PAGES > 0:
        curr = queue.pop(0)
        if curr in visited: continue
        visited.add(curr)
        MAX_PAGES -= 1

        try:
            resp = session.get(curr, timeout=10)
            if resp.status_code != 200: continue
            
            soup = BeautifulSoup(resp.text, "html.parser")
            
            for a in soup.find_all("a", href=True):
                text = a.get_text(" ", strip=True)
                href = urljoin(curr, a["href"])
                
                # PRE-FILTER: Don't even visit if it looks like junk
                if len(text) < 4 or RE_NAV_LINKS.search(text) or RE_BAD_URLS.search(href):
                    continue
                
                # TITLE MATCH
                if (RE_ASSOCIATE.search(text) or RE_STUDENT.search(text)) and not RE_BLOCKED_TITLE.search(text):
                    if href not in visited:
                        try:
                            # DEEP DIVE
                            job_resp = session.get(href, timeout=8)
                            if job_resp.status_code == 200:
                                desc = clean_html(job_resp.text)
                                is_fit, category, score = JobScorer.score_job(text, desc, href)
                                
                                if is_fit:
                                    found_jobs.append({
                                        "TITLE": text,
                                        "COMPANY": domain.split('.')[0].title(),
                                        "URL": href,
                                        "CATEGORY": category
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
# 4. AGGREGATOR SCRAPER
# ─────────────────────────────────────────────────────────────────────────────

def scrape_jobspy_wrapper() -> pd.DataFrame:
    if scrape_jobs is None: return pd.DataFrame()
    
    print("  Scraping Aggregators (ON & AB)...")
    search_term = "lawyer associate"
    locations = ["Ontario, Canada", "Alberta, Canada"]
    all_rows = []

    for loc in locations:
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
                    company = str(row.get("COMPANY", ""))
                    
                    is_fit, category, _ = JobScorer.score_job(title, desc, url)
                    if is_fit:
                        all_rows.append({
                            "TITLE": title,
                            "COMPANY": company,
                            "URL": url,
                            "CATEGORY": category
                        })
            time.sleep(2)
        except Exception: pass

    return pd.DataFrame(all_rows)


# ─────────────────────────────────────────────────────────────────────────────
# 5. UTILS
# ─────────────────────────────────────────────────────────────────────────────

def get_target_urls():
    # Only "Current Opportunities" specific pages
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
    """
    Deduplicates based on Title + Company (fuzzy match) AND URL.
    This prevents the same job appearing from Indeed AND LinkedIn.
    """
    if df.empty: return df
    
    # 1. Create a "Signature" for each job
    # e.g. "AssociatelawyerOsler"
    def make_sig(row):
        t = re.sub(r'\W+', '', str(row['TITLE']).lower())
        c = re.sub(r'\W+', '', str(row['COMPANY']).lower())
        return f"{t}_{c}"

    df['SIG'] = df.apply(make_sig, axis=1)
    
    # 2. Drop duplicates based on Signature (keeping first)
    df = df.drop_duplicates(subset=['SIG'])
    
    # 3. Drop duplicates based on URL (just in case)
    df['CLEAN_URL'] = df['URL'].apply(lambda x: str(x).split('?')[0])
    df = df.drop_duplicates(subset=['CLEAN_URL'])
    
    return df.drop(columns=['SIG', 'CLEAN_URL'])


# ─────────────────────────────────────────────────────────────────────────────
# 6. MAIN
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
    print("Strict Law Scraper (ON/AB | Clean Output)")
    print("="*60)

    # 1. SCRAPE
    df_direct = run_direct_scrape(get_target_urls())
    df_agg    = scrape_jobspy_wrapper()

    # 2. COMBINE & DEDUPLICATE
    combined = pd.concat([df_direct, df_agg], ignore_index=True, sort=False)
    unique_jobs = deduplicate_jobs(combined)

    # 3. HISTORY CHECK
    history_file = "job_history.json"
    if os.path.exists(history_file):
        with open(history_file, 'r') as f:
            history_ids = set(json.load(f))
    else:
        history_ids = set()

    # We check history against the URL
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
        send_email(final_df)
        
        with open(history_file, 'w') as f:
            json.dump(list(history_ids), f)
    else:
        print("No new jobs.")

if __name__ == "__main__":
    main()
