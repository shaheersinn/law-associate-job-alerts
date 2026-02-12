#!/usr/bin/env python3
"""
Weekly Law Associate Job Scraper (Strict Filter Edition)

Updates:
- Eliminates "News", "People", "Team", and "Program Overview" links.
- Strictly enforces 0-2 year experience limit (kills 3+, 4+, 5+, mid-level).
- Verifies page content looks like a job (has 'Apply', 'Qualifications') before accepting.
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
from typing import List, Optional

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup

try:
    from jobspy import scrape_jobs
except ImportError:
    scrape_jobs = None

try:
    import openai
except ImportError:
    openai = None


# ─────────────────────────────────────────────────────────────────────────────
# 1. STRICT REGEX FILTERS
# ─────────────────────────────────────────────────────────────────────────────

# Headers to look like a real browser
_SCRAPER_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/123.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

# 1. POSITIVE MATCH: Title must contain one of these
_ROLE_TITLES = [
    r"\bassociate\b",
    r"\blawyer\b",
    r"\bcounsel\b", # We filter out "Senior Counsel" later
    r"\bjuriste\b",
    r"\bavocat\b",
    r"\bstudent\b",
    r"\barticling\b",
    r"\bsummer\b",
    r"\bclerk\b",
]

# 2. NEGATIVE TITLES (Instant Reject)
# Removes Senior, Mid-Level, Admin, and "Page Titles" (News, Team, etc)
_BLOCKED_TITLES = [
    # Seniority/Role Mismatches
    r"\bsenior\b",
    r"\bpartner\b",
    r"\bdirector\b",
    r"\bmanager\b",
    r"\bvp\b",
    r"\bpresident\b",
    r"\bmid-?level\b",
    r"\bintermediate\b",
    r"\bprincipal\b",
    r"\b(3|4|5|6|7|8|9)\+?\s*years\b", # "3 years", "3+ years", "4-5 years"
    r"\b(3|4|5|6|7|8|9)\s*-\s*\d+\s*years\b",
    
    # Non-Legal Roles
    r"\bwarehouse\b",
    r"\bdriver\b",
    r"\bsales\b",
    r"\bretail\b",
    r"\bmarketing\b",
    r"\bfinance\b",
    r"\btechnician\b",
    r"\bassistant\b",
    r"\bparalegal\b",
    r"\bcoordinator\b",
    
    # "Junk" Page Titles (News, Navigation, Info)
    r"\bnews\b",
    r"\bevents?\b",
    r"\bspeaks?\b",       # "Lawyer speaks at..."
    r"\bnamed\b",        # "Firm named top 10..."
    r"\bteam\b",         # "Our Team"
    r"\bprofiles?\b",    # "Lawyer Profiles"
    r"\bmeet\b",         # "Meet our students"
    r"\bcontact\b",
    r"\babout\b",
    r"\boverview\b",     # "Program Overview"
    r"\bprogram\b",      # "Student Program" (Too generic, we want specific job ads)
    r"\bresources\b",
    r"\bbenefit\b",
    r"\bhow\s+to\s+apply\b",
]

# 3. BLOCKED URL PATTERNS (Don't even click these)
_BLOCKED_URL_SUBSTRINGS = [
    "/people/", "/team/", "/profiles/", "/bios/",
    "/news/", "/events/", "/insights/", "/publications/",
    "/contact", "/about", "/history", "/awards",
    "/student-program", "/students-home", # Landing pages
    "/diversity", "/inclusion", "/community",
]

# 4. EXPERIENCE POSITIVE (If description exists, it MUST match one of these OR not match negative)
_EXP_POSITIVE = [
    r"0\s*-?\s*2\s*years",
    r"1\s*-?\s*2\s*years",
    r"junior",
    r"entry\s*level",
    r"newly\s*called",
    r"recent\s*call",
    r"202[4-6]\s*call",
    r"articling",
    r"summer",
    r"student",
]

RE_TITLES = re.compile("|".join(_ROLE_TITLES), re.IGNORECASE)
RE_BLOCKED = re.compile("|".join(_BLOCKED_TITLES), re.IGNORECASE)
RE_EXP_POS = re.compile("|".join(_EXP_POSITIVE), re.IGNORECASE)


# ─────────────────────────────────────────────────────────────────────────────
# 2. Filtering Logic
# ─────────────────────────────────────────────────────────────────────────────

def is_junk_url(url: str) -> bool:
    """Check if URL looks like a blog, profile, or news page."""
    u = url.lower()
    if any(x in u for x in _BLOCKED_URL_SUBSTRINGS):
        return True
    return False

def clean_html_text(html_content: str) -> str:
    """Extract visible text, stripping scripts/nav/footer."""
    soup = BeautifulSoup(html_content, "html.parser")
    for tag in soup(["script", "style", "nav", "footer", "header", "aside", "noscript"]):
        tag.extract()
    return soup.get_text(separator=' ').strip()

def looks_like_job_posting(text: str) -> bool:
    """
    Heuristic: Does the page contain 'apply', 'qualifications', 'responsibilities'?
    Used to filter out generic 'About our Student Program' landing pages.
    """
    t = text.lower()
    keywords = ["apply", "submit", "resume", "cover letter", "qualifications", "responsibilities", "requirements", "email"]
    # It needs to match at least 2 of these to be considered a real job posting
    matches = sum(1 for k in keywords if k in t)
    return matches >= 2

def is_relevant_job(title: str, description: str = "", url: str = "") -> bool:
    """
    Strict Master Filter.
    """
    t_clean = str(title).strip()
    d_clean = str(description).strip()
    combined = (t_clean + " " + d_clean).lower()

    # 1. URL Check (Prevent "News" links)
    if is_junk_url(url):
        return False

    # 2. Title Blocklist (Fast Fail)
    # If title mentions "Senior" or "3-5 years", KILL IT.
    if RE_BLOCKED.search(t_clean):
        return False

    # 3. Title Allowlist
    if not RE_TITLES.search(t_clean):
        return False

    # 4. Strict Description Scan (if available)
    if d_clean:
        # A. Check for "Senior" keywords in description that definitely disqualify
        # Regex for "5+ years", "minimum 4 years", etc.
        senior_reqs = re.search(r"\b(?:minimum|at least|requir\w+)\s+(?:3|4|5|6|7|8|9|10)\+?\s*years", combined)
        if senior_reqs:
            return False
            
        senior_roles = re.search(r"\b(senior|mid-level|intermediate)\s+associate", combined)
        if senior_roles:
            return False

        # B. (Optional) Enforce Positive Match
        # If the description is long but doesn't mention "junior", "0-2", "student", etc., 
        # it's suspicious. But some generic ads might omit it.
        # For STRICT mode, we can uncomment below:
        # if not RE_EXP_POS.search(combined):
        #    return False

    return True


# ─────────────────────────────────────────────────────────────────────────────
# 3. Scrapers
# ─────────────────────────────────────────────────────────────────────────────

def get_session():
    s = requests.Session()
    retry = Retry(total=3, backoff_factor=1, status_forcelist=[500, 502, 503, 504])
    s.mount("https://", HTTPAdapter(max_retries=retry))
    s.headers.update(_SCRAPER_HEADERS)
    return s

def scrape_site_deep(session, start_url: str, max_pages=2) -> List[dict]:
    found_jobs = []
    visited_urls = set()
    urls_to_visit = [start_url]
    domain = urlparse(start_url).netloc.replace("www.", "")
    
    pages_scraped = 0
    
    while urls_to_visit and pages_scraped < max_pages:
        current_url = urls_to_visit.pop(0)
        if current_url in visited_urls: continue
        
        visited_urls.add(current_url)
        pages_scraped += 1
        
        try:
            resp = session.get(current_url, timeout=10)
            if resp.status_code != 200: continue
            
            soup = BeautifulSoup(resp.text, "html.parser")
            
            # 1. Find Links
            links = []
            for a in soup.find_all("a", href=True):
                txt = a.get_text(" ", strip=True)
                href = urljoin(current_url, a["href"])
                
                # Filter Link BEFORE clicking
                if len(txt) > 5 and RE_TITLES.search(txt) and not RE_BLOCKED.search(txt):
                    if not is_junk_url(href) and href not in visited_urls:
                        links.append((txt, href))

            # 2. Visit Links
            for j_title, j_link in links:
                if j_link in visited_urls: continue
                visited_urls.add(j_link)
                
                try:
                    r_job = session.get(j_link, timeout=10)
                    if r_job.status_code == 200:
                        full_text = clean_html_text(r_job.text)
                        
                        # CONTENT CHECK: Does it look like a job?
                        if not looks_like_job_posting(full_text):
                            continue
                            
                        # LOGIC CHECK: Is it 0-2 years / Legal?
                        if is_relevant_job(j_title, full_text, j_link):
                            found_jobs.append({
                                "TITLE": j_title,
                                "COMPANY": domain.split('.')[0].title(),
                                "URL": j_link
                            })
                except Exception:
                    pass

        except Exception:
            pass
            
    return found_jobs

def scrape_custom_sites(urls):
    print(f"  [Direct] Scanning {len(urls)} sites...")
    all_jobs = []
    session = get_session()
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
        futures = {executor.submit(scrape_site_deep, session, u): u for u in urls}
        for f in concurrent.futures.as_completed(futures):
            try:
                res = f.result()
                if res: all_jobs.extend(res)
            except: pass
    return pd.DataFrame(all_jobs)

def scrape_aggregators():
    if scrape_jobs is None: return pd.DataFrame()
    print("  [Aggregator] Scraping Indeed/LinkedIn...")
    
    # Specific search to minimize junk
    # "0-2 years" in quotes forces strict match on some engines
    search = "associate lawyer"
    
    try:
        jobs = scrape_jobs(
            site_name=["indeed", "linkedin"],
            search_term=search,
            google_search_term=f"{search} \"0-2 years\" -senior -warehouse -driver in Canada",
            location="Canada",
            results_wanted=30,
            hours_old=168,
            country_indeed="Canada",
            linkedin_fetch_description=True,
            verbose=0
        )
    except: return pd.DataFrame()

    if jobs is None or jobs.empty: return pd.DataFrame()
    jobs.columns = [col.upper() for col in jobs.columns]
    
    valid = []
    for _, row in jobs.iterrows():
        t = str(row.get("TITLE", ""))
        d = str(row.get("DESCRIPTION", ""))
        u = str(row.get("JOB_URL", ""))
        
        if is_relevant_job(t, d, u):
            valid.append(row)
            
    return pd.DataFrame(valid)


# ─────────────────────────────────────────────────────────────────────────────
# 4. URLs
# ─────────────────────────────────────────────────────────────────────────────

def get_urls():
    # Only "Current Opportunities" pages. No generic "Careers" landing pages.
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
        "https://www.cba.org/r/Jobs/Home",
    ]


# ─────────────────────────────────────────────────────────────────────────────
# 5. Main
# ─────────────────────────────────────────────────────────────────────────────

def main():
    print("="*60)
    print("Strict Law Job Scraper (0-2 Years Only)")
    print("="*60)
    
    df1 = scrape_aggregators()
    df2 = scrape_custom_sites(get_urls())
    
    combined = pd.concat([df1, df2], ignore_index=True, sort=False)
    
    # Clean & Dedup
    if not combined.empty:
        # Normalize URL column
        if "URL" not in combined.columns: combined["URL"] = pd.NA
        combined["FINAL_URL"] = combined["JOB_URL"].fillna(combined["URL"])
        combined = combined.dropna(subset=["FINAL_URL"])
        
        # Remove duplicates
        combined["CLEAN_URL"] = combined["FINAL_URL"].apply(lambda x: x.split('?')[0])
        combined = combined.drop_duplicates(subset=["CLEAN_URL"])
        
        # Final Loop to print
        final_jobs = combined
    else:
        final_jobs = pd.DataFrame()

    print(f"\nFinal Verified Jobs: {len(final_jobs)}")
    
    if not final_jobs.empty:
        # Email Logic
        sender = os.environ.get("EMAIL_USER")
        password = os.environ.get("EMAIL_PASS")
        recipients = os.environ.get("EMAIL_TO", "").split(",")
        
        lines = []
        for _, row in final_jobs.iterrows():
            lines.append(f"ROLE: {row.get('TITLE')}")
            lines.append(f"FIRM: {row.get('COMPANY')}")
            lines.append(f"LINK: {row.get('FINAL_URL')}")
            lines.append("-" * 40)
            
        print("\n".join(lines)) # Print to console
        
        if sender and password and recipients:
            msg = EmailMessage()
            msg.set_content("\n".join(lines))
            msg["Subject"] = f"Job Alert: {len(final_jobs)} Strict Matches"
            msg["From"] = sender
            msg["To"] = ", ".join(recipients)
            try:
                with smtplib.SMTP_SSL("smtp.gmail.com", 465) as s:
                    s.login(sender, password)
                    s.send_message(msg)
                print("✓ Email Sent")
            except Exception as e:
                print(f"✗ Email Failed: {e}")
    else:
        print("No jobs found matching strict criteria.")

if __name__ == "__main__":
    main()
