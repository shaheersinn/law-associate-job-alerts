#!/usr/bin/env python3
"""
Weekly Law Associate Job Scraper (Deep Crawl Edition)

Improvements:
- Pagination: Follows 'Next' links up to 3 pages deep on firm sites.
- Deep Inspection: Visits every job link to read the full description.
- Smart Cleaning: Isolates job text from website menus/footers.
- Parallel Processing: heavily threaded for speed.
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
from typing import List, Set, Optional

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
# 1. Configuration & Regex (Strict Filters)
# ─────────────────────────────────────────────────────────────────────────────

# Browser Headers (Anti-Blocking)
_SCRAPER_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Connection": "keep-alive",
}

# 1. POSITIVE MATCH: Title must contain one of these
_ROLE_TITLES = [
    r"\bassociate\b",
    r"\blawyer\b",
    r"\bcounsel\b",
    r"\bjuriste\b",
    r"\bavocat\b",
    r"\bstudent\b",    # Includes articling/summer students
    r"\barticling\b",
    r"\bclerk\b",      # Law clerks
]

# 2. NEGATIVE MATCH: Title must NOT contain these
_BLOCKED_TITLES = [
    r"\bwarehouse\b",
    r"\bdriver\b",
    r"\bsales\b",
    r"\bretail\b",
    r"\bmanager\b",
    r"\bdirector\b",
    r"\bpartner\b",
    r"\bsenior\b",
    r"\bvp\b",
    r"\bpresident\b",
    r"\bmarketing\b",
    r"\bfinance\b",
    r"\btechnician\b",
    r"\bexecutive\s+assistant\b",
    r"\breceptionist\b",
]

# 3. CONTEXT MATCH: Full text must contain at least one legal term
_LEGAL_CONTEXT = [
    r"\blaw\b",
    r"\blegal\b",
    r"\blitigation\b",
    r"\bcorporate\b",
    r"\bcommercial\b",
    r"\bemployment\b",
    r"\btransactional\b",
    r"\badvocacy\b",
    r"\bbar\s*admission\b",
    r"\blaw\s*society\b",
    r"\bll\.?b\.?\b",
    r"\bj\.?d\.?\b",
]

RE_TITLES = re.compile("|".join(_ROLE_TITLES), re.IGNORECASE)
RE_BLOCKED = re.compile("|".join(_BLOCKED_TITLES), re.IGNORECASE)
RE_CONTEXT = re.compile("|".join(_LEGAL_CONTEXT), re.IGNORECASE)


# ─────────────────────────────────────────────────────────────────────────────
# 2. Advanced Helpers
# ─────────────────────────────────────────────────────────────────────────────

def get_session():
    """Create a robust session with retries."""
    session = requests.Session()
    retry = Retry(
        total=3,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["HEAD", "GET", "OPTIONS"]
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    session.headers.update(_SCRAPER_HEADERS)
    return session


def clean_html_text(html_content: str) -> str:
    """
    Smart Cleaning: Removes scripts, styles, navbars, and footers to 
    extract only the 'real' body text of the job description.
    """
    soup = BeautifulSoup(html_content, "html.parser")
    
    # Kill all script and style elements
    for script in soup(["script", "style", "nav", "footer", "header", "aside"]):
        script.extract()
        
    # Get text
    text = soup.get_text(separator=' ')
    
    # Break into lines and remove leading and trailing space on each
    lines = (line.strip() for line in text.splitlines())
    # Break multi-headlines into a line each
    chunks = (phrase.strip() for line in lines for phrase in line.split("  "))
    # Drop blank lines
    text = '\n'.join(chunk for chunk in chunks if chunk)
    return text


def is_relevant_job(title: str, description: str = "") -> bool:
    """
    Strict filtering logic.
    """
    t_clean = str(title).strip()
    d_clean = str(description).strip()
    combined = (t_clean + " " + d_clean).lower()

    # 1. Blocklist (Fast Fail)
    if RE_BLOCKED.search(t_clean):
        return False

    # 2. Title Allowlist
    if not RE_TITLES.search(t_clean):
        return False

    # 3. Context Check (Must sound legal)
    # If we have a description, check it. If not, rely on title + URL context.
    if d_clean and not RE_CONTEXT.search(combined):
        return False

    return True


# ─────────────────────────────────────────────────────────────────────────────
# 3. Deep Scraper (Pagination + Detail View)
# ─────────────────────────────────────────────────────────────────────────────

def find_next_page(soup: BeautifulSoup, current_url: str) -> Optional[str]:
    """
    Heuristic to find a 'Next' button or page number in the HTML.
    """
    # Common text patterns for pagination
    next_patterns = [
        re.compile(r"next", re.I),
        re.compile(r"›", re.I),
        re.compile(r">", re.I),
        re.compile(r"more", re.I)
    ]
    
    for link in soup.find_all("a", href=True):
        text = link.get_text(strip=True)
        # Check if text matches "Next" or similar
        for pat in next_patterns:
            if pat.search(text) and len(text) < 15: # "Next" shouldn't be a long sentence
                next_url = urljoin(current_url, link["href"])
                if next_url != current_url:
                    return next_url
                    
    return None


def scrape_site_deep(session, start_url: str, max_pages: int = 3) -> List[dict]:
    """
    Scrapes a site, follows pagination, and visits every job link.
    """
    found_jobs = []
    visited_urls = set()
    urls_to_visit = [start_url]
    domain = urlparse(start_url).netloc.replace("www.", "")
    
    pages_scraped = 0
    
    while urls_to_visit and pages_scraped < max_pages:
        current_url = urls_to_visit.pop(0)
        if current_url in visited_urls:
            continue
            
        print(f"    Scanning: {current_url}")
        visited_urls.add(current_url)
        pages_scraped += 1
        
        try:
            resp = session.get(current_url, timeout=15)
            if resp.status_code != 200:
                continue
                
            soup = BeautifulSoup(resp.text, "html.parser")
            
            # 1. Find Job Links
            job_links = []
            for a in soup.find_all("a", href=True):
                title = a.get_text(" ", strip=True)
                href = a["href"]
                
                # Basic pre-filter on title to avoid visiting "Privacy Policy"
                if len(title) > 3 and RE_TITLES.search(title) and not RE_BLOCKED.search(title):
                    full_link = urljoin(current_url, href)
                    if full_link not in visited_urls:
                        job_links.append((title, full_link))

            # 2. Visit Each Job Link (Deep Dive)
            for j_title, j_link in job_links:
                if j_link in visited_urls: 
                    continue
                
                try:
                    # Go to job page
                    j_resp = session.get(j_link, timeout=10)
                    visited_urls.add(j_link)
                    
                    if j_resp.status_code == 200:
                        full_desc = clean_html_text(j_resp.text)
                        
                        # FINAL DECISION: Is this a legal job?
                        if is_relevant_job(j_title, full_desc):
                            found_jobs.append({
                                "SITE": domain,
                                "TITLE": j_title,
                                "COMPANY": domain.split('.')[0].title(),
                                "CITY": "Canada", # Generic, hard to parse per-site
                                "DATE": datetime.now().strftime("%Y-%m-%d"),
                                "JOB_URL": j_link,
                                "DESCRIPTION": full_desc[:5000] # Limit size
                            })
                except Exception:
                    pass

            # 3. Look for Pagination (Add to queue)
            next_link = find_next_page(soup, current_url)
            if next_link and next_link not in visited_urls:
                urls_to_visit.append(next_link)

        except Exception as e:
            print(f"    Error on {domain}: {e}")
            
    return found_jobs


def scrape_custom_sites_parallel(urls: List[str], source_label: str) -> pd.DataFrame:
    print(f"  [{source_label}] Deep crawling {len(urls)} sites (Max 3 pages depth)...")
    all_jobs = []
    session = get_session()
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
        future_to_url = {executor.submit(scrape_site_deep, session, url): url for url in urls}
        
        for future in concurrent.futures.as_completed(future_to_url):
            try:
                data = future.result()
                if data:
                    all_jobs.extend(data)
            except Exception:
                pass
                
    return pd.DataFrame(all_jobs) if all_jobs else pd.DataFrame()


# ─────────────────────────────────────────────────────────────────────────────
# 4. JobSpy (Aggregator)
# ─────────────────────────────────────────────────────────────────────────────

def scrape_aggregators() -> pd.DataFrame:
    if scrape_jobs is None:
        return pd.DataFrame()
        
    print("  [Aggregator] Scraping Indeed/LinkedIn/Google...")
    # Broader search, strictly filtered later
    search_term = "lawyer associate"
    
    try:
        jobs = scrape_jobs(
            site_name=["indeed", "linkedin", "google"],
            search_term=search_term,
            google_search_term=f"{search_term} legal -warehouse -driver in Canada",
            location="Canada",
            results_wanted=50, 
            hours_old=168,
            country_indeed="Canada",
            linkedin_fetch_description=True,
            verbose=0
        )
    except Exception:
        return pd.DataFrame()

    if jobs is None or jobs.empty:
        return pd.DataFrame()
        
    jobs.columns = [col.upper() for col in jobs.columns]
    
    # Filter aggregator results with the same strict logic
    valid = []
    for _, row in jobs.iterrows():
        t = str(row.get("TITLE", ""))
        d = str(row.get("DESCRIPTION", ""))
        if is_relevant_job(t, d):
            valid.append(row)
            
    return pd.DataFrame(valid)


# ─────────────────────────────────────────────────────────────────────────────
# 5. URL Lists (Comprehensive)
# ─────────────────────────────────────────────────────────────────────────────

def get_target_urls():
    """
    Returns a combined list of Law Firm and Recruiter URLs.
    Includes separate tabs for Students vs Associates where applicable.
    """
    return [
        # --- LAW FIRMS ---
        "https://www.joinblakes.com/careers/associates/",
        "https://www.joinblakes.com/careers/students/",
        "https://www.bennettjones.com/en/Careers/Legal-Professionals",
        "https://www.bennettjones.com/en/Careers/Students",
        "https://www.fasken.com/en/careers/lawyers",
        "https://www.fasken.com/en/careers/students",
        "https://gowlingwlg.com/en/careers/current-opportunities/",
        "https://www.stikeman.com/en/careers/legal",
        "https://www.stikeman.com/en/careers/students",
        "https://www.dwpv.com/en/Careers/Lawyers",
        "https://www.dwpv.com/en/Careers/Students",
        "https://www.mccarthy.ca/en/careers/lawyers",
        "https://www.torys.com/careers/lawyers",
        "https://www.goodmans.ca/careers/associates",
        "https://www.goodmans.ca/careers/students",
        "https://www.blg.com/en/careers/current-opportunities",
        "https://www.millerthomson.com/en/careers/lawyers/",
        "https://cassels.com/join-us/career-opportunities-lawyers/",
        "https://www.airdberlis.com/join-us/current-opportunities",
        "https://www.lerners.ca/careers/lawyers/",
        "https://www.litigate.com/careers/lawyers",
        "https://www.wildlaw.ca/careers/lawyers/",
        "https://www.osler.com/en/careers/opportunities",
        "https://www.torkinmanes.com/careers/lawyers",
        "https://www.foglers.com/careers/legal-professionals/",
        "https://www.mindengross.com/careers/lawyers",
        
        # --- RECRUITERS ---
        "https://www.zsa.ca/job-board/",
        "https://thecounselnetwork.com/job-search/",
        "https://www.lifeafterlaw.com/opportunities",
        "https://thehellergroup.ca/opportunities/",
        "https://www.smithlegalsearch.com/opportunities/",
        "https://cartelinc.com/job-search/",
        "https://edgerecruitment.ca/vacancies/",
        "https://www.urbanlegal.ca/opportunities",
        "https://legaljobs.ca/jobs/",
        "https://www.cba.org/r/Jobs/Home",
    ]


# ─────────────────────────────────────────────────────────────────────────────
# 6. Main
# ─────────────────────────────────────────────────────────────────────────────

def main():
    start_time = time.time()
    print("="*60)
    print(f"Deep Law Job Scraper Started: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print("="*60)

    # 1. Aggregators
    df_agg = scrape_aggregators()
    
    # 2. Direct Sites (Deep Crawl)
    df_direct = scrape_custom_sites_parallel(get_target_urls(), "Direct Sites")

    # 3. Combine
    combined = pd.concat([df_agg, df_direct], ignore_index=True, sort=False)
    
    # 4. Deduplicate (URL based) & History Check
    history_file = "job_history.json"
    if os.path.exists(history_file):
        with open(history_file, 'r') as f:
            history_ids = set(json.load(f))
    else:
        history_ids = set()

    new_jobs = []
    if not combined.empty:
        # Normalize URL
        combined['CLEAN_URL'] = combined.apply(
            lambda x: (x.get("JOB_URL") or x.get("URL") or "").split('?')[0], axis=1
        )
        combined = combined.drop_duplicates(subset=['CLEAN_URL'])

        for _, row in combined.iterrows():
            if row['CLEAN_URL'] and row['CLEAN_URL'] not in history_ids:
                new_jobs.append(row)
                history_ids.add(row['CLEAN_URL'])
    
    final_df = pd.DataFrame(new_jobs)
    
    # 5. Save History
    with open(history_file, 'w') as f:
        json.dump(list(history_ids), f)

    print(f"\nTotal New Jobs Found: {len(final_df)}")

    # 6. Email
    if not final_df.empty:
        sender = os.environ.get("EMAIL_USER")
        password = os.environ.get("EMAIL_PASS")
        recipients = os.environ.get("EMAIL_TO", "").split(",")
        
        if sender and password and recipients:
            lines = [f"Found {len(final_df)} new roles:\n"]
            for _, row in final_df.iterrows():
                lines.append(f"ROLE: {row.get('TITLE')}")
                lines.append(f"FIRM: {row.get('COMPANY')}")
                lines.append(f"LINK: {row.get('JOB_URL') or row.get('URL')}")
                lines.append("-" * 40)
            
            body = "\n".join(lines)
            msg = EmailMessage()
            msg.set_content(body)
            msg["Subject"] = f"Legal Job Alert: {len(final_df)} New Roles"
            msg["From"] = sender
            msg["To"] = ", ".join(recipients)

            try:
                with smtplib.SMTP_SSL("smtp.gmail.com", 465) as smtp:
                    smtp.login(sender, password)
                    smtp.send_message(msg)
                print("✓ Email sent.")
            except Exception as e:
                print(f"✗ Email error: {e}")
        else:
            print(final_df[['TITLE', 'COMPANY', 'JOB_URL']].to_string())
    else:
        print("No new jobs.")

    print(f"Finished in {time.time() - start_time:.2f} seconds.")

if __name__ == "__main__":
    main()
