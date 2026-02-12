#!/usr/bin/env python3
"""
Weekly Law Associate Job Scraper (Fixed)

Updates:
- Filters 'Jobspy' results to remove "Warehouse" and non-legal roles.
- Prevents custom scraper from clicking navigation links like "How to Apply".
- Centralized filtering logic for consistent results across all sources.
"""

from __future__ import annotations

import os
import re
import smtplib
import json
import time
from email.message import EmailMessage
from urllib.parse import urljoin, urlparse
from datetime import datetime
from typing import List, Optional

import pandas as pd
import requests
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
# 1. Improved Regex Patterns
# ─────────────────────────────────────────────────────────────────────────────

# Must contain at least one of these in the TITLE or DESCRIPTION to be considered a "Law" job
_REQUIRED_LEGAL_TERMS = [
    r"\blaw\b",
    r"\blegal\b",
    r"\blawyer\b",
    r"\battorney\b",
    r"\bcounsel\b",
    r"\blitigation\b",
    r"\bcorporate\b",
    r"\bsolicitor\b",
    r"\barbitration\b",
    r"\bjuriste\b", # French context for Canada
]

# Must contain at least one of these in the TITLE to be a relevant role
_REQUIRED_ROLE_TITLES = [
    r"\bassociate\b",
    r"\bjunior\b",
    r"\bentry\s*level\b",
    r"\barticling\b",
    r"\bstudent\b",  # Kept 'student' as you had articling/students in your output, remove if you only want lawyers
]

# Explicitly exclude these roles (Warehouse, Sales, etc.)
_BLOCKED_TITLES = [
    r"\bwarehouse\b",
    r"\bsales\b",
    r"\bretail\b",
    r"\bdriver\b",
    r"\bcustomer\s+service\b",
    r"\bmanager\b",
    r"\bdirector\b",
    r"\bpartner\b",
    r"\bsenior\b",
    r"\bvp\b",
    r"\bpresident\b",
    r"\bhow\s+to\s+apply\b",
    r"\bcareer\b",
    r"\bjob\s+search\b",
    r"\bstudents?\b$", # Exclude links that are just "Students" (likely nav menu), but allow "Articling Student"
]

# Experience-specific regex (Positive)
_EXP_POSITIVE = [
    r"0\s*-?\s*2\s*years",
    r"0\s*to\s*2\s*years",
    r"1\s*-?\s*2\s*years",
    r"\bfirst[\s\-]*year\b",
    r"\bsecond[\s\-]*year\b",
    r"\bentry[-\s]*level\b",
    r"\bjunior\b",
    r"\barticling\b",
    r"\bnewly\s+called\b",
    r"\brecent\s+call\b",
    r"\b202[3-6]\s+call\b", # e.g. 2024 call
]

# Experience-specific regex (Negative)
_EXP_NEGATIVE = [
    r"\b(?:3|4|5|6|7|8|9|10)\+?\s*years\b",
    r"\b(?:5)\s*-\s*(?:7)\s*years\b",
]

# Compiling patterns
RE_LEGAL_CONTEXT = re.compile("|".join(_REQUIRED_LEGAL_TERMS), re.IGNORECASE)
RE_ROLE_TITLES   = re.compile("|".join(_REQUIRED_ROLE_TITLES), re.IGNORECASE)
RE_BLOCKED       = re.compile("|".join(_BLOCKED_TITLES), re.IGNORECASE)
RE_EXP_POS       = re.compile("|".join(_EXP_POSITIVE), re.IGNORECASE)
RE_EXP_NEG       = re.compile("|".join(_EXP_NEGATIVE), re.IGNORECASE)

_SCRAPER_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
        " AppleWebKit/537.36 (KHTML, like Gecko)"
        " Chrome/120.0.0.0 Safari/537.36"
    )
}


# ─────────────────────────────────────────────────────────────────────────────
# 2. Filtering Logic (Centralized)
# ─────────────────────────────────────────────────────────────────────────────

def is_relevant_job(title: str, description: str = "") -> bool:
    """
    Master filter function. Returns True only if the job passes all strict checks.
    """
    title_clean = str(title).strip()
    desc_clean = str(description).strip()
    combined_text = (title_clean + " " + desc_clean).lower()
    
    # 1. Blocklist Check (Fast fail)
    if RE_BLOCKED.search(title_clean):
        return False

    # 2. Must look like a legal role title
    if not RE_ROLE_TITLES.search(title_clean):
        return False
    
    # 3. Must have legal context (keywords in title OR description)
    # This stops "Warehouse Associate" (has 'associate', but no 'law'/'legal'/'litigation')
    if not RE_LEGAL_CONTEXT.search(combined_text):
        return False

    # 4. Experience Check (if description is provided)
    if desc_clean:
        # If it explicitly asks for senior years, drop it
        if RE_EXP_NEG.search(desc_clean):
            return False
        # If explicitly entry level/0-2 years, keep it. 
        # Note: If neither present, we often keep it to be safe, or you can enforce POSITIVE match.
        # For now, we enforce positive match OR it's an articling role.
        if not RE_EXP_POS.search(combined_text):
             # Strict mode: If we can't find "0-2 years" or "junior", drop it.
             return False

    return True


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def get_env_variable(name: str, default: Optional[str] = None) -> str:
    value = os.environ.get(name, default)
    if value is None:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


def _safe_str(value) -> str:
    if value is None or (isinstance(value, float) and value != value):
        return ""
    return str(value).strip()


def load_history(history_path: str) -> set:
    if not os.path.exists(history_path):
        return set()
    try:
        with open(history_path, "r", encoding="utf-8") as f:
            data = json.load(f)
            if isinstance(data, list):
                return set(data)
    except Exception:
        pass
    return set()


def save_history(history_path: str, job_ids: set) -> None:
    try:
        with open(history_path, "w", encoding="utf-8") as f:
            json.dump(sorted(job_ids), f, indent=2)
    except Exception:
        pass


def remove_old_jobs(df: pd.DataFrame, max_age_days: int = 40) -> pd.DataFrame:
    if df.empty:
        return df

    cutoff = pd.Timestamp.utcnow() - pd.Timedelta(days=max_age_days)
    
    # Normalize date columns
    if "DATE" not in df.columns:
        df["DATE"] = pd.NA
    if "DATE_POSTED" in df.columns:
        df["DATE"] = df["DATE"].fillna(df["DATE_POSTED"])

    # Convert to datetime
    dt = pd.to_datetime(df["DATE"], errors="coerce", utc=True)
    
    # Keep rows with unknown date (NaT) OR fresh enough
    keep = dt.isna() | (dt >= cutoff)
    return df.loc[keep].copy()


# ─────────────────────────────────────────────────────────────────────────────
# Scrapers
# ─────────────────────────────────────────────────────────────────────────────

def _scrape_career_pages(page_urls: List[str], source_label: str) -> pd.DataFrame:
    jobs: List[dict] = []

    for url in page_urls:
        print(f"  [{source_label}] Scraping: {url}")
        try:
            resp = requests.get(url, headers=_SCRAPER_HEADERS, timeout=20)
        except Exception:
            continue
        if resp.status_code != 200:
            continue

        soup = BeautifulSoup(resp.text, "html.parser")
        base_domain = urlparse(url).netloc

        # Find all links
        for link in soup.find_all("a", href=True):
            raw_title = link.get_text(" ", strip=True) # use space separator to avoid "TitleSubtitle"
            if not raw_title:
                continue

            # --- PRE-CLICK FILTER ---
            # Don't click if the title is clearly junk or clearly not a legal role
            if not is_relevant_job(raw_title):
                continue

            job_url = urljoin(url, link["href"])
            
            # Skip if we are just reloading the same page or anchors
            if job_url == url or "#" in link["href"]:
                continue

            try:
                j_resp = requests.get(job_url, headers=_SCRAPER_HEADERS, timeout=10)
                time.sleep(0.5)
            except Exception:
                continue
            
            if j_resp.status_code != 200:
                continue

            desc_text = BeautifulSoup(j_resp.text, "html.parser").get_text(separator="\n").strip()

            # --- POST-CLICK FILTER ---
            # Now check the description for experience requirements
            if is_relevant_job(raw_title, desc_text):
                jobs.append({
                    "SITE":        base_domain,
                    "TITLE":       raw_title,
                    "COMPANY":     base_domain,
                    "CITY":        "Canada", # Placeholder
                    "STATE":       "",
                    "DATE":        datetime.now().strftime("%Y-%m-%d"),
                    "JOB_URL":     job_url,
                    "DESCRIPTION": desc_text,
                })

    return pd.DataFrame(jobs) if jobs else pd.DataFrame()


def perform_scrape(search_term: str, location: str, results_wanted: int = 100) -> pd.DataFrame:
    if scrape_jobs is None:
        return pd.DataFrame()

    # Use a more specific query for Google/Indeed to reduce noise
    google_search_term = f"{search_term} \"0-2 years\" -warehouse -driver -sales in {location}"

    jobs = scrape_jobs(
        site_name=["linkedin", "indeed", "google"],
        search_term=search_term,
        google_search_term=google_search_term,
        location=location,
        results_wanted=results_wanted,
        hours_old=168,
        country_indeed="Canada",
        linkedin_fetch_description=True,
        verbose=0,
    )

    if jobs is None or jobs.empty:
        return pd.DataFrame()

    jobs.columns = [col.upper() for col in jobs.columns]

    # --- FILTER JOBSPY RESULTS ---
    # Jobspy returns raw results. We must filter them manually.
    valid_rows = []
    for _, row in jobs.iterrows():
        title = _safe_str(row.get("TITLE"))
        desc = _safe_str(row.get("DESCRIPTION"))
        
        if is_relevant_job(title, desc):
            valid_rows.append(row)
            
    return pd.DataFrame(valid_rows, columns=jobs.columns)


def scrape_law_firm_sites() -> pd.DataFrame:
    """
    Scrape Canadian law firm career pages.
    UPDATED: Now points to specific 'Current Opportunities' or 'Lawyer' listing pages
    to avoid generic landing pages.
    """
    pages = [
        # Blakes (Associates specific)
        "https://www.joinblakes.com/careers/associates/",
        
        # Bennett Jones (Legal Professionals)
        "https://www.bennettjones.com/en/Careers/Legal-Professionals",
        
        # Fasken (Lawyers & Agents)
        "https://www.fasken.com/en/careers/lawyers",
        
        # Gowling WLG (Current Opportunities)
        "https://gowlingwlg.com/en/careers/current-opportunities/",
        
        # Stikeman Elliott (Legal Professionals)
        "https://www.stikeman.com/en/careers/legal",
        
        # Davies (DWPV) (Lawyers)
        "https://www.dwpv.com/en/Careers/Lawyers",
        
        # McCarthy Tetrault (Lawyers)
        "https://www.mccarthy.ca/en/careers/lawyers",
        
        # Torys (Lawyers)
        "https://www.torys.com/careers/lawyers",
        
        # Goodmans (Associates)
        "https://www.goodmans.ca/careers/associates",
        
        # BLG (Current Opportunities)
        "https://www.blg.com/en/careers/current-opportunities",
        
        # Miller Thomson (Lawyers)
        "https://www.millerthomson.com/en/careers/lawyers/",
        
        # Cassels (Lateral Opportunities)
        "https://cassels.com/join-us/career-opportunities-lawyers/",
        
        # Aird & Berlis (Current Opportunities)
        "https://www.airdberlis.com/join-us/current-opportunities",
        
        # Lerners (Lawyers)
        "https://www.lerners.ca/careers/lawyers/",
        
        # Lenczner Slaght (Lawyers)
        "https://www.litigate.com/careers/lawyers",
        
        # Wildeboer Dellelce
        "https://www.wildlaw.ca/careers/lawyers/",
        
        # Osler (Added - was missing)
        "https://www.osler.com/en/careers/opportunities",
    ]
    
    # Note: Some firms (Norton Rose, Dentons) use pure JavaScript/Workday portals 
    # that cannot be scraped by this simple script. They have been removed to prevent errors.
    
    return _scrape_career_pages(pages, "LawFirm")


def scrape_recruiter_sites() -> pd.DataFrame:
    """
    Scrape Canadian legal recruiter and job board sites.
    UPDATED: Points to the actual search results/job board pages.
    """
    pages = [
        # ZSA (General Job Board)
        "https://www.zsa.ca/job-board/",
        
        # The Counsel Network (Job Search)
        "https://thecounselnetwork.com/job-search/",
        
        # Life After Law (Opportunities)
        "https://www.lifeafterlaw.com/opportunities",
        
        # The Heller Group (Opportunities)
        "https://thehellergroup.ca/opportunities/",
        
        # Smith Legal Search
        "https://www.smithlegalsearch.com/opportunities/",
        
        # Cartel Inc (Job Search)
        "https://cartelinc.com/job-search/",
        
        # Edge Recruitment (Vacancies)
        "https://edgerecruitment.ca/vacancies/",
        
        # Urban Legal (Opportunities)
        "https://www.urbanlegal.ca/opportunities",
        
        # LegalJobs.ca (Direct listing)
        "https://legaljobs.ca/jobs/",
        
        # Job Bank (Filtered for 'Lawyer' in Canada)
        "https://www.jobbank.gc.ca/jobsearch/jobsearch?searchstring=lawyer&locationstring=",
        
        # Canadian Bar Association (Job Board)
        "https://www.cba.org/r/Jobs/Home",
    ]
    return _scrape_career_pages(pages, "Recruiter")

# ─────────────────────────────────────────────────────────────────────────────
# LLM / Email / Main
# ─────────────────────────────────────────────────────────────────────────────

def llm_filter(jobs: pd.DataFrame) -> pd.DataFrame:
    """Optional: Double check with AI if API key is present."""
    if openai is None or jobs.empty:
        return jobs
    
    api_key = os.environ.get("OPENAI_API_KEY")
    if not api_key:
        return jobs

    client = openai.OpenAI(api_key=api_key)
    kept_rows = []

    print(f"  [AI Filter] Checking {len(jobs)} candidates...")

    for _, row in jobs.iterrows():
        description = _safe_str(row.get("DESCRIPTION"))[:3000]
        title = _safe_str(row.get("TITLE"))
        
        prompt = (
            f"Job Title: {title}\n"
            "Analyze the job description below. Answer ONLY 'YES' or 'NO'.\n"
            "1. Is this strictly a LEGAL role (Lawyer, Associate, Articling)?\n"
            "2. Is it suitable for someone with 0-2 years of experience?\n"
            "3. Is it NOT a warehouse, sales, or administrative assistant role?\n\n"
            f"Description:\n{description}"
        )
        try:
            response = client.chat.completions.create(
                model="gpt-3.5-turbo",
                messages=[{"role": "user", "content": prompt}],
                max_tokens=5,
                temperature=0.0,
            )
            answer = response.choices[0].message.content.strip().upper()
            if "YES" in answer:
                kept_rows.append(row)
        except Exception:
            kept_rows.append(row)

    return pd.DataFrame(kept_rows, columns=jobs.columns)


def generate_summary_stats(current_jobs: pd.DataFrame, previous_ids: set) -> str:
    lines = [f"New jobs this run:      {len(current_jobs)}"]
    if not current_jobs.empty and "COMPANY" in current_jobs.columns:
        firm_counts = current_jobs["COMPANY"].str.title().value_counts().head(3)
        if not firm_counts.empty:
            lines.append("Top firms:            " + ", ".join(f"{n} ({c})" for n, c in firm_counts.items()))
    return "\n".join(lines)


def format_email_content(jobs: pd.DataFrame) -> str:
    if jobs.empty:
        return "No matching law associate jobs were found this week."

    lines = []
    for _, row in jobs.iterrows():
        title = _safe_str(row.get("TITLE"))
        company = _safe_str(row.get("COMPANY"))
        link = _safe_str(row.get("JOB_URL") or row.get("URL"))
        lines.append(f"{title} @ {company}\n{link}\n{'-'*30}")
    
    return "\n".join(lines)


def send_email(subject, body, sender, password, recipients):
    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = sender
    msg["To"] = ", ".join(recipients)
    msg.set_content(body)

    try:
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as smtp:
            smtp.login(sender, password)
            smtp.send_message(msg)
        print(f"✓ Email sent to: {', '.join(recipients)}")
    except Exception as e:
        print(f"Error sending email: {e}")


def main() -> None:
    sender = get_env_variable("EMAIL_USER")
    password = get_env_variable("EMAIL_PASS")
    recipients = [a.strip() for a in get_env_variable("EMAIL_TO").split(",") if a.strip()]
    
    print("=" * 60)
    print("Weekly Law Associate Job Scraper (Fixed)")
    print("=" * 60)

    # 1. Scrape
    all_jobs = perform_scrape("law associate", "Canada")
    firm_jobs = scrape_law_firm_sites()
    recruiter_jobs = scrape_recruiter_sites()

    # 2. Combine
    combined = pd.concat([all_jobs, firm_jobs, recruiter_jobs], ignore_index=True, sort=False)
    print(f"\nTotal raw candidates found: {len(combined)}")

    # 3. Clean & Deduplicate
    filtered = remove_old_jobs(combined)
    
    # Load History
    history_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), "job_history.json")
    history_ids = load_history(history_file)
    
    # Filter out seen jobs
    if "JOB_URL" not in filtered.columns:
        filtered["JOB_URL"] = filtered.get("URL", pd.NA) # Normalize URL col

    new_jobs = filtered[~filtered["JOB_URL"].isin(history_ids)].copy() if not filtered.empty else filtered

    # 4. LLM Filter (Final Guardrail)
    new_jobs = llm_filter(new_jobs)

    # 5. Save & Send
    if "JOB_URL" in new_jobs.columns:
        history_ids.update(new_jobs["JOB_URL"].dropna().tolist())
    save_history(history_file, history_ids)

    print(f"Final Count to Send: {len(new_jobs)}")

    if not new_jobs.empty:
        summary = generate_summary_stats(new_jobs, history_ids)
        body = summary + "\n\n" + format_email_content(new_jobs)
        send_email(f"Legal Jobs: {len(new_jobs)} Found", body, sender, password, recipients)
    else:
        print("No new jobs to email.")

if __name__ == "__main__":
    main()
