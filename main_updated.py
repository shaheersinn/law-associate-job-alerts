#!/usr/bin/env python3
"""
Weekly Law Associate Job Scraper

This script uses the python‑jobspy library to scrape job postings from multiple
boards, filters for first‑ and second‑year law associate roles (0–2 years of
experience) located in Canada, and emails a summary of those jobs.

To run locally, set the following environment variables or define them in your
shell:

    EMAIL_USER:    Sender Gmail account (e.g. example@gmail.com)
    EMAIL_PASS:    App password for the Gmail account
    EMAIL_TO:      Comma‑separated list of recipient addresses
    RESULTS_WANTED (optional): Number of results to request per board

When run via GitHub Actions, these variables are injected from repository secrets.
"""

from __future__ import annotations

import os
import re
import smtplib
import json
import time
from email.message import EmailMessage
from urllib.parse import urljoin, urlparse
from datetime import datetime, timedelta
from typing import List, Optional

import pandas as pd
try:
    from jobspy import scrape_jobs
except ImportError:
    scrape_jobs = None  # type: ignore
import requests
from requests.adapters import HTTPAdapter, Retry
from bs4 import BeautifulSoup

try:
    import openai  # type: ignore
except ImportError:
    openai = None


def get_env_variable(name: str, default: str | None = None) -> str:
    """Retrieve an environment variable or raise an error if missing."""
    value = os.environ.get(name, default)
    if value is None:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


def perform_scrape(search_term: str, location: str, results_wanted: int = 100) -> pd.DataFrame:
    """
    Scrape jobs using the jobspy library.

    Parameters
    ----------
    search_term : str
        The primary query to search for across boards.
    location : str
        Geographic location for the job search.
    results_wanted : int, optional
        Maximum number of results to return from each board, by default 100.

    Returns
    -------
    pd.DataFrame
        A DataFrame containing aggregated job postings.
    """
    # Build a Google Jobs query that includes our keywords and emphasises early‑career
    google_search_term = (
        f"{search_term} jobs first year second year 0-2 years experience in {location}"
    )

    # Request jobs from multiple boards.  JobSpy supports LinkedIn, Indeed, Google and
    # ZipRecruiter among others【123892292451392†L266-L273】.  We pass a list of board names
    # and request a modest number of results to avoid excessive scraping.  Setting
    # `hours_old=168` restricts results to the past week.
    if scrape_jobs is None:
        # jobspy is not available; return an empty DataFrame with expected columns
        columns = [
            "TITLE",
            "COMPANY",
            "CITY",
            "STATE",
            "DATE",
            "JOB_URL",
            "DESCRIPTION",
            "DATE_POSTED",
        ]
        return pd.DataFrame(columns=columns)

    jobs = scrape_jobs(
        site_name=["linkedin", "indeed", "google", "zip_recruiter"],
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

    # Standardise column names for easier processing
    jobs.columns = [col.upper() for col in jobs.columns]
    return jobs


def scrape_law_firm_sites() -> pd.DataFrame:
    """Scrape predefined Canadian law firm career pages for associate roles.

    Many law firm websites publish their open positions on dedicated career pages.  This
    helper attempts to fetch each site’s careers section, extract links to job
    descriptions containing the word "associate" and basic legal keywords, and then
    follow those links to inspect the experience requirements.  If the job
    description mentions 0–2 years of experience or first/second‑year level
    keywords, the job is recorded for later filtering.

    Returns
    -------
    pd.DataFrame
        A DataFrame with columns similar to the JobSpy output: SITE, TITLE,
        COMPANY, CITY, STATE, DATE, JOB_URL and DESCRIPTION.
    """
    # List of law firm career pages to query.  These URLs point directly to the
    # careers or opportunities sections of each firm.  Some sites may block
    # automated access; in that case the request will be skipped.
    # Expand the list of law firm career pages.  In addition to the original
    # selection, include other major firms suggested by the user.  Where available,
    # we point directly to the current opportunities section for lawyers.  Some
    # sites use dynamic job portals (e.g. Workday), which may not expose
    # listings via simple HTML.  Even if no postings are found, these pages
    # are still queried to detect associate roles when they appear.
    # Assemble a list of known Canadian law firm career pages.  Many of these URLs
    # come directly from user input.  Where a firm uses a job portal service
    # (e.g. Workday) that is embedded under a longer path, we link to the
    # landing page.  We intentionally include duplicates here and deduplicate
    # later because some firms have multiple entry points for careers.
    firm_career_pages = [
        # Core national firms
        "https://www.osler.com/en/careers/",  # Osler, Hoskin & Harcourt LLP
        "https://www.blakes.com/careers/",  # Blake, Cassels & Graydon LLP
        "https://www.bennettjones.com/Careers",  # Bennett Jones LLP
        "https://www.fasken.com/en/careers",  # Fasken Martineau DuMoulin LLP
        "https://gowlingwlg.com/en/careers/",  # Gowling WLG
        "https://www.stikeman.com/en/careers",  # Stikeman Elliott LLP
        "https://www.dwpv.com/en/Careers",  # Davies Ward Phillips & Vineberg LLP
        "https://www.mccarthy.ca/en/careers",  # McCarthy Tétrault LLP
        "https://www.torys.com/en/careers",  # Torys LLP
        "https://www.litigate.com/careers",  # Lenczner Slaght (litigate.com)

        # Additional major firms and regional powerhouses
        "https://www.goodmans.ca/careers/current-opportunities",  # Goodmans LLP
        "https://www.blg.com/en/careers/legal-professionals/current-opportunities",  # Borden Ladner Gervais LLP (BLG)
        "https://www.nortonrosefulbright.com/en-ca/careers",  # Norton Rose Fulbright Canada LLP
        "https://www.dentons.com/en/careers",  # Dentons Canada LLP
        "https://www.millerthomson.com/en/careers",  # Miller Thomson LLP
        "https://cassels.com/join-us/career-opportunities-lawyers/",  # Cassels Brock & Blackwell LLP

        # Newly added firms per user request
        "https://www.airdberlis.com/join-us",  # Aird & Berlis LLP
        "https://www.lerners.ca/careers",  # Lerners LLP
        "https://www.blaney.com/careers",  # Blaney McMurtry LLP
        "https://www.goodmans.ca/careers/current-opportunities",  # Goodmans LLP (duplicate for dedup)
        "https://cassels.com/join-us/career-opportunities-lawyers/",  # Cassels (duplicate)
    ]

    # Deduplicate while preserving order
    firm_career_pages = list(dict.fromkeys(firm_career_pages))

    jobs: list[dict] = []
    # Positive experience patterns to identify early‑career roles
    positive_patterns = [
        r"0\s*-?\s*2\s*years",
        r"0\s*to\s*2\s*years",
        r"1\s*-?\s*2\s*years",
        r"\bfirst\s*year\b",
        r"\bsecond\s*year\b",
        r"\bentry[-\s]*level\b",
        r"\bjunior\b",
        r"\barticling\s+associate\b",
        r"\bcalled\s+to\s+the\s+bar\s+within\s*2\s*years\b",
        r"\bnewly\s+called\b",
        r"\brecent\s+call\b",
        r"\b1\s*-?\s*2\s*years\s*PQE\b",
    ]
    positive_regex = re.compile("|".join(positive_patterns), re.IGNORECASE)

    # Negative patterns to exclude senior or non‑associate roles
    negative_patterns = [
        r"\bsenior\b",
        r"\bpartner\b",
        r"\b(3|4|5|6|7|8|9|10)\+?\s*years\b",
        r"\blead\s+counsel\b",
        r"\bmanager\b",
        r"\bexecutive\b",
    ]
    negative_regex = re.compile("|".join(negative_patterns), re.IGNORECASE)

    # Use a standard desktop user‑agent to reduce the chance of blocking
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
            " AppleWebKit/537.36 (KHTML, like Gecko)"
            " Chrome/109 Safari/537.36"
        )
    }

    for url in firm_career_pages:
        try:
            resp = requests.get(url, headers=headers, timeout=20)
        except Exception:
            # Skip sites that cannot be reached
            continue
        if resp.status_code != 200:
            # Many sites protect against bots; ignore non‑200 responses
            continue
        soup = BeautifulSoup(resp.text, "html.parser")
        base_domain = urlparse(url).netloc

        # Collect job links on the careers page.  We look for anchors containing
        # the word "associate" and at least one legal keyword; this reduces
        # noise from support roles (e.g., IT positions).  Titles are kept to
        # improve readability later.
        for link in soup.find_all("a", href=True):
            title = link.get_text(strip=True)
            if not title:
                continue
            lower_title = title.lower()
            # Must contain 'associate' and a legal keyword
            if "associate" not in lower_title:
                continue
            if not any(k in lower_title for k in ("law", "legal", "lawyer")):
                continue
            job_url = urljoin(url, link["href"])
            # Attempt to fetch the job posting
            try:
                j_resp = requests.get(job_url, headers=headers, timeout=20)
            except Exception:
                continue
            if j_resp.status_code != 200:
                continue
            job_soup = BeautifulSoup(j_resp.text, "html.parser")
            text = job_soup.get_text(separator="\n").strip()
            if not text:
                continue
            # Skip roles that clearly mention seniority or high experience
            if negative_regex.search(text):
                continue
            # Only keep roles mentioning early‑career patterns
            if not positive_regex.search(text):
                continue
            jobs.append(
                {
                    "SITE": base_domain,
                    "TITLE": title,
                    "COMPANY": base_domain,
                    "CITY": "",  # Law firm postings often embed location in description
                    "STATE": "",
                    "DATE": "",
                    "JOB_URL": job_url,
                    "DESCRIPTION": text,
                }
            )
    if jobs:
        return pd.DataFrame(jobs)
    return pd.DataFrame()


def scrape_recruiter_sites() -> pd.DataFrame:
    """
    Scrape legal recruiter and job board websites for law associate roles.

    The user requested additional sites such as zsa.ca, thecounselnetwork.com,
    lifeafterlaw.com, thehellergroup.ca, smithlegalsearch.com,
    cartelinc.com, edgerecruitment.ca and urbanlegal.ca/careers.  Many of
    these sites list legal job opportunities on static pages.  This helper
    attempts to collect links containing "associate" and legal keywords from
    the landing pages and then inspects each posting for early‑career
    experience patterns.  If a posting contains negative patterns
    (e.g. "senior", "partner", or 3+ years), it is discarded.

    Returns
    -------
    pd.DataFrame
        A DataFrame of recruiter jobs with fields similar to the law firm
        scraper: SITE, TITLE, COMPANY, CITY, STATE, DATE, JOB_URL, DESCRIPTION.
    """
    # A mix of legal recruiters and law‑specific job boards suggested by the user.
    # These pages often list legal openings not found on mainstream boards.
    recruiter_pages = [
        "https://www.zsa.ca/",  # ZSA Legal Recruitment
        "https://www.thecounselnetwork.com/",  # The Counsel Network
        "https://www.lifeafterlaw.com/",  # Life After Law
        "https://www.thehellergroup.ca/",  # The Heller Group
        "https://www.smithlegalsearch.com/",  # Smith Legal Search
        "https://cartelinc.com/",  # Cartel Inc.
        "https://edgerecruitment.ca/",  # Edge Recruitment
        "https://www.urbanlegal.ca/careers",  # Urban Legal Recruitment
        # Additional Canadian legal job boards and organisations
        "https://www.legaljobs.ca/",  # LegalJobs.ca
        "https://lexology.com/jobs",  # Lexology Jobs
        "https://www.clawbie.com/",  # Clawbie (Canadian legal jobs)
        "https://www.cba.org/Careers",  # Canadian Bar Association careers
        "https://www.lsuc.on.ca/",  # Law Society of Ontario (jobs may be posted)
        "https://www.ontario.ca/jobs",  # Ontario government jobs
        "https://www.workopolis.com/",  # Workopolis (job board)
        "https://www.jobbank.gc.ca/",  # Government of Canada Job Bank
    ]
    # Use the same positive and negative patterns as the law firm scraper
    positive_patterns = [
        r"0\s*-?\s*2\s*years",
        r"0\s*to\s*2\s*years",
        r"1\s*-?\s*2\s*years",
        r"\bfirst\s*year\b",
        r"\bsecond\s*year\b",
        r"\bentry[-\s]*level\b",
        r"\bjunior\b",
        r"\barticling\s+associate\b",
        r"\bcalled\s+to\s+the\s+bar\s+within\s*2\s*years\b",
        r"\bnewly\s+called\b",
        r"\brecent\s+call\b",
        r"\b1\s*-?\s*2\s*years\s*PQE\b",
    ]
    positive_regex = re.compile("|".join(positive_patterns), re.IGNORECASE)
    negative_patterns = [
        r"\bsenior\b",
        r"\bpartner\b",
        r"\b(3|4|5|6|7|8|9|10)\+?\s*years\b",
        r"\blead\s+counsel\b",
        r"\bmanager\b",
        r"\bexecutive\b",
    ]
    negative_regex = re.compile("|".join(negative_patterns), re.IGNORECASE)

    jobs: list[dict] = []
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
            " AppleWebKit/537.36 (KHTML, like Gecko)"
            " Chrome/109 Safari/537.36"
        )
    }
    for page_url in recruiter_pages:
        try:
            resp = requests.get(page_url, headers=headers, timeout=20)
        except Exception:
            continue
        if resp.status_code != 200:
            continue
        soup = BeautifulSoup(resp.text, "html.parser")
        base_domain = urlparse(page_url).netloc
        # Look for anchor tags likely representing job postings.  Many of these
        # recruiter sites list jobs as <a> elements with the job title.  We
        # require the link text to include "associate" and at least one legal
        # keyword to reduce noise.
        for link in soup.find_all("a", href=True):
            title = link.get_text(strip=True)
            if not title:
                continue
            lower_title = title.lower()
            if "associate" not in lower_title:
                continue
            if not any(k in lower_title for k in ("law", "legal", "lawyer")):
                continue
            job_url = urljoin(page_url, link["href"])
            # Fetch job detail page
            try:
                j_resp = requests.get(job_url, headers=headers, timeout=20)
            except Exception:
                continue
            if j_resp.status_code != 200:
                continue
            job_soup = BeautifulSoup(j_resp.text, "html.parser")
            text = job_soup.get_text(separator="\n").strip()
            if not text:
                continue
            if negative_regex.search(text):
                continue
            if not positive_regex.search(text):
                continue
            jobs.append(
                {
                    "SITE": base_domain,
                    "TITLE": title,
                    "COMPANY": base_domain,
                    "CITY": "",
                    "STATE": "",
                    "DATE": "",
                    "JOB_URL": job_url,
                    "DESCRIPTION": text,
                }
            )
    if jobs:
        return pd.DataFrame(jobs)
    return pd.DataFrame()


def load_history(history_path: str) -> set[str]:
    """Load the set of previously sent job identifiers (URLs) from a JSON file."""
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


def save_history(history_path: str, job_ids: set[str]) -> None:
    """Persist the set of job identifiers to a JSON file."""
    try:
        with open(history_path, "w", encoding="utf-8") as f:
            json.dump(sorted(job_ids), f, indent=2)
    except Exception:
        pass


def remove_old_jobs(df: pd.DataFrame, max_age_days: int = 40) -> pd.DataFrame:
    """
    Remove jobs older than a given age threshold.  This operates on the DATE or
    DATE_POSTED column if available.  Jobs lacking a date are retained.

    Parameters
    ----------
    df : pd.DataFrame
        The DataFrame of jobs to filter.
    max_age_days : int, optional
        Maximum age in days for a job to be considered fresh.  Defaults to 40.

    Returns
    -------
    pd.DataFrame
        The filtered DataFrame.
    """
    if df.empty:
        return df
    cutoff = datetime.utcnow() - timedelta(days=max_age_days)
    def is_recent(date_str: str) -> bool:
        if not date_str:
            return True
        for fmt in ("%Y-%m-%d", "%d %b %Y", "%b %d, %Y", "%d %B %Y"):
            try:
                parsed = datetime.strptime(date_str.strip(), fmt)
                return parsed >= cutoff
            except Exception:
                continue
        return True
    # Create a boolean mask; combine DATE and DATE_POSTED fields if available
    dates = df.get("DATE", "").fillna("")
    date_posted = df.get("DATE_POSTED", "").fillna("")
    mask = dates.combine(date_posted, lambda d1, d2: d1 or d2).apply(is_recent)
    return df[mask].copy()


def rate_limited_request(session: requests.Session, url: str, headers: dict[str, str], timeout: int = 20) -> Optional[str]:
    """
    Perform an HTTP GET request with simple rate limiting and retry logic.

    A requests.Session is used to leverage connection pooling.  We apply a
    short sleep between consecutive requests to reduce the chance of being
    blocked by the server.  Retries are handled by the session’s adapter.

    Parameters
    ----------
    session : requests.Session
        The session through which to issue the request.
    url : str
        The URL to fetch.
    headers : dict[str, str]
        HTTP headers to include.
    timeout : int
        Request timeout in seconds.

    Returns
    -------
    Optional[str]
        The response text if successful, otherwise None.
    """
    try:
        resp = session.get(url, headers=headers, timeout=timeout)
        time.sleep(1)  # simple delay to avoid rapid-fire requests
    except Exception:
        return None
    if resp.status_code != 200:
        return None
    return resp.text


def filter_jobs(df: pd.DataFrame) -> pd.DataFrame:
    """
    Filter the scraped jobs to include only first‑/second‑year law associate roles.

    The function checks the title and description for legal keywords and
    experience‑level patterns.
    """
    if df.empty:
        return df

    # Combine title and description for easier searching
    df = df.copy()
    df["TEXT"] = (df["TITLE"].fillna("") + "\n" + df["DESCRIPTION"].fillna("")).str.lower()

    # Keywords that must be present to imply the job is for a law associate
    legal_keywords = [
        r"\bassociate\b",
        r"\blaw\b",
        r"\blegal\b",
        r"\blawyer\b",
    ]

    # Positive experience patterns capturing early‑career variants
    positive_patterns = [
        r"0\s*-?\s*2\s*years",
        r"0\s*to\s*2\s*years",
        r"1\s*-?\s*2\s*years",
        r"\bfirst\s*year\b",
        r"\bsecond\s*year\b",
        r"\bentry[-\s]*level\b",
        r"\bjunior\b",
        r"\barticling\s+associate\b",
        r"\bcalled\s+to\s+the\s+bar\s+within\s*2\s*years\b",
        r"\bnewly\s+called\b",
        r"\brecent\s+call\b",
        r"\b1\s*-?\s*2\s*years\s*PQE\b",
    ]
    # Negative patterns to exclude senior roles
    negative_patterns = [
        r"\bsenior\b",
        r"\bpartner\b",
        r"\b(3|4|5|6|7|8|9|10)\+?\s*years\b",
        r"\blead\s+counsel\b",
        r"\bmanager\b",
        r"\bexecutive\b",
    ]

    # Compile regex patterns for efficiency
    legal_regex = re.compile("|".join(legal_keywords), re.IGNORECASE)
    positive_regex = re.compile("|".join(positive_patterns), re.IGNORECASE)
    negative_regex = re.compile("|".join(negative_patterns), re.IGNORECASE)

    # Apply filters
    matches_legal = df["TEXT"].str.contains(legal_regex)
    matches_positive = df["TEXT"].str.contains(positive_regex)
    matches_negative = df["TEXT"].str.contains(negative_regex)

    filtered = df[matches_legal & matches_positive & ~matches_negative].copy()

    # Drop duplicates by job URL if present
    if "JOB_URL" in filtered.columns:
        filtered = filtered.drop_duplicates(subset=["JOB_URL"])
    return filtered


def llm_filter(jobs: pd.DataFrame) -> pd.DataFrame:
    """
    Optionally filter jobs using a large language model (LLM).

    Each job description is passed to an LLM (OpenAI) with a question asking
    whether the job requires 0–2 years of experience and is for a law
    associate position in Canada.  The model is expected to answer "yes"
    or "no" followed by a brief explanation.  If the answer is "yes", the job
    is kept; otherwise it is removed.

    If the `openai` package is unavailable or the `OPENAI_API_KEY` environment
    variable is not set, this function simply returns the original DataFrame
    without filtering.

    Parameters
    ----------
    jobs : pd.DataFrame
        DataFrame of jobs to evaluate.

    Returns
    -------
    pd.DataFrame
        Filtered DataFrame containing only jobs for which the LLM responded
        positively.
    """
    if openai is None:
        return jobs
    api_key = os.environ.get("OPENAI_API_KEY")
    if not api_key:
        return jobs
    openai.api_key = api_key  # type: ignore
    kept_rows = []
    for _, row in jobs.iterrows():
        description = row.get("DESCRIPTION", "") or ""
        # Limit description length to avoid exceeding context limits
        prompt_desc = description[:4000]
        prompt = (
            "You are a helpful assistant for filtering job postings. "
            "Please read the following job description and answer succinctly.\n"
            f"Job description: {prompt_desc}\n"
            "Question: Does this job require 0–2 years of experience and is it for "
            "a law associate position in Canada? Answer 'yes' or 'no' with a brief reason."
        )
        try:
            response = openai.ChatCompletion.create(  # type: ignore
                model="gpt-3.5-turbo",
                messages=[{"role": "user", "content": prompt}],
                max_tokens=50,
                temperature=0.0,
            )
            answer = response.choices[0].message.content.strip().lower()  # type: ignore
            if answer.startswith("yes"):
                kept_rows.append(row)
        except Exception:
            # On any API error, keep the job to avoid accidental exclusion
            kept_rows.append(row)
    if kept_rows:
        return pd.DataFrame(kept_rows)
    return pd.DataFrame(columns=jobs.columns)


def format_email_content(jobs: pd.DataFrame) -> str:
    """
    Construct a plain‑text email summarising the filtered jobs.
    """
    if jobs.empty:
        return (
            "No matching law associate jobs were found this week.\n\n"
            "Your alert agent searched LinkedIn, Indeed, Google Jobs and ZipRecruiter,"
            " but none of the recent postings met the 0–2 years' experience criteria."
        )

    lines: List[str] = []
    for _, row in jobs.iterrows():
        site = row.get("SITE", "Unknown site").title()
        title = row.get("TITLE", "Unknown title")
        company = row.get("COMPANY", "Unknown company")
        city = row.get("CITY", "")
        state = row.get("STATE", "")
        location = ", ".join(filter(None, [city, state]))
        date_posted = row.get("DATE", row.get("DATE_POSTED", ""))
        link = row.get("JOB_URL", row.get("URL", ""))
        description = row.get("DESCRIPTION", "").strip().replace("\n", " ")
        snippet = description[:200] + ("..." if len(description) > 200 else "")

        lines.append(
            f"Site: {site}\n"
            f"Title: {title}\n"
            f"Company: {company}\n"
            f"Location: {location}\n"
            f"Date posted: {date_posted}\n"
            f"Link: {link}\n"
            f"Summary: {snippet}\n"
            f"---\n"
        )
    body = (
        f"Here are the latest first‑ and second‑year law associate job postings as of "
        f"{datetime.utcnow():%Y-%m-%d} (UTC):\n\n"
    )
    body += "\n".join(lines)
    return body


def generate_summary_stats(current_jobs: pd.DataFrame, previous_ids: set[str]) -> str:
    """
    Generate a short summary of job trends between the current run and the job history.

    The summary reports how many new jobs were found this week, which firms appear
    most frequently among the new postings, and the top cities (if available).

    Parameters
    ----------
    current_jobs : pd.DataFrame
        DataFrame of jobs that will be emailed (i.e. new jobs only).
    previous_ids : set[str]
        The set of job URLs that were previously sent.

    Returns
    -------
    str
        A plain‑text summary paragraph.
    """
    num_new = len(current_jobs)
    # Compute counts per firm/company
    firm_counts = (
        current_jobs.get("COMPANY").str.title().value_counts().head(3)
        if not current_jobs.empty
        else pd.Series(dtype=int)
    )
    city_counts = (
        current_jobs.get("CITY").str.title().value_counts().head(3)
        if not current_jobs.empty
        else pd.Series(dtype=int)
    )
    # Determine the number of jobs in history to estimate last run's jobs
    prev_total = len(previous_ids)
    summary_lines: List[str] = []
    summary_lines.append(f"New jobs this run: {num_new}")
    summary_lines.append(f"Previously sent jobs: {prev_total}")
    if not firm_counts.empty:
        firms_summary = ", ".join(f"{name} ({count})" for name, count in firm_counts.items())
        summary_lines.append(f"Top hiring firms: {firms_summary}")
    if not city_counts.empty:
        cities_summary = ", ".join(f"{name} ({count})" for name, count in city_counts.items())
        summary_lines.append(f"Top cities: {cities_summary}")
    return "\n".join(summary_lines)


def format_email_html(jobs: pd.DataFrame) -> str:
    """
    Construct an HTML email summarising the filtered jobs.

    Each job is presented with a bold title, company, location and a link to the
    original posting.  A brief snippet of the description is included.  A
    timestamp notes when the report was generated.  If no jobs are found, a
    message is returned indicating this.

    Parameters
    ----------
    jobs : pd.DataFrame
        DataFrame of filtered jobs.

    Returns
    -------
    str
        HTML string ready for the email body.
    """
    if jobs.empty:
        return (
            "<p>No matching law associate jobs were found this week.</p>"
            "<p>Your alert agent searched multiple job boards and firm websites, "
            "but none of the recent postings met the 0–2 years' experience criteria.</p>"
        )
    lines: List[str] = []
    for _, row in jobs.iterrows():
        site = row.get("SITE", "Unknown site").title()
        title = row.get("TITLE", "Unknown title")
        company = row.get("COMPANY", "Unknown company")
        city = row.get("CITY", "")
        state = row.get("STATE", "")
        location = ", ".join(filter(None, [city, state]))
        date_posted = row.get("DATE", row.get("DATE_POSTED", ""))
        link = row.get("JOB_URL", row.get("URL", ""))
        description = row.get("DESCRIPTION", "").strip().replace("\n", " ")
        snippet = description[:200] + ("..." if len(description) > 200 else "")
        lines.append(
            f"<div style='margin-bottom:20px;'>"
            f"<strong>{title}</strong><br>"
            f"<em>{company} – {site}</em><br>"
            f"<span>Location: {location}</span><br>"
            f"<span>Date posted: {date_posted}</span><br>"
            f"<a href='{link}'>Apply here</a><br>"
            f"<span>{snippet}</span>"
            f"</div>"
        )
    html_body = (
        f"<p>Here are the latest first‑ and second‑year law associate job postings as of "
        f"{datetime.utcnow():%Y-%m-%d} (UTC):</p>" + "\n".join(lines)
    )
    return html_body


def send_email(
    subject: str,
    body: str,
    sender: str,
    password: str,
    recipients: List[str],
    html: Optional[str] = None,
) -> None:
    """
    Send an email via Gmail’s SMTP server.

    If an HTML body is provided, the message will be sent as a multipart email
    containing both plain‑text and HTML versions.  Otherwise, a plain text
    message is sent.
    """
    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = sender
    msg["To"] = ", ".join(recipients)
    msg.set_content(body)
    if html:
        msg.add_alternative(html, subtype="html")

    # Use SSL connection for Gmail; port 465 is the default
    with smtplib.SMTP_SSL("smtp.gmail.com", 465) as smtp:
        smtp.login(sender, password)
        smtp.send_message(msg)


def main() -> None:
    # Load configuration
    sender = get_env_variable("EMAIL_USER")
    password = get_env_variable("EMAIL_PASS")
    recipients_raw = get_env_variable("EMAIL_TO")
    recipients = [addr.strip() for addr in recipients_raw.split(",") if addr.strip()]

    # Check for dry‑run mode: if set, no email will be sent
    dry_run = os.environ.get("DRY_RUN", "0").lower() in {"1", "true", "yes"}

    # Parameter for number of results; default to 100 if not provided
    try:
        results_wanted = int(os.environ.get("RESULTS_WANTED", "100"))
    except ValueError:
        results_wanted = 100

    # Define the search term and location
    search_term = "law associate"
    location = "Canada"

    # Scrape public job boards via JobSpy
    all_jobs = perform_scrape(search_term, location, results_wanted)

    # Scrape Canadian law firm career pages
    firm_jobs = scrape_law_firm_sites()

    # Scrape recruiter and legal job board sites
    recruiter_jobs = scrape_recruiter_sites()

    # Combine all job sources
    combined_jobs = pd.concat([all_jobs, firm_jobs, recruiter_jobs], ignore_index=True, sort=False)

    # Filter jobs for first‑/second‑year roles
    filtered_jobs = filter_jobs(combined_jobs)

    # Remove jobs older than 40 days
    filtered_jobs = remove_old_jobs(filtered_jobs, max_age_days=40)

    # Deduplicate against history
    history_file = os.path.join(os.path.dirname(__file__), "job_history.json")
    history_ids = load_history(history_file)
    # Preserve a copy of previous IDs for summary statistics
    prev_ids = history_ids.copy()
    new_jobs_mask = ~filtered_jobs["JOB_URL"].isin(history_ids)
    new_jobs = filtered_jobs[new_jobs_mask].copy()

    # Optional LLM filtering: call an LLM to verify each job fits the 0–2 year associate criteria
    new_jobs = llm_filter(new_jobs)

    # Update history with new job URLs
    history_ids.update(new_jobs["JOB_URL"].dropna().tolist())
    save_history(history_file, history_ids)

    # Generate summary statistics comparing this run to previous history
    summary_text = generate_summary_stats(new_jobs, prev_ids)

    # Build email content (plain text and HTML)
    plain_body = summary_text + "\n\n" + format_email_content(new_jobs)
    html_body = (
        "<p>" + "<br>".join(summary_text.split("\n")) + "</p>" + format_email_html(new_jobs)
    )
    subject = "Law Associate Job Alerts (0–2 years experience)"

    if dry_run:
        # Print the email content instead of sending
        print(subject)
        print(plain_body)
        return

    # Send the email (both plain and HTML)
    send_email(subject, plain_body, sender, password, recipients, html=html_body)


if __name__ == "__main__":
    main()