#!/usr/bin/env python3
"""
Weekly Law Associate Job Scraper

This script uses the python-jobspy library to scrape job postings from multiple
boards, filters for first- and second-year law associate roles (0-2 years of
experience) located in Canada, and emails a summary of those jobs.

To run locally, set the following environment variables or define them in your
shell:

    EMAIL_USER:    Sender Gmail account (e.g. example@gmail.com)
    EMAIL_PASS:    Gmail App Password (NOT your regular Gmail password)
                   Generate one at: myaccount.google.com/apppasswords
    EMAIL_TO:      Comma-separated list of recipient addresses
    RESULTS_WANTED (optional): Number of results to request per board
    DRY_RUN (optional): Set to "1" to print email instead of sending it
    OPENAI_API_KEY (optional): If set, enables LLM-based job filtering

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
from bs4 import BeautifulSoup

# NOTE: HTTPAdapter and Retry were imported but never used in the original — removed.

try:
    import openai  # type: ignore
except ImportError:
    openai = None


# ─────────────────────────────────────────────────────────────────────────────
# Shared regex patterns (compiled once, reused across all scrapers + filter)
# ─────────────────────────────────────────────────────────────────────────────

_POSITIVE_PATTERNS = [
    r"0\s*-?\s*2\s*years",
    r"0\s*to\s*2\s*years",
    r"1\s*-?\s*2\s*years",
    r"\bfirst[\s\-]*year\b",
    r"\bsecond[\s\-]*year\b",
    r"\bentry[-\s]*level\b",
    r"\bjunior\b",
    r"\barticling\s+associate\b",
    r"\bcalled\s+to\s+the\s+bar\s+within\s*2\s*years\b",
    r"\bnewly\s+called\b",
    r"\brecent\s+call\b",
    r"\b1\s*-?\s*2\s*years\s*PQE\b",
]

_NEGATIVE_PATTERNS = [
    r"\bsenior\b",
    r"\bpartner\b",
    r"\b(3|4|5|6|7|8|9|10)\+?\s*years\b",
    r"\blead\s+counsel\b",
    r"\bmanager\b",
    r"\bexecutive\b",
]

_LEGAL_KEYWORDS = [
    r"\bassociate\b",
    r"\blaw\b",
    r"\blegal\b",
    r"\blawyer\b",
]

POSITIVE_REGEX = re.compile("|".join(_POSITIVE_PATTERNS), re.IGNORECASE)
NEGATIVE_REGEX = re.compile("|".join(_NEGATIVE_PATTERNS), re.IGNORECASE)
LEGAL_REGEX    = re.compile("|".join(_LEGAL_KEYWORDS),    re.IGNORECASE)

_SCRAPER_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
        " AppleWebKit/537.36 (KHTML, like Gecko)"
        " Chrome/120.0.0.0 Safari/537.36"
    )
}


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def get_env_variable(name: str, default: Optional[str] = None) -> str:
    """Retrieve an environment variable or raise an error if missing."""
    value = os.environ.get(name, default)
    if value is None:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


def _safe_str(value) -> str:
    """Return a clean string from a DataFrame cell value, handling NaN/None."""
    if value is None or (isinstance(value, float) and value != value):
        return ""
    return str(value).strip()


def load_history(history_path: str) -> set:
    """Load the set of previously sent job URLs from a JSON file."""
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
    """Persist the set of job URLs to a JSON file."""
    try:
        with open(history_path, "w", encoding="utf-8") as f:
            json.dump(sorted(job_ids), f, indent=2)
    except Exception:
        pass


def remove_old_jobs(df: pd.DataFrame, max_age_days: int = 40) -> pd.DataFrame:
    """
    Remove jobs older than max_age_days. Jobs with no parseable date are kept.

    FIX: Original used df.get("DATE") which is a dict method, not a DataFrame
    method, and would silently return None instead of the column. Replaced
    with proper column access guarded by an 'in df.columns' check.
    """
    if df.empty:
        return df

    cutoff = datetime.utcnow() - timedelta(days=max_age_days)

    def is_recent(date_str: str) -> bool:
        if not date_str:
            return True
        for fmt in ("%Y-%m-%d", "%d %b %Y", "%b %d, %Y", "%d %B %Y"):
            try:
                return datetime.strptime(date_str.strip(), fmt) >= cutoff
            except ValueError:
                continue
        return True

    dates       = df["DATE"].fillna("")        if "DATE"        in df.columns else pd.Series("", index=df.index)
    date_posted = df["DATE_POSTED"].fillna("") if "DATE_POSTED" in df.columns else pd.Series("", index=df.index)
    combined    = dates.where(dates != "", date_posted)
    return df[combined.apply(is_recent)].copy()


def _scrape_career_pages(page_urls: List[str], source_label: str) -> pd.DataFrame:
    """
    Shared scraping loop for career-page URLs.
    Used by both scrape_law_firm_sites() and scrape_recruiter_sites().
    """
    jobs: List[dict] = []

    for url in page_urls:
        print(f"  [{source_label}] Scraping: {url}")
        try:
            resp = requests.get(url, headers=_SCRAPER_HEADERS, timeout=20)
        except Exception as exc:
            print(f"    SKIP — could not reach: {exc}")
            continue
        if resp.status_code != 200:
            print(f"    SKIP — HTTP {resp.status_code}")
            continue

        soup        = BeautifulSoup(resp.text, "html.parser")
        base_domain = urlparse(url).netloc

        for link in soup.find_all("a", href=True):
            title = link.get_text(strip=True)
            if not title:
                continue
            lower_title = title.lower()
            if "associate" not in lower_title:
                continue
            if not any(k in lower_title for k in ("law", "legal", "lawyer")):
                continue

            job_url = urljoin(url, link["href"])
            try:
                j_resp = requests.get(job_url, headers=_SCRAPER_HEADERS, timeout=20)
                time.sleep(0.5)  # polite delay between requests
            except Exception:
                continue
            if j_resp.status_code != 200:
                continue

            text = BeautifulSoup(j_resp.text, "html.parser").get_text(separator="\n").strip()
            if not text:
                continue
            if NEGATIVE_REGEX.search(text):
                continue
            if not POSITIVE_REGEX.search(text):
                continue

            jobs.append({
                "SITE":        base_domain,
                "TITLE":       title,
                "COMPANY":     base_domain,
                "CITY":        "",
                "STATE":       "",
                "DATE":        "",
                "JOB_URL":     job_url,
                "DESCRIPTION": text,
            })

    return pd.DataFrame(jobs) if jobs else pd.DataFrame()


# ─────────────────────────────────────────────────────────────────────────────
# Scrapers
# ─────────────────────────────────────────────────────────────────────────────

def perform_scrape(search_term: str, location: str, results_wanted: int = 100) -> pd.DataFrame:
    """Scrape jobs using the jobspy library."""
    if scrape_jobs is None:
        print("  WARNING: jobspy not installed. Skipping job board scrape.")
        return pd.DataFrame()

    google_search_term = (
        f"{search_term} jobs first year second year 0-2 years experience in {location}"
    )

    # FIX: ZipRecruiter removed — returns 403 Cloudflare WAF block on GitHub Actions IPs.
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
    return jobs


def scrape_law_firm_sites() -> pd.DataFrame:
    """Scrape predefined Canadian law firm career pages for associate roles."""
    pages = list(dict.fromkeys([
        "https://recruiting.ultipro.ca/CAR5001CARS/JobBoard/65254eda-a168-4846-86ed-442ed6042262/?q=&o=postedDateDesc",
        "https://www.joinblakes.com/jobs/?orderby=date&order=desc",
        "https://www.bennettjones.com/Careers",
        "https://www.fasken.com/en/careers",
        "https://gowlingwlg.com/en/careers/",
        "https://www.stikeman.com/en/careers",
        "https://www.dwpv.com/en/Careers",
        "https://www.mccarthy.ca/en/careers",
        "https://www.torys.com/en/careers",
        "https://www.litigate.com/careers",
        "https://www.goodmans.ca/careers/current-opportunities",
        "https://www.blg.com/en/careers/legal-professionals/current-opportunities",
        "https://www.nortonrosefulbright.com/en-ca/careers",
        "https://www.dentons.com/en/careers",
        "https://www.millerthomson.com/en/careers",
        "https://cassels.com/join-us/career-opportunities-lawyers/",
        "https://www.airdberlis.com/join-us",
        "https://www.lerners.ca/careers",
        "https://www.blaney.com/careers",
    ]))
    return _scrape_career_pages(pages, "LawFirm")


def scrape_recruiter_sites() -> pd.DataFrame:
    """Scrape Canadian legal recruiter and job board sites."""
    pages = list(dict.fromkeys([
        "https://www.zsa.ca/current-opportunities/?search_keywords=&search_location=",
        "https://www.thecounselnetwork.com/",
        "https://www.lifeafterlaw.com/",
        "https://www.thehellergroup.ca/",
        "https://www.smithlegalsearch.com/",
        "https://cartelinc.com/",
        "https://edgerecruitment.ca/",
        "https://www.urbanlegal.ca/careers",
        "https://www.legaljobs.ca/",
        "https://www.cba.org/Careers",
        "https://www.workopolis.com/",
        "https://www.jobbank.gc.ca/",
    ]))
    return _scrape_career_pages(pages, "Recruiter")


# ─────────────────────────────────────────────────────────────────────────────
# Filtering
# ─────────────────────────────────────────────────────────────────────────────

def filter_jobs(df: pd.DataFrame) -> pd.DataFrame:
    """Filter scraped jobs to first-/second-year law associate roles only."""
    if df.empty:
        return df

    df = df.copy()
    df["TEXT"] = (
        df["TITLE"].fillna("") + "\n" + df["DESCRIPTION"].fillna("")
    ).str.lower()

    matches_legal    = df["TEXT"].str.contains(LEGAL_REGEX)
    matches_positive = df["TEXT"].str.contains(POSITIVE_REGEX)
    matches_negative = df["TEXT"].str.contains(NEGATIVE_REGEX)

    filtered = df[matches_legal & matches_positive & ~matches_negative].copy()

    if "JOB_URL" in filtered.columns:
        filtered = filtered.drop_duplicates(subset=["JOB_URL"])

    return filtered


def llm_filter(jobs: pd.DataFrame) -> pd.DataFrame:
    """
    Optionally verify jobs with an LLM (OpenAI).

    FIX: Original used the deprecated openai.ChatCompletion.create() which was
    removed in openai>=1.0.0. Updated to use the current openai.OpenAI() client.
    """
    if openai is None:
        return jobs
    api_key = os.environ.get("OPENAI_API_KEY")
    if not api_key:
        return jobs

    client    = openai.OpenAI(api_key=api_key)
    kept_rows = []

    for _, row in jobs.iterrows():
        description = _safe_str(row.get("DESCRIPTION"))[:4000]
        prompt = (
            "Read the job description below and answer only 'yes' or 'no'.\n"
            "Does this job require 0-2 years of experience and is it for a "
            "law associate position in Canada?\n\n"
            f"Description:\n{description}"
        )
        try:
            response = client.chat.completions.create(
                model="gpt-3.5-turbo",
                messages=[{"role": "user", "content": prompt}],
                max_tokens=10,
                temperature=0.0,
            )
            answer = response.choices[0].message.content.strip().lower()
            if answer.startswith("yes"):
                kept_rows.append(row)
        except Exception:
            kept_rows.append(row)  # keep on error to avoid accidental exclusion

    if kept_rows:
        return pd.DataFrame(kept_rows, columns=jobs.columns)
    return pd.DataFrame(columns=jobs.columns)


# ─────────────────────────────────────────────────────────────────────────────
# Email formatting
# ─────────────────────────────────────────────────────────────────────────────

def generate_summary_stats(current_jobs: pd.DataFrame, previous_ids: set) -> str:
    """Generate a plain-text summary of this run's results."""
    lines: List[str] = [
        f"New jobs this run:      {len(current_jobs)}",
        f"Previously sent jobs:   {len(previous_ids)}",
    ]

    if not current_jobs.empty:
        # FIX: Use df[col] not df.get(col) — get() is a dict method, not DataFrame
        if "COMPANY" in current_jobs.columns:
            firm_counts = current_jobs["COMPANY"].str.title().value_counts().head(3)
            if not firm_counts.empty:
                lines.append(
                    "Top hiring firms:       "
                    + ", ".join(f"{n} ({c})" for n, c in firm_counts.items())
                )
        if "CITY" in current_jobs.columns:
            city_counts = (
                current_jobs["CITY"].replace("", pd.NA).dropna()
                .str.title().value_counts().head(3)
            )
            if not city_counts.empty:
                lines.append(
                    "Top cities:             "
                    + ", ".join(f"{n} ({c})" for n, c in city_counts.items())
                )

    return "\n".join(lines)


def format_email_content(jobs: pd.DataFrame) -> str:
    """Construct a plain-text email body."""
    if jobs.empty:
        return (
            "No matching law associate jobs were found this week.\n\n"
            "Your alert agent searched LinkedIn, Indeed, Google Jobs, Canadian law"
            " firm career pages, and legal recruiter sites, but none of the recent"
            " postings met the 0-2 years experience criteria for Canada."
        )

    lines: List[str] = []
    for _, row in jobs.iterrows():
        site        = _safe_str(row.get("SITE"))        or "Unknown site"
        title       = _safe_str(row.get("TITLE"))       or "Unknown title"
        company     = _safe_str(row.get("COMPANY"))     or "Unknown company"
        city        = _safe_str(row.get("CITY"))
        state       = _safe_str(row.get("STATE"))
        location    = ", ".join(filter(None, [city, state])) or "Canada"
        date_posted = _safe_str(row.get("DATE") or row.get("DATE_POSTED")) or "N/A"
        link        = _safe_str(row.get("JOB_URL")  or row.get("URL"))
        description = _safe_str(row.get("DESCRIPTION")).replace("\n", " ")
        snippet     = description[:300] + ("..." if len(description) > 300 else "")

        lines.append(
            f"Site:        {site.title()}\n"
            f"Title:       {title}\n"
            f"Company:     {company}\n"
            f"Location:    {location}\n"
            f"Date posted: {date_posted}\n"
            f"Link:        {link}\n"
            f"Summary:     {snippet}\n"
            f"{'-' * 60}\n"
        )

    header = (
        f"Weekly Law Associate Job Digest — {datetime.utcnow():%B %d, %Y} (UTC)\n"
        f"{'=' * 60}\n"
        f"Found {len(jobs)} matching role(s) for first/second-year associates in Canada.\n\n"
    )
    return header + "\n".join(lines)


def format_email_html(jobs: pd.DataFrame) -> str:
    """Construct an HTML email body."""
    if jobs.empty:
        return (
            "<p>No matching law associate jobs were found this week.</p>"
            "<p>Your alert agent searched multiple job boards and firm websites,"
            " but none of the recent postings met the 0-2 years experience criteria.</p>"
        )

    lines: List[str] = []
    for _, row in jobs.iterrows():
        site        = _safe_str(row.get("SITE"))        or "Unknown site"
        title       = _safe_str(row.get("TITLE"))       or "Unknown title"
        company     = _safe_str(row.get("COMPANY"))     or "Unknown company"
        city        = _safe_str(row.get("CITY"))
        state       = _safe_str(row.get("STATE"))
        location    = ", ".join(filter(None, [city, state])) or "Canada"
        date_posted = _safe_str(row.get("DATE") or row.get("DATE_POSTED")) or "N/A"
        link        = _safe_str(row.get("JOB_URL")  or row.get("URL"))
        description = _safe_str(row.get("DESCRIPTION")).replace("\n", " ")
        snippet     = description[:300] + ("..." if len(description) > 300 else "")

        lines.append(
            f"<div style='margin-bottom:24px;font-family:Arial,sans-serif;'>"
            f"<strong style='font-size:16px;'>{title}</strong><br>"
            f"<em>{company} &mdash; {site.title()}</em><br>"
            f"&#128205; {location}<br>"
            f"&#128197; Date posted: {date_posted}<br>"
            f"<a href='{link}' style='color:#1a73e8;'>&#128196; Apply here</a><br>"
            f"<p style='color:#555;font-size:13px;'>{snippet}</p>"
            f"</div><hr>"
        )

    return (
        f"<h2>Weekly Law Associate Job Digest</h2>"
        f"<p><strong>{datetime.utcnow():%B %d, %Y} (UTC)</strong>"
        f" &mdash; {len(jobs)} matching role(s) found.</p>"
        + "\n".join(lines)
    )


# ─────────────────────────────────────────────────────────────────────────────
# Email sending
# ─────────────────────────────────────────────────────────────────────────────

def send_email(
    subject: str,
    body: str,
    sender: str,
    password: str,
    recipients: List[str],
    html: Optional[str] = None,
) -> None:
    """
    Send an email via Gmail SMTP using a Gmail App Password.

    FIX: Added SMTPAuthenticationError handling with clear instructions instead
    of a cryptic 535 5.7.8 traceback.
    """
    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"]    = sender
    msg["To"]      = ", ".join(recipients)
    msg.set_content(body)
    if html:
        msg.add_alternative(html, subtype="html")

    try:
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as smtp:
            smtp.login(sender, password)
            smtp.send_message(msg)
        print(f"✓ Email sent to: {', '.join(recipients)}")
    except smtplib.SMTPAuthenticationError:
        raise RuntimeError(
            "\n\nGmail authentication failed (535 5.7.8).\n"
            "You must use a Gmail App Password, NOT your regular Gmail password.\n"
            "To fix:\n"
            "  1. Go to myaccount.google.com/apppasswords\n"
            "  2. Generate a password for 'Mail'\n"
            "  3. Update EMAIL_PASS in your GitHub Secrets with the 16-character"
            " password (no spaces)\n"
        )
    except smtplib.SMTPException as exc:
        raise RuntimeError(f"Failed to send email: {exc}") from exc


# ─────────────────────────────────────────────────────────────────────────────
# Entry point
# ─────────────────────────────────────────────────────────────────────────────

def main() -> None:
    sender     = get_env_variable("EMAIL_USER")
    password   = get_env_variable("EMAIL_PASS")
    recipients = [
        a.strip()
        for a in get_env_variable("EMAIL_TO").split(",")
        if a.strip()
    ]
    dry_run    = os.environ.get("DRY_RUN", "0").lower() in {"1", "true", "yes"}

    try:
        results_wanted = int(os.environ.get("RESULTS_WANTED", "100"))
    except ValueError:
        results_wanted = 100

    print("=" * 60)
    print("Weekly Law Associate Job Scraper starting...")
    print(f"Run time: {datetime.utcnow():%Y-%m-%d %H:%M:%S} UTC")
    print("=" * 60)

    # ── Step 1: Scrape all sources ────────────────────────────────────────────
    print("\n[1/4] Scraping public job boards (LinkedIn, Indeed, Google)...")
    try:
        all_jobs = perform_scrape("law associate", "Canada", results_wanted)
        print(f"  → {len(all_jobs)} raw postings from job boards.")
    except Exception as exc:
        print(f"  WARNING: Job board scrape failed: {exc}")
        all_jobs = pd.DataFrame()

    print("\n[2/4] Scraping Canadian law firm career pages...")
    try:
        firm_jobs = scrape_law_firm_sites()
        print(f"  → {len(firm_jobs)} raw postings from law firm sites.")
    except Exception as exc:
        print(f"  WARNING: Law firm scrape failed: {exc}")
        firm_jobs = pd.DataFrame()

    print("\n[3/4] Scraping legal recruiter and job board sites...")
    try:
        recruiter_jobs = scrape_recruiter_sites()
        print(f"  → {len(recruiter_jobs)} raw postings from recruiter sites.")
    except Exception as exc:
        print(f"  WARNING: Recruiter scrape failed: {exc}")
        recruiter_jobs = pd.DataFrame()

    # ── Step 2: Combine, filter, deduplicate ──────────────────────────────────
    combined = pd.concat(
        [all_jobs, firm_jobs, recruiter_jobs], ignore_index=True, sort=False
    )
    print(f"\n  Total combined before filtering: {len(combined)}")

    filtered = filter_jobs(combined)
    filtered = remove_old_jobs(filtered, max_age_days=40)
    print(f"  Total after filtering:           {len(filtered)}")

    history_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), "job_history.json")
    history_ids  = load_history(history_file)
    prev_ids     = history_ids.copy()

    # FIX: Guard against missing JOB_URL column before calling .isin()
    if "JOB_URL" in filtered.columns and not filtered.empty:
        new_jobs = filtered[~filtered["JOB_URL"].isin(history_ids)].copy()
    else:
        new_jobs = filtered.copy()

    new_jobs = llm_filter(new_jobs)

    if "JOB_URL" in new_jobs.columns:
        history_ids.update(new_jobs["JOB_URL"].dropna().tolist())
    save_history(history_file, history_ids)

    print(f"  New jobs (not seen before):      {len(new_jobs)}")

    # ── Step 3: Build & send email ────────────────────────────────────────────
    print("\n[4/4] Building and sending email...")
    summary_text = generate_summary_stats(new_jobs, prev_ids)
    plain_body   = summary_text + "\n\n" + format_email_content(new_jobs)
    html_body    = "<p><pre>" + summary_text + "</pre></p>" + format_email_html(new_jobs)
    subject      = f"Weekly Law Associate Job Alerts — {datetime.utcnow():%B %d, %Y}"

    if dry_run:
        print("\n--- DRY RUN: email not sent ---")
        print(f"Subject: {subject}\n")
        print(plain_body)
        return

    send_email(subject, plain_body, sender, password, recipients, html=html_body)
    print("\nDone ✓")


if __name__ == "__main__":
    main()
