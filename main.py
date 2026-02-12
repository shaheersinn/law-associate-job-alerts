#!/usr/bin/env python3
"""
Weekly Law Associate Job Scraper

Scrapes job boards via python-jobspy + best-effort scraping of firm/recruiter sites,
filters for first-/second-year associate roles (0–2 years) in Canada, deduplicates,
and emails a weekly digest.

ENV VARS (GitHub Secrets):
  EMAIL_USER        Sender Gmail address (use an App Password)
  EMAIL_PASS        Gmail App Password (NOT your normal password)
  EMAIL_TO          Comma-separated recipients
  RESULTS_WANTED    Optional, default 100
  DRY_RUN           Optional "1" to print email instead of sending
  OPENAI_API_KEY    Optional: enables LLM verification (recommended)
  OPENAI_MODEL      Optional: default "gpt-4o-mini"
  DEBUG             Optional "1" to print filter diagnostics
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
    scrape_jobs = None  # type: ignore

try:
    import openai  # type: ignore
except ImportError:
    openai = None


# ─────────────────────────────────────────────────────────────────────────────
# Regex patterns
# ─────────────────────────────────────────────────────────────────────────────

_POSITIVE_PATTERNS = [
    # explicit 0–2 phrasing
    r"\b0\s*[-–to]+\s*2\s*years\b",
    r"\b0\s*to\s*2\s*years\b",
    r"\bup\s*to\s*2\s*years\b",
    r"\b0\s*[-–]\s*2\s*yrs\b",
    r"\b0\s*[-–]\s*2\s*year\b",
    r"\b1\s*[-–]\s*2\s*years\b",
    r"\b0\s*[-–]\s*1\s*years\b",
    r"\b0\s*[-–]\s*1\s*year\b",
    r"\b1\s*[-–]\s*2\s*yrs\b",
    r"\b0\s*[-–]\s*2\s*pqe\b",
    r"\b1\s*[-–]\s*2\s*pqe\b",

    # year level language
    r"\bfirst[\s\-]*year\b",
    r"\bsecond[\s\-]*year\b",
    r"\bentry[-\s]*level\b",
    r"\bjunior\b",
    r"\bnewly\s+called\b",
    r"\brecent\s+call\b",
    r"\bcalled\s+to\s+the\s+bar\s+within\s*2\s*years\b",

    # variants you asked to capture
    r"\barticling\s+associate\b",
    r"\b(?:new|recent)\s+(?:call|called)\b",
]

_NEGATIVE_PATTERNS = [
    r"\bsenior\b",
    r"\bpartner\b",
    r"\bprincipal\b",
    r"\bhead\s+of\b",
    r"\blead\s+counsel\b",
    r"\bmanaging\b",
    r"\bmanager\b",
    r"\bdirector\b",
    r"\bexecutive\b",
    r"\b(?:3|4|5|6|7|8|9|10)\+?\s*years\b",
    r"\b3\+\s*yrs\b",
    r"\b5\+\s*yrs\b",
    r"\bminimum\s+of\s+(?:3|4|5)\s+years\b",
    r"\b(?:3|4|5)\s*\+\s*years\b",
]

_LEGAL_KEYWORDS = [
    r"\bassociate\b",
    r"\blaw\b",
    r"\blegal\b",
    r"\blawyer\b",
    r"\bbar\b",
    r"\bsolicitor\b",
    r"\bbarrister\b",
]

POSITIVE_REGEX = re.compile("|".join(_POSITIVE_PATTERNS), re.IGNORECASE)
NEGATIVE_REGEX = re.compile("|".join(_NEGATIVE_PATTERNS), re.IGNORECASE)
LEGAL_REGEX    = re.compile("|".join(_LEGAL_KEYWORDS),    re.IGNORECASE)

_SCRAPER_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
        " AppleWebKit/537.36 (KHTML, like Gecko)"
        " Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def get_env_variable(name: str, default: Optional[str] = None) -> str:
    value = os.environ.get(name, default)
    if value is None:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


def _safe_str(value) -> str:
    if value is None:
        return ""
    try:
        if pd.isna(value):
            return ""
    except Exception:
        pass
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
    """Keep jobs with unknown date, or posted within max_age_days."""
    if df.empty:
        return df

    cutoff = pd.Timestamp.utcnow() - pd.Timedelta(days=max_age_days)

    primary  = df["DATE"] if "DATE" in df.columns else pd.Series([None] * len(df), index=df.index)
    fallback = df["DATE_POSTED"] if "DATE_POSTED" in df.columns else pd.Series([None] * len(df), index=df.index)
    combined = primary.where(primary.notna(), fallback)

    dt = pd.to_datetime(combined, errors="coerce", utc=True)
    keep = dt.isna() | (dt >= cutoff)

    return df.loc[keep].copy()


def _http_get(url: str, timeout: int = 20) -> Optional[requests.Response]:
    """GET with minimal retry/backoff for 429 and transient errors."""
    try:
        resp = requests.get(url, headers=_SCRAPER_HEADERS, timeout=timeout)
    except Exception:
        return None

    if resp.status_code == 429:
        time.sleep(3)
        try:
            resp = requests.get(url, headers=_SCRAPER_HEADERS, timeout=timeout)
        except Exception:
            return None

    return resp


# ─────────────────────────────────────────────────────────────────────────────
# Career/recruiter scraping (best-effort)
# ─────────────────────────────────────────────────────────────────────────────

def _scrape_career_pages(page_urls: List[str], source_label: str) -> pd.DataFrame:
    """
    Best-effort HTML scraping for job links on career pages.
    NOTE: Many large firms use JS-rendered ATS pages; those may still return 0.
    """
    jobs: List[dict] = []

    for url in page_urls:
        print(f"  [{source_label}] Scraping: {url}")
        resp = _http_get(url)
        if resp is None:
            print("    SKIP — could not reach")
            continue
        if resp.status_code != 200:
            print(f"    SKIP — HTTP {resp.status_code}")
            continue

        soup = BeautifulSoup(resp.text, "html.parser")
        base_domain = urlparse(url).netloc

        # Heuristic: only follow “job-like” links to avoid crawling the whole site
        candidate_links = []
        for a in soup.find_all("a", href=True):
            text = a.get_text(strip=True)
            href = a["href"].strip()
            if not href:
                continue

            combined = (text + " " + href).lower()

            # Must be “associate” somewhere in link text or URL, OR job-ish URL
            is_associateish = "associate" in combined
            is_jobish = any(k in combined for k in ("career", "careers", "job", "jobs", "opening", "opportunit", "position"))

            if is_associateish or is_jobish:
                candidate_links.append((text, href))

        # Limit to keep Actions runs sane
        candidate_links = candidate_links[:40]

        for title, href in candidate_links:
            job_url = urljoin(url, href)

            # same-domain only (avoid random external links)
            if urlparse(job_url).netloc and urlparse(job_url).netloc != base_domain:
                continue

            j_resp = _http_get(job_url)
            time.sleep(0.4)
            if j_resp is None or j_resp.status_code != 200:
                continue

            page_text = BeautifulSoup(j_resp.text, "html.parser").get_text(separator="\n").strip()
            if not page_text:
                continue

            # Do NOT enforce POSITIVE here (it causes 0). We only exclude obvious seniors.
            if NEGATIVE_REGEX.search(page_text):
                continue

            jobs.append({
                "SITE": base_domain,
                "TITLE": title or "Associate (link)",
                "COMPANY": base_domain,
                "CITY": "",
                "STATE": "",
                "DATE": "",
                "JOB_URL": job_url,
                "DESCRIPTION": page_text,
            })

    return pd.DataFrame(jobs) if jobs else pd.DataFrame()


# ─────────────────────────────────────────────────────────────────────────────
# Job board scraping via jobspy
# ─────────────────────────────────────────────────────────────────────────────

def perform_scrape(search_term: str, location: str, results_wanted: int = 100) -> pd.DataFrame:
    if scrape_jobs is None:
        print("  WARNING: python-jobspy not installed. Skipping job board scrape.")
        return pd.DataFrame()

    google_search_term = f"{search_term} first year second year 0-2 years PQE Canada"

    jobs = scrape_jobs(
        site_name=["linkedin", "indeed", "google"],
        search_term=search_term,
        google_search_term=google_search_term,
        location=location,
        results_wanted=results_wanted,
        hours_old=168,  # last 7 days
        country_indeed="Canada",
        linkedin_fetch_description=True,
        verbose=0,
    )

    if jobs is None or jobs.empty:
        return pd.DataFrame()

    jobs.columns = [col.upper() for col in jobs.columns]
    return jobs


def scrape_law_firm_sites() -> pd.DataFrame:
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
        "https://www.goodmans.ca/careers/current-opportunities",
        "https://www.blg.com/en/careers/legal-professionals/current-opportunities",
        "https://nrfcanada.wd10.myworkdayjobs.com/en-CA/NRFC",
        "https://www.dentons.com/en/careers",
        "https://www.millerthomson.com/en/careers",
        "https://cassels.com/join-us/career-opportunities-lawyers/",
        "https://www.airdberlis.com/join-us",
        "https://www.lerners.ca/careers",
    ]))
    return _scrape_career_pages(pages, "LawFirm")


def scrape_recruiter_sites() -> pd.DataFrame:
    pages = list(dict.fromkeys([
        "https://www.zsa.ca/current-opportunities/?search_keywords=&search_location=",
        "https://www.thecounselnetwork.com/",
        "https://www.lifeafterlaw.com/",
        "https://www.smithlegalsearch.com/",
        "https://cartelinc.com/",
        "https://edgerecruitment.ca/",
        "https://www.urbanlegal.ca/careers",
        "https://www.legaljobs.ca/",
        "https://www.jobbank.gc.ca/",
    ]))
    return _scrape_career_pages(pages, "Recruiter")


# ─────────────────────────────────────────────────────────────────────────────
# Filtering (fixed to avoid filtering everything to 0)
# ─────────────────────────────────────────────────────────────────────────────

def filter_jobs(df: pd.DataFrame) -> pd.DataFrame:
    """
    Candidate filter:
      - keep legal-ish roles
      - exclude clear seniors
      - if no OPENAI_API_KEY, require POSITIVE match (strict mode)
      - if OPENAI_API_KEY is set, let the LLM do the 0–2 verification later
    """
    if df.empty:
        return df

    df = df.copy()

    if "TITLE" not in df.columns:
        df["TITLE"] = ""
    if "DESCRIPTION" not in df.columns:
        df["DESCRIPTION"] = ""

    df["TEXT"] = (df["TITLE"].fillna("") + "\n" + df["DESCRIPTION"].fillna("")).astype(str).str.lower()

    matches_legal = df["TEXT"].str.contains(LEGAL_REGEX, na=False)
    matches_negative = df["TEXT"].str.contains(NEGATIVE_REGEX, na=False)

    candidates = df[matches_legal & ~matches_negative].copy()

    # If we DON'T have an OpenAI key, we must rely on regex positives
    if not os.environ.get("OPENAI_API_KEY"):
        matches_positive = candidates["TEXT"].str.contains(POSITIVE_REGEX, na=False)
        candidates = candidates[matches_positive].copy()

    if os.environ.get("DEBUG") == "1":
        total = len(df)
        print(f"DEBUG filter: total={total} legal={int(matches_legal.sum())} negative={int(matches_negative.sum())} candidates={len(candidates)}")
        if not os.environ.get("OPENAI_API_KEY"):
            # show examples that were legal & not negative but failed positive
            failed = df[matches_legal & ~matches_negative].copy()
            failed = failed[~failed["TEXT"].str.contains(POSITIVE_REGEX, na=False)]
            cols = [c for c in ["TITLE", "COMPANY", "JOB_URL"] if c in failed.columns]
            if not failed.empty and cols:
                print("DEBUG examples (legal but no explicit 0–2 wording):")
                print(failed[cols].head(10).to_string(index=False))

    if "JOB_URL" in candidates.columns:
        candidates = candidates.drop_duplicates(subset=["JOB_URL"])

    return candidates


def llm_filter(jobs: pd.DataFrame) -> pd.DataFrame:
    """
    LLM verification:
      “Does this job require 0–2 years AND is it a law associate role in Canada?”
    Runs only if OPENAI_API_KEY is set.
    """
    if openai is None:
        return jobs

    api_key = os.environ.get("OPENAI_API_KEY")
    if not api_key:
        return jobs

    model = os.environ.get("OPENAI_MODEL", "gpt-4o-mini")

    client = openai.OpenAI(api_key=api_key)
    kept_rows = []

    for _, row in jobs.iterrows():
        description = _safe_str(row.get("DESCRIPTION"))[:5000]
        title = _safe_str(row.get("TITLE"))[:200]
        company = _safe_str(row.get("COMPANY"))[:200]

        prompt = (
            "You are filtering Canadian legal job postings.\n"
            "Return ONLY one word: YES or NO.\n\n"
            "Question: Is this posting for a LAW ASSOCIATE role in CANADA that requires 0–2 years of experience (or equivalent junior/newly-called wording)?\n"
            "Reject senior roles (3+ years, senior associate, partner, lead counsel).\n\n"
            f"TITLE: {title}\n"
            f"COMPANY: {company}\n"
            f"DESCRIPTION:\n{description}\n"
        )

        try:
            resp = client.chat.completions.create(
                model=model,
                messages=[{"role": "user", "content": prompt}],
                max_tokens=3,
                temperature=0.0,
            )
            answer = (resp.choices[0].message.content or "").strip().upper()
            if answer.startswith("YES"):
                kept_rows.append(row)
        except Exception:
            # Keep on error to avoid “false zero”
            kept_rows.append(row)

    return pd.DataFrame(kept_rows, columns=jobs.columns) if kept_rows else pd.DataFrame(columns=jobs.columns)


# ─────────────────────────────────────────────────────────────────────────────
# Email formatting/sending
# ─────────────────────────────────────────────────────────────────────────────

def generate_summary_stats(current_jobs: pd.DataFrame, previous_ids: set) -> str:
    lines: List[str] = [
        f"New jobs this run:      {len(current_jobs)}",
        f"Previously sent jobs:   {len(previous_ids)}",
    ]
    return "\n".join(lines)


def format_email_content(jobs: pd.DataFrame) -> str:
    if jobs.empty:
        return (
            "No matching law associate jobs were found this week.\n\n"
            "Tip: If you set OPENAI_API_KEY, the agent can identify 0–2 year roles "
            "even when the posting doesn't literally say “0–2 years”."
        )

    lines: List[str] = []
    for _, row in jobs.iterrows():
        site = _safe_str(row.get("SITE")) or "Unknown site"
        title = _safe_str(row.get("TITLE")) or "Unknown title"
        company = _safe_str(row.get("COMPANY")) or "Unknown company"
        city = _safe_str(row.get("CITY"))
        state = _safe_str(row.get("STATE"))
        location = ", ".join([x for x in [city, state] if x]) or "Canada"
        date_posted = _safe_str(row.get("DATE") or row.get("DATE_POSTED")) or "N/A"
        link = _safe_str(row.get("JOB_URL") or row.get("URL"))
        description = _safe_str(row.get("DESCRIPTION")).replace("\n", " ")
        snippet = description[:320] + ("..." if len(description) > 320 else "")

        lines.append(
            f"Site:        {site}\n"
            f"Title:       {title}\n"
            f"Company:     {company}\n"
            f"Location:    {location}\n"
            f"Date posted: {date_posted}\n"
            f"Link:        {link}\n"
            f"Summary:     {snippet}\n"
            f"{'-'*60}\n"
        )

    header = (
        f"Weekly Law Associate Job Digest — {datetime.utcnow():%B %d, %Y} (UTC)\n"
        f"{'='*60}\n"
        f"Found {len(jobs)} matching role(s).\n\n"
    )
    return header + "\n".join(lines)


def send_email(
    subject: str,
    body: str,
    sender: str,
    password: str,
    recipients: List[str],
) -> None:
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
    except smtplib.SMTPAuthenticationError:
        raise RuntimeError(
            "Gmail authentication failed.\n"
            "Use a Gmail App Password (not your normal password) for EMAIL_PASS."
        )


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────

def main() -> None:
    sender = get_env_variable("EMAIL_USER")
    password = get_env_variable("EMAIL_PASS")
    recipients = [a.strip() for a in get_env_variable("EMAIL_TO").split(",") if a.strip()]
    dry_run = os.environ.get("DRY_RUN", "0").lower() in {"1", "true", "yes"}

    try:
        results_wanted = int(os.environ.get("RESULTS_WANTED", "100"))
    except ValueError:
        results_wanted = 100

    print("=" * 60)
    print("Weekly Law Associate Job Scraper starting...")
    print(f"Run time: {datetime.utcnow():%Y-%m-%d %H:%M:%S} UTC")
    print("=" * 60)

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

    combined = pd.concat([all_jobs, firm_jobs, recruiter_jobs], ignore_index=True, sort=False)
    print(f"\n  Total combined before filtering: {len(combined)}")

    # ✅ FIX: actually define filtered before using it
    filtered = filter_jobs(combined)
    filtered = remove_old_jobs(filtered, max_age_days=40)
    print(f"  Total after filtering:           {len(filtered)}")

    history_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), "job_history.json")
    history_ids = load_history(history_file)
    prev_ids = history_ids.copy()

    if "JOB_URL" in filtered.columns and not filtered.empty:
        new_jobs = filtered[~filtered["JOB_URL"].isin(history_ids)].copy()
    else:
        new_jobs = filtered.copy()

    # ✅ If OpenAI key exists, this is where “0–2 years” precision happens
    new_jobs = llm_filter(new_jobs)

    if "JOB_URL" in new_jobs.columns:
        history_ids.update(new_jobs["JOB_URL"].dropna().tolist())
    save_history(history_file, history_ids)

    print(f"  New jobs (not seen before):      {len(new_jobs)}")

    print("\n[4/4] Building and sending email...")
    summary_text = generate_summary_stats(new_jobs, prev_ids)
    plain_body = summary_text + "\n\n" + format_email_content(new_jobs)
    subject = f"Weekly Law Associate Job Alerts — {datetime.utcnow():%B %d, %Y}"

    if dry_run:
        print("\n--- DRY RUN: email not sent ---")
        print(f"Subject: {subject}\n")
        print(plain_body)
        return

    send_email(subject, plain_body, sender, password, recipients)
    print("\nDone ✓")


if __name__ == "__main__":
    main()
