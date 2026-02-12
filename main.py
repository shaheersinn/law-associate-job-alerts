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

When run via GitHub Actions, these variables are injected from repository secrets.
"""

from __future__ import annotations

import os
import re
import smtplib
from email.message import EmailMessage
from urllib.parse import urljoin, urlparse
from datetime import datetime
from typing import List

import pandas as pd
from jobspy import scrape_jobs
import requests
from bs4 import BeautifulSoup


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
    # Build a Google Jobs query that includes our keywords and emphasises early-career
    google_search_term = (
        f"{search_term} jobs first year second year 0-2 years experience in {location}"
    )

    # NOTE: ZipRecruiter is excluded — it returns 403 Forbidden (Cloudflare WAF block)
    # when run from GitHub Actions IPs and cannot be used reliably.
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

    # Standardise column names for easier processing
    jobs.columns = [col.upper() for col in jobs.columns]
    return jobs


def scrape_law_firm_sites() -> pd.DataFrame:
    """Scrape predefined Canadian law firm career pages for associate roles.

    Many law firm websites publish their open positions on dedicated career pages. This
    helper attempts to fetch each site's careers section, extract links to job
    descriptions containing the word "associate" and basic legal keywords, and then
    follow those links to inspect the experience requirements. If the job
    description mentions 0-2 years of experience or first/second-year level
    keywords, the job is recorded for later filtering.

    Returns
    -------
    pd.DataFrame
        A DataFrame with columns similar to the JobSpy output: SITE, TITLE,
        COMPANY, CITY, STATE, DATE, JOB_URL and DESCRIPTION.
    """
    firm_career_pages = [
        "https://www.osler.com/en/careers/",
        "https://www.blakes.com/careers/",
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
    ]

    # Deduplicate firm URLs
    firm_career_pages = list(dict.fromkeys(firm_career_pages))

    # Patterns indicating 0-2 years experience or entry-level roles
    experience_patterns = [
        r"0\s*-?\s*2\s*years",
        r"1\s*-?\s*2\s*years",
        r"\bfirst\s*year\b",
        r"\bsecond\s*year\b",
        r"\bentry[-\s]*level\b",
        r"\bjunior\b",
        r"\bnewly\s*called\b",
        r"\bnew\s*call\b",
    ]
    exp_regex = re.compile("|".join(experience_patterns), re.IGNORECASE)

    # Use a standard desktop user-agent to reduce the chance of blocking
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
            " AppleWebKit/537.36 (KHTML, like Gecko)"
            " Chrome/120.0.0.0 Safari/537.36"
        )
    }

    jobs = []

    for url in firm_career_pages:
        print(f"  Scraping: {url}")
        try:
            resp = requests.get(url, headers=headers, timeout=20)
        except Exception as exc:
            print(f"    SKIP — could not reach site: {exc}")
            continue
        if resp.status_code != 200:
            print(f"    SKIP — status {resp.status_code}")
            continue

        soup = BeautifulSoup(resp.text, "html.parser")
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
                j_resp = requests.get(job_url, headers=headers, timeout=20)
            except Exception:
                continue
            if j_resp.status_code != 200:
                continue
            job_soup = BeautifulSoup(j_resp.text, "html.parser")
            text = job_soup.get_text(separator="\n").strip()
            if not text:
                continue
            if not exp_regex.search(text):
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


def filter_jobs(df: pd.DataFrame) -> pd.DataFrame:
    """
    Filter the scraped jobs to include only first-/second-year law associate roles.

    The function checks the title and description for legal keywords and
    experience-level patterns, and explicitly excludes senior-level roles.
    """
    if df.empty:
        return df

    df = df.copy()
    df["TEXT"] = (df["TITLE"].fillna("") + "\n" + df["DESCRIPTION"].fillna("")).str.lower()

    # Must contain a legal keyword
    legal_keywords = [
        r"\bassociate\b",
        r"\blaw\b",
        r"\blegal\b",
        r"\blawyer\b",
    ]

    # Must match an early-career experience pattern
    experience_patterns = [
        r"0\s*-?\s*2\s*years",
        r"1\s*-?\s*2\s*years",
        r"\bfirst[\s\-]*year\b",
        r"\bsecond[\s\-]*year\b",
        r"\bentry[-\s]*level\b",
        r"\bjunior\b",
        r"\bnewly\s*called\b",
        r"\bnew\s*call\b",
        r"\bcalled to the bar within\b",
    ]

    # Must NOT contain senior-level indicators
    exclusion_patterns = [
        r"\bsenior\s*associate\b",
        r"\bpartner\b",
        r"\b[3-9]\+?\s*years\b",
        r"\b[1-9][0-9]+\s*years\b",
        r"\blead\s*counsel\b",
        r"\bmanaging\s*associate\b",
    ]

    legal_regex = re.compile("|".join(legal_keywords), re.IGNORECASE)
    exp_regex = re.compile("|".join(experience_patterns), re.IGNORECASE)
    excl_regex = re.compile("|".join(exclusion_patterns), re.IGNORECASE)

    matches_legal = df["TEXT"].str.contains(legal_regex)
    matches_exp = df["TEXT"].str.contains(exp_regex)
    is_excluded = df["TEXT"].str.contains(excl_regex)

    filtered = df[matches_legal & matches_exp & ~is_excluded].copy()

    # Drop duplicates by job URL
    if "JOB_URL" in filtered.columns:
        filtered = filtered.drop_duplicates(subset=["JOB_URL"])

    return filtered


def format_email_content(jobs: pd.DataFrame) -> str:
    """
    Construct a plain-text email summarising the filtered jobs.
    """
    if jobs.empty:
        return (
            "No matching law associate jobs were found this week.\n\n"
            "Your alert agent searched LinkedIn, Indeed, Google Jobs, and Canadian law"
            " firm career pages, but none of the recent postings met the"
            " 0-2 years experience criteria for Canada."
        )

    lines: List[str] = []
    for _, row in jobs.iterrows():
        site = str(row.get("SITE", "Unknown site")).title()
        title = str(row.get("TITLE", "Unknown title"))
        company = str(row.get("COMPANY", "Unknown company"))
        city = str(row.get("CITY", "") or "")
        state = str(row.get("STATE", "") or "")
        location = ", ".join(filter(None, [city, state]))
        date_posted = str(row.get("DATE", row.get("DATE_POSTED", "")) or "")
        link = str(row.get("JOB_URL", row.get("URL", "")) or "")
        description = str(row.get("DESCRIPTION", "") or "").strip().replace("\n", " ")
        snippet = description[:300] + ("..." if len(description) > 300 else "")

        lines.append(
            f"Site:        {site}\n"
            f"Title:       {title}\n"
            f"Company:     {company}\n"
            f"Location:    {location or 'Canada'}\n"
            f"Date posted: {date_posted or 'N/A'}\n"
            f"Link:        {link}\n"
            f"Summary:     {snippet}\n"
            f"{'-' * 60}\n"
        )

    header = (
        f"Weekly Law Associate Job Digest — {datetime.utcnow():%B %d, %Y} (UTC)\n"
        f"{'=' * 60}\n"
        f"Found {len(jobs)} matching role(s) for first/second-year law associates in Canada.\n\n"
    )
    return header + "\n".join(lines)


def send_email(subject: str, body: str, sender: str, password: str, recipients: List[str]) -> None:
    """
    Send an email using Gmail's SMTP server with an App Password.

    NOTE: You must use a Gmail App Password, NOT your regular Gmail password.
    Regular passwords will always fail with SMTPAuthenticationError (535 5.7.8).

    Steps to generate an App Password:
      1. Enable 2-Step Verification at myaccount.google.com/security
      2. Go to myaccount.google.com/apppasswords
      3. Select 'Mail' as the app and click Generate
      4. Copy the 16-character password (no spaces) into your EMAIL_PASS GitHub secret
    """
    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = sender
    msg["To"] = ", ".join(recipients)
    msg.set_content(body)

    try:
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as smtp:
            smtp.login(sender, password)
            smtp.send_message(msg)
        print(f"✓ Email sent successfully to: {', '.join(recipients)}")
    except smtplib.SMTPAuthenticationError:
        raise RuntimeError(
            "\n\nGmail authentication failed (535 5.7.8).\n"
            "You must use a Gmail App Password, not your regular password.\n"
            "Steps:\n"
            "  1. Go to myaccount.google.com/apppasswords\n"
            "  2. Generate a password for 'Mail'\n"
            "  3. Update your EMAIL_PASS GitHub secret with the 16-character password (no spaces)\n"
        )
    except smtplib.SMTPException as exc:
        raise RuntimeError(f"Failed to send email: {exc}") from exc


def main() -> None:
    # Load configuration from environment variables / GitHub Secrets
    sender = get_env_variable("EMAIL_USER")
    password = get_env_variable("EMAIL_PASS")
    recipients_raw = get_env_variable("EMAIL_TO")
    recipients = [addr.strip() for addr in recipients_raw.split(",") if addr.strip()]

    try:
        results_wanted = int(os.environ.get("RESULTS_WANTED", "100"))
    except ValueError:
        results_wanted = 100

    search_term = "law associate"
    location = "Canada"

    print("=" * 60)
    print("Weekly Law Associate Job Scraper starting...")
    print(f"Run time: {datetime.utcnow():%Y-%m-%d %H:%M:%S} UTC")
    print("=" * 60)

    # Step 1 — Scrape public job boards
    print("\n[1/3] Scraping public job boards (LinkedIn, Indeed, Google)...")
    try:
        all_jobs = perform_scrape(search_term, location, results_wanted)
        print(f"  → Found {len(all_jobs)} raw postings from job boards.")
    except Exception as exc:
        print(f"  WARNING: Job board scrape failed: {exc}")
        all_jobs = pd.DataFrame()

    # Step 2 — Scrape Canadian law firm career pages
    print("\n[2/3] Scraping Canadian law firm career pages...")
    try:
        firm_jobs = scrape_law_firm_sites()
        print(f"  → Found {len(firm_jobs)} raw postings from law firm sites.")
    except Exception as exc:
        print(f"  WARNING: Law firm site scrape failed: {exc}")
        firm_jobs = pd.DataFrame()

    # Step 3 — Combine, filter, and send
    print("\n[3/3] Filtering and sending email...")
    combined_jobs = pd.concat([all_jobs, firm_jobs], ignore_index=True, sort=False)
    print(f"  → Total combined before filtering: {len(combined_jobs)}")

    filtered_jobs = filter_jobs(combined_jobs)
    print(f"  → Total after filtering: {len(filtered_jobs)}")

    body = format_email_content(filtered_jobs)
    subject = f"Weekly Law Associate Job Alerts — {datetime.utcnow():%B %d, %Y}"

    send_email(subject, body, sender, password, recipients)
    print("\nDone.")


if __name__ == "__main__":
    main()
