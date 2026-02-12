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
    # Build a Google Jobs query that includes our keywords and emphasises early‑career
    google_search_term = (
        f"{search_term} jobs first year second year 0-2 years experience in {location}"
    )

    # Request jobs from multiple boards.  JobSpy supports LinkedIn, Indeed, Google and
    # ZipRecruiter among others【123892292451392†L266-L273】.  We pass a list of board names
    # and request a modest number of results to avoid excessive scraping.  Setting
    # `hours_old=168` restricts results to the past week.
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
    description mentions 0‑2 years of experience or first/second‑year level
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
    firm_career_pages = [
        "https://www.osler.com/en/careers/",  # Osler
        "https://www.blakes.com/careers/",  # Blake, Cassels & Graydon LLP
        "https://www.bennettjones.com/Careers",  # Bennett Jones LLP【759468253450880†L56-L74】
        "https://www.fasken.com/en/careers",  # Fasken Martineau DuMoulin LLP
        "https://gowlingwlg.com/en/careers/",  # Gowling WLG
        "https://www.stikeman.com/en/careers",  # Stikeman Elliott LLP
        "https://www.dwpv.com/en/Careers",  # Davies Ward Phillips & Vineberg LLP
        "https://www.mccarthy.ca/en/careers",  # McCarthy Tétrault LLP
        "https://www.torys.com/en/careers",  # Torys LLP
        "https://www.litigate.com/careers",  # Lenczner Slaght (litigate.com)
    ]

    jobs: list[dict] = []
    # Experience patterns reused from filter_jobs; compiled once here
    experience_patterns = [
        r"0\s*-?\s*2\s*years",
        r"1\s*-?\s*2\s*years",
        r"\bfirst\s*year\b",
        r"\bsecond\s*year\b",
        r"\bentry[-\s]*level\b",
        r"\bjunior\b",
    ]
    exp_regex = re.compile("|".join(experience_patterns), re.IGNORECASE)

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
            if "associate" not in lower_title:
                continue
            if not any(k in lower_title for k in ("law", "legal", "lawyer")):
                continue
            job_url = urljoin(url, link["href"])
            # Follow the job posting link to extract the description
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
            # Only record the job if the description mentions early‑career patterns
            if not exp_regex.search(text):
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
    df["TEXT"] = (df["TITLE"].fillna("") + "\n" + df["DESCRIPTION"].fillna(" ")).str.lower()

    # Keywords that must be present to imply the job is for a law associate
    legal_keywords = [
        r"\bassociate\b",
        r"\blaw\b",
        r"\blegal\b",
        r"\blawyer\b",
    ]

    # Patterns indicating 0‑2 years’ experience or entry‑level roles
    experience_patterns = [
        r"0\s*-?\s*2\s*years",  # 0-2 years
        r"1\s*-?\s*2\s*years",  # 1-2 years
        r"\bfirst\s*year\b",
        r"\bsecond\s*year\b",
        r"\bentry[-\s]*level\b",
        r"\bjunior\b",
    ]

    # Compile regex patterns for efficiency
    legal_regex = re.compile("|".join(legal_keywords), re.IGNORECASE)
    exp_regex = re.compile("|".join(experience_patterns), re.IGNORECASE)

    # Apply filters
    matches_legal = df["TEXT"].str.contains(legal_regex)
    matches_exp = df["TEXT"].str.contains(exp_regex)

    filtered = df[matches_legal & matches_exp].copy()

    # Drop duplicates by job URL if present
    if "JOB_URL" in filtered.columns:
        filtered = filtered.drop_duplicates(subset=["JOB_URL"])
    return filtered


def format_email_content(jobs: pd.DataFrame) -> str:
    """
    Construct a plain‑text email summarising the filtered jobs.
    """
    if jobs.empty:
        return (
            "No matching law associate jobs were found this week.\n\n"
            "Your alert agent searched LinkedIn, Indeed, Google Jobs and ZipRecruiter,"
            " but none of the recent postings met the 0‑2 years' experience criteria."
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


def send_email(subject: str, body: str, sender: str, password: str, recipients: List[str]) -> None:
    """
    Send an email using Gmail’s SMTP server.
    """
    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = sender
    msg["To"] = ", ".join(recipients)
    msg.set_content(body)

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

    # Combine job board results and law firm postings
    combined_jobs = pd.concat([all_jobs, firm_jobs], ignore_index=True, sort=False)

    # Filter jobs for first‑/second‑year roles
    filtered_jobs = filter_jobs(combined_jobs)

    # Build email content
    body = format_email_content(filtered_jobs)
    subject = "Weekly Law Associate Job Alerts (0‑2 years experience)"

    # Send the email
    send_email(subject, body, sender, password, recipients)


if __name__ == "__main__":
    main()
