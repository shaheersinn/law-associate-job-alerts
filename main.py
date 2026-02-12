#!/usr/bin/env python3
"""
Weekly Law Associate Job Scraper

Scrapes job postings from:
- job boards via python-jobspy
- best-effort scraping of firm/recruiter career pages

Filters for:
- LAW ASSOCIATE roles in Canada
- first/second year OR 0–2 years (or equivalent "newly called" wording)
- excludes seniors (3+ years, senior associate, partner, etc.)

Emails weekly digest.

ENV VARS (GitHub Secrets):
  EMAIL_USER
  EMAIL_PASS
  EMAIL_TO
  RESULTS_WANTED (optional, default 100)
  DRY_RUN (optional "1")
  OPENAI_API_KEY (optional: enables LLM verification)
  OPENAI_MODEL (optional, default "gpt-4o-mini")
  DEBUG (optional "1")
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

# “Associate” must exist, AND legal context must exist (law/legal/lawyer/etc.)
ASSOCIATE_REGEX = re.compile(r"\bassociate\b", re.IGNORECASE)

LEGAL_CONTEXT_REGEX = re.compile(
    r"|".join([
        r"\blaw\b",
        r"\blegal\b",
        r"\blawyer\b",
        r"\bsolicitor\b",
        r"\bbarrister\b",
        r"\bcalled\s+to\s+the\s+bar\b",
        r"\bbar\s+admission\b",
        r"\bjuris\s+doctor\b",
        r"\bj\.?d\.?\b",
        r"\bllb\b",
        r"\bll\.?b\.?\b",
        r"\bllm\b",
        r"\bin[-\s]*house\s+counsel\b",
        r"\bcounsel\b",
        r"\blitigation\b",
        r"\bcorporate\b",
        r"\bsecurities\b",
        r"\bm&a\b",
        r"\bmergers?\b",
        r"\bacquisitions?\b",
    ]),
    re.IGNORECASE
)

_POSITIVE_PATTERNS = [
    # explicit 0–2 phrasing
    r"\b0\s*[-–to]+\s*2\s*years\b",
    r"\b0\s*to\s*2\s*years\b",
    r"\bup\s*to\s*2\s*years\b",
    r"\b1\s*[-–]\s*2\s*years\b",
    r"\b0\s*[-–]\s*2\s*pqe\b",
    r"\b1\s*[-–]\s*2\s*pqe\b",
    r"\b0\s*[-–]\s*2\s*yrs\b",
    r"\b1\s*[-–]\s*2\s*yrs\b",

    # year level language
    r"\bfirst[\s\-]*year\b",
    r"\bsecond[\s\-]*year\b",
    r"\bentry[-\s]*level\b",
    r"\bjunior\b",
    r"\bnewly\s+called\b",
    r"\brecent\s+call\b",
    r"\bcalled\s+to\s+the\s+bar\s+within\s*2\s*years\b",
    r"\barticling\s+associate\b",
]

# Make groups non-capturing to avoid pandas warning about match groups
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
    r"\bminimum\s+of\s+(?:3|4|5)\s+years\b",
]

# Strong non-legal negatives to prevent Warehouse Associate etc.
NON_LEGAL_NEGATIVE_REGEX = re.compile(
    r"|".join([
        r"\bwarehouse\b",
        r"\bdistribution\b",
        r"\bretail\b",
        r"\bcashier\b",
        r"\bstore\b",
        r"\bstock\b",
        r"\bmerchandis",
        r"\bcustomer\s+service\b",
        r"\bsales\b",
        r"\bshipping\b",
        r"\breceiving\b",
        r"\bforklift\b",
    ]),
    re.IGNORECASE
)

# Student/career info pages that are NOT associate lawyer jobs
CAREER_INFO_NEGATIVE_REGEX = re.compile(
    r"|".join([
        r"\bstudents?\b",
        r"\bsummer\s+student\b",
        r"\barticling\s+student\b",
        r"\bstudent\s+program\b",
        r"\bhow\s+to\s+apply\b",
        r"\bapplication\s+process\b",
        r"\brecruitment\s+process\b",
    ]),
    re.IGNORECASE
)

POSITIVE_REGEX = re.compile("|".join(_POSITIVE_PATTERNS), re.IGNORECASE)
NEGATIVE_REGEX = re.compile("|".join(_NEGATIVE_PATTERNS), re.IGNORECASE)

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
    if df.empty:
        return df

    cutoff = pd.Timestamp.utcnow() - pd.Timedelta(days=max_age_days)

    primary = df["DATE"] if "DATE" in df.columns else pd.Series([None] * len(df), index=df.index)
    fallback = df["DATE_POSTED"] if "DATE_POSTED" in df.columns else pd.Series([None] * len(df), index=df.index)
    combined = primary.where(primary.notna(), fallback)

    dt = pd.to_datetime(combined, errors="coerce", utc=True)
    keep = dt.isna() | (dt >= cutoff)
    return df.loc[keep].copy()


def _http_get(url: str, timeout: int = 20) -> Optional[requests.Response]:
    try:
        resp = requests.get(url, headers=_SCRAPER_HEADERS, timeout=timeout)
    except Exception:
        return None

    # simple retry on rate limiting
    if resp.status_code == 429:
        time.sleep(3)
        try:
            resp = requests.get(url, headers=_SCRAPER_HEADERS, timeout=timeout)
        except Exception:
            return None
    return resp


# ─────────────────────────────────────────────────────────────────────────────
# Scrapers
# ─────────────────────────────────────────────────────────────────────────────

def perform_scrape(search_term: str, location: str, results_wanted: int = 100) -> pd.DataFrame:
    if scrape_jobs is None:
        print("  WARNING: python-jobspy not installed. Skipping job board scrape.")
        return pd.DataFrame()

    google_search_term = (
        f"{search_term} first year second year 0-2 years PQE lawyer associate {location}"
    )

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


def _looks_like_job_posting(url: str) -> bool:
    u = url.lower()
    # keep real job URLs; drop generic search pages / career info pages
    bad = ["jobsearch", "searchstring=", "locationstring=", "/students", "how-to-apply", "application-process"]
    if any(b in u for b in bad):
        return False
    good = ["job", "jobs", "opening", "opportunit", "position", "posting", "viewjob", "careers"]
    return any(g in u for g in good)


def _scrape_career_pages(page_urls: List[str], source_label: str) -> pd.DataFrame:
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

        candidate_links = []
        for a in soup.find_all("a", href=True):
            text = a.get_text(strip=True) or ""
            href = a["href"].strip()
            if not href:
                continue

            job_url = urljoin(url, href)
            if urlparse(job_url).netloc and urlparse(job_url).netloc != base_domain:
                continue

            if not _looks_like_job_posting(job_url):
                continue

            combined = (text + " " + job_url).lower()

            # Only follow links that look like actual roles
            # (associate/lawyer/legal/counsel/junior)
            if not any(k in combined for k in ["associate", "lawyer", "legal", "counsel", "junior", "pqe", "call"]):
                continue

            candidate_links.append((text, job_url))

        candidate_links = candidate_links[:40]

        for title, job_url in candidate_links:
            j_resp = _http_get(job_url)
            time.sleep(0.4)
            if j_resp is None or j_resp.status_code != 200:
                continue

            text = BeautifulSoup(j_resp.text, "html.parser").get_text(separator="\n").strip()
            if not text:
                continue

            # Skip obvious non-role pages (students/how-to-apply etc.)
            if CAREER_INFO_NEGATIVE_REGEX.search(text) and not ASSOCIATE_REGEX.search(text):
                continue

            jobs.append({
                "SITE": base_domain,
                "TITLE": title or "Posting",
                "COMPANY": base_domain,
                "CITY": "",
                "STATE": "",
                "DATE": "",
                "JOB_URL": job_url,
                "DESCRIPTION": text,
            })

    return pd.DataFrame(jobs) if jobs else pd.DataFrame()


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
# Filtering
# ─────────────────────────────────────────────────────────────────────────────

def _llm_is_enabled() -> bool:
    """Only treat LLM as enabled if the library is installed and a key is present."""
    return (openai is not None) and bool(os.environ.get("OPENAI_API_KEY"))


def filter_jobs(df: pd.DataFrame) -> pd.DataFrame:
    """
    Candidate filter:
      - must have "associate"
      - must have legal context ("law"/"legal"/"lawyer"/etc.)
      - exclude seniors + obvious non-legal associate roles
      - if LLM disabled, require POSITIVE regex too
    """
    if df.empty:
        return df

    df = df.copy()
    if "TITLE" not in df.columns:
        df["TITLE"] = ""
    if "DESCRIPTION" not in df.columns:
        df["DESCRIPTION"] = ""

    df["TEXT"] = (df["TITLE"].fillna("") + "\n" + df["DESCRIPTION"].fillna("")).astype(str)

    # Core: associate + legal context
    has_associate = df["TEXT"].str.contains(ASSOCIATE_REGEX, na=False)
    has_legal_ctx = df["TEXT"].str.contains(LEGAL_CONTEXT_REGEX, na=False)

    # Exclusions
    is_senior = df["TEXT"].str.contains(NEGATIVE_REGEX, na=False)
    is_nonlegal_assoc = df["TEXT"].str.contains(NON_LEGAL_NEGATIVE_REGEX, na=False)

    candidates = df[has_associate & has_legal_ctx & ~is_senior & ~is_nonlegal_assoc].copy()

    # If LLM is NOT enabled, enforce strict junior signals
    if not _llm_is_enabled():
        has_positive = candidates["TEXT"].str.contains(POSITIVE_REGEX, na=False)
        candidates = candidates[has_positive].copy()

    if os.environ.get("DEBUG") == "1":
        print(
            "DEBUG filter counts:",
            f"total={len(df)}",
            f"assoc={int(has_associate.sum())}",
            f"legalctx={int(has_legal_ctx.sum())}",
            f"senior={int(is_senior.sum())}",
            f"nonlegal_assoc={int(is_nonlegal_assoc.sum())}",
            f"candidates={len(candidates)}",
            f"llm_enabled={_llm_is_enabled()}",
        )

    if "JOB_URL" in candidates.columns:
        candidates = candidates.drop_duplicates(subset=["JOB_URL"])

    return candidates


def llm_filter(jobs: pd.DataFrame) -> pd.DataFrame:
    """
    LLM verification:
    - Only runs if openai library + OPENAI_API_KEY exist.
    - IMPORTANT FIX: on exception -> reject (do NOT keep junk).
    """
    if not _llm_is_enabled():
        return jobs

    client = openai.OpenAI(api_key=os.environ["OPENAI_API_KEY"])  # type: ignore
    model = os.environ.get("OPENAI_MODEL", "gpt-4o-mini")

    kept_rows = []
    for _, row in jobs.iterrows():
        title = _safe_str(row.get("TITLE"))[:200]
        desc = _safe_str(row.get("DESCRIPTION"))[:5000]

        prompt = (
            "Answer ONLY: YES or NO.\n\n"
            "Is this a Canadian LAW ASSOCIATE job requiring 0–2 years (or first/second year, "
            "newly called/recent call) and NOT a senior role?\n\n"
            f"TITLE: {title}\n"
            f"DESCRIPTION:\n{desc}\n"
        )

        try:
            resp = client.chat.completions.create(
                model=model,
                messages=[{"role": "user", "content": prompt}],
                max_tokens=3,
                temperature=0.0,
            )
            ans = (resp.choices[0].message.content or "").strip().upper()
            if ans.startswith("YES"):
                kept_rows.append(row)
        except Exception:
            # FIX: reject on error instead of keeping garbage
            continue

    return pd.DataFrame(kept_rows, columns=jobs.columns) if kept_rows else pd.DataFrame(columns=jobs.columns)


# ─────────────────────────────────────────────────────────────────────────────
# Email
# ─────────────────────────────────────────────────────────────────────────────

def format_email_content(jobs: pd.DataFrame) -> str:
    if jobs.empty:
        return (
            "No matching first-/second-year LAW ASSOCIATE jobs were found this week.\n\n"
            "If you're seeing too few results, add OPENAI_API_KEY so the LLM can "
            "detect junior roles even when postings don’t explicitly say “0–2 years”."
        )

    lines = []
    for _, row in jobs.iterrows():
        site = _safe_str(row.get("SITE")) or "Unknown"
        title = _safe_str(row.get("TITLE")) or "Unknown title"
        company = _safe_str(row.get("COMPANY")) or "Unknown company"
        link = _safe_str(row.get("JOB_URL") or row.get("URL"))
        date_posted = _safe_str(row.get("DATE") or row.get("DATE_POSTED")) or "N/A"
        desc = _safe_str(row.get("DESCRIPTION")).replace("\n", " ")
        snippet = desc[:320] + ("..." if len(desc) > 320 else "")

        lines.append(
            f"Site:        {site}\n"
            f"Title:       {title}\n"
            f"Company:     {company}\n"
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


def send_email(subject: str, body: str, sender: str, password: str, recipients: List[str]) -> None:
    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = sender
    msg["To"] = ", ".join(recipients)
    msg.set_content(body)

    with smtplib.SMTP_SSL("smtp.gmail.com", 465) as smtp:
        smtp.login(sender, password)
        smtp.send_message(msg)


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

    print("\n[3/4] Scraping recruiter / legal job board sites...")
    try:
        recruiter_jobs = scrape_recruiter_sites()
        print(f"  → {len(recruiter_jobs)} raw postings from recruiter sites.")
    except Exception as exc:
        print(f"  WARNING: Recruiter scrape failed: {exc}")
        recruiter_jobs = pd.DataFrame()

    combined = pd.concat([all_jobs, firm_jobs, recruiter_jobs], ignore_index=True, sort=False)
    print(f"\n  Total combined before filtering: {len(combined)}")

    filtered = filter_jobs(combined)
    filtered = remove_old_jobs(filtered, max_age_days=40)

    print(f"  Total after filtering:           {len(filtered)}")

    history_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), "job_history.json")
    history_ids = load_history(history_file)

    if "JOB_URL" in filtered.columns and not filtered.empty:
        new_jobs = filtered[~filtered["JOB_URL"].isin(history_ids)].copy()
    else:
        new_jobs = filtered.copy()

    # LLM verification (only if enabled)
    new_jobs = llm_filter(new_jobs)

    # Update history
    if "JOB_URL" in new_jobs.columns:
        history_ids.update(new_jobs["JOB_URL"].dropna().tolist())
    save_history(history_file, history_ids)

    subject = f"Weekly Law Associate Job Alerts — {datetime.utcnow():%B %d, %Y}"
    body = format_email_content(new_jobs)

    if dry_run:
        print("\n--- DRY RUN: email not sent ---")
        print(body)
        return

    send_email(subject, body, sender, password, recipients)
    print(f"✓ Email sent to: {', '.join(recipients)}")


if __name__ == "__main__":
    main()
