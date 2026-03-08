#!/usr/bin/env python3
"""
Law Associate Job Scraper — v4 (Clean Output Edition)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
BUG FIXES vs v3 (all confirmed from live log analysis):

DEAD DOMAINS REMOVED (were wasting 90+ seconds on retries):
  ✓ paliare.com        → DNS dead (NameResolutionError)
  ✓ nelligan.ca        → Connection refused
  ✓ jssh.ca            → DNS dead (NameResolutionError)
  ✓ hendersonheinrichs.com → DNS dead (NameResolutionError)
  ✓ legaljobsboard.com → ConnectTimeout × 3 = 90s wasted
  ✓ lerners.ca         → ReadTimeout

GARBAGE OUTPUT FIXED (0/10 results in last run were real jobs):
  ✓ "View our United States Summer Associate Program" — US job slipping through.
    Fixed: "United States", "U.S.", "American" added to RE_BAD_LOCATIONS;
           bad-location check now applied to the TITLE, not just description.
  ✓ "Lawyer Recruiting Contacts" — nav directory page.
    Fixed: "contacts", "directory", "recruiting contacts" added to blocked titles.
  ✓ "Student" / "Summer Students" / "Student positions" — single-word nav links.
    Fixed: MIN_TITLE_LENGTH = 15 chars; titles under 15 chars hard-rejected.
  ✓ emond.ca produces 4 garbage results — emond.ca is a LEGAL PUBLISHER
    (textbooks, practice exams, flashcards), NOT a law firm.
    Fixed: emond.ca removed from URLs; BLOCKED_DOMAINS set prevents re-adding.
  ✓ "Solicitor Practice Exam/Course/Flashcards/Shop All" — product pages.
    Fixed: "practice exam", "flashcard", "shop all", "bursary", "scholarship"
           added to RE_BLOCKED_TITLE.
  ✓ emond, paliare, nelligan, jssh removed from TRUSTED_CA_DOMAINS.

LOCATION ENFORCEMENT — jobs must be Ontario OR Alberta ONLY:
  ✓ RE_BAD_LOCATIONS now includes United States, U.S., American, New York,
    Chicago, London UK (not London ON), Paris, Sydney, etc.
  ✓ For non-trusted domains location is now a HARD reject, not a soft penalty.
"""

from __future__ import annotations

import os
import re
import json
import time
import logging
import concurrent.futures
from urllib.parse import urljoin, urlparse
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup

try:
    from jobspy import scrape_jobs
except ImportError:
    scrape_jobs = None

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
log = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
# 1. CONFIGURATION
# ─────────────────────────────────────────────────────────────────────────────

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept":          "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-CA,en-US;q=0.9,en;q=0.8",
    "Connection":      "keep-alive",
}

HISTORY_FILE   = os.environ.get("HISTORY_FILE",   "job_history.json")
WEIGHTS_FILE   = os.environ.get("WEIGHTS_FILE",   "model_weights.json")
RESULTS_FILE   = os.environ.get("RESULTS_FILE",   "results.json")
RESULTS_WANTED = int(os.environ.get("RESULTS_WANTED") or "30")
DASHBOARD_URL  = os.environ.get("DASHBOARD_URL",  "https://law-associate-job-alerts.vercel.app/")

# Minimum characters a title must have to be considered a real job posting
# Eliminates single-word nav links like "Student", "Summer Students" (< 15 chars)
MIN_TITLE_LENGTH = 15

# Domains that are NOT law firms — publishers, vendors, aggregators that produced
# garbage results. Hard-blocked regardless of what their pages say.
BLOCKED_DOMAINS = {
    "emond",           # Emond Publishing — legal textbooks/exam prep, NOT a law firm
    "legaljobsboard",  # Dead / consistently times out
    "shopify",         # General e-commerce
}

# ─────────────────────────────────────────────────────────────────────────────
# 2. COMPILED REGEX
# ─────────────────────────────────────────────────────────────────────────────

RE_ASSOCIATE = re.compile(
    r"\b(associate|lawyer|counsel|juriste|avocat|attorney|solicitor)\b",
    re.IGNORECASE,
)
RE_STUDENT = re.compile(
    r"\b(articling\s+student|articling\s+clerk|summer\s+student|summer\s+associate|"
    r"law\s+student|student[-\s]at[-\s]law|stagiaire)\b",
    re.IGNORECASE,
)

# BUG FIX: Removed "articling", "careers", "student" — these appear in real job titles.
# This regex now ONLY catches pure navigation anchors (short, generic phrases).
RE_NAV_LINK_EXACT = re.compile(
    r"^(our\s+team|profiles?|meet\s+our\s+team|join\s+us|who\s+we\s+are|about\s+us|"
    r"attorney\s+advertising|terms\s+of\s+use|privacy\s+policy|site\s+map|"
    r"back\s+to\s+search|view\s+all\s+jobs?|all\s+openings?|"
    r"apply\s+now|apply\s+online|submit\s+application|"
    r"read\s+more|learn\s+more|click\s+here|home|menu|search)$",
    re.IGNORECASE,
)

RE_BLOCKED_TITLE = re.compile(
    r"\b(senior\s+associate|senior\s+counsel|senior\s+lawyer|"
    r"managing\s+partner|equity\s+partner|non[-\s]equity\s+partner|"
    r"director|chief\s+legal|vp\s+legal|vice\s+president|"
    r"paralegal|legal\s+assistant|law\s+clerk|"
    r"clerk\s+typist|legal\s+secretary|"
    r"driver|warehouse|sales|marketing|receptionist|"
    # ── Garbage patterns confirmed from live log output ───────────────────
    r"recruiting\s+contacts|contacts|directory|"   # "Lawyer Recruiting Contacts"
    r"practice\s+exam|flashcard|"                   # Emond products
    r"shop\s+all|shop\s+our|bursary|scholarship|award|prize|"  # non-jobs
    r"course\s+outline|study\s+guide|bar\s+exam|"  # publisher content
    r"subscribe|newsletter|podcast|webinar)\b",
    re.IGNORECASE,
)

RE_BAD_URLS = re.compile(
    r"(/who-we-are/|/our-team/|/profiles/|/people/|/attorney-advertising|"
    r"/student-programs/|/summer-program|/blog/|/news/|/events/|/insights/|"
    r"\.(pdf|jpg|png|gif|zip)$)",
    re.IGNORECASE,
)

RE_EXP_KILLER = re.compile(
    r"\b((?:minimum|at\s+least|over|more\s+than)\s+)?(4|5|6|7|8|9|10)(\+|\s*[-–]\s*\d+)?\s*years",
    re.IGNORECASE,
)
# 3+ years is okay for junior; 4+ is senior
RE_SENIOR_ROLE = re.compile(
    r"\b(senior|mid[-\s]level|intermediate|experienced)\s+(associate|counsel|lawyer|attorney)\b",
    re.IGNORECASE,
)

_LOCATIONS = [
    r"\bOntario\b", r"\bAlberta\b", r"\bAB\b", r"\bON\b",
    r"\bToronto\b",  r"\bNorth\s+York\b", r"\bEtobicoke\b", r"\bScarborough\b",
    r"\bOttawa\b",   r"\bMississauga\b",   r"\bBrampton\b",  r"\bHamilton\b",
    r"\bLondon\b",   r"\bMarkham\b",       r"\bVaughan\b",   r"\bKitchener\b",
    r"\bWindsor\b",  r"\bOshawa\b",        r"\bBarrie\b",    r"\bGuelph\b",
    r"\bCalgary\b",  r"\bEdmonton\b",      r"\bRed\s+Deer\b",r"\bLethbridge\b",
    r"\bSt\.?\s+Albert\b", r"\bFort\s+McMurray\b", r"\bAirdrie\b",
]
RE_LOCATIONS     = re.compile("|".join(_LOCATIONS), re.IGNORECASE)
RE_BAD_LOCATIONS = re.compile(
    r"\b(Vancouver|British\s+Columbia|\bBC\b|Montreal|Quebec|\bQC\b|"
    r"Halifax|Nova\s+Scotia|Winnipeg|Manitoba|\bMB\b|"
    r"Saskatchewan|\bSK\b|New\s+Brunswick|\bNB\b|"
    r"Prince\s+Edward|Newfoundland|\bNL\b|"
    # ── US + International — confirmed catching US jobs in live run ────────
    r"United\s+States|\bUSA?\b|U\.S\.A?\.|American|"
    r"New\s+York|\bNYC?\b|Chicago|Houston|Los\s+Angeles|\bLA\b|"
    r"San\s+Francisco|\bSF\b|Washington\s+D\.?C\.?|Boston|Miami|"
    r"London\s+UK|London\s+England|London,\s*England|"  # not London ON
    r"Paris|Sydney|Melbourne|Singapore|Hong\s+Kong|Dubai|"
    r"United\s+Kingdom|\bUK\b|England|Australia|\bAUS\b)\b",
    re.IGNORECASE,
)

# Trusted Canadian law firm domains — skip strict location check for these.
# REMOVED: emond (publisher), paliare/nelligan/jssh (dead DNS)
TRUSTED_CA_DOMAINS = {
    "blakes", "bennettjones", "fasken", "gowlingwlg", "stikeman", "dwpv",
    "mccarthy", "torys", "goodmans", "blg", "millerthomson", "cassels",
    "airdberlis", "osler", "dentons", "nortonrosefulbright",
    "dlapiper", "bakermckenzie", "weirfoulds", "tgf", "blaney", "foglers",
    "fieldlaw", "bdplaw", "parlee", "davies", "stockwoods", "cavalluzzo",
    "goldblattpartners", "paliareroland", "hicksmorley",
    "cohenhighley", "brauti", "singleton", "glaholt",
    "shibleyrighton", "carters", "mross",
}


# ─────────────────────────────────────────────────────────────────────────────
# 3. ADAPTIVE MODEL WEIGHTS (SELF-TRAINING)
# ─────────────────────────────────────────────────────────────────────────────

DEFAULT_WEIGHTS: Dict[str, Any] = {
    "version":         3,
    "training_runs":   0,
    "last_trained":    None,
    "score_threshold": 5,
    "keyword_weights": {
        "associate": 10, "lawyer": 8, "counsel": 7,
        "attorney":  8,  "solicitor": 6, "juriste": 6,
        "articling": 8,  "student": 4,   "summer": 5,
    },
    "site_productivity": {},
    "run_history":       [],
    "total_jobs_found":  0,
}


def load_weights() -> Dict[str, Any]:
    if os.path.exists(WEIGHTS_FILE):
        try:
            with open(WEIGHTS_FILE) as f:
                data = json.load(f)
            for k, v in DEFAULT_WEIGHTS.items():
                if k not in data:
                    data[k] = v
            return data
        except Exception as e:
            log.warning(f"Could not load weights: {e}. Using defaults.")
    return dict(DEFAULT_WEIGHTS)


def save_weights(weights: Dict[str, Any]) -> None:
    try:
        with open(WEIGHTS_FILE, "w") as f:
            json.dump(weights, f, indent=2, default=str)
    except Exception as e:
        log.warning(f"Could not save weights: {e}")


def train_model(
    weights: Dict[str, Any],
    jobs_found: List[dict],
    sites_scanned: List[str],
    run_timestamp: str,
) -> Dict[str, Any]:
    hit_domains: set = set()
    for job in jobs_found:
        try:
            domain = urlparse(job.get("URL", "")).netloc.replace("www.", "")
            hit_domains.add(domain.split(".")[0].lower())
        except Exception:
            pass

    for site_url in sites_scanned:
        try:
            domain = urlparse(site_url).netloc.replace("www.", "")
            key    = domain.split(".")[0].lower()
            entry  = weights["site_productivity"].setdefault(
                key, {"hits": 0, "runs": 0, "rate": 0.0, "last_hit": None}
            )
            entry["runs"] += 1
            if key in hit_domains:
                entry["hits"]    += 1
                entry["last_hit"] = run_timestamp
            entry["rate"] = round(entry["hits"] / max(entry["runs"], 1), 3)
        except Exception:
            pass

    n         = len(jobs_found)
    threshold = weights["score_threshold"]
    if n == 0 and threshold > 3:
        threshold -= 1
        log.info(f"[Train] No results → lowering threshold to {threshold}")
    elif n > 30 and threshold < 12:
        threshold += 1
        log.info(f"[Train] Many results → raising threshold to {threshold}")
    weights["score_threshold"] = threshold

    kw = weights["keyword_weights"]
    for job in jobs_found:
        title = str(job.get("TITLE", "")).lower()
        for word in kw:
            if word in title:
                kw[word] = min(kw[word] + 1, 20)

    summary = {
        "timestamp":  run_timestamp,
        "jobs_found": n,
        "sites_hit":  list(hit_domains),
        "threshold":  weights["score_threshold"],
    }
    weights["run_history"]      = (weights["run_history"] + [summary])[-50:]
    weights["training_runs"]   += 1
    weights["total_jobs_found"] = weights.get("total_jobs_found", 0) + n
    weights["last_trained"]     = run_timestamp

    log.info(
        f"[Train] Run #{weights['training_runs']} | Jobs: {n} | "
        f"Threshold: {weights['score_threshold']} | All-time: {weights['total_jobs_found']}"
    )
    return weights


# ─────────────────────────────────────────────────────────────────────────────
# 4. JOB SCORER
# ─────────────────────────────────────────────────────────────────────────────

class JobScorer:
    def __init__(self, weights: Dict[str, Any]):
        self.threshold = weights.get("score_threshold", 5)
        self.kw        = weights.get("keyword_weights", DEFAULT_WEIGHTS["keyword_weights"])

    def score_job(
        self,
        title:       str,
        description: str,
        url:         str,
        company:     str = "",
    ) -> tuple[bool, str, int]:
        title     = str(title).strip()
        desc      = str(description).strip()
        url_l     = str(url).lower()
        full_text = (title + " " + desc + " " + company).lower()

        # A. Fast-fail checks (ordered cheapest → most expensive)

        # Title must be long enough to be a real job posting
        # Kills: "Student" (7), "Summer Students" (14), "Student positions" (17→pass)
        if len(title) < MIN_TITLE_LENGTH:
            return False, "Too Short", -100

        # Block known-garbage domains (publishers, dead sites)
        domain_key = urlparse(url).netloc.replace("www.", "").split(".")[0].lower()
        if domain_key in BLOCKED_DOMAINS:
            return False, "Blocked Domain", -100

        if RE_NAV_LINK_EXACT.match(title.strip()):
            return False, "Nav Link", -100
        if RE_BAD_URLS.search(url_l):
            return False, "Bad URL", -100
        if RE_BLOCKED_TITLE.search(title):
            return False, "Blocked Title", -100
        if RE_EXP_KILLER.search(full_text) or RE_SENIOR_ROLE.search(full_text):
            return False, "Too Senior", -100

        # Location check on the TITLE itself catches "United States Summer Associate..."
        if RE_BAD_LOCATIONS.search(title):
            return False, "Wrong Location (title)", -100

        # B. Categorise
        category = "Unknown"
        score    = 0
        if RE_ASSOCIATE.search(title):
            category = "Associate"
            score   += self.kw.get("associate", 10)
        elif RE_STUDENT.search(title):
            category = "Student"
            score   += self.kw.get("articling", 8)
        else:
            return False, "Not Legal", -100

        # C. Location — trusted CA domains skip this check.
        #    All others: bad location = hard reject; no location = hard reject
        #    (Ontario/Alberta only — we do not want jobs from other provinces or countries)
        if domain_key not in TRUSTED_CA_DOMAINS:
            if RE_BAD_LOCATIONS.search(full_text):
                return False, "Wrong Location", -100
            if not RE_LOCATIONS.search(full_text):
                # Hard reject for non-trusted domains with no ON/AB signal
                return False, "No ON/AB Location", -100

        # D. Content signal
        if desc and len(desc) > 100:
            if any(kw in full_text for kw in ["apply", "resume", "contact", "submit", "application"]):
                score += 2

        return (score >= self.threshold), category, score


# ─────────────────────────────────────────────────────────────────────────────
# 5. HTTP HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def get_session() -> requests.Session:
    s     = requests.Session()
    # Only 1 retry on connect errors (DNS/refused) — dead sites waste 90s with 3 retries
    # Keep 2 retries for transient server errors (500/502/503/504)
    retry = Retry(
        total=2,
        connect=1,           # only 1 retry on connection-level failures
        read=1,
        backoff_factor=1.0,
        status_forcelist=[429, 500, 502, 503, 504],
    )
    s.mount("https://", HTTPAdapter(max_retries=retry))
    s.mount("http://",  HTTPAdapter(max_retries=retry))
    s.headers.update(_HEADERS)
    return s


def clean_html(html: str) -> str:
    soup = BeautifulSoup(html, "html.parser")
    for tag in soup(["script", "style", "nav", "footer", "header", "noscript", "aside"]):
        tag.extract()
    return " ".join(soup.get_text(separator=" ").split())


def safe_get(session: requests.Session, url: str, timeout: int = 12) -> Optional[requests.Response]:
    try:
        resp = session.get(url, timeout=timeout)
        if resp.status_code == 200:
            return resp
    except Exception:
        pass
    return None


# ─────────────────────────────────────────────────────────────────────────────
# 6. GENERIC HTML SCRAPER (original approach, now with better filtering)
# ─────────────────────────────────────────────────────────────────────────────

def scrape_site_html(url: str, scorer: JobScorer) -> List[dict]:
    session    = get_session()
    found_jobs: List[dict] = []
    visited    = set()
    queue      = [url]
    domain     = urlparse(url).netloc.replace("www.", "")
    MAX_PAGES  = 3

    while queue and MAX_PAGES > 0:
        curr = queue.pop(0)
        if curr in visited:
            continue
        visited.add(curr)
        MAX_PAGES -= 1

        resp = safe_get(session, curr)
        if not resp:
            continue

        soup = BeautifulSoup(resp.text, "html.parser")

        for a in soup.find_all("a", href=True):
            text = a.get_text(" ", strip=True)
            href = urljoin(curr, a["href"])

            # Skip empties, nav links, bad URLs
            if len(text) < 5:
                continue
            if RE_NAV_LINK_EXACT.match(text.strip()):
                continue
            if RE_BAD_URLS.search(href):
                continue
            # Must match a legal role keyword
            if not (RE_ASSOCIATE.search(text) or RE_STUDENT.search(text)):
                continue
            if RE_BLOCKED_TITLE.search(text):
                continue

            if href not in visited:
                job_resp = safe_get(session, href, timeout=10)
                if job_resp:
                    desc    = clean_html(job_resp.text)
                    is_fit, cat, score = scorer.score_job(text, desc, href, domain)
                    if is_fit:
                        found_jobs.append({
                            "TITLE":    text,
                            "COMPANY":  domain.split(".")[0].title(),
                            "URL":      href,
                            "CATEGORY": cat,
                            "SCORE":    score,
                            "SOURCE":   "direct-html",
                            "DATE":     "",
                        })
                        visited.add(href)

    return found_jobs


# ─────────────────────────────────────────────────────────────────────────────
# 7. ATS PLATFORM SCRAPERS
# ─────────────────────────────────────────────────────────────────────────────

LAW_KEYWORDS = re.compile(
    r"\b(associate|lawyer|counsel|attorney|solicitor|articling|"
    r"juriste|avocat|legal\s+counsel|corporate\s+counsel|litigation)\b",
    re.IGNORECASE,
)

def _is_law_job(title: str, dept: str = "", loc: str = "") -> bool:
    return bool(LAW_KEYWORDS.search(title) or LAW_KEYWORDS.search(dept))

def _is_on_ab(location: str) -> bool:
    if not location:
        return True  # assume Canada if blank
    if RE_BAD_LOCATIONS.search(location):
        return False
    return True


# ── 7a. Workday ──────────────────────────────────────────────────────────────
# Workday exposes a public JSON API at:
#   https://<tenant>.wd1.myworkdayjobs.com/wday/cxs/<tenant>/<board>/jobs
# Returns paginated JSON with job listings.

WORKDAY_TENANTS = [
    # (tenant_slug, board_slug, company_display_name)
    ("fasken",          "Fasken_Careers",         "Fasken"),
    ("blg",             "BLG",                    "BLG"),
    ("gowlingwlg",      "GowlingWLG",             "Gowling WLG"),
    ("millerthomson",   "MillerThomson",           "Miller Thomson"),
    ("dentons",         "Dentons",                 "Dentons"),
    ("stikeman",        "StikElliot",              "Stikeman Elliott"),
    ("osler",           "Osler_External",          "Osler"),
    ("nortonrose",      "NRF_Canada",              "Norton Rose Fulbright"),
    ("mccarthy",        "McCarthyTetrault",        "McCarthy Tétrault"),
    ("bennettjones",    "BennettJones",            "Bennett Jones"),
    ("blakes",          "Blake_Cassels_Graydon",   "Blake Cassels & Graydon"),
    ("cassels",         "CasselsBrock",            "Cassels Brock"),
    ("airdberlis",      "AirdBerlis",              "Aird & Berlis"),
    ("torys",           "TorysLLP",                "Torys LLP"),
    ("legalaid",        "LegalAidOntario",         "Legal Aid Ontario"),
]


def scrape_workday_tenant(tenant: str, board: str, company: str, scorer: JobScorer) -> List[dict]:
    session = get_session()
    results = []
    url     = f"https://{tenant}.wd1.myworkdayjobs.com/wday/cxs/{tenant}/{board}/jobs"
    payload = {"limit": 20, "offset": 0, "searchText": "associate lawyer counsel articling"}

    try:
        resp = session.post(url, json=payload, timeout=15,
                            headers={**_HEADERS, "Content-Type": "application/json"})
        if resp.status_code != 200:
            return results
        data = resp.json()
        jobs = data.get("jobPostings", [])

        for job in jobs:
            title    = job.get("title", "")
            location = job.get("locationsText", "") or job.get("location", "")
            ext_path = job.get("externalPath", "")
            job_url  = f"https://{tenant}.wd1.myworkdayjobs.com/en-US/{board}/job/{ext_path}"
            posted   = job.get("postedOn", "")

            if not _is_law_job(title):
                continue
            if not _is_on_ab(location):
                continue

            is_fit, cat, score = scorer.score_job(title, location, job_url, company)
            if is_fit:
                results.append({
                    "TITLE":    title,
                    "COMPANY":  company,
                    "URL":      job_url,
                    "CATEGORY": cat,
                    "SCORE":    score,
                    "SOURCE":   "workday",
                    "DATE":     posted,
                })
    except Exception as e:
        log.debug(f"Workday {tenant}: {e}")

    return results


def scrape_all_workday(scorer: JobScorer) -> List[dict]:
    log.info(f"  [Workday] Scanning {len(WORKDAY_TENANTS)} tenants...")
    all_jobs: List[dict] = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=6) as ex:
        futures = {
            ex.submit(scrape_workday_tenant, t, b, c, scorer): c
            for t, b, c in WORKDAY_TENANTS
        }
        for f in concurrent.futures.as_completed(futures):
            try:
                all_jobs.extend(f.result())
            except Exception:
                pass
    return all_jobs


# ── 7b. Greenhouse ────────────────────────────────────────────────────────────
# Greenhouse boards expose: https://boards-api.greenhouse.io/v1/boards/{slug}/jobs
# Many in-house legal teams and boutique firms use Greenhouse.

GREENHOUSE_BOARDS = [
    # (board_slug, company_display_name)
    ("rbc",               "RBC Legal"),
    ("tdbank",            "TD Bank Legal"),
    ("shopify",           "Shopify Legal"),
    ("cppinvestments",    "CPP Investments Legal"),
    ("omers",             "OMERS Legal"),
    ("brookfield",        "Brookfield Asset Mgmt"),
    ("loblawcompanies",   "Loblaw Legal"),
    ("rogers",            "Rogers Legal"),
    ("telus",             "Telus Legal"),
    ("sunlifefinancial",  "Sun Life Legal"),
    ("manulife",          "Manulife Legal"),
    ("intactinsurance",   "Intact Legal"),
    ("enbridge",          "Enbridge Legal"),
    ("suncor",            "Suncor Legal"),
    ("agf",               "AGF Legal"),
    ("ocrcalgary",        "OCR Calgary"),
]


def scrape_greenhouse_board(slug: str, company: str, scorer: JobScorer) -> List[dict]:
    session = get_session()
    results = []
    url     = f"https://boards-api.greenhouse.io/v1/boards/{slug}/jobs?content=true"

    try:
        resp = safe_get(session, url)
        if not resp:
            return results
        data = resp.json()
        jobs = data.get("jobs", [])

        for job in jobs:
            title    = job.get("title", "")
            location = job.get("location", {}).get("name", "")
            job_url  = job.get("absolute_url", "")
            content  = BeautifulSoup(job.get("content", ""), "html.parser").get_text()
            posted   = job.get("updated_at", "")[:10]

            if not _is_law_job(title, loc=location):
                continue
            if not _is_on_ab(location):
                continue

            is_fit, cat, score = scorer.score_job(title, content, job_url, company)
            if is_fit:
                results.append({
                    "TITLE":    title,
                    "COMPANY":  company,
                    "URL":      job_url,
                    "CATEGORY": cat,
                    "SCORE":    score,
                    "SOURCE":   "greenhouse",
                    "DATE":     posted,
                })
    except Exception as e:
        log.debug(f"Greenhouse {slug}: {e}")

    return results


def scrape_all_greenhouse(scorer: JobScorer) -> List[dict]:
    log.info(f"  [Greenhouse] Scanning {len(GREENHOUSE_BOARDS)} boards...")
    all_jobs: List[dict] = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=6) as ex:
        futures = {
            ex.submit(scrape_greenhouse_board, s, c, scorer): c
            for s, c in GREENHOUSE_BOARDS
        }
        for f in concurrent.futures.as_completed(futures):
            try:
                all_jobs.extend(f.result())
            except Exception:
                pass
    return all_jobs


# ── 7c. Lever ────────────────────────────────────────────────────────────────
# Lever exposes: https://api.lever.co/v0/postings/{slug}?mode=json

LEVER_BOARDS = [
    ("weirfoulds",         "Weirfoulds LLP"),
    ("goodmans",           "Goodmans LLP"),
    ("dwpv",               "Davies Ward Phillips"),
    ("paliareroland",      "Paliare Roland"),
    ("cavalluzzo",         "Cavalluzzo LLP"),
    ("goldblattpartners",  "Goldblatt Partners"),
    ("stockwoods",         "Stockwoods LLP"),
    ("brauti",             "Brauti Thorning"),
    ("siskinds",           "Siskinds LLP"),
    ("heenanblaikie",      "Heenan Blaikie"),
]


def scrape_lever_board(slug: str, company: str, scorer: JobScorer) -> List[dict]:
    session = get_session()
    results = []
    url     = f"https://api.lever.co/v0/postings/{slug}?mode=json"

    try:
        resp = safe_get(session, url)
        if not resp:
            return results
        jobs = resp.json()
        if not isinstance(jobs, list):
            return results

        for job in jobs:
            title    = job.get("text", "")
            location = job.get("categories", {}).get("location", "")
            dept     = job.get("categories", {}).get("department", "")
            job_url  = job.get("hostedUrl", "")
            content  = " ".join(
                s.get("content", "") for s in job.get("descriptionBody", {}).get("blocks", [])
                if isinstance(s, dict)
            )
            posted   = datetime.fromtimestamp(
                job.get("createdAt", 0) / 1000, tz=timezone.utc
            ).strftime("%Y-%m-%d") if job.get("createdAt") else ""

            if not _is_law_job(title, dept, location):
                continue
            if not _is_on_ab(location):
                continue

            is_fit, cat, score = scorer.score_job(title, content, job_url, company)
            if is_fit:
                results.append({
                    "TITLE":    title,
                    "COMPANY":  company,
                    "URL":      job_url,
                    "CATEGORY": cat,
                    "SCORE":    score,
                    "SOURCE":   "lever",
                    "DATE":     posted,
                })
    except Exception as e:
        log.debug(f"Lever {slug}: {e}")

    return results


def scrape_all_lever(scorer: JobScorer) -> List[dict]:
    log.info(f"  [Lever] Scanning {len(LEVER_BOARDS)} boards...")
    all_jobs: List[dict] = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=6) as ex:
        futures = {
            ex.submit(scrape_lever_board, s, c, scorer): c
            for s, c in LEVER_BOARDS
        }
        for f in concurrent.futures.as_completed(futures):
            try:
                all_jobs.extend(f.result())
            except Exception:
                pass
    return all_jobs


# ── 7d. Government / Public Sector ──────────────────────────────────────────
# Federal and Provincial government jobs use a separate jobs portal.

def scrape_gc_jobs(scorer: JobScorer) -> List[dict]:
    """Scrape jobs.gc.ca for legal/counsel positions in ON/AB."""
    session  = get_session()
    results  = []
    searches = [
        "https://emploisfp-psjobs.cfp-psc.gc.ca/psrs-srfp/applicant/page1800?searchValue=counsel+lawyer&searchLocationValue=Ontario",
        "https://emploisfp-psjobs.cfp-psc.gc.ca/psrs-srfp/applicant/page1800?searchValue=counsel+lawyer&searchLocationValue=Alberta",
    ]
    for search_url in searches:
        resp = safe_get(session, search_url)
        if not resp:
            continue
        soup = BeautifulSoup(resp.text, "html.parser")
        for a in soup.find_all("a", href=True):
            text = a.get_text(" ", strip=True)
            href = urljoin(search_url, a["href"])
            if not _is_law_job(text):
                continue
            if RE_BLOCKED_TITLE.search(text):
                continue
            is_fit, cat, score = scorer.score_job(text, "", href, "Government of Canada")
            if is_fit:
                results.append({
                    "TITLE":    text,
                    "COMPANY":  "Government of Canada",
                    "URL":      href,
                    "CATEGORY": cat,
                    "SCORE":    score,
                    "SOURCE":   "gc-jobs",
                    "DATE":     "",
                })
    return results


def scrape_ontario_public_service(scorer: JobScorer) -> List[dict]:
    """Scrape Ontario Public Service careers for legal roles."""
    session = get_session()
    results = []
    url     = "https://www.gojobs.gov.on.ca/Jobs.aspx?KeyWord=counsel+lawyer+associate"
    resp    = safe_get(session, url)
    if not resp:
        return results
    soup = BeautifulSoup(resp.text, "html.parser")
    for a in soup.find_all("a", href=True):
        text = a.get_text(" ", strip=True)
        href = urljoin(url, a["href"])
        if not _is_law_job(text):
            continue
        is_fit, cat, score = scorer.score_job(text, "", href, "Ontario Public Service")
        if is_fit:
            results.append({
                "TITLE":    text,
                "COMPANY":  "Ontario Public Service",
                "URL":      href,
                "CATEGORY": cat,
                "SCORE":    score,
                "SOURCE":   "ontario-gov",
                "DATE":     "",
            })
    return results


# ─────────────────────────────────────────────────────────────────────────────
# 8. TARGET URLS — 70 DIRECT HTML SCRAPERS
# ─────────────────────────────────────────────────────────────────────────────

def get_target_urls() -> List[str]:
    return [
        # ── Bay Street / National Firms ───────────────────────────────────────
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
        "https://www.osler.com/en/careers/opportunities",

        # ── Ontario Regional Firms ────────────────────────────────────────────
        # REMOVED: lerners.ca (ReadTimeout), paliare.com (DNS dead),
        #          nelligan.ca (Connection refused), emond.ca (publisher not firm)
        "https://www.litigate.com/careers/lawyers",
        "https://www.wildlaw.ca/careers/lawyers/",
        "https://www.weirfoulds.com/careers/lawyers-and-law-students/",
        "https://www.tgf.ca/join-us/opportunities/",
        "https://www.blaney.com/en/careers/",
        "https://www.foglers.com/career-opportunity/",
        "https://www.singleton.com/careers/",
        "https://www.paliareroland.com/careers/",
        "https://www.cohenhighley.com/about/careers/",
        "https://www.hicksmorley.com/careers/",
        "https://www.glaholt.com/careers/",
        "https://www.shibleyrighton.com/firm/careers/",
        "https://www.carters.ca/careers/",
        "https://www.cavalluzzo.com/careers/",
        "https://goldblattpartners.com/careers/",
        "https://www.siskinds.com/careers/",
        "https://www.hicks.ca/careers/",
        "https://www.kellylawyers.ca/join-our-team/",
        "https://www.daviesward.com/en/careers/",
        "https://www.kmlaw.ca/careers/",               # Koskie Minsky
        "https://www.sotos.ca/careers/",
        "https://www.stockwoods.ca/careers/",
        "https://www.brauti.com/careers/",
        "https://www.fasken.com/en/careers/students",  # Articling students page

        # ── Alberta Regional Firms ────────────────────────────────────────────
        # REMOVED: jssh.ca (DNS dead), hendersonheinrichs.com (DNS dead)
        "https://www.fieldlaw.com/careers/associate-opportunities/",
        "https://www.bdplaw.com/careers/",
        "https://www.parlee.com/careers/",
        "https://www.mross.com/about-us/careers/",
        "https://www.burnetduckworth.com/careers/",
        "https://www.brownleelaw.com/careers/",
        "https://www.matholaw.ca/careers/",
        "https://www.shortoil.ca/careers/",            # Short Oil
        "https://www.mcleodlaw.com/careers/",

        # ── International Firms (Canadian offices only) ───────────────────────
        "https://www.dentons.com/en/careers/find-a-job",
        "https://www.nortonrosefulbright.com/en-ca/careers/job-listings",
        "https://www.dlapiper.com/en/canada/careers/",
        # BakerMcKenzie: use Canada-specific page to avoid US results
        "https://careers.bakermckenzie.com/global/en/c/canada-jobs",
        "https://careers.herbertsmithfreehills.com/go/Canada/",
        "https://careers.cliffordchance.com/content/careers/en/office/toronto.html",

        # ── Legal Recruitment / Job Boards ────────────────────────────────────
        # REMOVED: legaljobsboard.com (ConnectTimeout × 3 = 90s wasted),
        #          legaljobsboard.ca (sister site, same issue)
        "https://www.zsa.ca/job-board/",
        "https://thecounselnetwork.com/job-search/",
        "https://legaljobs.ca/jobs/",

        # ── Bar / Law Society Boards ──────────────────────────────────────────
        "https://www.oba.org/Careers",
        "https://lawsociety.ab.ca/about/programs-and-initiatives/careers/",
        "https://www.lso.ca/careers",
        "https://www.cba.org/For-Lawyers/Employment",

        # ── In-House / Corporate Legal (Canadian postings) ────────────────────
        "https://jobs.rbc.com/ca/en/search-results?keywords=legal+counsel",
        "https://jobs.td.com/en-CA/jobs/?keyword=legal+counsel",
        "https://careers.sunlife.com/en/search/?search=legal+counsel",
        "https://careers.manulife.com/global/en/search-results?keywords=counsel",
        "https://www.enbridge.com/careers/job-search?q=legal+counsel",
        "https://suncor.com/en/careers/current-openings?search=counsel",
        "https://careers.telus.com/search/#q=legal+counsel&t=Jobs&x5=Canada",
        "https://careers.bmo.com/ca/en/search-results?keywords=legal+counsel",
        "https://jobs.scotiabank.com/search/?q=legal+counsel&l=Ontario",

        # ── Law School Career Boards ──────────────────────────────────────────
        "https://ultravires.ca/jobs/",               # U of T Law student newspaper job board
        "https://www.osgoode.yorku.ca/careers/",

        # ── Canadian Job Aggregators (location-pinned searches) ───────────────
        "https://ca.indeed.com/jobs?q=associate+lawyer&l=Ontario&fromage=7",
        "https://ca.indeed.com/jobs?q=associate+lawyer&l=Alberta&fromage=7",
        "https://ca.indeed.com/jobs?q=articling+student&l=Ontario&fromage=14",
        "https://ca.indeed.com/jobs?q=articling+student&l=Alberta&fromage=14",
        "https://www.glassdoor.ca/Job/ontario-associate-lawyer-jobs-SRCH_IL.0,7_IS3559_KO8,24.htm",
        "https://ca.talent.com/jobs?k=associate+lawyer&l=ontario",
        "https://www.eluta.ca/search?q=associate+lawyer&l=Toronto",
    ]


# ─────────────────────────────────────────────────────────────────────────────
# 9. AGGREGATOR SCRAPER (JobSpy — Indeed / LinkedIn / Google)
# ─────────────────────────────────────────────────────────────────────────────

def scrape_jobspy_wrapper(scorer: JobScorer) -> pd.DataFrame:
    if scrape_jobs is None:
        log.warning("python-jobspy not installed; skipping aggregators.")
        return pd.DataFrame()

    log.info("  [JobSpy] Scraping Indeed / LinkedIn / Google...")
    search_terms = [
        "associate lawyer",
        "articling student",
        "legal counsel",
        "corporate counsel",
    ]
    locations = ["Ontario, Canada", "Alberta, Canada"]
    all_rows: List[dict] = []

    for search_term in search_terms:
        for loc in locations:
            try:
                jobs = scrape_jobs(
                    site_name=["indeed", "linkedin", "google"],
                    search_term=search_term,
                    google_search_term=(
                        f'"{search_term}" "0-3 years" -senior -warehouse -driver site:ca'
                    ),
                    location=loc,
                    results_wanted=RESULTS_WANTED,
                    hours_old=168,
                    country_indeed="Canada",
                    linkedin_fetch_description=True,
                    verbose=0,
                )
                if jobs is not None and not jobs.empty:
                    jobs.columns = [c.upper() for c in jobs.columns]
                    for _, row in jobs.iterrows():
                        title   = str(row.get("TITLE",       ""))
                        desc    = str(row.get("DESCRIPTION", ""))
                        url     = str(row.get("JOB_URL",     ""))
                        company = str(row.get("COMPANY",     ""))
                        date    = str(row.get("DATE_POSTED", ""))
                        is_fit, cat, score = scorer.score_job(title, desc, url, company)
                        if is_fit:
                            all_rows.append({
                                "TITLE":    title,
                                "COMPANY":  company,
                                "URL":      url,
                                "CATEGORY": cat,
                                "SCORE":    score,
                                "SOURCE":   "jobspy",
                                "DATE":     date,
                            })
                time.sleep(1)
            except Exception as e:
                log.debug(f"JobSpy error [{search_term} / {loc}]: {e}")

    return pd.DataFrame(all_rows) if all_rows else pd.DataFrame()


# ─────────────────────────────────────────────────────────────────────────────
# 10. ORCHESTRATE ALL SCRAPERS
# ─────────────────────────────────────────────────────────────────────────────

def run_direct_scrape(urls: List[str], scorer: JobScorer) -> pd.DataFrame:
    all_jobs: List[dict] = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        futures = {executor.submit(scrape_site_html, u, scorer): u for u in urls}
        for future in concurrent.futures.as_completed(futures):
            try:
                data = future.result()
                if data:
                    all_jobs.extend(data)
            except Exception:
                pass
    return pd.DataFrame(all_jobs) if all_jobs else pd.DataFrame()


def run_all_scrapers(scorer: JobScorer) -> pd.DataFrame:
    frames = []

    # 1. Direct HTML scrape (70 sites)
    target_urls = get_target_urls()
    log.info(f"\n── Direct HTML scrape ({len(target_urls)} sites) ─────────────")
    frames.append(run_direct_scrape(target_urls, scorer))

    # 2. Workday ATS
    log.info("\n── Workday ATS ──────────────────────────────────────────")
    wd_jobs = scrape_all_workday(scorer)
    if wd_jobs:
        frames.append(pd.DataFrame(wd_jobs))
    log.info(f"  → {len(wd_jobs)} raw hits from Workday")

    # 3. Greenhouse ATS
    log.info("\n── Greenhouse ATS ───────────────────────────────────────")
    gh_jobs = scrape_all_greenhouse(scorer)
    if gh_jobs:
        frames.append(pd.DataFrame(gh_jobs))
    log.info(f"  → {len(gh_jobs)} raw hits from Greenhouse")

    # 4. Lever ATS
    log.info("\n── Lever ATS ────────────────────────────────────────────")
    lv_jobs = scrape_all_lever(scorer)
    if lv_jobs:
        frames.append(pd.DataFrame(lv_jobs))
    log.info(f"  → {len(lv_jobs)} raw hits from Lever")

    # 5. Government portals
    log.info("\n── Government portals ───────────────────────────────────")
    gov_jobs  = scrape_gc_jobs(scorer) + scrape_ontario_public_service(scorer)
    if gov_jobs:
        frames.append(pd.DataFrame(gov_jobs))
    log.info(f"  → {len(gov_jobs)} raw hits from Government")

    # 6. JobSpy aggregators (Indeed / LinkedIn / Google)
    log.info("\n── JobSpy aggregators ───────────────────────────────────")
    frames.append(scrape_jobspy_wrapper(scorer))

    non_empty = [f for f in frames if f is not None and not f.empty]
    if not non_empty:
        return pd.DataFrame()
    return pd.concat(non_empty, ignore_index=True, sort=False)


# ─────────────────────────────────────────────────────────────────────────────
# 11. DEDUPLICATION
# ─────────────────────────────────────────────────────────────────────────────

def deduplicate_jobs(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    df = df.copy()  # FIX: prevents SettingWithCopyWarning

    def make_sig(row: pd.Series) -> str:
        t = re.sub(r"\W+", "", str(row.get("TITLE",   "")).lower())
        c = re.sub(r"\W+", "", str(row.get("COMPANY", "")).lower())
        return f"{t}_{c}"

    df["SIG"]       = df.apply(make_sig, axis=1)
    df              = df.drop_duplicates(subset=["SIG"])
    df["CLEAN_URL"] = df["URL"].apply(lambda x: str(x).split("?")[0])
    df              = df.drop_duplicates(subset=["CLEAN_URL"])
    return df.drop(columns=["SIG", "CLEAN_URL"])


# ─────────────────────────────────────────────────────────────────────────────
# 12. RESULTS JSON
# ─────────────────────────────────────────────────────────────────────────────

def append_results(jobs: List[dict], run_ts: str, weights: Dict[str, Any]) -> None:
    if os.path.exists(RESULTS_FILE):
        try:
            with open(RESULTS_FILE) as f:
                data = json.load(f)
        except Exception:
            data = {"runs": []}
    else:
        data = {"runs": []}

    run_entry = {
        "run_id":       run_ts.replace(":", "").replace("-", "").replace(" ", "_"),
        "timestamp":    run_ts,
        "jobs_found":   len(jobs),
        "training_run": weights.get("training_runs", 0),
        "threshold":    weights.get("score_threshold", 5),
        "jobs":         jobs,
    }

    data["runs"]           = (data["runs"] + [run_entry])[-200:]
    data["last_updated"]   = run_ts
    data["total_all_time"] = weights.get("total_jobs_found", 0)
    data["training_runs"]  = weights.get("training_runs", 0)
    data["score_threshold"]= weights.get("score_threshold", 5)
    data["site_productivity"] = weights.get("site_productivity", {})

    try:
        with open(RESULTS_FILE, "w") as f:
            json.dump(data, f, indent=2, default=str)
        log.info(f"✓ Results saved to {RESULTS_FILE}")
    except Exception as e:
        log.warning(f"Could not save results: {e}")


# ─────────────────────────────────────────────────────────────────────────────
# 13. TELEGRAM ALERT
# ─────────────────────────────────────────────────────────────────────────────

def send_telegram(df: pd.DataFrame, weights: Dict[str, Any]) -> None:
    bot_token = os.environ.get("TELEGRAM_BOT_TOKEN")
    chat_id   = os.environ.get("TELEGRAM_CHAT_ID")

    if not (bot_token and chat_id):
        print("\n[!] TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID missing. No alert sent.")
        return

    associates = df[df["CATEGORY"] == "Associate"] if not df.empty else pd.DataFrame()
    students   = df[df["CATEGORY"] == "Student"]   if not df.empty else pd.DataFrame()
    now_str    = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    lines = [
        f"<b>⚖️ Law Jobs — ON/AB | {now_str}</b>",
        f"<b>{len(df)} new listing(s) found</b>",
        (f"<i>Model run #{weights.get('training_runs','?')} · "
         f"Threshold: {weights.get('score_threshold',5)} · "
         f"All-time: {weights.get('total_jobs_found',0)} jobs</i>"),
    ]

    if not associates.empty:
        lines.append("\n<b>🏛 ASSOCIATES / LAWYERS</b>")
        for _, row in associates.iterrows():
            score_str = f" [{row['SCORE']}]" if row.get("SCORE") else ""
            src_str   = f" <i>({row['SOURCE']})</i>" if row.get("SOURCE") else ""
            lines.append(
                f"• <a href=\"{row['URL']}\">{row['TITLE']}</a> "
                f"— {row['COMPANY']}{score_str}{src_str}"
            )

    if not students.empty:
        lines.append("\n<b>🎓 ARTICLING / STUDENTS</b>")
        for _, row in students.iterrows():
            src_str = f" <i>({row['SOURCE']})</i>" if row.get("SOURCE") else ""
            lines.append(
                f"• <a href=\"{row['URL']}\">{row['TITLE']}</a> "
                f"— {row['COMPANY']}{src_str}"
            )

    if df.empty:
        lines.append(
            "\n<i>No new positions this run. "
            "The model will lower its threshold next cycle.</i>"
        )

    lines.append(f"\n📊 <a href=\"{DASHBOARD_URL}\">Open Dashboard</a>")

    full_message = "\n".join(lines)
    chunks  = [full_message[i : i + 4096] for i in range(0, len(full_message), 4096)]
    api_url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    success = True

    for chunk in chunks:
        payload = {
            "chat_id":                  chat_id,
            "text":                     chunk,
            "parse_mode":               "HTML",
            "disable_web_page_preview": True,
        }
        try:
            resp = requests.post(api_url, json=payload, timeout=10)
            if not resp.ok:
                print(f"✗ Telegram error: {resp.status_code} — {resp.text}")
                success = False
        except Exception as e:
            print(f"✗ Telegram request failed: {e}")
            success = False

    if success:
        print("✓ Telegram alert sent.")


# ─────────────────────────────────────────────────────────────────────────────
# 14. MAIN
# ─────────────────────────────────────────────────────────────────────────────

def main() -> None:
    run_ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    print("=" * 60)
    print("Law Associate Job Scraper — v3 Maximum Coverage")
    print(f"Run: {run_ts}")
    print("=" * 60)

    weights = load_weights()
    scorer  = JobScorer(weights)

    print(f"\n[Model]  Score threshold : {scorer.threshold}")
    print(f"[Model]  Training run #   : {weights['training_runs']}")
    print(f"[Model]  All-time jobs    : {weights.get('total_jobs_found', 0)}")
    print()

    # 1. Run all scrapers
    combined = run_all_scrapers(scorer)

    # 2. Deduplicate
    unique_jobs = deduplicate_jobs(combined)
    log.info(f"After dedup: {len(unique_jobs)} unique candidates")

    # 3. History filter
    if os.path.exists(HISTORY_FILE):
        with open(HISTORY_FILE) as f:
            history_ids: set = set(json.load(f))
    else:
        history_ids = set()

    final_jobs: List[dict] = []
    for _, row in unique_jobs.iterrows():
        clean_url = str(row.get("URL", "")).split("?")[0]
        if clean_url not in history_ids:
            final_jobs.append(row.to_dict())
            history_ids.add(clean_url)

    final_df = pd.DataFrame(final_jobs)

    # 4. Print output
    print(f"\n{'='*60}")
    print(f"✓ Final Verified New Jobs: {len(final_df)}")
    if not final_df.empty:
        cols = [c for c in ["TITLE", "COMPANY", "CATEGORY", "SOURCE", "SCORE"] if c in final_df.columns]
        print(final_df[cols].to_string(index=False))

    # 5. Self-training
    weights = train_model(weights, final_jobs, get_target_urls(), run_ts)
    save_weights(weights)

    # 6. Persist results for dashboard
    append_results(final_jobs, run_ts, weights)

    # 7. Save history
    with open(HISTORY_FILE, "w") as f:
        json.dump(list(history_ids), f)

    # 8. Telegram
    send_telegram(final_df, weights)


if __name__ == "__main__":
    main()
