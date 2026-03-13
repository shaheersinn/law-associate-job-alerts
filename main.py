#!/usr/bin/env python3
"""
Law Associate Job Scraper — v6 (Live-Log Bug-Fix Edition)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
BUGS FIXED vs v5 (confirmed from 2026-03-11 live-log analysis):

DEAD DOMAINS REMOVED (confirmed dead in 2026-03-11 logs):
  ✓ www.davies.ca         → SSL cert mismatch (hostname). Fixed → davies.ca
  ✓ www.gutierrezlaw.ca   → DNS dead (NameResolutionError)
  ✓ sookermacleod.com     → DNS dead (NameResolutionError)
  ✓ www.mustardlaw.ca     → DNS dead (NameResolutionError)
  ✓ www.burntsand.com     → ConnectTimeoutError (12s)
  ✓ careers.oba.org       → ReadTimeoutError. Fixed → www.oba.org/Careers

FALSE POSITIVES BLOCKED (confirmed from 2026-03-11 live output):
  ✓ "Summer CPA Articling Student, Assurance and Accounting" × 2
    → CPA = Chartered Professional Accountant; not law.
    Fix: RE_BLOCKED_TITLE gains r"\bcpa\s+articling\b"
  ✓ "Flextronics Corporation: Counsel to Flextronics during the CCAA..."
    → TGF past-matter description page, NOT a job posting.
    Fix: RE_CASE_DESCRIPTION blocks "Counsel to [Org]" + CCAA/matter patterns
  ✓ "Expert Opportunity - Lawyer ($225/hr up to $4.5k/week)"
    → Freelance expert-witness gig, NOT a law-firm associate role.
    Fix: RE_FREELANCE blocks hourly-rate and expert-opportunity patterns.

WORKFLOW BUG (dashboard never updated):
  ✓ Old job_alert.yml had NO `git commit` step and NO `permissions: write`.
    results.json was written to runner disk but never pushed to the repo, so
    the Vercel dashboard read a permanently empty file.
    Fix: new job_alert.yml (v4 actions, separate restore/save, git-commit step).

TELEGRAM:
  ✓ Multi-chunk messages sent as separate API calls could flood the chat.
    Fix: build the full message first, then send in one HTTP POST (Telegram
    supports up to 4096 chars per message). If total > 4096, we trim the
    job list to fit, appending a "… and N more" footer rather than splitting.

MODEL IMPROVEMENTS:
  + Dead-domain self-learning: domains that produce 0 jobs over 5+ consecutive
    runs are logged to weights["low_productivity_domains"] and skipped on the
    next scrape, removing them automatically.
  + Source reliability matrix: per-source precision estimate tracks how many
    hits each source produces per run.
  + Enhanced junior signals: new regex catches "0-2 PQE", "PQE 0-2",
    "newly admitted", "fresh call" patterns.
  + Threshold floor/ceiling tightened (3–10 instead of 3–12) to reduce both
    false positives and false negatives.
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

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
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

MIN_TITLE_LENGTH = 15
# How many consecutive zero-hit runs before a domain is soft-disabled
DEAD_DOMAIN_THRESHOLD = 5

BLOCKED_DOMAINS = {
    "emond",           # Legal publisher (textbooks), NOT a law firm
    "legaljobsboard",  # Dead / timeout
}

# Domains with SSL certificate mismatches — use verify=False as fallback.
SSL_SOFT_DOMAINS = {
    "hicks.ca",
    "mcleodlaw.com",
    # BUG FIX: davies.ca cert is valid but NOT for www.davies.ca — use bare domain
    "davies.ca",
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
    r"general\s+counsel|deputy\s+general\s+counsel|associate\s+general\s+counsel|"
    r"chief\s+compliance|chief\s+privacy|"
    r"paralegal|legal\s+assistant|law\s+clerk|"
    r"clerk\s+typist|legal\s+secretary|"
    r"driver|warehouse|sales|marketing|receptionist|"
    r"recruiting\s+contacts|contacts|directory|"
    r"practice\s+exam|flashcard|"
    r"shop\s+all|shop\s+our|bursary|scholarship|award|prize|"
    r"course\s+outline|study\s+guide|bar\s+exam|"
    r"subscribe|newsletter|podcast|webinar|"
    # BUG FIX: CPA = Chartered Professional Accountant, NOT a law articling student
    r"cpa\s+articling|cpa\s+student|co-op\s+cpa|co\s+op\s+cpa|"
    r"assurance\s+and\s+accounting|tax\s+articling|"
    # BUG FIX: "counsel to [company]" = past-matter description (e.g. TGF website)
    r"counsel\s+to\s+(?:the\s+)?(?:\w+\s+){0,4}(?:during|in\s+the|on\s+the|regarding))\b",
    re.IGNORECASE,
)

# BUG FIX: Block freelance / expert-witness / contract listings with hourly rates
RE_FREELANCE = re.compile(
    r"(\$\d+[\,\d]*\s*/\s*(?:hr|hour)|"
    r"\bexpert\s+opportunity\b|"
    r"\bcontract\s+lawyer\b|"
    r"\bfreelance\s+lawyer\b|"
    r"\bper\s+diem\s+lawyer\b|"
    r"\bexpert\s+witness\b)",
    re.IGNORECASE,
)

# BUG FIX: Catch ordinal-year seniority in titles.
RE_YEAR_TOO_SENIOR = re.compile(
    r"\b(third|fourth|fifth|sixth|seventh|3rd|4th|5th|6th|7th)\s+year\b",
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
RE_SENIOR_ROLE = re.compile(
    r"\b(senior|mid[-\s]level|intermediate|experienced)\s+(associate|counsel|lawyer|attorney)\b",
    re.IGNORECASE,
)

# Enhanced junior signals — now also catches PQE variants
RE_JUNIOR_SIGNALS = re.compile(
    r"\b(newly\s+called|new\s+call|recent\s+call|newly\s+admitted|fresh\s+call|"
    r"0[-–]2\s+years?|1[-–]3\s+years?|0[-–]3\s+years?|"
    r"0[-–]2\s+pqe|pqe\s+0[-–]2|pqe\s+1[-–]3|"
    r"first[\s-]year|second[\s-]year|1st[\s-]year|2nd[\s-]year|"
    r"junior\s+associate|entry[\s-]level|new\s+graduate|recent\s+graduate|"
    r"called\s+to\s+the\s+bar\s+(?:in\s+)?(?:20[0-9]{2}|within\s+[123]\s+years?)|"
    r"bar\s+admission\s+(?:in\s+)?20[0-9]{2}|"
    r"recent(?:ly)?\s+called)\b",
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
    r"\bSudbury\b",  r"\bThunder\s+Bay\b", r"\bPeterborough\b",
]
RE_LOCATIONS     = re.compile("|".join(_LOCATIONS), re.IGNORECASE)
RE_BAD_LOCATIONS = re.compile(
    r"\b(Vancouver|British\s+Columbia|\bBC\b|Montreal|Quebec|\bQC\b|"
    r"Halifax|Nova\s+Scotia|Winnipeg|Manitoba|\bMB\b|"
    r"Saskatchewan|\bSK\b|New\s+Brunswick|\bNB\b|"
    r"Prince\s+Edward|Newfoundland|\bNL\b|"
    r"United\s+States|\bUSA?\b|U\.S\.A?\.|American|"
    r"New\s+York|\bNYC?\b|Chicago|Houston|Los\s+Angeles|\bLA\b|"
    r"San\s+Francisco|\bSF\b|Washington\s+D\.?C\.?|Boston|Miami|"
    r"London\s+UK|London\s+England|London,\s*England|"
    r"Paris|Sydney|Melbourne|Singapore|Hong\s+Kong|Dubai|"
    r"United\s+Kingdom|\bUK\b|England|Australia|\bAUS\b)\b",
    re.IGNORECASE,
)

TRUSTED_CA_DOMAINS = {
    "blakes", "bennettjones", "fasken", "gowlingwlg", "stikeman", "dwpv",
    "mccarthy", "torys", "goodmans", "blg", "millerthomson", "cassels",
    "airdberlis", "osler", "dentons", "nortonrosefulbright",
    "dlapiper", "bakermckenzie", "weirfoulds", "tgf", "blaney", "foglers",
    "fieldlaw", "bdplaw", "parlee", "davies", "stockwoods", "cavalluzzo",
    "goldblattpartners", "paliareroland", "hicksmorley",
    "cohenhighley", "brauti", "singleton", "glaholt",
    "shibleyrighton", "carters", "mross", "legalaidontario",
    "siskinds", "litigate", "wildlaw", "kmlaw", "sotos",
}


# ─────────────────────────────────────────────────────────────────────────────
# 3. ADAPTIVE MODEL WEIGHTS (SELF-TRAINING)
# ─────────────────────────────────────────────────────────────────────────────

DEFAULT_WEIGHTS: Dict[str, Any] = {
    "version":         6,
    "training_runs":   0,
    "last_trained":    None,
    "score_threshold": 5,
    "keyword_weights": {
        "associate":  10, "lawyer":    8, "counsel":    7,
        "attorney":    8, "solicitor": 6, "juriste":    6,
        "articling":   8, "student":   4, "summer":     5,
    },
    "site_productivity":        {},
    "source_stats":             {},
    "low_productivity_domains": [],   # self-pruned dead domains
    "run_history":              [],
    "total_jobs_found":         0,
    "false_positive_domains":   [],
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
    weights:       Dict[str, Any],
    jobs_found:    List[dict],
    sites_scanned: List[str],
    run_timestamp: str,
) -> Dict[str, Any]:
    """
    Update model weights based on this run's results.

    Enhancements in v6:
    - Dead-domain self-pruning: domains with 0 hits over DEAD_DOMAIN_THRESHOLD
      consecutive runs are added to low_productivity_domains for future skipping.
    - Source reliability: tracks avg jobs/run per source for dashboard display.
    - Threshold is clamped to [3, 10] (was [3, 12]) to avoid extreme drift.
    """
    hit_domains:   set = set()
    source_counts: Dict[str, int] = {}

    for job in jobs_found:
        try:
            domain = urlparse(job.get("URL", "")).netloc.replace("www.", "")
            hit_domains.add(domain.split(".")[0].lower())
        except Exception:
            pass
        source = job.get("SOURCE", "unknown")
        source_counts[source] = source_counts.get(source, 0) + 1

    # ── Site productivity tracking + dead-domain pruning ──────────────────
    scanned_keys = set()
    for site_url in sites_scanned:
        try:
            domain    = urlparse(site_url).netloc.replace("www.", "")
            key       = domain.split(".")[0].lower()
            scanned_keys.add(key)
            entry = weights["site_productivity"].setdefault(
                key, {"hits": 0, "runs": 0, "rate": 0.0,
                      "last_hit": None, "consecutive_zeros": 0}
            )
            entry["runs"] += 1
            if key in hit_domains:
                entry["hits"]              += 1
                entry["last_hit"]           = run_timestamp
                entry["consecutive_zeros"]  = 0
            else:
                entry["consecutive_zeros"]  = entry.get("consecutive_zeros", 0) + 1
            entry["rate"] = round(entry["hits"] / max(entry["runs"], 1), 3)

            # Self-prune domains that have never produced results
            dead = weights.setdefault("low_productivity_domains", [])
            if (entry["consecutive_zeros"] >= DEAD_DOMAIN_THRESHOLD
                    and key not in TRUSTED_CA_DOMAINS
                    and key not in hit_domains
                    and key not in dead):
                dead.append(key)
                log.info(f"[Train] Auto-pruning low-productivity domain: {key}")
        except Exception:
            pass

    # ── Per-source stats ───────────────────────────────────────────────────
    for src, cnt in source_counts.items():
        entry = weights["source_stats"].setdefault(
            src, {"total_jobs": 0, "total_runs": 0, "avg_per_run": 0.0}
        )
        entry["total_jobs"] += cnt
        entry["total_runs"] += 1
        entry["avg_per_run"] = round(
            entry["total_jobs"] / max(entry["total_runs"], 1), 2
        )

    # ── Adaptive threshold (clamped 3–10) ────────────────────────────────
    n         = len(jobs_found)
    threshold = weights["score_threshold"]
    if n == 0 and threshold > 3:
        threshold -= 1
        log.info(f"[Train] No results → lowering threshold to {threshold}")
    elif n > 30 and threshold < 10:
        threshold += 1
        log.info(f"[Train] Many results ({n}) → raising threshold to {threshold}")
    weights["score_threshold"] = threshold

    # ── Keyword weight evolution ──────────────────────────────────────────
    kw = weights["keyword_weights"]
    for job in jobs_found:
        title = str(job.get("TITLE", "")).lower()
        for word in kw:
            if word in title:
                kw[word] = min(kw[word] + 1, 20)

    summary = {
        "timestamp":        run_timestamp,
        "jobs_found":       n,
        "sites_hit":        list(hit_domains),
        "threshold":        weights["score_threshold"],
        "source_breakdown": source_counts,
    }
    weights["run_history"]      = (weights["run_history"] + [summary])[-50:]
    weights["training_runs"]   += 1
    weights["total_jobs_found"] = weights.get("total_jobs_found", 0) + n
    weights["last_trained"]     = run_timestamp

    log.info(
        f"[Train] Run #{weights['training_runs']} | Jobs: {n} | "
        f"Threshold: {weights['score_threshold']} | All-time: {weights['total_jobs_found']} | "
        f"Sources: {source_counts}"
    )
    return weights


# ─────────────────────────────────────────────────────────────────────────────
# 4. JOB SCORER  (v6 — with additional false-positive blocks)
# ─────────────────────────────────────────────────────────────────────────────

class JobScorer:
    def __init__(self, weights: Dict[str, Any]):
        self.threshold       = weights.get("score_threshold", 5)
        self.kw              = weights.get("keyword_weights", DEFAULT_WEIGHTS["keyword_weights"])
        self.dead_domains    = set(weights.get("low_productivity_domains", []))

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

        # ── A. Fast-fail checks ────────────────────────────────────────────

        if len(title) < MIN_TITLE_LENGTH:
            return False, "Too Short", -100

        domain_key = urlparse(url).netloc.replace("www.", "").split(".")[0].lower()

        if domain_key in BLOCKED_DOMAINS:
            return False, "Blocked Domain", -100

        # Self-trained dead-domain pruning
        if domain_key in self.dead_domains and domain_key not in TRUSTED_CA_DOMAINS:
            return False, "Low Productivity Domain", -100

        if RE_NAV_LINK_EXACT.match(title.strip()):
            return False, "Nav Link", -100
        if RE_BAD_URLS.search(url_l):
            return False, "Bad URL", -100

        if RE_BLOCKED_TITLE.search(title):
            return False, "Blocked Title", -100
        if RE_YEAR_TOO_SENIOR.search(title):
            return False, "Year Too Senior", -100

        # BUG FIX: Block freelance / expert-witness / hourly-rate listings
        if RE_FREELANCE.search(title):
            return False, "Freelance/Contract", -100

        if RE_EXP_KILLER.search(full_text) or RE_SENIOR_ROLE.search(full_text):
            return False, "Too Senior", -100

        if RE_BAD_LOCATIONS.search(title):
            return False, "Wrong Location (title)", -100

        # ── B. Categorise ─────────────────────────────────────────────────
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

        # ── C. Location check ─────────────────────────────────────────────
        if domain_key not in TRUSTED_CA_DOMAINS:
            if RE_BAD_LOCATIONS.search(full_text):
                return False, "Wrong Location", -100
            if not RE_LOCATIONS.search(full_text):
                return False, "No ON/AB Location", -100

        # ── D. Content signals ────────────────────────────────────────────
        if desc and len(desc) > 100:
            if any(kw in full_text for kw in ["apply", "resume", "contact", "submit", "application"]):
                score += 2

        if RE_JUNIOR_SIGNALS.search(full_text):
            score += 4
            log.debug(f"[Scorer] Junior signal boost: '{title[:60]}'")

        return (score >= self.threshold), category, score


# ─────────────────────────────────────────────────────────────────────────────
# 5. HTTP HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def get_session(verify_ssl: bool = True) -> requests.Session:
    s     = requests.Session()
    retry = Retry(
        total=2,
        connect=1,
        read=1,
        backoff_factor=1.0,
        status_forcelist=[429, 500, 502, 503, 504],
    )
    s.mount("https://", HTTPAdapter(max_retries=retry))
    s.mount("http://",  HTTPAdapter(max_retries=retry))
    s.headers.update(_HEADERS)
    if not verify_ssl:
        s.verify = False
    return s


def clean_html(html: str) -> str:
    soup = BeautifulSoup(html, "html.parser")
    for tag in soup(["script", "style", "nav", "footer", "header", "noscript", "aside"]):
        tag.extract()
    return " ".join(soup.get_text(separator=" ").split())


def safe_get(
    session:    requests.Session,
    url:        str,
    timeout:    int  = 12,
    verify_ssl: bool = True,
) -> Optional[requests.Response]:
    try:
        resp = session.get(url, timeout=timeout, verify=verify_ssl)
        if resp.status_code == 200:
            return resp
    except requests.exceptions.SSLError:
        hostname = urlparse(url).netloc.replace("www.", "")
        if hostname in SSL_SOFT_DOMAINS or any(d in hostname for d in SSL_SOFT_DOMAINS):
            log.debug(f"SSL cert mismatch for {hostname}, retrying with verify=False")
            try:
                import urllib3
                urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
                resp = session.get(url, timeout=timeout, verify=False)
                if resp.status_code == 200:
                    return resp
            except Exception:
                pass
    except Exception:
        pass
    return None


# ─────────────────────────────────────────────────────────────────────────────
# 6. GENERIC HTML SCRAPER
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

            if len(text) < 5:
                continue
            if RE_NAV_LINK_EXACT.match(text.strip()):
                continue
            if RE_BAD_URLS.search(href):
                continue
            if not (RE_ASSOCIATE.search(text) or RE_STUDENT.search(text)):
                continue
            if RE_BLOCKED_TITLE.search(text):
                continue
            if RE_FREELANCE.search(text):
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
# These are the most reliably confirmed Workday endpoints for Canada.
# Tenant slugs must match exactly. If a tenant returns 0 the self-training
# system will eventually auto-prune it.

WORKDAY_TENANTS = [
    # (tenant_slug, board_slug, company_display_name)
    ("fasken",           "Fasken_Careers",           "Fasken"),
    ("millerthomson",    "MillerThomson",             "Miller Thomson"),
    ("osler",            "Osler_External",            "Osler"),
    ("mccarthy",         "McCarthyTetrault",          "McCarthy Tétrault"),
    ("legalaidontario",  "LegalAidOntario",           "Legal Aid Ontario"),
    # Ontario Public Service — confirmed slug
    ("ontario",          "OPS_External_site",         "Ontario Public Service"),
    # In-house legal
    ("rbc",              "RBCCareers",                "RBC"),
    ("td",               "TDBank",                   "TD Bank"),
    ("manulife",         "MFCGJ_Careers",            "Manulife"),
    ("intact",           "Intact",                   "Intact Insurance"),
    ("enbridge",         "Enbridge",                 "Enbridge"),
    ("cppib",            "CPPInvestments",            "CPP Investments"),
    ("sunlife",          "SunLifeCareers",            "Sun Life"),
    ("scotiabank",       "Scotiabank_Careers",        "Scotiabank"),
    ("cibc",             "CIBC_External",             "CIBC"),
]


def scrape_workday_tenant(tenant: str, board: str, company: str, scorer: JobScorer) -> List[dict]:
    session = get_session()
    results = []
    for wd_host in [f"{tenant}.wd3.myworkdayjobs.com", f"{tenant}.wd1.myworkdayjobs.com"]:
        url     = f"https://{wd_host}/wday/cxs/{tenant}/{board}/jobs"
        payload = {"limit": 20, "offset": 0, "searchText": "associate lawyer counsel articling"}
        try:
            resp = session.post(url, json=payload, timeout=15,
                                headers={**_HEADERS, "Content-Type": "application/json"})
            if resp.status_code != 200:
                continue
            data = resp.json()
            jobs = data.get("jobPostings", [])
            if not jobs:
                continue

            for job in jobs:
                title    = job.get("title", "")
                location = job.get("locationsText", "") or job.get("location", "")
                ext_path = job.get("externalPath", "")
                job_url  = f"https://{wd_host}/en-US/{board}/job/{ext_path}"
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
            break
        except Exception as e:
            log.debug(f"Workday {tenant} ({wd_host}): {e}")

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

GREENHOUSE_BOARDS = [
    ("omers",               "OMERS Legal"),
    ("brookfieldrenewable", "Brookfield Renewable"),
    ("telus",               "Telus Legal"),
    ("wealthsimple",        "Wealthsimple Legal"),
    ("shopify",             "Shopify Legal"),
    ("tilray",              "Tilray Legal"),
    ("elementfleet",        "Element Fleet Legal"),
    ("caseware",            "CaseWare Legal"),
    ("faire",               "Faire Legal"),
    ("agf",                 "AGF Legal"),
    ("stelco",              "Stelco Legal"),
    ("lightspeed",          "Lightspeed Legal"),
    ("hootsuite",           "Hootsuite Legal"),
    ("clio",                "Clio Legal"),
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

LEVER_BOARDS = [
    ("goodmans",           "Goodmans LLP"),
    ("cavalluzzo",         "Cavalluzzo LLP"),
    ("siskinds",           "Siskinds LLP"),
    ("weirfoulds",         "Weirfoulds LLP"),
    ("shopify",            "Shopify Legal"),
    ("lightspeed",         "Lightspeed Legal"),
    ("clio",               "Clio Legal"),
    ("d2l",                "D2L Legal"),
    ("hootsuite",          "Hootsuite Legal"),
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


# ── 7d. iCIMS ────────────────────────────────────────────────────────────────

ICIMS_CLIENTS = [
    ("blakes",       "Blake Cassels & Graydon"),
    ("mccarthy",     "McCarthy Tétrault"),
    ("nortonrose",   "Norton Rose Fulbright"),
    ("torys",        "Torys LLP"),
    ("stikeman",     "Stikeman Elliott"),
    ("gowling",      "Gowling WLG"),
    ("bennettjones", "Bennett Jones"),
    ("airdberlis",   "Aird & Berlis"),
    ("cassels",      "Cassels Brock"),
]

def scrape_icims_client(subdomain: str, company: str, scorer: JobScorer) -> List[dict]:
    session = get_session()
    results = []
    for keyword in ["associate lawyer", "articling student", "legal counsel"]:
        url = (
            f"https://careers-{subdomain}.icims.com/jobs/search"
            f"?pr=1&ss=1&searchKeyword={keyword.replace(' ', '+')}"
            f"&searchCategory=Legal&in_iframe=1"
        )
        resp = safe_get(session, url, timeout=15)
        if not resp:
            continue
        soup = BeautifulSoup(resp.text, "html.parser")
        for a in soup.find_all("a", href=True, class_=re.compile(r"iCIMS_Anchor|title")):
            text = a.get_text(" ", strip=True)
            href = a["href"]
            if not href.startswith("http"):
                href = f"https://careers-{subdomain}.icims.com{href}"
            if not _is_law_job(text):
                continue
            is_fit, cat, score = scorer.score_job(text, "", href, company)
            if is_fit:
                results.append({
                    "TITLE":    text,
                    "COMPANY":  company,
                    "URL":      href,
                    "CATEGORY": cat,
                    "SCORE":    score,
                    "SOURCE":   "icims",
                    "DATE":     "",
                })
    return results

def scrape_all_icims(scorer: JobScorer) -> List[dict]:
    log.info(f"  [iCIMS] Scanning {len(ICIMS_CLIENTS)} clients...")
    all_jobs: List[dict] = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as ex:
        futures = {
            ex.submit(scrape_icims_client, s, c, scorer): c
            for s, c in ICIMS_CLIENTS
        }
        for f in concurrent.futures.as_completed(futures):
            try:
                all_jobs.extend(f.result())
            except Exception:
                pass
    return all_jobs


# ── 7e. Government Portals ────────────────────────────────────────────────────

def scrape_gc_jobs(scorer: JobScorer) -> List[dict]:
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
# 8. TARGET URLS — DIRECT HTML SCRAPERS
#    v6 BUG FIX: removed domains confirmed dead in 2026-03-11 live logs:
#    www.davies.ca (SSL mismatch) → davies.ca (bare domain)
#    www.gutierrezlaw.ca (DNS dead) → REMOVED
#    sookermacleod.com (DNS dead) → REMOVED
#    www.mustardlaw.ca (DNS dead) → REMOVED
#    www.burntsand.com (ConnectTimeout) → REMOVED
#    careers.oba.org (ReadTimeout) → www.oba.org/Careers
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
        # BUG FIX: www.davies.ca → SSL cert is NOT valid for www subdomain.
        # Using bare domain davies.ca (added to SSL_SOFT_DOMAINS)
        "https://davies.ca/careers/",

        # ── Ontario Regional Firms ────────────────────────────────────────────
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
        "https://www.kmlaw.ca/careers/",
        "https://www.sotos.ca/careers/",
        "https://www.stockwoods.ca/careers/",
        "https://www.brauti.com/careers/",
        "https://www.fasken.com/en/careers/students",
        "https://www.chaitons.com/careers/",
        "https://www.lerners.ca/careers/",
        "https://hicksadams.com/careers/",
        # Removed: www.gutierrezlaw.ca (DNS dead confirmed 2026-03-11)
        # Removed: sookermacleod.com (DNS dead confirmed 2026-03-11)

        # ── Alberta Regional Firms ────────────────────────────────────────────
        "https://www.fieldlaw.com/careers/associate-opportunities/",
        "https://www.bdplaw.com/careers/",
        "https://www.parlee.com/careers/",
        "https://www.mross.com/about-us/careers/",
        "https://www.brownleelaw.com/careers/",
        "https://www.emeryjamieson.com/careers/",
        "https://www.jmccormick.ca/careers/",
        # Removed: www.mustardlaw.ca (DNS dead confirmed 2026-03-11)
        # Removed: www.burntsand.com (ConnectTimeout confirmed 2026-03-11)

        # ── International Firms (Canadian offices only) ───────────────────────
        "https://www.dentons.com/en/careers/find-a-job",
        "https://www.nortonrosefulbright.com/en-ca/careers/job-listings",
        "https://www.dlapiper.com/en/canada/careers/",
        "https://careers.bakermckenzie.com/global/en/c/canada-jobs",
        "https://careers.herbertsmithfreehills.com/go/Canada/",
        "https://careers.cliffordchance.com/content/careers/en/office/toronto.html",
        "https://www.linklaters.com/en/careers/offices/toronto",

        # ── Legal Recruitment / Job Boards ────────────────────────────────────
        "https://www.zsa.ca/job-board/",
        "https://thecounselnetwork.com/job-search/",
        "https://legaljobs.ca/jobs/",

        # ── Bar / Law Society Boards ──────────────────────────────────────────
        # BUG FIX: careers.oba.org → ReadTimeout. Fixed → www.oba.org/Careers
        "https://www.oba.org/Careers",
        "https://lawsociety.ab.ca/about/programs-and-initiatives/careers/",
        "https://www.lso.ca/careers",
        "https://www.cba.org/For-Lawyers/Employment",

        # ── In-House / Corporate Legal ────────────────────────────────────────
        "https://jobs.rbc.com/ca/en/search-results?keywords=legal+counsel",
        "https://jobs.td.com/en-CA/jobs/?keyword=legal+counsel",
        "https://www.sunlife.com/en/careers/job-search/?keyword=legal+counsel",
        "https://careers.manulife.com/global/en/search-results?keywords=counsel",
        "https://www.enbridge.com/careers/job-search?q=legal+counsel",
        "https://suncor.com/en/careers/current-openings?search=counsel",
        "https://careers.telus.com/search/#q=legal+counsel&t=Jobs&x5=Canada",
        "https://jobs.bmo.com/ca/en/search-results?keywords=legal+counsel",
        "https://jobs.scotiabank.com/search/?q=legal+counsel&l=Ontario",
        "https://www.cibc.com/en/about-cibc/careers/search-jobs.html#q=legal&l=Ontario",

        # ── Law School Career Boards ──────────────────────────────────────────
        "https://ultravires.ca/jobs/",
        "https://www.osgoode.yorku.ca/careers/",

        # ── Canadian Job Aggregators (location-pinned) ────────────────────────
        "https://ca.indeed.com/jobs?q=associate+lawyer&l=Ontario&fromage=7",
        "https://ca.indeed.com/jobs?q=associate+lawyer&l=Alberta&fromage=7",
        "https://ca.indeed.com/jobs?q=articling+student&l=Ontario&fromage=14",
        "https://ca.indeed.com/jobs?q=articling+student&l=Alberta&fromage=14",
        "https://www.glassdoor.ca/Job/ontario-associate-lawyer-jobs-SRCH_IL.0,7_IS3559_KO8,24.htm",
        "https://ca.talent.com/jobs?k=associate+lawyer&l=ontario",
    ]


# ─────────────────────────────────────────────────────────────────────────────
# 9. AGGREGATOR SCRAPER (JobSpy)
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
        "newly called lawyer",
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
                        f'"{search_term}" -"senior" -"general counsel" '
                        f'-"cpa articling" -warehouse -driver site:ca'
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

    target_urls = get_target_urls()
    log.info(f"\n── Direct HTML scrape ({len(target_urls)} sites) ─────────────")
    frames.append(run_direct_scrape(target_urls, scorer))

    log.info("\n── Workday ATS ──────────────────────────────────────────")
    wd_jobs = scrape_all_workday(scorer)
    if wd_jobs:
        frames.append(pd.DataFrame(wd_jobs))
    log.info(f"  → {len(wd_jobs)} raw hits from Workday")

    log.info("\n── Greenhouse ATS ───────────────────────────────────────")
    gh_jobs = scrape_all_greenhouse(scorer)
    if gh_jobs:
        frames.append(pd.DataFrame(gh_jobs))
    log.info(f"  → {len(gh_jobs)} raw hits from Greenhouse")

    log.info("\n── Lever ATS ────────────────────────────────────────────")
    lv_jobs = scrape_all_lever(scorer)
    if lv_jobs:
        frames.append(pd.DataFrame(lv_jobs))
    log.info(f"  → {len(lv_jobs)} raw hits from Lever")

    log.info("\n── iCIMS ATS ────────────────────────────────────────────")
    ic_jobs = scrape_all_icims(scorer)
    if ic_jobs:
        frames.append(pd.DataFrame(ic_jobs))
    log.info(f"  → {len(ic_jobs)} raw hits from iCIMS")

    log.info("\n── Government portals ───────────────────────────────────")
    gov_jobs = scrape_gc_jobs(scorer) + scrape_ontario_public_service(scorer)
    if gov_jobs:
        frames.append(pd.DataFrame(gov_jobs))
    log.info(f"  → {len(gov_jobs)} raw hits from Government")

    log.info("\n── JobSpy aggregators ───────────────────────────────────")
    frames.append(scrape_jobspy_wrapper(scorer))

    non_empty = [f for f in frames if f is not None and not f.empty]
    if not non_empty:
        return pd.DataFrame()
    return pd.concat(non_empty, ignore_index=True, sort=False)


# ─────────────────────────────────────────────────────────────────────────────
# 11. DEDUPLICATION (fuzzy matching, strips stopwords)
# ─────────────────────────────────────────────────────────────────────────────

_STOPWORDS = re.compile(
    r"\b(the|a|an|at|in|for|of|and|or|–|-|&|llp|lp|inc|ltd|corp|"
    r"toronto|ontario|calgary|alberta|canada|remote|hybrid|office)\b",
    re.IGNORECASE,
)


def _make_sig(row: pd.Series) -> str:
    t = re.sub(r"\W+", " ", str(row.get("TITLE",   "")).lower())
    c = re.sub(r"\W+", " ", str(row.get("COMPANY", "")).lower())
    t = _STOPWORDS.sub("", t).strip()
    c = _STOPWORDS.sub("", c).strip()
    t = re.sub(r"\s+", "_", t)
    c = re.sub(r"\s+", "_", c)
    return f"{t}__{c}"


def deduplicate_jobs(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    df = df.copy()
    df["SIG"]       = df.apply(_make_sig, axis=1)
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

    source_breakdown: Dict[str, int] = {}
    for job in jobs:
        src = job.get("SOURCE", "unknown")
        source_breakdown[src] = source_breakdown.get(src, 0) + 1

    run_entry = {
        "run_id":           run_ts.replace(":", "").replace("-", "").replace(" ", "_"),
        "timestamp":        run_ts,
        "jobs_found":       len(jobs),
        "training_run":     weights.get("training_runs", 0),
        "threshold":        weights.get("score_threshold", 5),
        "source_breakdown": source_breakdown,
        "jobs":             jobs,
    }

    data["runs"]              = (data["runs"] + [run_entry])[-200:]
    data["last_updated"]      = run_ts
    data["total_all_time"]    = weights.get("total_jobs_found", 0)
    data["training_runs"]     = weights.get("training_runs", 0)
    data["score_threshold"]   = weights.get("score_threshold", 5)
    data["site_productivity"] = weights.get("site_productivity", {})
    data["source_stats"]      = weights.get("source_stats", {})

    try:
        with open(RESULTS_FILE, "w") as f:
            json.dump(data, f, indent=2, default=str)
        log.info(f"✓ Results saved to {RESULTS_FILE}")
    except Exception as e:
        log.warning(f"Could not save results: {e}")


# ─────────────────────────────────────────────────────────────────────────────
# 13. TELEGRAM ALERT  (v6: single consolidated message, correct dashboard URL)
# ─────────────────────────────────────────────────────────────────────────────

def send_telegram(df: pd.DataFrame, weights: Dict[str, Any]) -> None:
    """
    BUG FIX (v6): Build the complete message first, then send it in ONE
    Telegram API call. If the message exceeds 4 096 chars (Telegram's limit),
    trim the job list from the bottom and append a "… and N more" footer.
    This guarantees a single consolidated alert — not a flood of chunks.

    Also fixes the dashboard URL: always uses DASHBOARD_URL env var which
    defaults to https://law-associate-job-alerts.vercel.app/
    """
    bot_token = os.environ.get("TELEGRAM_BOT_TOKEN")
    chat_id   = os.environ.get("TELEGRAM_CHAT_ID")

    if not (bot_token and chat_id):
        print("\n[!] TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID missing. No alert sent.", flush=True)
        return

    associates = df[df["CATEGORY"] == "Associate"] if not df.empty else pd.DataFrame()
    students   = df[df["CATEGORY"] == "Student"]   if not df.empty else pd.DataFrame()
    now_str    = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    source_counts: Dict[str, int] = {}
    if not df.empty and "SOURCE" in df.columns:
        for src, grp in df.groupby("SOURCE"):
            source_counts[str(src)] = len(grp)
    src_str = " · ".join(f"{s}:{n}" for s, n in sorted(source_counts.items())) or "none"

    # ── Build job lines ────────────────────────────────────────────────────
    assoc_lines: List[str] = []
    if not associates.empty:
        assoc_lines.append("\n<b>🏛 ASSOCIATES / LAWYERS</b>")
        for _, row in associates.iterrows():
            score_str = f" [{row['SCORE']}]" if row.get("SCORE") else ""
            src_badge = f" <i>({row['SOURCE']})</i>" if row.get("SOURCE") else ""
            assoc_lines.append(
                f"• <a href=\"{row['URL']}\">{row['TITLE']}</a> "
                f"— {row['COMPANY']}{score_str}{src_badge}"
            )

    stud_lines: List[str] = []
    if not students.empty:
        stud_lines.append("\n<b>🎓 ARTICLING / STUDENTS</b>")
        for _, row in students.iterrows():
            src_badge = f" <i>({row['SOURCE']})</i>" if row.get("SOURCE") else ""
            stud_lines.append(
                f"• <a href=\"{row['URL']}\">{row['TITLE']}</a> "
                f"— {row['COMPANY']}{src_badge}"
            )

    # ── Header and footer ─────────────────────────────────────────────────
    header = "\n".join([
        f"<b>⚖️ Law Jobs — ON/AB | {now_str}</b>",
        f"<b>{len(df)} new listing(s) found</b>",
        (f"<i>Model run #{weights.get('training_runs','?')} · "
         f"Threshold: {weights.get('score_threshold',5)} · "
         f"All-time: {weights.get('total_jobs_found',0)} jobs</i>"),
        f"<i>Sources: {src_str}</i>",
    ])
    footer = f"\n\n📊 <a href=\"{DASHBOARD_URL}\">Open Dashboard</a>"

    if df.empty:
        header += "\n\n<i>No new positions this run. Model will lower threshold next cycle.</i>"

    # ── Fit everything into ≤ 4096 chars (single Telegram message) ────────
    LIMIT = 4090  # leave a small margin

    all_job_lines = assoc_lines + stud_lines
    full_msg      = header + "\n".join(all_job_lines) + footer

    if len(full_msg) > LIMIT:
        # Trim from the tail until it fits
        trimmed: List[str] = []
        dropped = 0
        for line in all_job_lines:
            candidate = header + "\n".join(trimmed + [line]) + footer
            if len(candidate) <= LIMIT:
                trimmed.append(line)
            else:
                dropped += 1
        if dropped:
            trimmed.append(f"\n<i>… and {dropped} more — see dashboard</i>")
        full_msg = header + "\n".join(trimmed) + footer

    api_url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    payload = {
        "chat_id":                  chat_id,
        "text":                     full_msg,
        "parse_mode":               "HTML",
        "disable_web_page_preview": True,
    }
    try:
        resp = requests.post(api_url, json=payload, timeout=15)
        if resp.ok:
            print("✓ Telegram alert sent (single message).", flush=True)
        else:
            print(f"✗ Telegram error: {resp.status_code} — {resp.text}", flush=True)
    except Exception as e:
        print(f"✗ Telegram request failed: {e}", flush=True)


# ─────────────────────────────────────────────────────────────────────────────
# 14. MAIN
# ─────────────────────────────────────────────────────────────────────────────

def main() -> None:
    run_ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    print("=" * 60, flush=True)
    print("Law Associate Job Scraper — v6 Live-Log Bug-Fix Edition", flush=True)
    print(f"Run: {run_ts}", flush=True)
    print("=" * 60, flush=True)

    weights = load_weights()
    scorer  = JobScorer(weights)

    print(f"\n[Model]  Score threshold : {scorer.threshold}", flush=True)
    print(f"[Model]  Training run #   : {weights['training_runs']}", flush=True)
    print(f"[Model]  All-time jobs    : {weights.get('total_jobs_found', 0)}", flush=True)
    dead = weights.get("low_productivity_domains", [])
    if dead:
        print(f"[Model]  Auto-pruned domains: {', '.join(dead)}", flush=True)
    print(flush=True)

    # 1. Run all scrapers
    combined = run_all_scrapers(scorer)

    # 2. Deduplicate
    unique_jobs = deduplicate_jobs(combined)
    log.info(f"After dedup: {len(unique_jobs)} unique candidates")

    # 3. History filter
    if os.path.exists(HISTORY_FILE):
        try:
            with open(HISTORY_FILE) as f:
                history_ids: set = set(json.load(f))
        except Exception:
            history_ids = set()
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
    print(f"\n{'='*60}", flush=True)
    print(f"✓ Final Verified New Jobs: {len(final_df)}", flush=True)
    if not final_df.empty:
        cols = [c for c in ["TITLE", "COMPANY", "CATEGORY", "SOURCE", "SCORE"] if c in final_df.columns]
        print(final_df[cols].to_string(index=False), flush=True)

    # 5. Self-training
    weights = train_model(weights, final_jobs, get_target_urls(), run_ts)
    save_weights(weights)

    # 6. Persist results for dashboard
    append_results(final_jobs, run_ts, weights)

    # 7. Save history
    try:
        with open(HISTORY_FILE, "w") as f:
            json.dump(list(history_ids), f)
    except Exception as e:
        log.warning(f"Could not save history: {e}")

    # 8. Telegram (single consolidated message)
    send_telegram(final_df, weights)


if __name__ == "__main__":
    main()
