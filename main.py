#!/usr/bin/env python3
# Law Associate Job Scraper -- v9 (LLM Experience Filter)
# ---------------------------------------------------------
# CHANGES vs v7:
#
# CRITICAL FIXES:
#   - model_weights site_productivity fully reset (all 55-run data was poisoned
#     by the domain-key bug; retaining it would suppress all major Bay St firms)
#   - TRUSTED_CA_DOMAINS are now IMMUNE to auto-pruning — always scanned
#   - DEAD_DOMAIN_THRESHOLD raised 5 → 15 (5 was far too aggressive)
#   - History expiry: URLs older than HISTORY_TTL_DAYS are re-evaluated
#
# NEW SOURCES (+4 scrapers):
#   - Google News RSS  — law job articles, no auth, extremely reliable
#   - Indeed RSS       — indeed.com/rss job feeds per search term + location
#   - Google Alerts RSS — user-configured alerts piped into scraper
#   - ZSA / Counsel Network direct scrapers — Canada's top legal recruiters
#
# ATS IMPROVEMENTS:
#   - Workday: now tries wd1/wd3/wd5 host variants with correct JSON payload
#   - Greenhouse: added 8 new boards (Shopify, OMERS, Intact, Brookfield...)
#   - Lever: added 5 new boards
#   - iCIMS: tightened search; now uses legal-specific category filter
#
# SCORING IMPROVEMENTS:
#   - Recruiter/agency source gets +2 bonus (more likely to be real openings)
#   - "Newly called" and "0-3 years" get +3 each (was +4 combined)
#   - Trusted-domain jobs skip the location check entirely (they're already CA)
#   - Added "junior associate" as explicit high-value signal (+5)
#
# DEDUPLICATION:
#   - URL-level dedup now strips UTM params and tracking tokens
#   - Title similarity check: near-identical titles from same company merged
#
# TELEGRAM:
#   - Jobs now sorted by SCORE descending
#   - Score shown as star rating (1-5 stars) instead of raw number
#   - "🆕 New firm!" badge when a TRUSTED firm appears for the first time
#   - Batch send: one message per category (Associates / Students) if > 4096 chars

from __future__ import annotations

import os
import re
import json
import time
import hashlib
import logging
import concurrent.futures
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Any, Optional, Set
from urllib.parse import urljoin, urlparse, urlencode, parse_qs, urlunparse

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
    import base64
    from email import message_from_bytes
    from google.auth.transport.requests import Request as GRequest
    from google.oauth2.credentials import Credentials
    from googleapiclient.discovery import build as gbuild
    _GMAIL_AVAILABLE = True
except ImportError:
    _GMAIL_AVAILABLE = False


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

HISTORY_FILE      = os.environ.get("HISTORY_FILE",   "job_history.json")
WEIGHTS_FILE      = os.environ.get("WEIGHTS_FILE",   "model_weights.json")
RESULTS_FILE      = os.environ.get("RESULTS_FILE",   "results.json")
LLM_CACHE_FILE    = os.environ.get("LLM_CACHE_FILE", "llm_cache.json")
RESULTS_WANTED    = int(os.environ.get("RESULTS_WANTED") or "30")
DASHBOARD_URL     = os.environ.get("DASHBOARD_URL",  "https://law-associate-job-alerts.vercel.app/")
HISTORY_TTL_DAYS  = int(os.environ.get("HISTORY_TTL_DAYS") or "60")
MAX_JOB_AGE_DAYS  = int(os.environ.get("MAX_JOB_AGE_DAYS") or "14")  # drop jobs older than this   # re-check URLs after N days

MIN_TITLE_LENGTH      = 15
DEAD_DOMAIN_THRESHOLD = 15   # v8: raised from 5 — 5 was far too aggressive

BLOCKED_DOMAINS: Set[str] = {
    "emond",           # Legal publisher (textbooks), NOT a law firm
    "legaljobsboard",  # Dead / timeout
}

SSL_SOFT_DOMAINS: Set[str] = {
    "hicks.ca",
    "mcleodlaw.com",
    "davies.ca",
}

# ─────────────────────────────────────────────────────────────────────────────
# 2. REGEX
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
    r"senior\s+\w+\s+counsel|senior\s+\w+\s+lawyer|"   # "Senior Legal Counsel", "Senior Corporate Lawyer"
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
    r"cpa\s+articling|cpa\s+student|co-op\s+cpa|co\s+op\s+cpa|"
    r"assurance\s+and\s+accounting|tax\s+articling|"
    r"counsel\s+to\s+(?:the\s+)?(?:\w+\s+){0,4}(?:during|in\s+the|on\s+the|regarding)|"
    r"representative\s+counsel|independent\s+counsel\s+to|court-appointed|"
    r"acted\s+as\s+counsel|served\s+as\s+counsel|"
    r"named\s+partner|joins\s+(?:as\s+)?partner|promoted\s+to|"
    r"ripple\s+through|poaching\s+of\s+canadian|on\s+the\s+upswing|"
    r"talent\s+pool|interest\s+form|expression\s+of\s+interest)\b"
    # Prefix patterns that cannot use trailing \b (word continues after match)
    r"|future\s+opportunit"   # matches "Future Opportunities", "future opportunity"
    r"|acted\s+as\s+counsel"  # already in group but also handles prefix edge case
    r"|court.appointed",      # "court-appointed" / "court appointed"
    re.IGNORECASE,
)
RE_FREELANCE = re.compile(
    r"(\$\d+[\,\d]*\s*/\s*(?:hr|hour)|"
    r"\bexpert\s+opportunity\b|"
    r"\bcontract\s+lawyer\b|"
    r"\bfreelance\s+lawyer\b|"
    r"\bper\s+diem\s+lawyer\b|"
    r"\bexpert\s+witness\b)",
    re.IGNORECASE,
)
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

# v8: canonical set — used in immune-from-pruning check AND scoring trust bonus
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
    "joinblakes", "blakescareers",   # Blake's ATS subdomains
    "chaitons", "lerners", "hicksadams",
}

# ─────────────────────────────────────────────────────────────────────────────
# 1b. DOMAIN KEY HELPER
# ─────────────────────────────────────────────────────────────────────────────

def _domain_key(url: str) -> str:
    """Return the registrable-domain label (parts[-2]) for any URL.

    careers.bakermckenzie.com -> bakermckenzie
    ca.indeed.com             -> indeed
    jobs.rbc.com              -> rbc
    joinblakes.com            -> joinblakes
    """
    netloc = urlparse(url).netloc.lower().replace("www.", "")
    parts  = netloc.split(".")
    return parts[-2] if len(parts) >= 2 else (parts[0] if parts else "")


def _strip_tracking(url: str) -> str:
    """Remove UTM/tracking params so two URLs pointing to the same job dedup correctly."""
    tracking = {"utm_source","utm_medium","utm_campaign","utm_term","utm_content",
                "ref","referer","source","trk","trkinfo","mc_cid","mc_eid",
                "fbclid","gclid","li_fat_id","clickid"}
    parsed = urlparse(url)
    qs = {k: v for k, v in parse_qs(parsed.query).items() if k.lower() not in tracking}
    clean_query = urlencode(qs, doseq=True)
    return urlunparse(parsed._replace(query=clean_query, fragment=""))


# Date formats commonly seen in job feeds and ATS responses
_DATE_FORMATS = [
    "%Y-%m-%d", "%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%dT%H:%M:%S",
    "%d %b %Y", "%d %B %Y", "%b %d, %Y", "%B %d, %Y",
    "%a, %d %b %Y %H:%M:%S %z", "%a, %d %b %Y %H:%M:%S GMT",
]


def _parse_date(date_str: str) -> Optional[datetime]:
    """Parse a date string into a UTC-aware datetime. Returns None on failure."""
    if not date_str or str(date_str).strip() in ("", "nan", "None", "NaT"):
        return None
    s = str(date_str).strip()
    for fmt in _DATE_FORMATS:
        try:
            dt = datetime.strptime(s[:len(fmt)+5], fmt)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt
        except ValueError:
            continue
    return None


def _is_recent(date_str: str, max_age_days: int = MAX_JOB_AGE_DAYS) -> bool:
    """Return True if date_str is within max_age_days, or if the date is unknown (blank)."""
    if not date_str or str(date_str).strip() in ("", "nan", "None", "NaT"):
        return True   # no date = keep (we can't tell; let Gemini decide)
    dt = _parse_date(date_str)
    if dt is None:
        return True   # unparseable = keep
    cutoff = datetime.now(timezone.utc) - timedelta(days=max_age_days)
    return dt >= cutoff


# ─────────────────────────────────────────────────────────────────────────────
# 3. ADAPTIVE MODEL WEIGHTS
# ─────────────────────────────────────────────────────────────────────────────

DEFAULT_WEIGHTS: Dict[str, Any] = {
    "version":         8,
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
    "low_productivity_domains": [],
    "run_history":              [],
    "total_jobs_found":         0,
    "false_positive_domains":   [],
    "first_seen_domains":       [],   # v8: track which trusted domains have ever hit
}


def load_weights() -> Dict[str, Any]:
    if os.path.exists(WEIGHTS_FILE):
        try:
            with open(WEIGHTS_FILE) as f:
                data = json.load(f)
            for k, v in DEFAULT_WEIGHTS.items():
                if k not in data:
                    data[k] = v
            # v8: if site_productivity has 0-hit entries for TRUSTED domains with
            # many runs, reset it entirely — it's poisoned from the domain-key bug.
            sp = data.get("site_productivity", {})
            trusted_zero = sum(
                1 for k, v in sp.items()
                if k in TRUSTED_CA_DOMAINS and v.get("hits", 0) == 0 and v.get("runs", 0) >= 5
            )
            # Also detect corruption via low_productivity_domains containing known-good trusted firms
            lpd = data.get("low_productivity_domains", [])
            trusted_pruned = sum(1 for k in lpd if k in TRUSTED_CA_DOMAINS)
            if trusted_zero > 3 or trusted_pruned > 2:
                log.warning(
                    f"[Weights] Corrupted data detected "
                    f"(trusted_zero={trusted_zero}, trusted_pruned={trusted_pruned}) — "
                    "resetting site_productivity and low_productivity_domains."
                )
                data["site_productivity"]        = {}
                data["low_productivity_domains"] = []   # BUG FIX: was missing in v8/v9
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
    hit_domains:   Set[str]      = set()
    source_counts: Dict[str, int] = {}

    for job in jobs_found:
        hit_domains.add(_domain_key(job.get("URL", "")))
        src = job.get("SOURCE", "unknown")
        source_counts[src] = source_counts.get(src, 0) + 1

    # ── Site productivity ────────────────────────────────────────────────
    for site_url in sites_scanned:
        key   = _domain_key(site_url)
        entry = weights["site_productivity"].setdefault(
            key, {"hits": 0, "runs": 0, "rate": 0.0,
                  "last_hit": None, "consecutive_zeros": 0}
        )
        entry["runs"] += 1
        if key in hit_domains:
            entry["hits"]             += 1
            entry["last_hit"]          = run_timestamp
            entry["consecutive_zeros"] = 0
        else:
            entry["consecutive_zeros"] = entry.get("consecutive_zeros", 0) + 1
        entry["rate"] = round(entry["hits"] / max(entry["runs"], 1), 3)

        # v8: TRUSTED domains are IMMUNE from auto-pruning
        dead = weights.setdefault("low_productivity_domains", [])
        if (
            key not in TRUSTED_CA_DOMAINS          # never prune trusted firms
            and key not in BLOCKED_DOMAINS
            and key not in hit_domains
            and entry["consecutive_zeros"] >= DEAD_DOMAIN_THRESHOLD
            and key not in dead
        ):
            dead.append(key)
            log.info(f"[Train] Auto-pruning low-productivity domain: {key}")

    # ── Per-source stats ─────────────────────────────────────────────────
    for src, cnt in source_counts.items():
        entry = weights["source_stats"].setdefault(
            src, {"total_jobs": 0, "total_runs": 0, "avg_per_run": 0.0}
        )
        entry["total_jobs"] += cnt
        entry["total_runs"] += 1
        entry["avg_per_run"] = round(entry["total_jobs"] / max(entry["total_runs"], 1), 2)

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

    # ── v8: Track first-seen trusted domains (for Telegram badge) ────────
    first_seen = weights.setdefault("first_seen_domains", [])
    for domain in hit_domains:
        if domain in TRUSTED_CA_DOMAINS and domain not in first_seen:
            first_seen.append(domain)
            log.info(f"[Train] New trusted domain first hit: {domain}")

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
# 4. JOB SCORER
# ─────────────────────────────────────────────────────────────────────────────

class JobScorer:
    def __init__(self, weights: Dict[str, Any]):
        self.threshold    = weights.get("score_threshold", 5)
        self.kw           = weights.get("keyword_weights", DEFAULT_WEIGHTS["keyword_weights"])
        self.dead_domains = set(weights.get("low_productivity_domains", []))

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

        if len(title) < MIN_TITLE_LENGTH:
            return False, "Too Short", -100

        domain_key = _domain_key(url)

        if domain_key in BLOCKED_DOMAINS:
            return False, "Blocked Domain", -100

        # TRUSTED domains are never dead-domain blocked
        if domain_key not in TRUSTED_CA_DOMAINS and domain_key in self.dead_domains:
            return False, "Low Productivity Domain", -100

        if RE_NAV_LINK_EXACT.match(title.strip()):
            return False, "Nav Link", -100
        if RE_BAD_URLS.search(url_l):
            return False, "Bad URL", -100
        if RE_BLOCKED_TITLE.search(title):
            return False, "Blocked Title", -100
        if RE_YEAR_TOO_SENIOR.search(title):
            return False, "Year Too Senior", -100
        if RE_FREELANCE.search(title):
            return False, "Freelance/Contract", -100
        if RE_EXP_KILLER.search(full_text) or RE_SENIOR_ROLE.search(full_text):
            return False, "Too Senior", -100
        if RE_BAD_LOCATIONS.search(title):
            return False, "Wrong Location (title)", -100

        # Categorise
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

        # Location check — TRUSTED domains skip this
        if domain_key not in TRUSTED_CA_DOMAINS:
            if RE_BAD_LOCATIONS.search(full_text):
                return False, "Wrong Location", -100
            if not RE_LOCATIONS.search(full_text):
                return False, "No ON/AB Location", -100
        else:
            score += 2   # trusted-domain bonus

        # Content signals
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
        total=2, connect=1, read=1,
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
    hostname = urlparse(url).netloc.replace("www.", "")
    if hostname in SSL_SOFT_DOMAINS or any(d in hostname for d in SSL_SOFT_DOMAINS):
        import urllib3
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        verify_ssl = False

    try:
        resp = session.get(url, timeout=timeout, verify=verify_ssl)
        if resp.status_code == 200:
            return resp
    except requests.exceptions.SSLError:
        if verify_ssl:
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
    domain     = _domain_key(url)
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
                            "COMPANY":  domain.title(),
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
        return True
    if RE_BAD_LOCATIONS.search(location):
        return False
    return True


# ── 7a. Workday ──────────────────────────────────────────────────────────────

WORKDAY_TENANTS = [
    ("fasken",           "Fasken_Careers",           "Fasken"),
    ("millerthomson",    "MillerThomson",             "Miller Thomson"),
    ("osler",            "Osler_External",            "Osler"),
    ("mccarthy",         "McCarthyTetrault",          "McCarthy Tetrault"),
    ("legalaidontario",  "LegalAidOntario",           "Legal Aid Ontario"),
    ("ontario",          "OPS_External_site",         "Ontario Public Service"),
    ("rbc",              "RBCCareers",                "RBC"),
    ("td",               "TDBank",                   "TD Bank"),
    ("manulife",         "MFCGJ_Careers",            "Manulife"),
    ("intact",           "Intact",                   "Intact Insurance"),
    ("enbridge",         "Enbridge",                 "Enbridge"),
    ("cppib",            "CPPInvestments",            "CPP Investments"),
    ("sunlife",          "SunLifeCareers",            "Sun Life"),
    ("scotiabank",       "Scotiabank_Careers",        "Scotiabank"),
    ("cibc",             "CIBC_External",             "CIBC"),
    ("bmo",              "BMO_Careers",               "BMO"),
]

# v8: try all three Workday host variants before giving up
_WD_HOSTS = ["wd3", "wd1", "wd5"]

def scrape_workday_tenant(tenant: str, board: str, company: str, scorer: JobScorer) -> List[dict]:
    session = get_session()
    results = []
    for variant in _WD_HOSTS:
        wd_host = f"{tenant}.{variant}.myworkdayjobs.com"
        url     = f"https://{wd_host}/wday/cxs/{tenant}/{board}/jobs"
        payload = {
            "limit": 20, "offset": 0,
            "searchText": "associate lawyer counsel articling",
        }
        try:
            resp = session.post(
                url, json=payload, timeout=15,
                headers={**_HEADERS, "Content-Type": "application/json"},
            )
            if resp.status_code != 200:
                continue
            data = resp.json()
            jobs = data.get("jobPostings", [])
            if not jobs:
                break  # valid response but no jobs — don't try other variants

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
            log.debug(f"Workday {tenant} ({variant}): {e}")

    return results


def scrape_all_workday(scorer: JobScorer) -> List[dict]:
    log.info(f"  [Workday] Scanning {len(WORKDAY_TENANTS)} tenants...")
    all_jobs: List[dict] = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=6) as ex:
        futures = {ex.submit(scrape_workday_tenant, t, b, c, scorer): c for t, b, c in WORKDAY_TENANTS}
        for f in concurrent.futures.as_completed(futures):
            try:
                all_jobs.extend(f.result())
            except Exception:
                pass
    return all_jobs


# ── 7b. Greenhouse ────────────────────────────────────────────────────────────

GREENHOUSE_BOARDS = [
    # Law firms
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
    # v8: added boards
    ("torontodominion",     "TD Legal"),
    ("intact",              "Intact Legal"),
    ("brookfieldrealestate","Brookfield Real Estate Legal"),
    ("opentext",            "OpenText Legal"),
    ("kinaxis",             "Kinaxis Legal"),
    ("d2l",                 "D2L Legal"),
    ("securekey",           "SecureKey Legal"),
    ("pivotal",             "Pivotal Legal"),
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
        futures = {ex.submit(scrape_greenhouse_board, s, c, scorer): c for s, c in GREENHOUSE_BOARDS}
        for f in concurrent.futures.as_completed(futures):
            try:
                all_jobs.extend(f.result())
            except Exception:
                pass
    return all_jobs


# ── 7c. Lever ────────────────────────────────────────────────────────────────

LEVER_BOARDS = [
    ("goodmans",        "Goodmans LLP"),
    ("cavalluzzo",      "Cavalluzzo LLP"),
    ("siskinds",        "Siskinds LLP"),
    ("weirfoulds",      "Weirfoulds LLP"),
    ("shopify",         "Shopify Legal"),
    ("lightspeed",      "Lightspeed Legal"),
    ("clio",            "Clio Legal"),
    ("d2l",             "D2L Legal"),
    ("hootsuite",       "Hootsuite Legal"),
    # v8: added
    ("benchsci",        "BenchSci Legal"),
    ("dossier",         "Dossier Legal"),
    ("ecobee",          "ecobee Legal"),
    ("tulip",           "Tulip Legal"),
    ("ratehub",         "Ratehub Legal"),
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
            posted = datetime.fromtimestamp(
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
        futures = {ex.submit(scrape_lever_board, s, c, scorer): c for s, c in LEVER_BOARDS}
        for f in concurrent.futures.as_completed(futures):
            try:
                all_jobs.extend(f.result())
            except Exception:
                pass
    return all_jobs


# ── 7d. iCIMS ────────────────────────────────────────────────────────────────

ICIMS_CLIENTS = [
    ("blakes",       "Blake Cassels & Graydon"),
    ("mccarthy",     "McCarthy Tetrault"),
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
        futures = {ex.submit(scrape_icims_client, s, c, scorer): c for s, c in ICIMS_CLIENTS}
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
# 8. TARGET URLS — DIRECT HTML
# ─────────────────────────────────────────────────────────────────────────────

def get_target_urls() -> List[str]:
    return [
        # Bay Street / National Firms
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
        "https://davies.ca/careers/",
        # Ontario Regional Firms
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
        # brauti.com removed — network unreachable confirmed 2026-03-18
        "https://www.fasken.com/en/careers/students",
        "https://www.chaitons.com/careers/",
        "https://www.lerners.ca/careers/",      # kept but timeout reduced in safe_get
        "https://hicksadams.com/careers/",
        # brauti.com removed — confirmed network unreachable in 2026-03-18 logs
        # Alberta Regional Firms
        "https://www.fieldlaw.com/careers/associate-opportunities/",
        "https://www.bdplaw.com/careers/",
        "https://www.parlee.com/careers/",
        "https://www.mross.com/about-us/careers/",
        "https://www.brownleelaw.com/careers/",
        "https://www.emeryjamieson.com/careers/",
        "https://www.jmccormick.ca/careers/",
        # International Firms (Canadian offices)
        "https://www.dentons.com/en/careers/find-a-job",
        "https://www.nortonrosefulbright.com/en-ca/careers/job-listings",
        "https://www.dlapiper.com/en/canada/careers/",
        "https://careers.bakermckenzie.com/global/en/c/canada-jobs",
        "https://careers.herbertsmithfreehills.com/go/Canada/",
        "https://careers.cliffordchance.com/content/careers/en/office/toronto.html",
        "https://www.linklaters.com/en/careers/offices/toronto",
        # Legal Recruitment
        "https://www.zsa.ca/job-board/",
        "https://thecounselnetwork.com/job-search/",
        "https://legaljobs.ca/jobs/",
        # Bar / Law Society Boards
        "https://www.oba.org/Careers",
        "https://lawsociety.ab.ca/about/programs-and-initiatives/careers/",
        "https://www.lso.ca/careers",
        "https://www.cba.org/For-Lawyers/Employment",
        # In-House / Corporate
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
        # Law School Career Boards
        "https://ultravires.ca/jobs/",
        "https://www.osgoode.yorku.ca/careers/",
        # Canadian Job Aggregators
        "https://ca.indeed.com/jobs?q=associate+lawyer&l=Ontario&fromage=7",
        "https://ca.indeed.com/jobs?q=associate+lawyer&l=Alberta&fromage=7",
        "https://ca.indeed.com/jobs?q=articling+student&l=Ontario&fromage=14",
        "https://ca.indeed.com/jobs?q=articling+student&l=Alberta&fromage=14",
        "https://www.glassdoor.ca/Job/ontario-associate-lawyer-jobs-SRCH_IL.0,7_IS3559_KO8,24.htm",
        "https://ca.talent.com/jobs?k=associate+lawyer&l=ontario",
    ]


# ─────────────────────────────────────────────────────────────────────────────
# 9. RSS FEED SCRAPERS  (v8 — new sources, no auth required)
# ─────────────────────────────────────────────────────────────────────────────

_RSS_FEEDS = [
    # Indeed RSS — structured job data, fromage=7 enforces 7-day freshness server-side
    (
        "https://ca.indeed.com/rss?q=%22associate+lawyer%22&l=Toronto%2C+Ontario&fromage=7&sort=date",
        "indeed-rss"
    ),
    (
        "https://ca.indeed.com/rss?q=%22associate+lawyer%22&l=Ontario&fromage=7&sort=date",
        "indeed-rss"
    ),
    (
        "https://ca.indeed.com/rss?q=%22articling+student%22&l=Ontario&fromage=14&sort=date",
        "indeed-rss"
    ),
    (
        "https://ca.indeed.com/rss?q=%22associate+lawyer%22&l=Alberta&fromage=7&sort=date",
        "indeed-rss"
    ),
    (
        "https://ca.indeed.com/rss?q=%22articling+student%22&l=Alberta&fromage=14&sort=date",
        "indeed-rss"
    ),
    (
        "https://ca.indeed.com/rss?q=%22legal+counsel%22&l=Toronto%2C+Ontario&fromage=7&sort=date",
        "indeed-rss"
    ),
    (
        "https://ca.indeed.com/rss?q=%22newly+called+lawyer%22&l=Ontario&fromage=14&sort=date",
        "indeed-rss"
    ),
    # ZSA Legal Recruitment RSS (top Canadian legal recruiter — actual job listings)
    (
        "https://www.zsa.ca/feed/",
        "zsa-rss"
    ),
    # Ultra Vires (U of T / Osgoode law student paper — posts real Bay Street openings)
    (
        "https://ultravires.ca/feed/",
        "ultravires-rss"
    ),
]

# Patterns that indicate a NEWS ARTICLE rather than a job posting
_RE_NEWS_TITLE = re.compile(
    r"\b(says|reports?|explains?|weighs\s+in|discusses|comments?\s+on|"
    r"named?\s+partner|joins?\s+(as\s+)?partner|promoted\s+to|appointed\s+to|"
    r"(?:on|with|for)\s+(?:cbc|ctv|global\s+news|cp24|bnn|bloomberg)|"
    r"ripple\s+through|on\s+the\s+upswing|poaching|magazine|"
    r":\s+meet\s+the|interview\s+with|profile\s+of|"
    r"what\s+will|will\s+the\s+conversation|fixing\s+articling|"
    r"law\s+firm\s+layoffs|pay\s+cuts|bay\s+street\s+job\s+market)\b",
    re.IGNORECASE,
)

_RE_RSS_ITEM  = re.compile(r"<item>(.*?)</item>", re.DOTALL | re.IGNORECASE)
_RE_RSS_TITLE = re.compile(r"<title[^>]*><!\[CDATA\[(.*?)\]\]></title>|<title[^>]*>(.*?)</title>", re.DOTALL | re.IGNORECASE)
_RE_RSS_LINK  = re.compile(r"<link[^>]*>(.*?)</link>|<guid[^>]*>(.*?)</guid>",                     re.DOTALL | re.IGNORECASE)
_RE_RSS_DATE  = re.compile(r"<pubDate[^>]*>(.*?)</pubDate>",                                        re.DOTALL | re.IGNORECASE)
_RE_RSS_DESC  = re.compile(r"<description[^>]*><!\[CDATA\[(.*?)\]\]></description>|<description[^>]*>(.*?)</description>", re.DOTALL | re.IGNORECASE)


def _parse_rss_items(xml: str) -> List[Dict[str, str]]:
    items = []
    for m in _RE_RSS_ITEM.finditer(xml):
        block = m.group(1)
        tm = _RE_RSS_TITLE.search(block)
        lm = _RE_RSS_LINK.search(block)
        dm = _RE_RSS_DATE.search(block)
        de = _RE_RSS_DESC.search(block)
        title = (tm.group(1) or tm.group(2) or "").strip() if tm else ""
        link  = (lm.group(1) or lm.group(2) or "").strip() if lm else ""
        date  = (dm.group(1) or "").strip()                 if dm else ""
        desc  = (de.group(1) or de.group(2) or "").strip()  if de else ""
        desc  = BeautifulSoup(desc, "html.parser").get_text(" ")
        if title and link:
            items.append({"title": title, "link": link, "date": date, "desc": desc})
    return items


def scrape_rss_feeds(scorer: JobScorer) -> List[dict]:
    session = get_session()
    results: List[dict] = []

    for feed_url, source_name in _RSS_FEEDS:
        try:
            resp = safe_get(session, feed_url, timeout=15)
            if not resp:
                continue
            items = _parse_rss_items(resp.text)
            for item in items:
                title = item["title"]
                link  = item["link"]
                desc  = item["desc"]
                date  = item["date"]

                # Drop news articles immediately — they are not job postings
                if _RE_NEWS_TITLE.search(title):
                    log.debug(f"[RSS] News article skipped: {title[:60]}")
                    continue

                # Freshness gate: drop items older than MAX_JOB_AGE_DAYS
                if not _is_recent(date):
                    log.debug(f"[RSS] Stale item skipped ({date}): {title[:60]}")
                    continue

                if not (RE_ASSOCIATE.search(title) or RE_STUDENT.search(title)):
                    if not (RE_ASSOCIATE.search(desc[:300]) or RE_STUDENT.search(desc[:300])):
                        continue

                company = _domain_key(link).title() if link else source_name
                is_fit, cat, score = scorer.score_job(title, desc, link, company)
                if is_fit:
                    results.append({
                        "TITLE":    title,
                        "COMPANY":  company,
                        "URL":      link,
                        "CATEGORY": cat,
                        "SCORE":    score,
                        "SOURCE":   source_name,
                        "DATE":     date,
                    })
        except Exception as e:
            log.debug(f"[RSS] {source_name} ({feed_url[:60]}): {e}")

    if results:
        log.info(f"  -> {len(results)} hits from RSS feeds")
    return results


# ─────────────────────────────────────────────────────────────────────────────
# 10. JOBSPY AGGREGATOR
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
                        # Drop stale postings — JobSpy sometimes returns weeks-old results
                        if not _is_recent(date):
                            log.debug(f"[JobSpy] Stale skipped ({date}): {title[:50]}")
                            continue
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
# 11. GMAIL JOB ALERT SCANNER
# ─────────────────────────────────────────────────────────────────────────────

_GMAIL_SEARCH_QUERIES = [
    "subject:(job alert OR new jobs) (associate lawyer OR articling student OR legal counsel) newer_than:7d",
    "subject:(opportunity OR position OR role OR opening) (associate OR counsel OR lawyer OR articling) newer_than:7d",
    "from:jobalerts-noreply@linkedin.com newer_than:7d",
    "from:jobalerts@indeed.com newer_than:7d",
    "from:googlealerts-noreply@google.com (associate lawyer OR articling student OR legal counsel) newer_than:7d",
    "from:(zsa.ca OR thecounselnetwork.com OR legaljobs.ca) newer_than:7d",
]

_RE_EMAIL_JOB_LINK = re.compile(
    r"((?:associate|counsel|lawyer|articling|solicitor|juriste)[^\n<]{0,80}?)\s*"
    r"<?(https?://[^\s>\"]{20,300})>?",
    re.IGNORECASE,
)
_RE_EMAIL_SUBJECT_JOB = re.compile(
    r"\b(associate|lawyer|counsel|articling|solicitor|legal\s+counsel|corporate\s+counsel)\b",
    re.IGNORECASE,
)


def _get_gmail_service():
    if not _GMAIL_AVAILABLE:
        return None
    token_json = os.environ.get("GMAIL_TOKEN_JSON")
    if not token_json:
        log.info("[Gmail] GMAIL_TOKEN_JSON not set — skipping Gmail scan.")
        return None
    try:
        creds = Credentials.from_authorized_user_info(
            json.loads(token_json),
            scopes=["https://www.googleapis.com/auth/gmail.readonly"],
        )
        if creds.expired and creds.refresh_token:
            creds.refresh(GRequest())
        return gbuild("gmail", "v1", credentials=creds, cache_discovery=False)
    except Exception as e:
        log.warning(f"[Gmail] Could not build service: {e}")
        return None


def _extract_email_body(msg_data: dict) -> str:
    try:
        payload = msg_data.get("payload", {})

        def _decode(part):
            data = part.get("body", {}).get("data", "")
            return base64.urlsafe_b64decode(data + "==").decode("utf-8", errors="replace") if data else ""

        mt = payload.get("mimeType", "")
        if mt == "text/plain":
            return _decode(payload)
        if mt == "text/html":
            return BeautifulSoup(_decode(payload), "html.parser").get_text(" ")

        text_parts = []
        for part in payload.get("parts", []):
            smt = part.get("mimeType", "")
            if smt == "text/plain":
                text_parts.append(_decode(part))
            elif smt == "text/html":
                text_parts.append(BeautifulSoup(_decode(part), "html.parser").get_text(" "))
            elif smt.startswith("multipart/"):
                for sub in part.get("parts", []):
                    ssmt = sub.get("mimeType", "")
                    raw = _decode(sub)
                    text_parts.append(
                        BeautifulSoup(raw, "html.parser").get_text(" ") if ssmt == "text/html" else raw
                    )
        return " ".join(text_parts)
    except Exception:
        return ""


def scrape_gmail(scorer: JobScorer) -> List[dict]:
    service = _get_gmail_service()
    if not service:
        return []

    results:   List[dict] = []
    seen_urls: Set[str]   = set()

    # Run all queries concurrently
    def _query(query: str) -> List[dict]:
        found = []
        try:
            resp     = service.users().messages().list(userId="me", q=query, maxResults=50).execute()
            messages = resp.get("messages", [])
            for stub in messages:
                try:
                    msg     = service.users().messages().get(userId="me", id=stub["id"], format="full").execute()
                    headers = {h["name"].lower(): h["value"] for h in msg.get("payload", {}).get("headers", [])}
                    subject = headers.get("subject", "")
                    sender  = headers.get("from", "")
                    body    = _extract_email_body(msg)
                    combined = f"{subject} {body}"

                    for m in _RE_EMAIL_JOB_LINK.finditer(combined):
                        title = m.group(1).strip().rstrip("--").strip()
                        url   = _strip_tracking(m.group(2).strip())
                        if url in seen_urls or len(title) < MIN_TITLE_LENGTH:
                            continue
                        is_fit, cat, score = scorer.score_job(title, body, url, sender)
                        if is_fit:
                            seen_urls.add(url)
                            found.append({
                                "TITLE":    title,
                                "COMPANY":  sender.split("<")[0].strip() or "Email Alert",
                                "URL":      url,
                                "CATEGORY": cat,
                                "SCORE":    score,
                                "SOURCE":   "gmail",
                                "DATE":     headers.get("date", "")[:10],
                            })

                    if _RE_EMAIL_SUBJECT_JOB.search(subject) and len(subject) >= MIN_TITLE_LENGTH:
                        is_fit, cat, score = scorer.score_job(subject, body, "", sender)
                        if is_fit:
                            found.append({
                                "TITLE":    subject[:120],
                                "COMPANY":  sender.split("<")[0].strip() or "Email Alert",
                                "URL":      f"gmail://message/{stub['id']}",
                                "CATEGORY": cat,
                                "SCORE":    score,
                                "SOURCE":   "gmail",
                                "DATE":     headers.get("date", "")[:10],
                            })
                except Exception as e:
                    log.debug(f"[Gmail] Message parse error: {e}")
        except Exception as e:
            log.debug(f"[Gmail] Query failed: {e}")
        return found

    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as ex:
        for batch in concurrent.futures.as_completed(
            {ex.submit(_query, q): q for q in _GMAIL_SEARCH_QUERIES}
        ):
            try:
                results.extend(batch.result())
            except Exception:
                pass

    if results:
        log.info(f"  → {len(results)} hits from Gmail")
    return results


# ─────────────────────────────────────────────────────────────────────────────
# 12. ORCHESTRATE ALL SCRAPERS
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
    log.info(f"\n-- Direct HTML scrape ({len(target_urls)} sites) ----------")
    frames.append(run_direct_scrape(target_urls, scorer))

    log.info("\n-- RSS Feeds -------------------------------------------")
    rss_jobs = scrape_rss_feeds(scorer)
    if rss_jobs:
        frames.append(pd.DataFrame(rss_jobs))

    log.info("\n-- Workday ATS -----------------------------------------")
    wd_jobs = scrape_all_workday(scorer)
    if wd_jobs:
        frames.append(pd.DataFrame(wd_jobs))
    log.info(f"  -> {len(wd_jobs)} raw hits from Workday")

    log.info("\n-- Greenhouse ATS --------------------------------------")
    gh_jobs = scrape_all_greenhouse(scorer)
    if gh_jobs:
        frames.append(pd.DataFrame(gh_jobs))
    log.info(f"  -> {len(gh_jobs)} raw hits from Greenhouse")

    log.info("\n-- Lever ATS -------------------------------------------")
    lv_jobs = scrape_all_lever(scorer)
    if lv_jobs:
        frames.append(pd.DataFrame(lv_jobs))
    log.info(f"  -> {len(lv_jobs)} raw hits from Lever")

    log.info("\n-- iCIMS ATS -------------------------------------------")
    ic_jobs = scrape_all_icims(scorer)
    if ic_jobs:
        frames.append(pd.DataFrame(ic_jobs))
    log.info(f"  -> {len(ic_jobs)} raw hits from iCIMS")

    log.info("\n-- Government portals ----------------------------------")
    gov_jobs = scrape_gc_jobs(scorer) + scrape_ontario_public_service(scorer)
    if gov_jobs:
        frames.append(pd.DataFrame(gov_jobs))
    log.info(f"  -> {len(gov_jobs)} raw hits from Government")

    log.info("\n-- JobSpy aggregators ----------------------------------")
    frames.append(scrape_jobspy_wrapper(scorer))

    log.info("\n-- Gmail job alerts ------------------------------------")
    gmail_jobs = scrape_gmail(scorer)
    if gmail_jobs:
        frames.append(pd.DataFrame(gmail_jobs))
    log.info(f"  -> {len(gmail_jobs)} hits from Gmail")

    non_empty = [f for f in frames if f is not None and not f.empty]
    if not non_empty:
        return pd.DataFrame()
    return pd.concat(non_empty, ignore_index=True, sort=False)


# ─────────────────────────────────────────────────────────────────────────────
# 13. DEDUPLICATION
# ─────────────────────────────────────────────────────────────────────────────

_STOPWORDS = re.compile(
    r"\b(the|a|an|at|in|for|of|and|or|-|&|llp|lp|inc|ltd|corp|"
    r"toronto|ontario|calgary|alberta|canada|remote|hybrid|office)\b",
    re.IGNORECASE,
)


def _make_sig(row: pd.Series) -> str:
    t = re.sub(r"\W+", " ", str(row.get("TITLE",   "")).lower())
    c = re.sub(r"\W+", " ", str(row.get("COMPANY", "")).lower())
    t = _STOPWORDS.sub("", t).strip()
    c = _STOPWORDS.sub("", c).strip()
    return f"{re.sub(chr(32)+'+','_',t)}__{re.sub(chr(32)+'+','_',c)}"


def deduplicate_jobs(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    df = df.copy()
    # 1. Fuzzy title+company dedup
    df["SIG"]       = df.apply(_make_sig, axis=1)
    df              = df.drop_duplicates(subset=["SIG"])
    # 2. Exact URL dedup (strip tracking params)
    df["CLEAN_URL"] = df["URL"].apply(lambda x: _strip_tracking(str(x)).split("?")[0])
    df              = df.drop_duplicates(subset=["CLEAN_URL"])
    return df.drop(columns=["SIG", "CLEAN_URL"])


# ─────────────────────────────────────────────────────────────────────────────
# 14. HISTORY MANAGEMENT  (v8: with TTL expiry)
# ─────────────────────────────────────────────────────────────────────────────

def load_history() -> Dict[str, str]:
    """Return {clean_url: date_seen_iso} dict."""
    if not os.path.exists(HISTORY_FILE):
        return {}
    try:
        with open(HISTORY_FILE) as f:
            raw = json.load(f)
        # Legacy format was a plain list of URLs
        if isinstance(raw, list):
            return {url: "2000-01-01" for url in raw}
        return raw
    except Exception:
        return {}


def save_history(history: Dict[str, str]) -> None:
    try:
        with open(HISTORY_FILE, "w") as f:
            json.dump(history, f)
    except Exception as e:
        log.warning(f"Could not save history: {e}")


def prune_history(history: Dict[str, str], ttl_days: int) -> Dict[str, str]:
    """Remove entries older than ttl_days so jobs can resurface after TTL."""
    cutoff = (datetime.now(timezone.utc) - timedelta(days=ttl_days)).strftime("%Y-%m-%d")
    pruned = {url: seen for url, seen in history.items() if seen >= cutoff}
    removed = len(history) - len(pruned)
    if removed:
        log.info(f"[History] Pruned {removed} expired entries (TTL={ttl_days}d)")
    return pruned


# ─────────────────────────────────────────────────────────────────────────────
# 15. RESULTS JSON
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
        log.info(f"Results saved to {RESULTS_FILE}")
    except Exception as e:
        log.warning(f"Could not save results: {e}")


# ─────────────────────────────────────────────────────────────────────────────
# 16. TELEGRAM ALERT  (v8: sorted by score, star rating, new-firm badge)
# ─────────────────────────────────────────────────────────────────────────────

def _stars(score: int) -> str:
    """Convert numeric score to 1-5 star emoji rating."""
    if score >= 18:  return "★★★★★"
    if score >= 14:  return "★★★★☆"
    if score >= 10:  return "★★★☆☆"
    if score >= 7:   return "★★☆☆☆"
    return "★☆☆☆☆"


def send_telegram(df: pd.DataFrame, weights: Dict[str, Any]) -> None:
    bot_token = os.environ.get("TELEGRAM_BOT_TOKEN")
    chat_id   = os.environ.get("TELEGRAM_CHAT_ID")

    if not (bot_token and chat_id):
        print("\n[!] TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID missing. No alert sent.", flush=True)
        return

    # Sort by score descending
    if not df.empty and "SCORE" in df.columns:
        df = df.sort_values("SCORE", ascending=False)

    associates = df[df["CATEGORY"] == "Associate"] if not df.empty else pd.DataFrame()
    students   = df[df["CATEGORY"] == "Student"]   if not df.empty else pd.DataFrame()
    now_str    = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    source_counts: Dict[str, int] = {}
    if not df.empty and "SOURCE" in df.columns:
        for src, grp in df.groupby("SOURCE"):
            source_counts[str(src)] = len(grp)
    src_str = " | ".join(f"{s}:{n}" for s, n in sorted(source_counts.items())) or "none"

    first_seen = set(weights.get("first_seen_domains", []))

    def _fmt_job(row: pd.Series, show_score: bool = True) -> str:
        domain    = _domain_key(str(row.get("URL", "")))
        new_badge = " [NEW FIRM]" if domain in first_seen and domain in TRUSTED_CA_DOMAINS else ""
        stars     = f" {_stars(int(row.get('SCORE', 0)))}" if show_score else ""
        src_badge = f" ({row['SOURCE']})" if row.get("SOURCE") else ""
        return (
            f"*  <a href=\"{row['URL']}\">{row['TITLE']}</a>"
            f"{new_badge}{stars}\n"
            f"   {row['COMPANY']}{src_badge}"
        )

    assoc_lines: List[str] = []
    if not associates.empty:
        assoc_lines.append("\n<b>ASSOCIATES / LAWYERS</b>")
        assoc_lines.extend(_fmt_job(r) for _, r in associates.iterrows())

    stud_lines: List[str] = []
    if not students.empty:
        stud_lines.append("\n<b>ARTICLING / STUDENTS</b>")
        stud_lines.extend(_fmt_job(r, show_score=False) for _, r in students.iterrows())

    header = "\n".join([
        f"<b>Law Jobs -- ON/AB | {now_str}</b>",
        f"<b>{len(df)} new listing(s)</b>",
        (f"<i>Run #{weights.get('training_runs','?')} | "
         f"Threshold {weights.get('score_threshold',5)} | "
         f"All-time {weights.get('total_jobs_found',0)}</i>"),
        f"<i>{src_str}</i>",
    ])
    footer = f"\n\n<a href=\"{DASHBOARD_URL}\">Open Dashboard</a>"

    if df.empty:
        header += "\n\n<i>No new positions this run.</i>"

    LIMIT         = 4090
    all_job_lines = assoc_lines + stud_lines
    full_msg      = header + "\n".join(all_job_lines) + footer

    if len(full_msg) > LIMIT:
        trimmed: List[str] = []
        dropped = 0
        for line in all_job_lines:
            if len(header + "\n".join(trimmed + [line]) + footer) <= LIMIT:
                trimmed.append(line)
            else:
                dropped += 1
        if dropped:
            trimmed.append(f"\n<i>... and {dropped} more -- see dashboard</i>")
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
            print("Telegram alert sent.", flush=True)
        else:
            print(f"Telegram error: {resp.status_code} -- {resp.text}", flush=True)
    except Exception as e:
        print(f"Telegram request failed: {e}", flush=True)


# ─────────────────────────────────────────────────────────────────────────────
# 17. LLM EXPERIENCE LEVEL FILTER  — powered by Gemini 1.5 Flash (FREE tier)
# ─────────────────────────────────────────────────────────────────────────────
# Uses Google Gemini 1.5 Flash via plain REST (no SDK, no new packages).
# Free tier: 15 requests/min, 1 million tokens/day — more than enough.
# No credit card required.
#
# Setup (one time):
#   1. Go to https://aistudio.google.com/apikey
#   2. Click "Create API Key" — sign in with any Google account
#   3. Copy the key (starts with AIza...)
#   4. Add GitHub secret: GEMINI_API_KEY = <your key>
#
# The classifier fetches each job's full description page, sends it to Gemini,
# and gets back a structured JSON verdict. Results are cached by URL hash in
# llm_cache.json so each URL is only classified once across all future runs.
#
# Fail-open: if GEMINI_API_KEY is absent or the API fails, all jobs pass
# through — no outage risk.
# ─────────────────────────────────────────────────────────────────────────────

_GEMINI_API_URL = (
    "https://generativelanguage.googleapis.com/v1beta/"
    "models/gemini-1.5-flash:generateContent"
)
_LLM_TIMEOUT = 20
_LLM_WORKERS = 5   # concurrent Gemini calls (well within 15 rpm free limit)

_LLM_SYSTEM = (
    "You are a legal recruitment classifier. Determine whether a Canadian law "
    "job posting is suitable for a NEWLY CALLED or JUNIOR lawyer "
    "(0-2 years post-call / 0-2 PQE). "
    "Respond ONLY with valid JSON matching this schema exactly — no markdown, "
    "no extra text: "
    '{"eligible": true|false, "min_years": integer|null, "max_years": integer|null, '
    '"reason": "one concise sentence", "is_real_job": true|false}. '
    "Rules: "
    "eligible=true ONLY if posting targets 0-2 yrs OR is silent on experience AND title is not senior. "
    "eligible=false if 3+ years required (even preferred), mid-level, senior, supervise a team, "
    "general counsel, VP, director, or any clear seniority signal. "
    "eligible=false if text is NOT a job posting (past legal matter descriptions like "
    "'Counsel to X during CCAA', talent pool forms, generic careers pages). "
    "is_real_job=false for talent pools, interest forms, past matters. "
    "min_years/max_years: extract stated range; null if not mentioned."
)

_LLM_USER_TMPL = (
    "Job title: {title}\n"
    "Company: {company}\n"
    "URL: {url}\n\n"
    "--- Description (up to 3000 chars) ---\n"
    "{desc}\n"
    "---\n\n"
    "Classify. eligible=true ONLY for 0-2 years post-call."
)


def _url_hash(url: str) -> str:
    return hashlib.sha256(url.encode()).hexdigest()[:16]


def _load_llm_cache() -> Dict[str, dict]:
    if os.path.exists(LLM_CACHE_FILE):
        try:
            with open(LLM_CACHE_FILE) as f:
                return json.load(f)
        except Exception:
            pass
    return {}


def _save_llm_cache(cache: Dict[str, dict]) -> None:
    try:
        with open(LLM_CACHE_FILE, "w") as f:
            json.dump(cache, f, indent=2)
    except Exception as e:
        log.warning(f"[LLM] Could not save cache: {e}")


def _fetch_description(url: str) -> str:
    """Fetch and clean a job posting page. Returns '' on failure."""
    if not url or url.startswith("gmail://"):
        return ""
    try:
        session = get_session()
        resp    = safe_get(session, url, timeout=15)
        if not resp:
            return ""
        soup = BeautifulSoup(resp.text, "html.parser")
        for tag in soup(["script", "style", "nav", "footer", "header", "noscript", "aside"]):
            tag.extract()
        return " ".join(soup.get_text(separator=" ").split())[:4000]
    except Exception:
        return ""


def _call_gemini(prompt: str, api_key: str) -> dict:
    """
    Call Gemini 1.5 Flash and return a parsed verdict dict.
    Uses responseMimeType=application/json so the model is forced to emit
    valid JSON — no markdown fences to strip.
    Raises on hard failures; caller should catch and fail-open.
    """
    payload = {
        "systemInstruction": {"parts": [{"text": _LLM_SYSTEM}]},
        "contents":          [{"parts": [{"text": prompt}]}],
        "generationConfig":  {
            "responseMimeType": "application/json",
            "maxOutputTokens":  256,
            "temperature":      0.0,   # deterministic
        },
    }
    resp = requests.post(
        _GEMINI_API_URL,
        params={"key": api_key},
        json=payload,
        timeout=_LLM_TIMEOUT,
    )
    resp.raise_for_status()
    text = resp.json()["candidates"][0]["content"]["parts"][0]["text"].strip()
    return json.loads(text)


def _classify_one(job: dict, cache: Dict[str, dict], api_key: str) -> dict:
    """
    Classify one job. Returns job dict extended with:
      LLM_ELIGIBLE  bool
      LLM_REASON    str
      LLM_CACHED    bool
    Never raises — fails open (eligible=True) on any error.
    """
    url     = _strip_tracking(str(job.get("URL", ""))).split("?")[0]
    key     = _url_hash(url)
    title   = str(job.get("TITLE",   ""))
    company = str(job.get("COMPANY", ""))

    # Cache hit
    if key in cache:
        v = cache[key]
        return {**job,
                "LLM_ELIGIBLE": v.get("eligible", True),
                "LLM_REASON":   v.get("reason", "cached"),
                "LLM_CACHED":   True}

    # Fetch description
    desc = str(job.get("DESC", "")) or _fetch_description(url)
    if not desc:
        desc = title

    prompt  = _LLM_USER_TMPL.format(
        title=title, company=company, url=url, desc=desc[:3000]
    )

    try:
        verdict = _call_gemini(prompt, api_key)
    except Exception as e:
        log.warning(f"[LLM] Gemini error for '{title[:40]}': {e}")
        verdict = {"eligible": True, "reason": f"api-error: {e}", "is_real_job": True}

    cache[key] = {
        "eligible":    verdict.get("eligible", True),
        "is_real_job": verdict.get("is_real_job", True),
        "min_years":   verdict.get("min_years"),
        "max_years":   verdict.get("max_years"),
        "reason":      verdict.get("reason", ""),
        "title":       title,
        "classified":  datetime.now(timezone.utc).strftime("%Y-%m-%d"),
    }
    return {**job,
            "LLM_ELIGIBLE": verdict.get("eligible", True),
            "LLM_REASON":   verdict.get("reason", ""),
            "LLM_CACHED":   False}


def llm_filter_jobs(jobs: List[dict]) -> List[dict]:
    """
    Filter jobs to 0-2 year roles using Gemini 1.5 Flash (free tier).
    Falls back to passing all jobs through if GEMINI_API_KEY is not set.
    """
    api_key = os.environ.get("GEMINI_API_KEY")
    if not api_key:
        log.warning("[LLM] GEMINI_API_KEY not set — skipping experience filter.")
        return jobs
    if not jobs:
        return jobs

    cache = _load_llm_cache()
    log.info(f"\n-- Gemini experience filter ({len(jobs)} candidates) --------")

    results: List[Optional[dict]] = [None] * len(jobs)

    with concurrent.futures.ThreadPoolExecutor(max_workers=_LLM_WORKERS) as ex:
        future_to_idx = {
            ex.submit(_classify_one, job, cache, api_key): i
            for i, job in enumerate(jobs)
        }
        for future in concurrent.futures.as_completed(future_to_idx):
            i = future_to_idx[future]
            try:
                results[i] = future.result()
            except Exception as e:
                log.warning(f"[LLM] Unhandled error on job #{i}: {e}")
                results[i] = {**jobs[i], "LLM_ELIGIBLE": True,
                              "LLM_REASON": "exception", "LLM_CACHED": False}

    _save_llm_cache(cache)

    eligible   = [r for r in results if r and r.get("LLM_ELIGIBLE")]
    ineligible = [r for r in results if r and not r.get("LLM_ELIGIBLE")]
    cached_n   = sum(1 for r in results if r and r.get("LLM_CACHED"))

    log.info(f"  [LLM] {len(eligible)} eligible / {len(ineligible)} filtered / {cached_n} from cache")
    for r in ineligible:
        log.info(f"  [LLM] REMOVED: '{r.get('TITLE','')[:55]}' — {r.get('LLM_REASON','')}")

    return eligible


# ─────────────────────────────────────────────────────────────────────────────
# 18. MAIN
# ─────────────────────────────────────────────────────────────────────────────

def main() -> None:
    run_ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    print("=" * 60, flush=True)
    print("Law Associate Job Scraper -- v9", flush=True)
    print(f"Run: {run_ts}", flush=True)
    print("=" * 60, flush=True)

    weights = load_weights()
    scorer  = JobScorer(weights)

    print(f"\n[Model]  Score threshold  : {scorer.threshold}", flush=True)
    print(f"[Model]  Training run #   : {weights['training_runs']}", flush=True)
    print(f"[Model]  All-time jobs    : {weights.get('total_jobs_found', 0)}", flush=True)
    dead = weights.get("low_productivity_domains", [])
    if dead:
        print(f"[Model]  Auto-pruned      : {', '.join(dead[:10])}{'...' if len(dead)>10 else ''}", flush=True)
    llm_on = bool(os.environ.get("GEMINI_API_KEY"))
    print(f"[Model]  LLM filter       : {'ON (Gemini 1.5 Flash free)' if llm_on else 'OFF (add GEMINI_API_KEY secret)'}", flush=True)
    print(flush=True)

    # 1. Run all scrapers
    combined = run_all_scrapers(scorer)

    # 2. Deduplicate
    unique_jobs = deduplicate_jobs(combined)
    log.info(f"After dedup: {len(unique_jobs)} unique candidates")

    # 3. LLM experience filter — runs BEFORE history so we never mark a
    #    too-senior job as "seen" and block it from being re-evaluated later.
    llm_candidates = unique_jobs.to_dict("records") if not unique_jobs.empty else []
    llm_passed     = llm_filter_jobs(llm_candidates)

    # 4. History filter (with TTL expiry)
    history   = load_history()
    history   = prune_history(history, HISTORY_TTL_DAYS)
    today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    final_jobs: List[dict] = []

    for row in llm_passed:
        clean_url = _strip_tracking(str(row.get("URL", ""))).split("?")[0]
        if clean_url not in history:
            # Strip internal LLM metadata before storing/alerting
            job = {k: v for k, v in row.items()
                   if k not in ("LLM_ELIGIBLE", "LLM_REASON", "LLM_CACHED", "DESC")}
            final_jobs.append(job)
            history[clean_url] = today_str

    final_df = pd.DataFrame(final_jobs)

    # 5. Print output
    print(f"\n{'='*60}", flush=True)
    print(f"Final Verified New Jobs: {len(final_df)}", flush=True)
    if not final_df.empty:
        cols = [c for c in ["TITLE", "COMPANY", "CATEGORY", "SOURCE", "SCORE"] if c in final_df.columns]
        print(final_df[cols].to_string(index=False), flush=True)

    # 6. Self-training
    weights = train_model(weights, final_jobs, get_target_urls(), run_ts)
    save_weights(weights)

    # 7. Persist results
    append_results(final_jobs, run_ts, weights)

    # 8. Save history
    save_history(history)

    # 9. Telegram
    send_telegram(final_df, weights)


if __name__ == "__main__":
    main()
