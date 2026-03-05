# Law Associate Job Alert Agent

This repository contains an automated job‑scraping agent built with Python.  The agent searches multiple job boards and law firm career pages for **first‑ and second‑year law associate** positions across Canada and emails you a curated list of matching jobs on a twice‑weekly schedule.  The implementation has evolved beyond simple keyword matching to use more sophisticated filtering, deduplication and scheduling.

## What it does

* **Multiple job boards & advanced filtering:** Utilises the open‑source [`python‑jobspy`](https://github.com/speedyapply/JobSpy) library, which concurrently scrapes job postings from several popular boards – including LinkedIn, Indeed, Google Jobs and ZipRecruiter.  The package aggregates the results into a DataFrame and exposes fields such as the job title, company, location and description【123892292451392†L266-L274】.  The agent then applies a set of **positive patterns** (0–2 years, “first‑year”, “junior”, “newly called”, etc.) and **negative patterns** (e.g. “senior”, “partner”, “3+ years”) to identify genuine early‑career law positions while excluding senior roles.
* **Scrapes law firm career pages:**  Beyond job boards, the agent queries the careers pages of a broad range of Canadian firms – including Osler; Blake Cassels & Graydon; Bennett Jones; Fasken; Gowling WLG; Stikeman Elliott; Davies Ward Phillips & Vineberg; McCarthy Tétrault; Torys; Lenczner Slaght; **Goodmans**; **Borden Ladner Gervais (BLG)**; **Norton Rose Fulbright**; **Dentons**; **Miller Thomson**; **Cassels Brock & Blackwell**; **Aird & Berlis**; **Lerners**; **Blaney McMurtry**; and other firms.  For each site, the script follows links labelled “associate” or similar and inspects the description for experience requirements.  Posts that mention seniority or 3+ years’ experience are discarded.
* **Scrapes legal recruiter and law‑specific job boards:**  The agent also examines Canadian legal recruitment websites and specialty job boards such as **zsa.ca**, **thecounselnetwork.com**, **lifeafterlaw.com**, **thehellergroup.ca**, **smithlegalsearch.com**, **cartelinc.com**, **edgerecruitment.ca**, **urbanlegal.ca/careers**, **legaljobs.ca**, **lexology.com/jobs**, **clawbie.com**, **cba.org/Careers** (Canadian Bar Association), **lsuc.on.ca** (Law Society of Ontario), **ontario.ca/jobs**, **workopolis.com**, and **jobbank.gc.ca**.  These sites often list associate openings that aren’t on mainstream boards.  The scraper applies the same positive/negative pattern logic to these postings.
* **Deduplication & freshness:**  To avoid repeat alerts, the agent stores the URLs of previously emailed jobs in a lightweight JSON file.  Each run loads this history and only includes newly discovered roles.  Jobs older than 40 days (based on their posted date when available) are filtered out to keep the digest current.  The email includes a short **summary section** comparing this run to previous runs (e.g. number of new jobs, top hiring firms, top cities) to give you quick insight into trends.
* **Twice‑weekly scheduling and rate limiting:**  A GitHub Actions workflow runs the script every **Tuesday and Thursday at 07:00 Eastern Time (12:00 UTC)**.  Simple rate limiting and retry logic is used when making HTTP requests to reduce the risk of IP blocking.
* **HTML digest:**  The email is formatted in both plain text and HTML.  Each listing shows the job title, company/firm, location, posting date, a snippet of the description and a direct link to apply.  If you prefer a plain‑text digest, you can customise the formatting in `main.py`.
* **Extensible design:**  The filtering function uses regular expressions to capture a wide range of early‑career phrasing (including **“articling associate”, “junior associate”, “recent call”** and **“called to the bar within 2 years”**) and excludes senior roles.  In addition, you can enable an optional **LLM filter**: by setting an `OPENAI_API_KEY` secret and installing the `openai` package, the agent will ask a language model for each posting whether it is a 0–2‑year law associate role in Canada.  Only postings receiving a “yes” response are included.  This can handle edge cases where simple keyword matching may fail.  Without an API key, the script falls back to regex‑based filtering.
* **Dry‑run & logging:**  Set the environment variable `DRY_RUN=1` when running locally or via GitHub Actions to perform a dry run.  In this mode the script prints the email contents to stdout instead of sending.  A history file is still updated so you can test the workflow without spamming your inbox.

## Configuration

1. **Create repository secrets.**  Go to your GitHub repository’s **Settings → Secrets and variables → Actions** and add the following secrets:

   | Secret name      | Purpose                                                    |
   |------------------|------------------------------------------------------------|
   | `EMAIL_USER`     | The address of the Gmail account used to send alerts.      |
   | `EMAIL_PASS`     | An App Password for the Gmail account (see below).         |
   | `EMAIL_TO`       | Your receiving email address (e.g. `shahirsinn@gmail.com`).|

   We recommend using a Gmail **app password** rather than your regular password.  App passwords can be created under “Security → App passwords” in your Google account settings.

2. **Adjust search parameters (optional).**  Inside `main.py` you can tune the search term, number of results and other filters.  The default settings search for **“law associate”** jobs located in **Canada** with up to **100 results** per run.

3. **Schedule.**  The GitHub Actions workflow, located at `.github/workflows/job_alert.yml`, runs **every Tuesday and Thursday at 12:00 UTC** (07:00 Eastern Time).  You can modify the `cron` expression in the workflow file to change the schedule (for example, to run only once a week).  The `DRY_RUN` environment variable can be set to `1` to test the workflow without sending emails.

4. **Install dependencies locally.**  To test the script locally before pushing to GitHub, run:

```
pip install -r requirements.txt
python main.py
```

The script will read the email credentials from environment variables; you can set them manually in your shell for local testing.

## How it works

1. **Scrape public job boards:**  `main.py` calls `scrape_jobs` from the `python‑jobspy` library to query LinkedIn, Indeed, Google Jobs and ZipRecruiter simultaneously.  According to JobSpy’s documentation, it “scrapes job postings from LinkedIn, Indeed, Glassdoor, Google, ZipRecruiter & other job boards concurrently”【123892292451392†L266-L273】 and returns a DataFrame containing job information【123892292451392†L286-L307】.
2. **Scrape law firm career pages:**  A custom function `scrape_law_firm_sites` loops through a list of Canadian law firm career pages and gathers links that contain the term “associate” along with legal keywords.  It fetches each posting and retains those with 0–2‑year experience patterns.  This step brings in early‑career roles posted directly on firm websites.
3. **Filter:**  The combined set of jobs is filtered by checking the title and description against keywords related to the legal profession and the 0–2 years’ experience range.
4. **Send email:**  Using Python’s built‑in `smtplib` and the `email` module, the script composes an email summarising each matching job.  The email lists the platform, title, company, location, posting date and a link to apply.  The message is sent via your Gmail account using SSL.
5. **Automate:**  A GitHub Actions workflow installs dependencies, runs the script and supplies the email credentials via repository secrets.  The workflow triggers according to the defined schedule, automatically delivering weekly updates without manual intervention.

## Caveats & ethics

* This script uses basic keyword filtering.  It does not perform sophisticated natural‑language processing, so some relevant jobs may be missed if the description doesn’t include the keywords, and some non‑legal roles may slip through if their descriptions happen to contain the keywords.
* **Respect website terms:**  Some job boards restrict automated scraping.  JobSpy is designed for educational or personal use only, and it’s your responsibility to ensure compliance with each site’s terms of service when running this agent.
* **Email credentials:**  Store your email password as a GitHub secret and avoid committing it to the repository.  App passwords are recommended for improved security.
