# Law Associate Job Alert Agent

This repository contains a simple job‑scraping agent built with Python.  The agent searches multiple job boards for **first‑ and second‑year law associate** positions across Canada and emails you a curated list of matching jobs on a weekly schedule.

## What it does

* Utilises the open‑source [`python‑jobspy`](https://github.com/speedyapply/JobSpy) library, which concurrently scrapes job postings from several popular job boards – including LinkedIn, Indeed, Google Jobs and ZipRecruiter.  The package aggregates the results into a Pandas data frame and exposes fields such as the job title, company, location and description【123892292451392†L266-L274】.
* Queries the **careers pages of leading Canadian law firms** (Osler, Blake Cassels & Graydon, Bennett Jones, Fasken, Gowling WLG, Stikeman Elliott, Davies Ward Phillips & Vineberg, McCarthy Tétrault, Torys, and Lenczner Slaght) and collects postings for associate‑level roles.  The script follows links labelled “associate” on each firm’s careers page and inspects the description for experience requirements.  Only positions mentioning 0‑2 years of experience or phrases such as “first‑year” or “second‑year” are included.  For example, Bennett Jones lists several associate roles on its careers page【759468253450880†L56-L74】, and individual job descriptions often include explicit experience ranges (e.g., the Corporate Energy associate posting states that the ideal candidate should have **6–8 years of experience**【433251462159370†L62-L79】).  The agent filters these postings to capture only early‑career opportunities.
* Filters the scraped jobs to include only those that:
  * Contain _“associate”, “law”, “legal”_ or _“lawyer”_ in the title or description, ensuring the role is for a lawyer rather than another industry.
  * Mention **0‑2 years** of experience or phrases like **“first‑year”, “second‑year”, “entry‑level”** in the job description.  This catches positions advertised for new associates or candidates with no more than two years’ experience.
* Bundles the filtered jobs into a simple email summary and sends it to your specified email address once per week.

## Configuration

1. **Create repository secrets.**  Go to your GitHub repository’s **Settings → Secrets and variables → Actions** and add the following secrets:

   | Secret name      | Purpose                                                    |
   |------------------|------------------------------------------------------------|
   | `EMAIL_USER`     | The address of the Gmail account used to send alerts.      |
   | `EMAIL_PASS`     | An App Password for the Gmail account (see below).         |
   | `EMAIL_TO`       | Your receiving email address (e.g. `shahirsinn@gmail.com`).|

   We recommend using a Gmail **app password** rather than your regular password.  App passwords can be created under “Security → App passwords” in your Google account settings.

2. **Adjust search parameters (optional).**  Inside `main.py` you can tune the search term, number of results and other filters.  The default settings search for **“law associate”** jobs located in **Canada** with up to **100 results** per run.

3. **Schedule.**  The GitHub Actions workflow, located at `.github/workflows/job_alert.yml`, is configured to run every **Monday at 14:00 UTC** (09:00 Eastern Time).  You can modify the `cron` expression in the workflow file to change the schedule.

4. **Install dependencies locally.**  To test the script locally before pushing to GitHub, run:

```
pip install -r requirements.txt
python main.py
```

The script will read the email credentials from environment variables; you can set them manually in your shell for local testing.

## How it works

1. **Scrape public job boards:**  `main.py` calls `scrape_jobs` from the `python‑jobspy` library to query LinkedIn, Indeed, Google Jobs and ZipRecruiter simultaneously.  According to JobSpy’s documentation, it “scrapes job postings from LinkedIn, Indeed, Glassdoor, Google, ZipRecruiter & other job boards concurrently”【123892292451392†L266-L273】 and returns a DataFrame containing job information【123892292451392†L286-L307】.
2. **Scrape law firm career pages:**  A custom function `scrape_law_firm_sites` loops through a list of Canadian law firm career pages and gathers links that contain the term “associate” along with legal keywords.  It fetches each posting and retains those with 0‑2‑year experience patterns.  This step brings in early‑career roles posted directly on firm websites.
3. **Filter:**  The combined set of jobs is filtered by checking the title and description against keywords related to the legal profession and the 0‑2 years’ experience range.
4. **Send email:**  Using Python’s built‑in `smtplib` and the `email` module, the script composes an email summarising each matching job.  The email lists the platform, title, company, location, posting date and a link to apply.  The message is sent via your Gmail account using SSL.
5. **Automate:**  A GitHub Actions workflow installs dependencies, runs the script and supplies the email credentials via repository secrets.  The workflow triggers according to the defined schedule, automatically delivering weekly updates without manual intervention.

## Caveats & ethics

* This script uses basic keyword filtering.  It does not perform sophisticated natural‑language processing, so some relevant jobs may be missed if the description doesn’t include the keywords, and some non‑legal roles may slip through if their descriptions happen to contain the keywords.
* **Respect website terms:**  Some job boards restrict automated scraping.  JobSpy is designed for educational or personal use only, and it’s your responsibility to ensure compliance with each site’s terms of service when running this agent.
* **Email credentials:**  Store your email password as a GitHub secret and avoid committing it to the repository.  App passwords are recommended for improved security.
