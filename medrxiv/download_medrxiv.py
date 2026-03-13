"""
Download medRxiv preprints by subject.

Iterates through medRxiv subjects, fetching publication metadata month-by-month
using the medRxiv Details API, and downloads the PDFs.

Features:
- Downloads complete subjects sequentially
- Stops when total downloaded size in the run reaches a configurable limit (default 40GB)
- Saves progress to resume easily

Usage:
    python download_medrxiv.py --output-dir ./medrxiv --max-gb 40
"""

import os
import re
import sys
import json
import asyncio
import aiohttp
from datetime import datetime, date
from dateutil.relativedelta import relativedelta
from pathlib import Path

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
MEDRXIV_API_BASE = "https://api.biorxiv.org/details/medrxiv"
START_DATE = datetime(2019, 6, 1)  # medRxiv launch date

# Extracted from medRxiv website collections
SUBJECTS = [
    "Addiction Medicine",
    "Allergy and Immunology",
    "Anesthesia",
    "Cardiovascular Medicine",
    "Dentistry and Oral Medicine",
    "Dermatology",
    "Emergency Medicine",
    "Endocrinology (including Diabetes Mellitus and Metabolic Disease)",
    "Epidemiology",
    "Forensic Medicine",
    "Gastroenterology",
    "Genetic and Genomic Medicine",
    "Geriatric Medicine",
    "Health Economics",
    "Health Informatics",
    "Health Policy",
    "Health Systems and Quality Improvement",
    "Hematology",
    "HIV/AIDS",
    "Infectious Diseases",
    "Intensive Care and Critical Care Medicine",
    "Medical Education",
    "Medical Ethics",
    "Nephrology",
    "Neurology",
    "Nursing",
    "Nutrition",
    "Obstetrics and Gynecology",
    "Occupational and Environmental Health",
    "Oncology",
    "Ophthalmology",
    "Orthopedics",
    "Otolaryngology",
    "Pain Medicine",
    "Palliative Medicine",
    "Pathology",
    "Pediatrics",
    "Pharmacology and Therapeutics",
    "Primary Care Research",
    "Psychiatry and Clinical Psychology",
    "Public and Global Health",
    "Radiology and Imaging",
    "Rehabilitation Medicine and Physical Therapy",
    "Respiratory Medicine",
    "Rheumatology",
    "Sexual and Reproductive Health",
    "Sports Medicine",
    "Surgery",
    "Toxicology",
    "Transplantation",
    "Urology"
]

DELAY_BETWEEN_PAGES = 1      # seconds between API pagination requests
DELAY_BETWEEN_DOWNLOADS = 1  # seconds between parallel batches
DELAY_ON_429 = 30            # seconds to wait on HTTP 429
MAX_RETRIES_429 = 5

headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36",
    "Accept": "application/json"
}

# ---------------------------------------------------------------------------
# Utility Functions
# ---------------------------------------------------------------------------

def sanitize_filename(name):
    if not name:
        return "untitled"
    name = re.sub(r'[<>:"/\\|?*]', "", name)
    return name.strip(". ")[:150] or "untitled"

def sanitize_foldername(name):
    if not name:
        return "untitled"
    name = re.sub(r'[<>:"/\\|?*]', "", name)
    name = name.replace("/", "-")
    return name.strip(". ")[:150] or "untitled"

def get_doi_safe(doi):
    return doi.replace("/", "_")

async def fetch_api_with_retry(session, url, max_retries=MAX_RETRIES_429, delay_429=DELAY_ON_429):
    for attempt in range(max_retries):
        try:
            async with session.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=45)) as resp:
                if resp.status == 200:
                    return await resp.json(), 200
                elif resp.status == 429:
                    wait = delay_429 * (attempt + 1)
                    print(f"     HTTP 429 Rate limited on API. Waiting {wait}s... (attempt {attempt + 1}/{max_retries})")
                    await asyncio.sleep(wait)
                else:
                    return None, resp.status
        except asyncio.TimeoutError:
            print(f"    Timeout on {url}. Retrying ({attempt + 1}/{max_retries})...")
            await asyncio.sleep(5)
        except aiohttp.ClientError as e:
            print(f"     Client error fetching {url}: {e}. Retrying ({attempt + 1}/{max_retries})...")
            await asyncio.sleep(5)
        except Exception as e:
            print(f"     Unexpected error fetching {url}: {e}. Retrying ({attempt + 1}/{max_retries})...")
            await asyncio.sleep(5)
    return None, 429

# ---------------------------------------------------------------------------
# Download Logic
# ---------------------------------------------------------------------------

async def download_pdf(session, url, filepath):
    """Download a single PDF. Returns downloaded bytes or 0 if failed/skipped."""
    if filepath.exists():
        # Already downloaded in a previous run
        return 0

    for attempt in range(3):
        try:
            async with session.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=180)) as resp:
                if resp.status == 200:
                    content = await resp.read()
                    if content[:4] != b"%PDF":
                        # Sometimes medRxiv returns HTML instead of PDF if the URL is wrong or redirected
                        print(f"    SKIP (not a PDF): {filepath.name[:50]}")
                        return 0
                    filepath.write_bytes(content)
                    print(f"    OK: {filepath.name[:50]} ({len(content) // 1024} KB)")
                    await asyncio.sleep(0.5)
                    return len(content)
                elif resp.status == 429:
                    wait = DELAY_ON_429 * (attempt + 1)
                    print(f"    RATE LIMITED on {filepath.name[:30]}. Waiting {wait}s...")
                    await asyncio.sleep(wait)
                else:
                    print(f"    FAIL: {filepath.name[:50]} HTTP {resp.status}")
                    return 0
        except asyncio.TimeoutError:
            print(f"    TIMEOUT: {filepath.name[:50]}. Retrying ({attempt + 1}/3)...")
            await asyncio.sleep(5)
        except aiohttp.ClientError as e:
            print(f"    NETWORK ERROR: {filepath.name[:50]} — {e}. Retrying ({attempt + 1}/3)...")
            await asyncio.sleep(5)
        except Exception as e:
            print(f"    FAIL: {filepath.name[:50]} — {e}")
            return 0
    return 0

# ---------------------------------------------------------------------------
# Core Routine
# ---------------------------------------------------------------------------

async def process_subject(session, subject, output_dir, progress, max_bytes, total_downloaded_bytes):
    subj_name = subject
    folder_name = sanitize_foldername(subj_name)
    subject_dir = output_dir / folder_name
    subject_dir.mkdir(parents=True, exist_ok=True)

    # Convert subject to API category format
    # medRxiv API categories use hyphens and lowercase, though the Details API 
    # category filter usually follows the string in the 'category' field.
    # We'll use the lowercase, hyphenated version which often matches.
    api_category = subj_name.lower()
    api_category = api_category.replace(" (including diabetes mellitus and metabolic disease)", "")
    api_category = api_category.replace(" and ", "-").replace("/", "-").replace(" ", "-")
    # Quick fix for multiple hyphens
    api_category = re.sub(r'-+', '-', api_category)

    # Special cases for medRxiv categories if they differ from bioRxiv logic
    if subj_name == "Infectious Diseases":
        api_category = "infectious_diseases" # Based on grep results earlier
    elif subj_name == "HIV/AIDS":
        api_category = "hiv-aids"

    # Initialize subject progress if not present
    if subj_name not in progress["subject_progress"]:
        progress["subject_progress"][subj_name] = {
            "current_year": START_DATE.year,
            "current_month": START_DATE.month,
            "downloaded_dois": []
        }
    
    # Ensure downloaded_dois list exists for older progress files
    if "downloaded_dois" not in progress["subject_progress"][subj_name]:
        progress["subject_progress"][subj_name]["downloaded_dois"] = []
        
    downloaded_dois = set(progress["subject_progress"][subj_name]["downloaded_dois"])
    
    start_y = progress["subject_progress"][subj_name]["current_year"]
    start_m = progress["subject_progress"][subj_name]["current_month"]
    current_iter_date = datetime(start_y, start_m, 1)
    end_date_limit = datetime.now()

    print(f"\n" + "=" * 60)
    print(f"Processing Subject: {subj_name} (API: {api_category})")
    print(f"Starting from: {current_iter_date.strftime('%Y-%m')}")
    print("=" * 60)

    while current_iter_date <= end_date_limit:
        next_iter_date = current_iter_date + relativedelta(months=1) - relativedelta(days=1)
        # Prevent querying into the future
        if next_iter_date > end_date_limit:
            next_iter_date = end_date_limit

        d_start = current_iter_date.strftime("%Y-%m-%d")
        d_end = next_iter_date.strftime("%Y-%m-%d")

        print(f"\n  Fetching interval: {d_start} to {d_end}")
        
        cursor = 0
        while True:
            # Check limits before making API calls
            if total_downloaded_bytes >= max_bytes:
                print(f"\n[LIMIT REACHED] Downloaded {(total_downloaded_bytes / 1024**3):.2f} GB out of {(max_bytes / 1024**3):.2f} GB limit.")
                yield total_downloaded_bytes, True # Limit reached
                return
            
            url = f"{MEDRXIV_API_BASE}/{d_start}/{d_end}/{cursor}?category={api_category}"
            data, status = await fetch_api_with_retry(session, url)
            
            if status != 200 or not data:
                print(f"    Failed to fetch API {d_start} - {d_end} at cursor {cursor}. Aborting subject for now.")
                yield total_downloaded_bytes, False
                return

            messages = data.get("messages", [])
            if not messages:
                break
            
            msg = messages[0]
            if msg.get("status") != "ok":
                # Maybe no papers in this timeframe
                break

            count = msg.get("count", 0)
            if count == 0:
                break

            collection = data.get("collection", [])
            print(f"    Found {len(collection)} papers at cursor {cursor}")
            
            for paper in collection:
                if total_downloaded_bytes >= max_bytes:
                    print(f"\n[LIMIT REACHED] Max storage reached during downloading.")
                    yield total_downloaded_bytes, True
                    return
                
                doi = paper.get("doi")
                version = paper.get("version", "1")
                title = paper.get("title", "Untitled")

                if not doi:
                    continue
                
                doi_safe = get_doi_safe(doi)
                if doi_safe in downloaded_dois:
                    print(f"    SKIP (already downloaded/tracked): {doi_safe}")
                    continue
                
                safe_title = sanitize_filename(title)[:100]
                filename = f"{safe_title}_{doi_safe}_v{version}.pdf"
                filepath = subject_dir / filename

                 # Direct medRxiv PDF URL
                 # URL format: https://www.medrxiv.org/content/10.1101/2023.01.03.22283605v1.full.pdf
                pdf_url = f"https://www.medrxiv.org/content/{doi}v{version}.full.pdf"

                bytes_dl = await download_pdf(session, pdf_url, filepath)
                if bytes_dl > 0 or filepath.exists():
                    downloaded_dois.add(doi_safe)
                    progress["subject_progress"][subj_name]["downloaded_dois"].append(doi_safe)
                
                total_downloaded_bytes += bytes_dl

            # advance cursor
            cursor += count
            await asyncio.sleep(DELAY_BETWEEN_PAGES)
            
            # If count returned is less than 100, it's the last page
            if count < 100:
                break

        # Move to next month and update progress
        current_iter_date = current_iter_date + relativedelta(months=1)
        progress["subject_progress"][subj_name]["current_year"] = current_iter_date.year
        progress["subject_progress"][subj_name]["current_month"] = current_iter_date.month
        # Save progress at the end of every month block
        yield total_downloaded_bytes, None  # Signal to save progress
        
    # If we naturally finish the subject up to the current date
    yield total_downloaded_bytes, "FINISHED"

# ---------------------------------------------------------------------------
# Main Execution
# ---------------------------------------------------------------------------

async def main():
    import argparse
    parser = argparse.ArgumentParser(description="Download medRxiv preprints by subject")
    parser.add_argument("--output-dir", default="./medrxiv", help="Root output directory")
    parser.add_argument("--max-gb", type=float, default=40.0, help="Maximum gigabytes to download. Script stops after reaching this limit (default: 40).")
    args = parser.parse_args()

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    progress_file = output_dir / "progress.json"
    if progress_file.exists():
        try:
            progress = json.loads(progress_file.read_text())
        except Exception:
            progress = {"completed_subjects": [], "subject_progress": {}}
    else:
        progress = {"completed_subjects": [], "subject_progress": {}}
        
    def save_progress():
        progress_file.write_text(json.dumps(progress, indent=2))

    max_bytes = int(args.max_gb * 1024 * 1024 * 1024)
    total_downloaded_bytes = 0
    limit_reached = False
    
    connector = aiohttp.TCPConnector(ssl=False, limit=5)
    async with aiohttp.ClientSession(connector=connector) as session:
        for subj in SUBJECTS:
            if subj in progress.get("completed_subjects", []):
                print(f"Skipping {subj} (already completed)")
                continue

            # Process subject month-by-month
            # We use an async generator to allow saving progress mid-subject
            processor = process_subject(session, subj, output_dir, progress, max_bytes, total_downloaded_bytes)
            
            subject_aborted = False
            # Iterate the async generator
            async for bytes_down, status_flag in processor:
                total_downloaded_bytes = bytes_down
                save_progress()
                if status_flag == True:
                    limit_reached = True
                    break
                elif status_flag == "FINISHED":
                    break
                elif status_flag == False:
                    subject_aborted = True
                    break
            
            if limit_reached:
                save_progress()
                break
                
            # If we didn't hit the limit and the generator finished, the subject is complete up to today
            if not limit_reached and not subject_aborted:
                if "completed_subjects" not in progress:
                    progress["completed_subjects"] = []
                progress["completed_subjects"].append(subj)
                save_progress()
                print(f"✅ Subject {subj} fully completely processed up to current date.")
                
    print("\n" + "=" * 60)
    print("DONE")
    print(f"Total downloaded in this run: {(total_downloaded_bytes / 1024**3):.2f} GB")
    if limit_reached:
        print(f"Stopped because limit of {args.max_gb} GB was reached.")
    print("=" * 60)

if __name__ == "__main__":
    asyncio.run(main())
