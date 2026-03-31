import aiohttp
import asyncio
import os
import json
import re
from urllib.parse import urlparse, parse_qs, unquote
import argparse
from pathlib import Path

headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36",
    "Accept": "application/json",
    "Content-Type": "application/json"
}

PROGRESS_FILE = Path("nasa/progress.json")

def load_progress(subject):
    if PROGRESS_FILE.exists():
        try:
            with PROGRESS_FILE.open('r') as f:
                data = json.load(f)
        except json.JSONDecodeError:
            data = {}
    else:
        data = {}

    if "subjects" not in data:
        data["subjects"] = {}
        
    if subject not in data["subjects"]:
        data["subjects"][subject] = {
            "downloaded_ids": [],
            "from_record": 0
        }
        
    return data

def save_progress(data):
    # Ensure directory exists before saving
    PROGRESS_FILE.parent.mkdir(parents=True, exist_ok=True)
    with PROGRESS_FILE.open('w') as f:
        json.dump(data, f, indent=4)

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

async def download_file(session, url, filepath):
    """Download a single file using aiohttp for binary writing."""
    for attempt in range(3):
        try:
            async with session.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=180)) as resp:
                if resp.status == 200:
                    content = await resp.read()
                    if content[:4] != b"%PDF":
                        print(f"    SKIP (not a PDF): {filepath.name[:50]}")
                        return False, 0
                    filepath.write_bytes(content)
                    print(f"    OK: {filepath.name[:50]} ({len(content) // 1024} KB)")
                    await asyncio.sleep(0.5)
                    return True, len(content)
                else:
                    print(f"    FAIL: {filepath.name[:50]} HTTP {resp.status}")
                    return False, 0
        except Exception as e:
            print(f"    FAIL download attempt {attempt+1}: {filepath.name[:50]} — {e}")
            await asyncio.sleep(2)
    return False, 0

async def fetch_page(session, subject, from_record, size=25):
    """Fetch a page of metadata from NASA NTRS API using POST."""
    url = "https://ntrs.nasa.gov/api/citations/search"
    payload = {
        "subjectCategory": [subject],
        "page": {
            "from": from_record,
            "size": size
        }
    }
    
    for attempt in range(3):
        try:
            async with session.post(url, headers=headers, json=payload, timeout=aiohttp.ClientTimeout(total=30)) as resp:
                if resp.status == 200:
                    return await resp.json()
                elif resp.status == 429:
                    print(f"    Rate limited on API. Waiting 10s...")
                    await asyncio.sleep(10)
                else:
                    print(f"    API fetch failed HTTP {resp.status}")
                    return None
        except Exception as e:
            print(f"    API fetch error attempt {attempt+1}: {e}")
            await asyncio.sleep(2)
    return None

async def main(url="https://ntrs.nasa.gov/search?subjectCategory=Plasma%20Physics&page=%7B%22from%22:0,%22size%22:25%7D"):
    print("="*60)
    print(f"NASA NTRS Downloader")
    print(f"Source URL: {url}")
    print("="*60)
    
    # 1. Parse the subject from the URL
    parsed_url = urlparse(url)
    query_params = parse_qs(parsed_url.query)
    
    if 'subjectCategory' not in query_params:
        print("Error: Could not find 'subjectCategory' in the URL query string.")
        return
        
    subject = query_params['subjectCategory'][0]
    subject = unquote(subject)
    print(f"Subject extracted: {subject}")
    
    # Load progress
    progress_data = load_progress(subject)
    downloaded_ids = set(progress_data["subjects"][subject]["downloaded_ids"])
    from_record = progress_data["subjects"][subject]["from_record"]
    
    # 2. Setup folders
    base_dir = Path("nasa")
    subject_dir = base_dir / sanitize_foldername(subject)
    subject_dir.mkdir(parents=True, exist_ok=True)
    print(f"Downloading to: {subject_dir.absolute()}")
    print(f"Resuming from record: {from_record}")
    print(f"Previously downloaded files: {len(downloaded_ids)}")
    
    # 3. Download Process
    downloads_made = 0
    total_bytes = 0
    page_size = 25
    
    connector = aiohttp.TCPConnector(ssl=False, limit=5)
    
    async with aiohttp.ClientSession(connector=connector) as session:
        while True:
            print(f"\nFetching records starting from {from_record}...")
            data = await fetch_page(session, subject, from_record, page_size)
            
            if not data or 'results' not in data:
                print("Failed to get data or no results. Stopping.")
                break
                
            results = data['results']
            total = data.get('stats', {}).get('total', 0)
            
            if not results:
                print("No more results.")
                break
                
            print(f"Processing {len(results)} items (Total available: {total})")
            
            for item in results:
                title = item.get('title', 'Unknown Title')
                sub_id = item.get('id', 'UnknownID')
                cui_submission_id = item.get('copyright', {}).get('submissionId', sub_id)
                downloads = item.get('downloads', [])
                
                # Check progress tracker first
                if cui_submission_id in downloaded_ids:
                    print(f"  SKIP (Already tracked): {title[:60]}...")
                    continue
                
                # Find PDF download link
                pdf_url = None
                for d in downloads:
                    if d.get('mimetype') == 'application/pdf' and 'links' in d and 'pdf' in d['links']:
                        pdf_url = "https://ntrs.nasa.gov" + d['links']['pdf']
                        break
                
                if not pdf_url:
                    print(f"  SKIP (No valid PDF link): {title[:60]}...")
                    # Even if there's no PDF, mark it as tracked so we don't keep polling it needlessly
                    downloaded_ids.add(cui_submission_id)
                    continue
                
                filename_safe = sanitize_filename(f"{title}_{cui_submission_id}.pdf")
                file_path = subject_dir / filename_safe
                
                if file_path.exists():
                    print(f"  SKIP (File exists natively): {title[:60]}...")
                    downloaded_ids.add(cui_submission_id)
                    continue
                    
                print(f"  Downloading: {title[:60]}...")
                success, b = await download_file(session, pdf_url, file_path)
                if success and b > 0:
                    downloads_made += 1
                    total_bytes += b
                    downloaded_ids.add(cui_submission_id)
            
            # Save Progress iteratively
            from_record += len(results)
            progress_data["subjects"][subject]["downloaded_ids"] = list(downloaded_ids)
            progress_data["subjects"][subject]["from_record"] = from_record
            save_progress(progress_data)
            
            # If we've reached the end
            if from_record >= total:
                print("\nReached the end of the required records.")
                break
            
            await asyncio.sleep(1) # Polite delay
                
    print("\n" + "="*60)
    print("DONE")
    print(f"Total new PDFs downloaded: {downloads_made}")
    print(f"Total data transferred: {total_bytes / (1024*1024):.2f} MB")
    print("="*60)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Download from NASA NTRS")
    parser.add_argument("--url", help="NASA NTRS search URL", default="https://ntrs.nasa.gov/search?subjectCategory=Plasma%20Physics&page=%7B%22from%22:0,%22size%22:25%7D")
    args = parser.parse_args()
    
    asyncio.run(main(args.url))

