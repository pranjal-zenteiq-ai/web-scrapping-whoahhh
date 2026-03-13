"""
Download David Tong's Cambridge physics lecture notes.

Crawls the teaching page, finds all subjects, scans each subject's
page for PDF links, and downloads them. Features duplicate detection via progress.json.
"""

import os
import re
import json
import asyncio
import aiohttp
from urllib.parse import urljoin, unquote
from pathlib import Path
from bs4 import BeautifulSoup

BASE_URL = "https://www.damtp.cam.ac.uk/user/tong/teaching.html"
OUTPUT_DIR = Path("cambridge")

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36",
}

def sanitize_foldername(name):
    if not name:
        return "untitled"
    # Decode URL-encoded characters (like %20)
    name = unquote(name)
    # Remove characters invalid in Windows/Linux filenames
    name = re.sub(r'[<>:"/\\|?*]', "", name)
    name = name.replace("\n", " ").strip(". ")
    return name[:150] or "untitled"

async def fetch_html(session, url):
    try:
        async with session.get(url, headers=HEADERS, timeout=30) as resp:
            if resp.status == 200:
                return await resp.text()
            else:
                print(f"    Failed to fetch {url}: HTTP {resp.status}")
    except Exception as e:
        print(f"    Error fetching {url}: {e}")
    return None

async def download_pdf(session, url, filepath):
    if filepath.exists():
        return True
    
    for attempt in range(3):
        try:
            async with session.get(url, headers=HEADERS, timeout=aiohttp.ClientTimeout(total=180)) as resp:
                if resp.status == 200:
                    content = await resp.read()
                    if content[:4] == b"%PDF":
                        filepath.write_bytes(content)
                        print(f"    OK: {filepath.name} ({len(content)//1024} KB)")
                        return True
                    else:
                        print(f"    SKIP (not a PDF): {filepath.name}")
                        return False
                else:
                    print(f"    FAIL: HTTP {resp.status} on {url}")
                    return False
        except asyncio.TimeoutError:
            print(f"    TIMEOUT: {filepath.name}. Retrying ({attempt+1}/3)...")
            await asyncio.sleep(2)
        except Exception as e:
            print(f"    ERROR: {filepath.name} - {e}. Retrying ({attempt+1}/3)...")
            await asyncio.sleep(2)
    return False

async def main():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    
    progress_file = OUTPUT_DIR / "progress.json"
    if progress_file.exists():
        progress = json.loads(progress_file.read_text())
    else:
        progress = {"downloaded_pdfs": []}
        
    downloaded_pdfs = set(progress["downloaded_pdfs"])
    
    def save_progress():
        progress["downloaded_pdfs"] = list(downloaded_pdfs)
        progress_file.write_text(json.dumps(progress, indent=2))
        
    connector = aiohttp.TCPConnector(ssl=False, limit=10)
    async with aiohttp.ClientSession(connector=connector) as session:
        print("Fetching main course list...")
        html = await fetch_html(session, BASE_URL)
        if not html:
            print("Could not fetch main page. Exiting.")
            return

        soup = BeautifulSoup(html, 'html.parser')
        subjects = []
        
        # Scrape subjects: <a href="subject.html"><h2>Subject Title</h2></a>
        for a_tag in soup.find_all('a', href=True):
            h2 = a_tag.find('h2')
            if h2:
                subject_name = h2.text.strip()
                subject_url = urljoin(BASE_URL, a_tag['href'])
                if subject_name and subject_url:
                    subjects.append((subject_name, subject_url))
        
        # Deduplicate globally while preserving order
        seen = set()
        unique_subjects = []
        for s, u in subjects:
            if s not in seen:
                seen.add(s)
                unique_subjects.append((s, u))
                
        print(f"Found {len(unique_subjects)} coursework subjects.")
        
        for subj_name, subj_url in unique_subjects:
            print(f"\n============================================================")
            print(f"Processing Subject: {subj_name}")
            print(f"URL: {subj_url}")
            print(f"============================================================")
            
            subj_dir = OUTPUT_DIR / sanitize_foldername(subj_name)
            subj_dir.mkdir(parents=True, exist_ok=True)
            
            subj_html = await fetch_html(session, subj_url)
            if not subj_html:
                print("  Failed to fetch subject HTML. Skipping.")
                continue
                
            subj_soup = BeautifulSoup(subj_html, 'html.parser')
            
            # Find all PDF links on the subject's page
            pdf_links = []
            for a_tag in subj_soup.find_all('a', href=True):
                href = a_tag['href']
                if href.lower().endswith('.pdf'):
                    pdf_link = urljoin(subj_url, href)
                    pdf_links.append(pdf_link)
                    
            print(f"  Found {len(pdf_links)} PDF links.")
            
            for pdf_url in pdf_links:
                if pdf_url in downloaded_pdfs:
                    print(f"    SKIP (already downloaded): {pdf_url.split('/')[-1]}")
                    continue
                    
                filename = unquote(pdf_url.split("/")[-1])
                if not filename.lower().endswith('.pdf'):
                    filename = "document.pdf"
                    
                filename = sanitize_foldername(filename)
                filepath = subj_dir / filename
                
                success = await download_pdf(session, pdf_url, filepath)
                if success:
                    downloaded_pdfs.add(pdf_url)
                    save_progress()
            
            print(f"✅ Subject {subj_name} processed.")

    print("\nDONE.")

if __name__ == "__main__":
    asyncio.run(main())
