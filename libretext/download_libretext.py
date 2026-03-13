"""
Unified LibreTexts PDF Downloader.

Crawls a specified LibreTexts library (e.g., eng, math) and path (/Courses, /Bookshelves),
discovers campuses/categories, and downloads consolidated PDFs.
Organizes files into a flattened structure: <output_dir>/<category>/<book_name>.pdf

Usage:
  python download_libre.py --url https://eng.libretexts.org --path /Courses
  python download_libre.py --url https://math.libretexts.org --path /Courses
"""

import os
import re
import json
import asyncio
import aiohttp
import argparse
from pathlib import Path
from bs4 import BeautifulSoup
from urllib.parse import urljoin, unquote, quote

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36"
}

def sanitize(name):
    if not name:
        return "untitled"
    name = unquote(name)
    name = re.sub(r'[<>:"/\\|?*]', "", name)
    name = name.replace("\n", " ")
    return re.sub(r'\s+', ' ', name).strip(". ")[:150] or "untitled"

async def get_json_ld(session, url):
    try:
        async with session.get(url, headers=HEADERS, timeout=30) as resp:
            if resp.status != 200:
                return None
            html = await resp.text()
            soup = BeautifulSoup(html, 'html.parser')
            scripts = soup.find_all('script', type='application/ld+json')
            for script in scripts:
                try:
                    data = json.loads(script.string)
                    if 'mainEntity' in data or ('@type' in data and data['@type'] in ['CollectionPage', 'WebPage']):
                        return data
                except (json.JSONDecodeError, TypeError):
                    continue
            return None
    except Exception as e:
        print(f"Error fetching JSON-LD for {url}: {e}")
        return None

async def extract_page_id(session, url):
    try:
        async with session.get(url, headers=HEADERS, timeout=20) as resp:
            if resp.status == 200:
                html = await resp.text()
                match = re.search(r'"id":\s*(\d+)', html)
                if match:
                    return match.group(1)
                match = re.search(r'deki/pages/(\d+)', html)
                if match:
                    return match.group(1)
            return None
    except:
        return None

class UnifiedLibreDownloader:
    def __init__(self, base_url, crawl_path, output_dir, max_gb=80.0, num_workers=8):
        self.base_url = base_url.rstrip("/")
        self.start_url = f"{self.base_url}/{crawl_path.lstrip('/')}"
        self.output_dir = Path(output_dir)
        self.max_bytes = int(max_gb * 1024**3)
        self.total_downloaded = 0
        self.num_workers = num_workers
        self.downloaded_ids = set()
        self.queue = asyncio.Queue()
        self.semaphore = asyncio.Semaphore(num_workers)
        
        # Unique progress file per subdomain and path
        subdomain = self.base_url.split("//")[-1].split(".")[0]
        path_slug = sanitize(crawl_path)
        self.progress_file = self.output_dir / f"progress_{subdomain}_{path_slug}.json"
        
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.load_progress()

    def load_progress(self):
        if self.progress_file.exists():
            try:
                data = json.loads(self.progress_file.read_text())
                self.downloaded_ids = set(data.get("downloaded_ids", []))
                self.total_downloaded = data.get("total_downloaded", 0)
            except:
                pass

    def save_progress(self):
        data = {
            "downloaded_ids": list(self.downloaded_ids),
            "total_downloaded": self.total_downloaded
        }
        self.progress_file.write_text(json.dumps(data, indent=2))

    async def download_worker(self, session):
        while True:
            item = await self.queue.get()
            if item is None:
                break
                
            cat_name, book_name, page_id, url = item
            
            if page_id in self.downloaded_ids:
                print(f"  SKIP (already downloaded): {book_name}")
                self.queue.task_done()
                continue

            if self.total_downloaded >= self.max_bytes:
                print(f"  LIMIT REACHED. Skipping {book_name}")
                self.queue.task_done()
                continue

            # Flattened structure: <cat>/<book>.pdf
            safe_cat = sanitize(cat_name)
            safe_book = sanitize(book_name)
            cat_path = self.output_dir / safe_cat
            cat_path.mkdir(parents=True, exist_ok=True)
            
            filename = f"{safe_book}.pdf"
            filepath = cat_path / filename
            
            # PDF API URL
            pdf_url = f"{self.base_url}/@api/deki/pages/{page_id}/pdf/{quote(book_name)}.pdf"
            
            async with self.semaphore:
                print(f"  Downloading: {book_name} (from {cat_name})...")
                try:
                    async with session.get(pdf_url, headers=HEADERS, timeout=aiohttp.ClientTimeout(total=900)) as resp:
                        if resp.status == 200:
                            content = await resp.read()
                            if content[:4] == b"%PDF":
                                filepath.write_bytes(content)
                                size = len(content)
                                self.total_downloaded += size
                                self.downloaded_ids.add(page_id)
                                self.save_progress()
                                print(f"    OK: {book_name} ({(size/1024**2):.1f} MB)")
                            else:
                                print(f"    FAIL: Link for {book_name} did not return a PDF.")
                        else:
                            print(f"    FAIL: HTTP {resp.status} for {book_name}")
                except Exception as e:
                    print(f"    ERROR downloading {book_name}: {e}")
            
            self.queue.task_done()

    async def run(self):
        connector = aiohttp.TCPConnector(ssl=False, limit=20)
        async with aiohttp.ClientSession(connector=connector) as session:
            print(f"Starting crawl at {self.start_url}")
            root_data = await get_json_ld(session, self.start_url)
            if not root_data:
                print("Failed to find root collection list.")
                return

            entities = []
            if 'mainEntity' in root_data and 'itemListElement' in root_data['mainEntity']:
                entities = root_data['mainEntity']['itemListElement']
            
            print(f"Found {len(entities)} categories/campuses.")
            
            workers = [asyncio.create_task(self.download_worker(session)) for _ in range(self.num_workers)]

            for entity in entities:
                ent_name = entity['name']
                ent_url = entity['url']
                
                print(f"\nScanning: {ent_name}")
                ent_data = await get_json_ld(session, ent_url)
                
                if not ent_data or 'mainEntity' not in ent_data:
                    print(f"  No sub-items found for {ent_name}")
                    continue
                
                for item in ent_data['mainEntity'].get('itemListElement', []):
                    item_name = item['name']
                    item_url = item['url']
                    
                    page_id = None
                    if 'thumbnailUrl' in item:
                        match = re.search(r'/pages/(\d+)/', item['thumbnailUrl'])
                        if match:
                            page_id = match.group(1)
                    
                    if not page_id:
                        page_id = await extract_page_id(session, item_url)
                    
                    if page_id:
                        await self.queue.put((ent_name, item_name, page_id, item_url))
                    else:
                        print(f"  Could not find Page ID for: {item_name}")

            await self.queue.join()
            for _ in range(self.num_workers):
                await self.queue.put(None)
            await asyncio.gather(*workers)

async def main():
    parser = argparse.ArgumentParser(description="Unified LibreTexts Downloader")
    parser.add_argument("--url", type=str, required=True, help="Base library URL (e.g. https://eng.libretexts.org)")
    parser.add_argument("--path", type=str, default="/Courses", help="Crawl path (e.g. /Courses or /Bookshelves)")
    parser.add_argument("--output", type=str, default="libretexts", help="Output directory")
    parser.add_argument("--max-gb", type=float, default=80.0, help="Max GB limit")
    parser.add_argument("--workers", type=int, default=8, help="Number of parallel workers")
    args = parser.parse_args()
    
    downloader = UnifiedLibreDownloader(
        base_url=args.url, 
        crawl_path=args.path, 
        output_dir=args.output,
        max_gb=args.max_gb, 
        num_workers=args.workers
    )
    await downloader.run()
    print("\nAll done.")

if __name__ == "__main__":
    asyncio.run(main())
