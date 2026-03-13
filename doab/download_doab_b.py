"""
Download all DOAB books under letter "B" subjects.

Scrapes subjects starting with "B" from DOAB browse page, then for each
subject scrapes all books, resolves PDF download links, and downloads
PDFs into B/<subject_name>/ folder structure.

Usage:
    python download_doab_B.py --output-dir ./B
"""

import os
import re
import sys
import json
import asyncio
import aiohttp
from pathlib import Path
from urllib.parse import urlparse, urljoin, unquote
from html.parser import HTMLParser

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
DOAB_BASE = "https://directory.doabooks.org"
DOAB_REST = "https://directory.doabooks.org/rest"
OAPEN_REST = "https://library.oapen.org/rest"

# Browse URL for letter B subjects (rpp=100 to minimize pagination)
BROWSE_B_URL = (
    "https://directory.doabooks.org//browse?"
    "rpp=100&sort_by=-1&type=classification_text"
    "&etal=-1&starts_with=B&order=ASC"
)

# Rate-limit settings
DELAY_BETWEEN_SUBJECTS = 3       # seconds between subject page requests
DELAY_BETWEEN_BOOKS = 1          # seconds between book page requests
DELAY_BETWEEN_DOWNLOADS = 1      # seconds between file downloads
DELAY_ON_429 = 30                # seconds to wait on HTTP 429
MAX_RETRIES_429 = 5              # max retries on 429 before giving up

# ---------------------------------------------------------------------------
# HTML Parsing Utilities
# ---------------------------------------------------------------------------

class SubjectListParser(HTMLParser):
    """Parse the 'browse by subject' page to extract subject names + URLs."""

    def __init__(self):
        super().__init__()
        self.subjects = []  # List of {"name": ..., "url": ...}
        self.next_page = None
        self._in_a = False
        self._href = None
        self._text = ""

    def handle_starttag(self, tag, attrs):
        attrs_dict = dict(attrs)
        if tag == "a" and "href" in attrs_dict:
            href = attrs_dict["href"]
            # Subject links match: /browse?type=classification_text&value=...
            if "type=classification_text" in href and "value=" in href and "starts_with" not in href:
                self._in_a = True
                self._href = href
                self._text = ""
            # Next-page link
            if "next-page-link" in attrs_dict.get("class", ""):
                self.next_page = href

    def handle_endtag(self, tag):
        if tag == "a" and self._in_a:
            name = self._text.strip()
            if name and self._href:
                self.subjects.append({"name": name, "url": self._href})
            self._in_a = False
            self._href = None

    def handle_data(self, data):
        if self._in_a:
            self._text += data


class BookListParser(HTMLParser):
    """Parse a book listing page to extract book handle URLs + titles."""

    def __init__(self):
        super().__init__()
        self.books = []
        self.next_page = None
        self.current_a_href = None
        self.current_text = ""

    def handle_starttag(self, tag, attrs):
        attrs_dict = dict(attrs)
        if tag == "a" and "href" in attrs_dict:
            href = attrs_dict["href"]
            if "/handle/" in href:
                self.current_a_href = href
                self.current_text = ""
            if "next-page-link" in attrs_dict.get("class", ""):
                self.next_page = href

    def handle_endtag(self, tag):
        if tag == "a" and self.current_a_href:
            title = self.current_text.strip()
            if title:
                self.books.append({"url": self.current_a_href, "title": title})
            self.current_a_href = None

    def handle_data(self, data):
        if self.current_a_href:
            self.current_text += data


class BookPageParser(HTMLParser):
    """Parse individual book page for PDF / download links."""

    def __init__(self):
        super().__init__()
        self.pdf_links = []
        self.external_links = []
        self.current_a = False
        self.current_href = None
        self.current_text = ""

    def handle_starttag(self, tag, attrs):
        if tag == "a":
            attrs_dict = dict(attrs)
            if "href" in attrs_dict:
                self.current_href = attrs_dict["href"]
                self.current_a = True
                self.current_text = ""

    def handle_endtag(self, tag):
        if tag == "a" and self.current_a:
            text = self.current_text.strip().lower()
            link = self.current_href.strip()
            link_low = link.lower()

            if "pdf" in link_low or "pdf" in text or "download" in link_low or "download" in text or "bitstream" in link_low:
                if not link_low.startswith("javascript"):
                    self.pdf_links.append(link)

            if link_low.startswith("http") and "directory.doabooks.org" not in link_low:
                if "doi.org" in link_low or "oapen.org" in link_low or "tudelft.nl" in link_low or "publisher" in text:
                    self.external_links.append(link)

            self.current_a = False
            self.current_href = None

    def handle_data(self, data):
        if self.current_a:
            self.current_text += data


class ExternalPageParser(HTMLParser):
    """Parse external publisher pages for PDF download links."""

    def __init__(self):
        super().__init__()
        self.pdf_links = []
        self._in_a = False
        self._href = None
        self._text = ""

    def handle_starttag(self, tag, attrs):
        if tag in ("a", "button"):
            attrs_dict = dict(attrs)
            if "href" in attrs_dict:
                self._in_a = True
                self._href = attrs_dict["href"]
                self._text = ""

    def handle_endtag(self, tag):
        if tag in ("a", "button") and self._in_a:
            if self._href:
                sub_href = self._href
                sub_text = self._text.lower()
                if "pdf" in sub_href.lower() or "download" in sub_href.lower() or "pdf" in sub_text:
                    if not sub_href.startswith("#") and not sub_href.startswith("javascript"):
                        self.pdf_links.append(sub_href)
            self._in_a = False
            self._href = None

    def handle_data(self, data):
        if self._in_a:
            self._text += data


# ---------------------------------------------------------------------------
# Utility Functions
# ---------------------------------------------------------------------------

def sanitize_filename(name):
    if not name:
        return "untitled"
    name = re.sub(r'[<>:"/\\|?*]', "", name)
    return name.strip(". ")[:200] or "untitled"


def sanitize_foldername(name):
    """Sanitize subject name for use as a folder name."""
    if not name:
        return "untitled"
    name = re.sub(r'[<>:"/\\|?*]', "", name)
    name = name.replace("/", "-")
    return name.strip(". ")[:150] or "untitled"


async def fetch_with_retry(session, url, max_retries=MAX_RETRIES_429, delay_429=DELAY_ON_429):
    """Fetch a URL with automatic retry on HTTP 429."""
    headers = {"User-Agent": "Mozilla/5.0 (X11; Linux x86_64; rv:115.0) Gecko/20100101 Firefox/115.0"}
    for attempt in range(max_retries):
        try:
            async with session.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=30)) as resp:
                if resp.status == 200:
                    return await resp.text(), 200
                elif resp.status == 429:
                    wait = delay_429 * (attempt + 1)  # Linear backoff
                    print(f"    ⚠ HTTP 429 rate-limited. Waiting {wait}s... (attempt {attempt + 1}/{max_retries})")
                    await asyncio.sleep(wait)
                else:
                    return None, resp.status
        except asyncio.TimeoutError:
            print(f"    ⚠ Timeout on {url}. Retrying ({attempt + 1}/{max_retries})...")
            await asyncio.sleep(5)
        except Exception as e:
            print(f"    ⚠ Error fetching {url}: {e}")
            return None, -1
    return None, 429  # All retries exhausted


# ---------------------------------------------------------------------------
# Phase 1: Scrape all subjects starting with "B"
# ---------------------------------------------------------------------------

async def fetch_subjects(session, start_url):
    """Fetch all subject categories for letter B (with pagination)."""
    subjects = []
    seen_names = set()
    current_url = start_url

    while current_url:
        print(f"Scraping subjects page: {current_url}")
        html, status = await fetch_with_retry(session, current_url)
        if html is None:
            print(f"  Failed to fetch subjects page (status {status}). Stopping.")
            break

        parser = SubjectListParser()
        parser.feed(html)

        # Fallback: regex-based next-page detection
        if not parser.next_page:
            match = re.search(
                r'<a[^>]+href="([^"]+)"[^>]*>\s*(?:Next|next)[^<]*</a>',
                html, re.IGNORECASE,
            )
            if match:
                parser.next_page = match.group(1)

        new_count = 0
        for subj in parser.subjects:
            if subj["name"] not in seen_names:
                seen_names.add(subj["name"])
                subj["url"] = urljoin(DOAB_BASE, subj["url"])
                subjects.append(subj)
                new_count += 1

        print(f"  Found {new_count} new subjects (total: {len(subjects)})")

        if parser.next_page:
            next_url = urljoin(DOAB_BASE, parser.next_page)
            if next_url == current_url:
                break
            current_url = next_url
            await asyncio.sleep(2)
        else:
            break

    return subjects


# ---------------------------------------------------------------------------
# Phase 2: Scrape books for a single subject (with pagination)
# ---------------------------------------------------------------------------

async def fetch_books_for_subject(session, subject_url):
    """Fetch all book handles listed under a subject page."""
    books = []
    seen_urls = set()
    current_url = subject_url

    while current_url:
        html, status = await fetch_with_retry(session, current_url)
        if html is None:
            if status == 429:
                print(f"    ⛔ Rate limit exhausted for subject page. Skipping.")
            else:
                print(f"    ⛔ HTTP {status} on subject page.")
            return None  # Return None to signal failure (don't mark as completed)

        parser = BookListParser()
        parser.feed(html)

        # Fallback next-page regex
        if not parser.next_page:
            match = re.search(
                r'<a[^>]+href="([^"]+)"[^>]*>\s*(?:Next|next)[^<]*</a>',
                html, re.IGNORECASE,
            )
            if match:
                parser.next_page = match.group(1)

        for book in parser.books:
            full_url = urljoin(DOAB_BASE, book["url"])
            if full_url not in seen_urls:
                seen_urls.add(full_url)
                books.append({"url": full_url, "title": book["title"]})

        if parser.next_page:
            next_url = urljoin(DOAB_BASE, parser.next_page)
            if next_url == current_url:
                break
            current_url = next_url
            await asyncio.sleep(DELAY_BETWEEN_BOOKS)
        else:
            break

    return books


# ---------------------------------------------------------------------------
# Phase 3: Resolve PDF URL for a book
# ---------------------------------------------------------------------------

async def find_pdf_url(session, book_page_url):
    """Fetch the DOAB book page and find a downloadable PDF link."""
    headers = {"User-Agent": "Mozilla/5.0"}
    try:
        html, status = await fetch_with_retry(session, book_page_url)
        if html is None:
            return None

        parser = BookPageParser()
        parser.feed(html)

        # 1. Direct .pdf links
        best_link = None
        for link in parser.pdf_links:
            if link.startswith("#"):
                continue
            full_link = urljoin(book_page_url, link)
            if ".pdf" in full_link.lower():
                return full_link
            if best_link is None and (
                "download" in full_link.lower() or "bitstream" in full_link.lower()
            ):
                best_link = full_link

        if best_link:
            return best_link

        # 2. External links (OAPEN, DOI, publisher)
        for ext_link in list(set(parser.external_links)):
            # OAPEN handle → REST API
            if "library.oapen.org/handle/" in ext_link:
                handle_id = ext_link.split("/handle/")[-1]
                try:
                    url = f"{OAPEN_REST}/handle/{handle_id}?format=json"
                    async with session.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=15)) as resp:
                        if resp.status == 200:
                            item = await resp.json()
                            oapen_uuid = item.get("uuid")
                            if oapen_uuid:
                                b_url = f"{OAPEN_REST}/items/{oapen_uuid}/bitstreams?format=json"
                                async with session.get(b_url, headers=headers, timeout=aiohttp.ClientTimeout(total=15)) as b_resp:
                                    if b_resp.status == 200:
                                        b_list = await b_resp.json()
                                        for bs in b_list:
                                            b_name = bs.get("name", "").lower()
                                            if bs.get("bundleName") == "ORIGINAL" and b_name.endswith(".pdf"):
                                                return f"https://library.oapen.org{bs['retrieveLink']}"
                except Exception:
                    pass

            # Follow DOI / publisher links
            if any(x in ext_link for x in ("doi.org", "tudelft.nl", "oapen.org", "book", "springer")):
                try:
                    async with session.get(ext_link, headers=headers, allow_redirects=True, timeout=aiohttp.ClientTimeout(total=20)) as ext_resp:
                        if ext_resp.status in (200, 301, 302, 303, 307):
                            ext_html = await ext_resp.text()
                            ext_parser = ExternalPageParser()
                            ext_parser.feed(ext_html)
                            for sub_href in ext_parser.pdf_links:
                                final_url = urljoin(str(ext_resp.url), sub_href)
                                return final_url
                except Exception:
                    pass

        return None
    except Exception as e:
        print(f"    Error finding PDF for {book_page_url}: {e}")
        return None


# ---------------------------------------------------------------------------
# Download a single book
# ---------------------------------------------------------------------------

async def download_book(session, book, output_dir):
    """Download a single book PDF to output_dir."""
    title = book["title"]
    url = book.get("download_link")

    if not url:
        print(f"    SKIP (no link): {title[:70]}")
        return False

    ext = ".pdf" if ".pdf" in url.lower() else ".epub" if ".epub" in url.lower() else ".pdf"
    filename = f"{sanitize_filename(title)}{ext}"
    filepath = output_dir / filename

    if filepath.exists():
        print(f"    SKIP (exists): {filename[:70]}")
        return True

    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/115.0.0.0 Safari/537.36"
        ),
    }

    for attempt in range(3):
        try:
            async with session.get(
                url, headers=headers, timeout=aiohttp.ClientTimeout(total=180)
            ) as resp:
                if resp.status == 200:
                    content = await resp.read()
                    # Verify it's actually a PDF (starts with %PDF)
                    if content[:4] != b"%PDF" and ext == ".pdf":
                        print(f"    SKIP (not a PDF): {filename[:70]}")
                        return False
                    filepath.write_bytes(content)
                    print(f"    OK: {filename[:70]} ({len(content) // 1024} KB)")
                    await asyncio.sleep(DELAY_BETWEEN_DOWNLOADS)
                    return True
                elif resp.status == 429:
                    wait = DELAY_ON_429 * (attempt + 1)
                    print(f"    RATE LIMITED on download {filename[:50]}. Waiting {wait}s...")
                    await asyncio.sleep(wait)
                else:
                    print(f"    FAIL: {filename[:70]} HTTP {resp.status}")
                    return False
        except asyncio.TimeoutError:
            print(f"    TIMEOUT: {filename[:70]}. Retrying ({attempt + 1}/3)...")
            await asyncio.sleep(5)
        except Exception as e:
            print(f"    FAIL: {filename[:70]} — {e}")
            return False

    return False


# ---------------------------------------------------------------------------
# Progress Tracking
# ---------------------------------------------------------------------------

def load_progress(progress_file):
    if progress_file.exists():
        return json.loads(progress_file.read_text())
    return {"completed_subjects": []}


def save_progress(progress_file, progress):
    progress_file.write_text(json.dumps(progress, indent=2))


# ---------------------------------------------------------------------------
# Main Routine
# ---------------------------------------------------------------------------

async def main():
    import argparse

    parser = argparse.ArgumentParser(
        description="Download DOAB books for letter B, organized by subject."
    )
    parser.add_argument(
        "--output-dir", default="./B",
        help="Root output directory (default: ./B)",
    )
    parser.add_argument(
        "--reset-progress", action="store_true",
        help="Reset progress and start fresh.",
    )
    args = parser.parse_args()

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    progress_file = output_dir / "progress.json"
    if args.reset_progress and progress_file.exists():
        progress_file.unlink()
        print("Progress reset.\n")

    progress = load_progress(progress_file)

    connector = aiohttp.TCPConnector(ssl=False, limit=3)
    async with aiohttp.ClientSession(connector=connector) as session:
        # ── Phase 1: Scrape all subjects under "B" ──────────────────────
        print("=" * 60)
        print("PHASE 1: Scraping subject categories for letter B")
        print("=" * 60)
        subjects = await fetch_subjects(session, BROWSE_B_URL)
        print(f"\nTotal subjects found: {len(subjects)}\n")

        if not subjects:
            print("No subjects found. Exiting.")
            return

        # ── Phase 2 & 3: For each subject, scrape books and download ───
        total_downloaded = 0
        total_failed = 0
        total_skipped_subjects = 0

        for subj_idx, subject in enumerate(subjects, 1):
            subj_name = subject["name"]
            subj_url = subject["url"]
            folder_name = sanitize_foldername(subj_name)

            if subj_name in progress["completed_subjects"]:
                print(f"[{subj_idx}/{len(subjects)}] SKIP (done): {subj_name}")
                continue

            print("\n" + "=" * 60)
            print(f"[{subj_idx}/{len(subjects)}] Subject: {subj_name}")
            print(f"  URL: {subj_url}")
            print("=" * 60)

            # Wait between subjects to avoid rate limiting
            await asyncio.sleep(DELAY_BETWEEN_SUBJECTS)

            # Phase 2: Fetch books for this subject
            books = await fetch_books_for_subject(session, subj_url)

            # If None, it means we got rate-limited — skip but DON'T mark as completed
            if books is None:
                print(f"  ⛔ Could not fetch books (rate limited). Will retry next run.")
                total_skipped_subjects += 1
                # Wait extra time before next subject
                await asyncio.sleep(DELAY_ON_429)
                continue

            # Create subject folder
            subject_dir = output_dir / folder_name
            subject_dir.mkdir(parents=True, exist_ok=True)

            print(f"  Books found: {len(books)}")

            if not books:
                # Genuinely empty subject — mark as completed
                progress["completed_subjects"].append(subj_name)
                save_progress(progress_file, progress)
                continue

            # De-duplicate
            seen, unique = set(), []
            for b in books:
                if b["url"] not in seen:
                    seen.add(b["url"])
                    unique.append(b)

            print(f"  Unique books: {len(unique)}")

            # Phase 3: Resolve download links + download
            ok = fail = 0
            for book_idx, book in enumerate(unique, 1):
                # Resolve PDF URL
                pdf_url = await find_pdf_url(session, book["url"])
                book["download_link"] = pdf_url

                # Download
                success = await download_book(session, book, subject_dir)
                if success:
                    ok += 1
                else:
                    fail += 1

                if book_idx % 5 == 0:
                    print(f"  Progress: {book_idx}/{len(unique)} (OK: {ok}, Fail: {fail})")

                await asyncio.sleep(DELAY_BETWEEN_BOOKS)

            total_downloaded += ok
            total_failed += fail
            print(f"  ✅ Subject done: {ok} downloaded, {fail} failed")

            # Mark subject as completed
            progress["completed_subjects"].append(subj_name)
            save_progress(progress_file, progress)

        # ── Summary ─────────────────────────────────────────────────────
        print("\n" + "=" * 60)
        print("ALL DONE")
        print(f"  Total downloaded: {total_downloaded}")
        print(f"  Total failed: {total_failed}")
        if total_skipped_subjects:
            print(f"  Subjects skipped (rate limited): {total_skipped_subjects}")
            print(f"  → Re-run the script to retry those subjects.")
        print(f"  Subjects completed: {len(progress['completed_subjects'])}/{len(subjects)}")
        print("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())
