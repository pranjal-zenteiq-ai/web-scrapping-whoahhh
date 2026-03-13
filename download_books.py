"""
Open Research Library - Science Books Downloader
Uses crawl4ai with NVIDIA Nemotron LLM to:
  1. Extract book titles + download links via CSS selectors
  2. Filter only English-language books using the LLM
  3. Download PDFs with clean, properly formatted filenames
"""

import asyncio
import json
import os
import re
import sys
import aiohttp
import aiofiles
from pathlib import Path
from dotenv import load_dotenv

# Add project root to path so crawl4ai can be imported
sys.path.insert(0, str(Path(__file__).parent))

from crawl4ai import (
    AsyncWebCrawler,
    BrowserConfig,
    CrawlerRunConfig,
    CacheMode,
    JsonCssExtractionStrategy,
    LLMExtractionStrategy,
    LLMConfig,
)

# ── Configuration ────────────────────────────────────────────────────────────

BASE_URL = (
"https://openresearchlibrary.org/search-results/"
'_cat=%22YAN000000%22%26_f=%22BOOK%22%26eft=false%26sort=true'
)

DOWNLOAD_DIR = Path(__file__).parent / "openresearchlibrary" / "young adult non-fiction"
ITEMS_PER_PAGE = 20
TOTAL_PAGES = 1  # 6 items / 20 per page

# Load env
load_dotenv(Path(__file__).parent / ".llm.env")
NVIDIA_API_KEY = os.getenv("NVIDIA_API_KEY")

if not NVIDIA_API_KEY:
    print("ERROR: NVIDIA_API_KEY not found in .llm.env")
    sys.exit(1)

# Set it in the environment so litellm's nvidia_nim provider can find it automatically
os.environ["NVIDIA_NIM_API_KEY"] = NVIDIA_API_KEY

# ── NVIDIA LLM Config ───────────────────────────────────────────────────────

LLM_PROVIDER = "nvidia_nim/nvidia/nemotron-3-nano-30b-a3b"

# ── CSS Extraction Schema ────────────────────────────────────────────────────

BOOK_SCHEMA = {
    "name": "books",
    "baseSelector": "div.bb-list-tile",
    "fields": [
        {
            "name": "title",
            "selector": "p.bb-list-tile-title",
            "type": "text",
        },
        {
            "name": "subtitle",
            "selector": "p.bb-list-tile-subtitle",
            "type": "text",
            "default": "",
        },
        {
            "name": "download_url",
            "selector": "a.btn-download",
            "type": "attribute",
            "attribute": "href",
        },
    ],
}


def sanitize_filename(name: str) -> str:
    """Clean book title for use as a filename."""
    name = re.sub(r'[<>:"/\\|?*]', '', name)
    name = re.sub(r'\s+', ' ', name).strip()
    if len(name) > 200:
        name = name[:200]
    return name


async def filter_english_books_with_llm(books: list) -> list:
    """
    Use NVIDIA Nemotron LLM to identify which books are in English.
    Sends all titles from a page in one batch for efficiency.
    Returns only the English-language books.
    """
    if not books:
        return []

    from litellm import acompletion

    # Build a numbered list of titles for the LLM
    titles_list = []
    for i, book in enumerate(books):
        title = book.get("title", "")
        subtitle = book.get("subtitle", "")
        full_title = f"{title} - {subtitle}" if subtitle else title
        titles_list.append(f"{i}: {full_title}")

    titles_text = "\n".join(titles_list)

    prompt = f"""Below is a numbered list of book titles. Your task is to determine the language of each title and return ONLY the ones that are in English.

Return a JSON array containing the index numbers of books whose titles are in English.
If a title is mostly English with a few foreign proper nouns or technical terms, count it as English.

CRITICAL INSTRUCTIONS: Do NOT provide any reasoning, explanations, or introductory text. Output ONLY the JSON array. Do not wrap it in markdown block quotes.

Book titles:
{titles_text}

Respond with ONLY a JSON array of integers, nothing else. Example: [0, 1, 3, 5]"""

    try:
        response = await acompletion(
            model=LLM_PROVIDER,
            messages=[{"role": "user", "content": prompt}],
            api_key=NVIDIA_API_KEY,
            temperature=0.0,
            max_tokens=2048,
        )

        message = response.choices[0].message
        content = message.content or ""
        
        # Some models output reasoning in content, some in a separate attribute.
        # We search the content (and reasoning_content if it exists) for the JSON array.
        if hasattr(message, 'reasoning_content') and message.reasoning_content:
            text_to_search = content + " " + message.reasoning_content
        else:
            text_to_search = content

        text_to_search = text_to_search.strip()
        # Extract JSON array from response
        match = re.search(r'\[[\d,\s]*\]', text_to_search)
        if match:
            english_indices = json.loads(match.group())
            english_books = [books[i] for i in english_indices if i < len(books)]
            return english_books
        else:
            print(f"  ⚠ LLM returned unexpected format: {text_to_search[:100]}")
            print("    Skipping this page to avoid downloading non-English books.")
            return []
    except Exception as e:
        print(f"  ⚠ LLM language filter error: {e}")
        print("    Skipping this page to avoid downloading non-English books.")
        return []




async def download_pdf(session: aiohttp.ClientSession, url: str, filepath: Path) -> bool:
    """Download a single PDF file."""
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=120)) as resp:
            if resp.status == 200:
                async with aiofiles.open(filepath, 'wb') as f:
                    async for chunk in resp.content.iter_chunked(8192):
                        await f.write(chunk)
                return True
            else:
                print(f"  ✗ HTTP {resp.status} for {filepath.name}")
                return False
    except Exception as e:
        print(f"  ✗ Download error for {filepath.name}: {e}")
        return False


async def main():
    """Main crawler logic."""
    DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)

    print("=" * 60)
    print("  Open Research Library - Science Books Downloader")
    print("=" * 60)
    print(f"  Target URL : {BASE_URL}")
    print(f"  Save to    : {DOWNLOAD_DIR}")
    print(f"  LLM        : {LLM_PROVIDER}")
    print(f"  Filter     : English-only (via NVIDIA Nemotron)")
    print("=" * 60)

    # ── LLM fallback config (NVIDIA Nemotron via NIM) ────────────────────
    llm_config = LLMConfig(
        provider=LLM_PROVIDER,
        api_token=NVIDIA_API_KEY,
    )

    # ── CSS extraction strategy (primary — fast, no LLM calls) ───────────
    css_strategy = JsonCssExtractionStrategy(schema=BOOK_SCHEMA, verbose=True)

    # ── LLM extraction strategy (fallback if CSS fails) ──────────────────
    llm_strategy = LLMExtractionStrategy(
        llm_config=llm_config,
        instruction=(
            "Extract all book entries from this page. For each book, provide: "
            "'title' (the book title text), 'subtitle' (subtitle if any), and "
            "'download_url' (the full href URL of the download button/link that "
            "points to a PDF). Return a JSON array of objects."
        ),
        extract_type="schema",
        schema='{"title": "string", "subtitle": "string", "download_url": "string"}',
        extra_args={"temperature": 0.0, "max_tokens": 4096},
        verbose=True,
    )

    all_books = []
    total_before_filter = 0

    browser_config = BrowserConfig(
        headless=True,
        viewport_width=1280,
        viewport_height=900,
        verbose=True,
    )

    async with AsyncWebCrawler(config=browser_config) as crawler:
        for page_num in range(1, TOTAL_PAGES + 1):
            print(f"\n{'─' * 40}")
            print(f"  Page {page_num}/{TOTAL_PAGES}")
            print(f"{'─' * 40}")

            if page_num == 1:
                # First page: navigate to URL
                config = CrawlerRunConfig(
                    cache_mode=CacheMode.BYPASS,
                    session_id="orl_session",
                    extraction_strategy=css_strategy,
                    wait_until="networkidle",
                    page_timeout=60000,
                    delay_before_return_html=8.0,
                )
                results = await crawler.arun(url=BASE_URL, config=config)
            else:
                # Subsequent pages: click Next button and wait for content
                js_click_next = """
                (async () => {
                    const nextBtn = document.querySelector('button.bb-pager-next-page-link');
                    if (nextBtn && !nextBtn.disabled) {
                        nextBtn.click();
                        // Wait for new content to load
                        await new Promise(r => setTimeout(r, 6000));
                    }
                })();
                """
                config = CrawlerRunConfig(
                    cache_mode=CacheMode.BYPASS,
                    session_id="orl_session",
                    js_code=js_click_next,
                    js_only=True,
                    extraction_strategy=css_strategy,
                    delay_before_return_html=5.0,
                )
                results = await crawler.arun(url=BASE_URL, config=config)

            # Process results
            books_on_page = []
            for result in (results if isinstance(results, list) else [results]):
                if result.success and result.extracted_content:
                    try:
                        data = json.loads(result.extracted_content)
                        if isinstance(data, list):
                            books_on_page.extend(data)
                    except json.JSONDecodeError:
                        print("  ⚠ CSS extraction returned invalid JSON")

            # If CSS extraction failed, try LLM fallback
            if not books_on_page:
                print("  ⚠ CSS extraction found 0 books - trying LLM fallback...")
                fallback_config = CrawlerRunConfig(
                    cache_mode=CacheMode.BYPASS,
                    session_id="orl_session",
                    js_only=True,
                    extraction_strategy=llm_strategy,
                    delay_before_return_html=2.0,
                )
                results = await crawler.arun(url=BASE_URL, config=fallback_config)
                for result in (results if isinstance(results, list) else [results]):
                    if result.success and result.extracted_content:
                        try:
                            data = json.loads(result.extracted_content)
                            if isinstance(data, list):
                                books_on_page.extend(data)
                        except json.JSONDecodeError:
                            print("  ✗ LLM extraction also failed")

            # Filter valid books (must have title and download_url)
            valid_books = [
                b for b in books_on_page
                if b.get("title") and b.get("download_url")
            ]

            total_before_filter += len(valid_books)
            print(f"  Extracted {len(valid_books)} books from page {page_num}")

            # ── Use NVIDIA LLM to filter English-only books ──────────────
            english_books = await filter_english_books_with_llm(valid_books)
            filtered_out = len(valid_books) - len(english_books)
            if filtered_out > 0:
                print(f"  🔤 Kept {len(english_books)} English books (filtered out {filtered_out} non-English)")
            else:
                print(f"  🔤 All {len(english_books)} books are English")

            for b in english_books:
                title_display = b['title']
                if b.get('subtitle'):
                    title_display += f" - {b['subtitle']}"
                print(f"    • {title_display[:70]}")

            all_books.extend(english_books)

            # Small delay between pages
            if page_num < TOTAL_PAGES:
                await asyncio.sleep(2)

    print(f"\n{'=' * 60}")
    print(f"  Extraction complete!")
    print(f"  Total extracted : {total_before_filter}")
    print(f"  English-only    : {len(all_books)}")
    print(f"  Starting downloads...")
    print(f"{'=' * 60}")

    # ── Download all PDFs ────────────────────────────────────────────────
    downloaded = 0
    skipped = 0
    failed = 0

    async with aiohttp.ClientSession() as session:
        for i, book in enumerate(all_books, 1):
            title = book["title"]
            subtitle = book.get("subtitle", "")
            url = book["download_url"]

            # Build filename from title + subtitle
            if subtitle:
                safe_name = sanitize_filename(f"{title} - {subtitle}")
            else:
                safe_name = sanitize_filename(title)

            if not safe_name:
                safe_name = f"book_{i}"

            filepath = DOWNLOAD_DIR / f"{safe_name}.pdf"

            # Skip if already downloaded
            if filepath.exists() and filepath.stat().st_size > 0:
                print(f"  [{i}/{len(all_books)}] ✓ Already exists: {safe_name}.pdf")
                skipped += 1
                continue

            print(f"  [{i}/{len(all_books)}] ↓ Downloading: {safe_name}.pdf")
            success = await download_pdf(session, url, filepath)
            if success:
                size_mb = filepath.stat().st_size / (1024 * 1024)
                print(f"  [{i}/{len(all_books)}] ✓ Saved ({size_mb:.1f} MB): {safe_name}.pdf")
                downloaded += 1
            else:
                failed += 1

            # Small delay between downloads
            await asyncio.sleep(0.5)

    # ── Summary ──────────────────────────────────────────────────────────
    print(f"\n{'=' * 60}")
    print(f"  DOWNLOAD SUMMARY")
    print(f"{'=' * 60}")
    print(f"  Total extracted   : {total_before_filter}")
    print(f"  English books     : {len(all_books)}")
    print(f"  Downloaded        : {downloaded}")
    print(f"  Already existed   : {skipped}")
    print(f"  Failed            : {failed}")
    print(f"  Save directory    : {DOWNLOAD_DIR}")
    print(f"{'=' * 60}")

    # Save manifest
    manifest_path = DOWNLOAD_DIR / "manifest.json"
    async with aiofiles.open(manifest_path, 'w') as f:
        await f.write(json.dumps(all_books, indent=2))
    print(f"  Manifest saved to: {manifest_path}")


if __name__ == "__main__":
    asyncio.run(main())
