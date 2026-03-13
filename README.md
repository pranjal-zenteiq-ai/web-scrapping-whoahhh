This is a script to download books from Open Research Library using NVIDIA Nemotron LLM to filter only English-language books.

To change the books to download, modify this:


  BASE_URL = (
"https://openresearchlibrary.org/search-results/"
'_cat=%22YAN000000%22%26_f=%22BOOK%22%26eft=false%26sort=true'
)
modify ths BASE_URL to change the books to download

DOWNLOAD_DIR = Path(__file__).parent / "openresearchlibrary" / "young adult non-fiction"
ITEMS_PER_PAGE = 20
TOTAL_PAGES = 1  # 6 items / 20 per page
 
update the DOWNLOAD_DIR to change the download directory and the total pages to download


