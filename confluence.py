import os
import re
import time
import json
import pickle
import requests
from tqdm import tqdm
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from bs4 import BeautifulSoup
from urllib.parse import urlparse, unquote, urljoin, urlunparse, parse_qs

"""
Confluence Space Downloader
- Logs in via Selenium (manual login supported), mirrors cookies into requests.Session
- Crawls a single SPACE, saving pages as full HTML (with <head>), and assets locally
- Rewrites internal links to relative local files (../pages/<Title>.html)
- Skips pages that have no content AND no internal same-space links
- Prevents cycles by skipping already visited URLs early and avoiding dup enqueue

Requires a config.json with at least:
{
  "base_url": "https://example.atlassian.net/wiki",  # or site /confluence base
  "space_key": "SPACE",
  "start_page": "Home",
  "cookies_file": "cookies.pkl"
}
"""

# =========================
# Load Configuration
# =========================
CONFIG_FILE = "config.json"
with open(CONFIG_FILE, "r", encoding="utf-8") as f:
    config = json.load(f)

BASE_URL = config["base_url"].rstrip("/")
SPACE_KEY = config["space_key"]
START_PAGE = f"{BASE_URL}/display/{SPACE_KEY}/{config['start_page']}"
COOKIES_FILE = config["cookies_file"]

# =========================
# Output Directories
# =========================
SAVE_DIR = SPACE_KEY
PAGE_DIR = os.path.join(SAVE_DIR, "pages")
ATTACHMENT_DIR = os.path.join(SAVE_DIR, "attachments")
IMAGE_DIR = os.path.join(SAVE_DIR, "images")
STYLE_DIR = os.path.join(SAVE_DIR, "styles")
SCRIPT_DIR = os.path.join(SAVE_DIR, "scripts")

os.makedirs(PAGE_DIR, exist_ok=True)
os.makedirs(ATTACHMENT_DIR, exist_ok=True)
os.makedirs(IMAGE_DIR, exist_ok=True)
os.makedirs(STYLE_DIR, exist_ok=True)
os.makedirs(SCRIPT_DIR, exist_ok=True)

# =========================
# WebDriver (Selenium) + requests.Session
# =========================
options = Options()
# options.add_argument("--headless=new")  # Uncomment for headless runs
options.add_experimental_option("excludeSwitches", ["enable-automation"]) 
options.add_experimental_option("useAutomationExtension", False)

driver = webdriver.Chrome(service=Service(), options=options)
session = requests.Session()

# =========================
# Cookie helpers
# =========================

def load_cookies():
    """Load cookies from disk into Selenium and requests.Session."""
    if os.path.exists(COOKIES_FILE):
        driver.get(BASE_URL)
        with open(COOKIES_FILE, "rb") as f:
            cookies = pickle.load(f)
            for cookie in cookies:
                try:
                    driver.add_cookie(cookie)
                except Exception:
                    pass
                session.cookies.set(cookie["name"], cookie["value"])  # domain-less set is fine for our session


def save_cookies():
    """Persist Selenium cookies and mirror into requests.Session."""
    cookies = driver.get_cookies()
    with open(COOKIES_FILE, "wb") as f:
        pickle.dump(cookies, f)
    for cookie in cookies:
        try:
            driver.add_cookie(cookie)
        except Exception:
            pass
        session.cookies.set(cookie["name"], cookie["value"]) 

# =========================
# Normalization & URL utilities
# =========================

def normalize_text(text: str) -> str:
    if not text:
        return ""
    text = unquote(text).replace("+", " ")
    text = re.sub(r"[^A-Za-z0-9\- _]+", "", text)
    text = re.sub(r"\s+", " ", text).strip().lower()
    return text


def get_space_and_title_from_url(url: str):
    """Return (space_key, title_str) for /display or /pages/viewpage.action URLs; else (None, None)."""
    u = urlparse(url)
    path = u.path or ""

    # /display/<SPACE>/<TITLE>
    if "/display/" in path:
        try:
            parts = [p for p in path.split("/") if p]
            i = parts.index("display")
            space = parts[i + 1]
            title = unquote(parts[i + 2]).replace("+", " ") if len(parts) > i + 2 else None
            return space, title
        except Exception:
            pass

    # /pages/viewpage.action?spaceKey=...&title=...
    if path.endswith("/pages/viewpage.action"):
        q = parse_qs(u.query)
        space = q.get("spaceKey", [None])[0]
        title = q.get("title", [None])[0]
        if title:
            title = unquote(title).replace("+", " ")
            return space, title
        if space:
            return space, None

    return None, None


def clean_page_title_for_filename(title: str) -> str:
    if not title:
        return "Untitled"
    title = unquote(title).replace("+", " ")
    title = re.sub(r"[^A-Za-z0-9\-_ ]+", "", title).strip()
    title = re.sub(r"\s+", "_", title)
    return title or "Untitled"


def make_page_filename_from_url(url: str) -> str:
    space, title = get_space_and_title_from_url(url)
    if not title:
        # fallback to last path segment (rare)
        seg = os.path.basename(urlparse(url).path)
        title = seg or "Untitled"
    return f"{clean_page_title_for_filename(title)}.html"


def is_same_space(url: str) -> bool:
    space, _ = get_space_and_title_from_url(url)
    return (space or "").lower() == (SPACE_KEY or "").lower()


# =========================
# Start page detection (login gate)
# =========================

def is_on_start_page(url: str) -> bool:
    u = urlparse(url)
    path = (u.path or "").rstrip("/")
    q = parse_qs(u.query)

    # /display/SPACE/TITLE
    if "/display/" in path:
        parts = [p for p in path.split("/") if p]
        try:
            i = parts.index("display")
            space = parts[i + 1]
            title = unquote(parts[i + 2]).replace("+", " ") if len(parts) > i + 2 else ""
            return (
                normalize_text(space) == normalize_text(SPACE_KEY)
                and normalize_text(title) == normalize_text(config["start_page"])
            )
        except Exception:
            pass

    # /pages/viewpage.action?spaceKey=...&title=...
    if path.endswith("/pages/viewpage.action"):
        space = q.get("spaceKey", [""])[0]
        title = q.get("title", [""])[0]
        return (
            normalize_text(space) == normalize_text(SPACE_KEY)
            and normalize_text(title) == normalize_text(config["start_page"])
        )

    return False


# =========================
# Filename helpers for assets
# =========================

def sanitize_filename(filename: str) -> str:
    fname = unquote(filename).split("/")[-1]
    fname = fname.split("?")[0]
    # Remove dangerous characters
    fname = re.sub(r"[^A-Za-z0-9._\-]+", "_", fname)
    return fname or "file"


# =========================
# URL cleaning and link extraction
# =========================
RESTRICTED_URLS = [
    "/pages/copypage.action", 
    "/pages/copyscaffoldfromajax.action",
    "/pages/createpage.action", 
    "/usage/report.action",
    "/plugins/confanalytics/analytics.action", 
    "/spaces/viewspacesummary.action",
    "/collector/pages.action", 
    "/pages/reorderpages.action", 
    "undefined"
]


def clean_url(url: str):
    u = urlparse(url)
    cleaned = u._replace(fragment="", query="")
    final_url = urlunparse(cleaned)

    if not final_url.startswith(BASE_URL):
        return None
    if "/label/" in final_url or any(bad in final_url for bad in RESTRICTED_URLS):
        return None
    return final_url


def extract_links():
    """Extracts and returns set of same-space, cleaned, absolute links from current page."""
    try:
        page_links = driver.execute_script(
            """
            return Array.from(document.querySelectorAll("a[href]")).map(a => a.href);
            """
        )
        valid = set()
        for link in page_links:
            cl = clean_url(link)
            if not cl:
                continue
            if not is_same_space(cl):  # confine crawl to this space
                continue
            if cl not in visited_pages:
                valid.add(cl)
        return valid
    except Exception as e:
        print(f"[Error] Extracting links: {e}")
        return set()


# =========================
# Download assets via requests
# =========================

def download_file(url: str, save_dir: str):
    # Ensure absolute URL
    if not url.startswith("http"):
        url = urljoin(BASE_URL, url)

    filename = sanitize_filename(url)
    file_path = os.path.join(save_dir, filename)

    if os.path.exists(file_path):
        return filename

    try:
        resp = session.get(url, stream=True)
        resp.raise_for_status()
        total = int(resp.headers.get("Content-Length", 0) or 0)

        with open(file_path, "wb") as f:
            if total > 0:
                with tqdm(total=total, unit="B", unit_scale=True, desc=filename) as pbar:
                    for chunk in resp.iter_content(8192):
                        if chunk:
                            f.write(chunk)
                            pbar.update(len(chunk))
            else:
                for chunk in resp.iter_content(8192):
                    if chunk:
                        f.write(chunk)
        print(f"[Downloaded] {filename}")
        return filename
    except Exception as e:
        print(f"[Error] Downloading {url}: {e}")
        try:
            if os.path.exists(file_path):
                os.remove(file_path)
        except Exception:
            pass
        return None


# =========================
# Save the current page as a full HTML document with assets and rewritten links
# =========================

def save_full_html(current_url: str, new_links: set) -> bool:
    """
    Write a full HTML doc (doctype/html/head/body) containing the page's wiki content.
    - Rewrites <link rel=stylesheet> and <script src> from <head> to local copies
    - Rewrites internal same-space anchors to ../pages/<Title>.html
    - Downloads images and attachments referenced inside the content
    - Skips writing if there is no content AND no internal workspace links

    Returns True if a file was saved; False if skipped or on error.
    """
    try:
        page_source = driver.page_source
        soup = BeautifulSoup(page_source, "html.parser")

        # Try classic editor container, then newer editor markers
        main_content = soup.find(class_="wiki-content")
        if not main_content:
            main_content = (
                soup.find(attrs={"data-testid": "ak-renderer-root"})
                or soup.find("main")
                or soup.find("article")
            )

        has_content = bool(main_content and main_content.get_text(strip=True))
        has_workspace_links = bool(new_links)
        if not has_content and not has_workspace_links:
            # Skip e.g., .action pages with no actual page content and no useful links
            return False

        # ---------- HEAD assets ----------
        head = soup.find("head") or soup.new_tag("head")

        # Stylesheets
        for link in list(head.find_all("link", href=True)):
            rel = (link.get("rel") or [])
            if "stylesheet" in [r.lower() for r in rel]:
                css_url = urljoin(BASE_URL, link["href"]) if not link["href"].startswith("http") else link["href"]
                local = download_file(css_url, STYLE_DIR)
                if local:
                    link["href"] = f"../styles/{local}"
                else:
                    link.decompose()

        # Scripts
        for script in list(head.find_all("script", src=True)):
            js_url = urljoin(BASE_URL, script["src"]) if not script["src"].startswith("http") else script["src"]
            local = download_file(js_url, SCRIPT_DIR)
            if local:
                script["src"] = f"../scripts/{local}"
            else:
                script.decompose()

        # Title/meta
        clean_name = make_page_filename_from_url(current_url).replace(".html", "")
        title_tag = head.find("title")
        if title_tag:
            title_tag.string = clean_name
        else:
            tt = soup.new_tag("title")
            tt.string = clean_name
            head.append(tt)
        if not head.find("meta", attrs={"charset": True}):
            meta = soup.new_tag("meta", charset="utf-8")
            head.insert(0, meta)

        # ---------- BODY content ----------
        if not main_content:
            main_content = soup.new_tag("div")

        # Rewrite internal links to local page files (same-space only)
        for a in main_content.find_all("a", href=True):
            abs_href = urljoin(BASE_URL, a["href"]) if not a["href"].startswith("http") else a["href"]
            cl = clean_url(abs_href)
            if not cl:
                continue
            if is_same_space(cl):
                a["href"] = f"../pages/{make_page_filename_from_url(cl)}"

        # Images
        for img in main_content.find_all("img", src=True):
            img_url = urljoin(BASE_URL, img["src"]) if not img["src"].startswith("http") else img["src"]
            local = download_file(img_url, IMAGE_DIR)
            if local:
                img["src"] = f"../images/{local}"

        # Attachments
        for a in main_content.find_all("a", href=True):
            href = a["href"]
            if "/download/attachments/" in href:
                att_url = urljoin(BASE_URL, href) if not href.startswith("http") else href
                local = download_file(att_url, ATTACHMENT_DIR)
                if local:
                    a["href"] = f"../attachments/{local}"

        # Build a minimal full document around the (rewritten) content
        out = BeautifulSoup("<!doctype html><html></html>", "html.parser")
        html = out.html
        out_head = out.new_tag("head")
        out_body = out.new_tag("body")

        for child in list(head.children):
            out_head.append(child)
        out_body.append(main_content)

        html.append(out_head)
        html.append(out_body)

        filename = make_page_filename_from_url(current_url)
        file_path = os.path.join(PAGE_DIR, filename)
        with open(file_path, "w", encoding="utf-8") as f:
            f.write(str(out))

        print(f"[Saved HTML] {filename}")
        return True

    except Exception as e:
        print(f"[Error] Saving HTML page: {e}")
        return False


# =========================
# Crawl
# =========================
visited_pages = set()
queue = [START_PAGE]

load_cookies()

# Navigate to start page (user may need to log in manually)
driver.get(START_PAGE)
print("Waiting for successful login...")
while True:
    time.sleep(1)
    current_url = driver.current_url
    if is_on_start_page(current_url):
        save_cookies()
        break
print("Login successful. Starting wiki crawl.")

while queue:
    current_url = queue.pop(0)

    # Skip if already visited (prevents cycles and redundant work)
    if current_url in visited_pages:
        continue

    print(f"Processing: {current_url}")
    try:
        driver.get(current_url)

        # Extract candidate next links first (based on rendered page)
        new_links = extract_links()
        print("Extracted links",new_links)

        # Save this page if it has content or useful links
        saved = save_full_html(current_url, new_links)

        # Enqueue only fresh, not-yet-visited and not already queued
        fresh = [u for u in (new_links - visited_pages) if u not in queue]
        queue.extend(fresh)

        visited_pages.add(current_url)

        with open("crawl_state.json", "w", encoding="utf-8") as f:
            json.dump({"visited": list(visited_pages), "queue": list(queue)}, f, indent=4)

    except Exception as e:
        print(f"[Error processing] {current_url}: {e}")

print("Finished dumping all files.")
driver.quit()
