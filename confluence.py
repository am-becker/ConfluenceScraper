import os
import time
import json
import pickle
import base64
import requests
from tqdm import tqdm
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from bs4 import BeautifulSoup
from urllib.parse import urlparse, unquote, urljoin, urlunparse, parse_qs

# Load Configuration
CONFIG_FILE = "config.json"
with open(CONFIG_FILE, "r") as f:
    config = json.load(f)

BASE_URL = config["base_url"]
SPACE_KEY = config["space_key"]
START_PAGE = f"{BASE_URL}/display/{SPACE_KEY}/{config['start_page']}"
COOKIES_FILE = config["cookies_file"]

# Define directories
SAVE_DIR = SPACE_KEY
PAGE_DIR = os.path.join(SAVE_DIR, "pages")
ATTACHMENT_DIR = os.path.join(SAVE_DIR, "attachments")
IMAGE_DIR = os.path.join(SAVE_DIR, "images")
STYLE_DIR = os.path.join(SAVE_DIR, "styles")
SCRIPT_DIR = os.path.join(SAVE_DIR, "scripts")

# Ensure directories exist
os.makedirs(PAGE_DIR, exist_ok=True)
os.makedirs(ATTACHMENT_DIR, exist_ok=True)
os.makedirs(IMAGE_DIR, exist_ok=True)
os.makedirs(STYLE_DIR, exist_ok=True)
os.makedirs(SCRIPT_DIR, exist_ok=True)

# Setup WebDriver options
options = Options()
#options.add_argument("--disable-gpu")
#options.add_argument("--headless")  # Run headless for efficiency
options.add_experimental_option("excludeSwitches", ["enable-automation"])
options.add_experimental_option("useAutomationExtension", False)
driver = webdriver.Chrome(service=Service(), options=options)

# Load cookies if they exist
session = requests.Session()

def load_cookies():
    # Load cookies if available
    if os.path.exists(COOKIES_FILE):
        driver.get(BASE_URL)
        with open(COOKIES_FILE, "rb") as f:
            cookies = pickle.load(f)
            for cookie in cookies:
                driver.add_cookie(cookie)
                session.cookies.set(cookie["name"], cookie["value"])

# Save cookies after login
def save_cookies():
    cookies = driver.get_cookies()
    with open(COOKIES_FILE, "wb") as f:
        pickle.dump(cookies, f)
    # Apply to requests
    for cookie in cookies:
        driver.add_cookie(cookie)
        session.cookies.set(cookie["name"], cookie["value"])


def normalize_text(text):
    """Normalize text by decoding, replacing + with space, stripping spaces, and converting to lowercase."""
    if not text:
        return ""  # Ensure we don't compare None vs. string

    text = unquote(text)  # Decode URL encoding (%20, %2B)
    text = text.replace("+", " ")  # Ensure + is treated as a space
    return text.strip().lower().replace(" ", "")  # Normalize spaces fully

def is_on_start_page(url):
    """Checks if the given URL matches the expected start page format."""
    parsed_url = urlparse(url)

    # Normalize expected start page title
    normalized_start_page = normalize_text(config['start_page'])

    # Case 1: /display/SPACE/TITLE format
    expected_display_url = f"/confluence/display/{SPACE_KEY}/{START_PAGE.replace(' ', '+')}".lower()
    if parsed_url.path.lower() == expected_display_url:
        return True

    # Case 2: /pages/viewpage.action?spaceKey=SPACE&title=TITLE format
    query_params = parse_qs(parsed_url.query)

    # Normalize spaceKey and title
    space_match = normalize_text(query_params.get("spaceKey", [""])[0]) == normalize_text(SPACE_KEY)
    title_match = normalize_text(query_params.get("title", [""])[0]) == normalized_start_page

    return space_match and title_match



# Sanitize filenames
def sanitize_filename(filename):
    filename = unquote(filename).split("/")[-1]
    return filename.split("?")[0].replace(" ", "_").replace("/", "_")

# Extract valid links
def extract_links():
    """Extracts valid links and adds them to the queue."""
    try:
        page_links = driver.execute_script("""
        return Array.from(document.querySelectorAll("a[href]"))
            .map(a => a.href);
        """)

        valid_links = set()
        for link in page_links:
            cleaned_link = clean_url(link)
            if cleaned_link and cleaned_link.startswith(BASE_URL) and cleaned_link not in visited_pages:
                valid_links.add(cleaned_link)

        return valid_links

    except Exception as e:
        print(f"[Error] Extracting links: {e}")
        return set()

# Remove fragments and restricted paths
RESTRICTED_URLS = [
    "/pages/copypage.action", "/pages/copyscaffoldfromajax.action",
    "/pages/createpage.action", "/usage/report.action",
    "/plugins/confanalytics/analytics.action", "/spaces/viewspacesummary.action",
    "/collector/pages.action", "/pages/reorderpages.action", "undefined"
]

def clean_url(url):
    parsed_url = urlparse(url)
    cleaned_url = parsed_url._replace(fragment="", query="")
    final_url = urlunparse(cleaned_url)

    if "/label/" in final_url or any(restricted in final_url for restricted in RESTRICTED_URLS):
        return None  # Ignore label pages and restricted pages

    return final_url

# Download file and save
def download_file(url, save_dir):
    filename = sanitize_filename(url)
    file_path = os.path.join(save_dir, filename)

    if os.path.exists(file_path):
        return filename  # File already exists

    # Ensure URL is absolute
    if not url.startswith("http"):
        url = urljoin(BASE_URL, url)  # Convert relative URL to absolute

    try:
        response = session.get(url, stream=True)
        response.raise_for_status()


        # Download file
        with open(file_path, "wb") as f:
            f.write(driver.execute_script("return fetch(arguments[0]).then(res => res.blob()).then(b => b.arrayBuffer()).then(buf => new Uint8Array(buf))", attachment_url))
        print(f"[Downloaded] {filename}")
    except Exception as e:
        print(f"[Error] Downloading attachments: {e}")

        with open(file_path, "wb") as f, tqdm(total=int(response.headers.get("Content-Length", 0)), unit="B", unit_scale=True, desc=filename) as pbar:
            for chunk in response.iter_content(8192):
                f.write(chunk)
                pbar.update(len(chunk))

        return filename

    except Exception as e:
        print(f"[Error] Downloading {url}: {e}")
        return None


# Save HTML page with updated links
def save_html_page():
    try:
        page_source = driver.page_source
        soup = BeautifulSoup(page_source, "html.parser")
        main_content = soup.find(class_="wiki-content")

        if not main_content:
            print("[Warning] No main content found.")
            return

        # Fix internal links
        for a in main_content.find_all("a", href=True):
            href = a["href"]
            if "/display/" in href:
                page_name = sanitize_filename(href) + ".html"
                a["href"] = f"../pages/{page_name}"

        # Download and embed images
        for img in main_content.find_all("img", src=True):
            img_url = urljoin(BASE_URL, img["src"])
            local_filename = download_file(img_url, IMAGE_DIR)
            if local_filename:
                img["src"] = f"../images/{local_filename}"

        # Download attachments
        for attachment in main_content.find_all("a", href=True):
            href = attachment["href"]
            if "/download/attachments/" in href:
                local_filename = download_file(href, ATTACHMENT_DIR)
                if local_filename:
                    attachment["href"] = f"../attachments/{local_filename}"

        # Download stylesheets
        for link in soup.find_all("link", {"rel": "stylesheet"}):
            css_url = urljoin(BASE_URL, link["href"])
            local_filename = download_file(css_url, STYLE_DIR)
            if local_filename:
                link["href"] = f"../styles/{local_filename}"

        # Download scripts
        for script in soup.find_all("script", src=True):
            js_url = urljoin(BASE_URL, script["src"])
            local_filename = download_file(js_url, SCRIPT_DIR)
            if local_filename:
                script["src"] = f"../scripts/{local_filename}"

        # Save the modified HTML
        page_title = driver.title.replace(" ", "_").replace("/", "_") + ".html"
        file_path = os.path.join(PAGE_DIR, page_title)

        with open(file_path, "w", encoding="utf-8") as f:
            f.write(str(main_content))

        print(f"[Saved HTML] {page_title}")

    except Exception as e:
        print(f"[Error] Saving HTML page: {e}")

# Main loop to crawl the wiki
visited_pages = set()
queue = [START_PAGE]

load_cookies()

# Attempt to get start page
driver.get(START_PAGE)

# Wait for manual login if needed
print("Waiting for successful login...")
while True:
    time.sleep(1)  # Check every 2 seconds
    current_url = driver.current_url

    if is_on_start_page(current_url):
        save_cookies()  # Save session for future runs
        break

print("Login successful. Starting wiki crawl.")

while queue:
    current_url = queue.pop(0)
    print(f"Processing: {current_url}")

    try:
        driver.get(current_url)
        new_links = extract_links()
        queue.extend(new_links - visited_pages)
        save_html_page()
        visited_pages.add(current_url)

        with open("crawl_state.json", "w") as f:
            json.dump({"visited": list(visited_pages), "queue": list(queue)}, f, indent=4)

    except Exception as e:
        print(f"[Error processing] {current_url}: {e}")

print("Finished dumping all files.")

driver.quit()
