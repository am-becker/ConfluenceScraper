import os
import re
import time
import json
import pickle
import html
import pathlib
from dataclasses import dataclass, field
from typing import Dict, Set, Optional, List, Tuple
from urllib.parse import urlparse, unquote, urljoin, urlunparse, parse_qs, urldefrag

import requests
from tqdm import tqdm
from bs4 import BeautifulSoup

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import (
    StaleElementReferenceException,
    TimeoutException,
    WebDriverException,
)

"""
Confluence Space Downloader — ID-first, hierarchical, offline viewer
Now supports iterating over multiple Confluence start links via config["links"].
Each link should be "SPACE_KEY/START_PAGE" (e.g., "RocketTeam/MIT+Rocket+Team+Home").
"""

# =========================
# Load Configuration
# =========================
CONFIG_FILE = "config.json"
with open(CONFIG_FILE, "r", encoding="utf-8") as f:
    config = json.load(f)

BASE_URL = config["base_url"].rstrip("/")
COOKIES_FILE = config["cookies_file"]
OFFLINE_AUTHOR = config.get("offline_author", "Aaron Becker, V0.2α")
CHROME_PROFILE_DIR = config.get("chrome_profile_dir", ".chrome-profile")

# Backward compatibility: allow legacy keys if "links" not provided
_links_from_config = config.get("links")
if not _links_from_config:
    # expect legacy: space_key + start_page
    legacy_space = config.get("space_key")
    legacy_start = config.get("start_page")
    if not (legacy_space and legacy_start):
        raise RuntimeError('Provide either {"links": [...]} OR legacy {"space_key","start_page"} in config.json')
    _links_from_config = [f"{legacy_space}/{legacy_start}"]

# Parse links into (SPACE_KEY, START_PAGE) tuples
def parse_link(link: str) -> Tuple[str, str]:
    parts = [p for p in (link or "").split("/") if p]
    if len(parts) < 2:
        raise ValueError(f'Invalid link "{link}". Expected "SPACE_KEY/START_PAGE"')
    space_key = parts[0]
    start_page = "/".join(parts[1:])  # in case someone includes extra segments
    return space_key, start_page

LINKS: List[Tuple[str, str]] = [parse_link(l) for l in _links_from_config]

# =========================
# Globals that change per link (set inside the loop)
# =========================
SPACE_KEY: str = ""
CURRENT_START_PAGE: str = ""
START_PAGE_URL: str = ""
ROOT_DIR: str = ""
GRAPH = None  # set to SpaceGraph per link

# =========================
# WebDriver + requests.Session
# =========================
options = Options()
# options.add_argument("--headless=new")
options.add_experimental_option("excludeSwitches", ["disable-popup-blocking", "enable-automation"])
options.add_experimental_option("useAutomationExtension", False)
options.page_load_strategy = "eager"
options.add_argument("--disable-extensions")
options.add_argument("--no-first-run")
options.add_argument("--no-default-browser-check")
options.add_argument("--js-flags=--max-old-space-size=4096")  # MB, adjust 2048/4096/8192

# critical: persist the full browser session
options.add_argument(f"--user-data-dir={os.path.abspath(CHROME_PROFILE_DIR)}")
options.add_argument("--profile-directory=Default")

def make_driver() -> webdriver.Chrome:
    d = webdriver.Chrome(service=Service(), options=options)
    d.set_page_load_timeout(25)
    return d

driver = make_driver()
session = requests.Session()

VISITS_BEFORE_RESTART = 10
visits = 0
restarts = 0

def maybe_restart_driver(force=False):
    global driver, visits, restarts
    if visits >= VISITS_BEFORE_RESTART or force:
        print("Restarting driver...")
        restarts += 1
        try:
            driver.quit()
        except Exception:
            pass

        driver = make_driver()

        # Give it a little time
        time.sleep(1)

        # restore login session
        driver.get(ORIGIN)
        time.sleep(2)
        cookies = read_cookies_from_pickle()
        if cookies:
            push_cookies_to_browser(cookies) # ensure cookies are applied
        driver.get(ORIGIN)

        visits = 0

_u = urlparse(BASE_URL)
ORIGIN = f"{_u.scheme}://{_u.netloc}"
CONFLUENCE_NETLOC = _u.netloc

# =========================
# Cookie helpers (robust)
# =========================
def _normalize_cookie(c: dict) -> dict:
    c = c.copy()
    if "expiry" in c:
        try:
            c["expiry"] = int(c["expiry"])
        except Exception:
            c.pop("expiry", None)
    if c.get("sameSite", None) is None:
        c.pop("sameSite", None)
    return c

def read_cookies_from_pickle() -> List[dict]:
    if not os.path.exists(COOKIES_FILE):
        return []
    with open(COOKIES_FILE, "rb") as f:
        cookies = pickle.load(f) or []
    return cookies

def push_cookies_to_browser(cookies: List[dict]):
    # NOTE: you must be on ORIGIN before calling this (driver.get(ORIGIN))
    added = 0
    for c in cookies:
        try:
            driver.add_cookie(_normalize_cookie(c))
            added += 1
        except Exception as e:
            print(f"[Cookie] Browser skip {c.get('name')}: {e}")
    print(f"[Cookie] Restored {added} cookies to Selenium for {CONFLUENCE_NETLOC}")

def push_cookies_to_requests(cookies: List[dict]):
    """Mirror Selenium cookies into requests with proper keys to avoid de-duping."""
    import time as _time
    session.cookies.clear()
    mirrored = 0
    for c in cookies:
        try:
            # skip expired cookies
            if c.get("expiry") is not None:
                try:
                    if int(c["expiry"]) <= int(_time.time()):
                        continue
                except Exception:
                    pass

            domain = c.get("domain") or CONFLUENCE_NETLOC
            path = c.get("path") or "/"
            secure = bool(c.get("secure", False))
            rest = {}
            # Preserve HttpOnly & SameSite if present
            if c.get("httpOnly") is True:
                rest["HttpOnly"] = True
            if c.get("sameSite"):
                rest["SameSite"] = c["sameSite"]

            rc = requests.cookies.create_cookie(
                name=c["name"],
                value=c["value"],
                domain=domain,
                path=path,
                secure=secure,
                rest=rest
            )
            session.cookies.set_cookie(rc)
            mirrored += 1
        except Exception as e:
            print(f"[Cookie] Requests skip {c.get('name')}: {e}")
    print(f"[Cookie] Mirrored {mirrored}/{len(cookies)} cookies into requests.Session")

def save_cookies():
    cookies = driver.get_cookies()
    with open(COOKIES_FILE, "wb") as f:
        pickle.dump(cookies, f)
    print(f"[Cookie] Saved {len(cookies)} cookies.")
    push_cookies_to_requests(cookies)

# =========================
# URL / text utils
# =========================
RESTRICTED_URLS = {
    "/pages/copypage.action",
    "/pages/copyscaffoldfromajax.action",
    "/pages/createpage.action",
    "/usage/report.action",
    "/plugins/confanalytics/analytics.action",
    "/spaces/viewspacesummary.action",
    "/collector/pages.action",
    "/pages/reorderpages.action",
    "undefined",
}
WINDOWS_RESERVED = {
    "con", "prn", "aux", "nul",
    *(f"com{i}" for i in range(1, 10)),
    *(f"lpt{i}" for i in range(1, 10)),
}

def normalize_text(s: str) -> str:
    if not s:
        return ""
    s = unquote(s).replace("+", " ")
    s = re.sub(r"\s+", " ", s).strip().lower()
    return s

def get_space_and_title_from_url(url: str) -> Tuple[Optional[str], Optional[str]]:
    u = urlparse(url)
    p = u.path or ""
    if "/display/" in p:
        try:
            parts = [x for x in p.split("/") if x]
            i = parts.index("display")
            space = parts[i + 1] if len(parts) > i + 1 else None
            title = unquote(parts[i + 2]).replace("+", " ") if len(parts) > i + 2 else None
            return space, title
        except Exception:
            pass
    if p.endswith("/pages/viewpage.action"):
        q = parse_qs(u.query)
        space = q.get("spaceKey", [None])[0]
        title = q.get("title", [None])[0]
        if title:
            title = unquote(title).replace("+", " ")
        return space, title
    return None, None

def same_space(url: str) -> bool:
    space, _ = get_space_and_title_from_url(url)
    return (space or "").lower() == SPACE_KEY.lower()

def clean_url(url: str) -> Optional[str]:
    if not url:
        return None
    if not (url.startswith("http://") or url.startswith("https://")):
        url = urljoin(BASE_URL, url)
    u = urlparse(url)
    if not (u.scheme and u.netloc):
        return None
    if not url.startswith(BASE_URL):
        return None
    if any(bad in url for bad in RESTRICTED_URLS) or "/label/" in url:
        return None
    cleaned = u._replace(fragment="", query="")
    return urlunparse(cleaned)

def sanitize_slug(name: str, fallback: str) -> str:
    if not name:
        base = fallback
    else:
        s = unquote(name)
        s = s.replace("/", "").replace("\\", "")
        s = re.sub(r"[^A-Za-z0-9 _\-.]+", "", s)
        s = re.sub(r"\s+", "", s)
        base = s.strip() or fallback
    base = base.strip(". ") or fallback
    if base.lower() in WINDOWS_RESERVED:
        base = f"{base}_page"
    return base[:120] or fallback

def ensure_dir(path: str):
    pathlib.Path(path).mkdir(parents=True, exist_ok=True)

def rel_href(from_dir: str, to_file: str) -> str:
    rp = os.path.relpath(to_file, start=from_dir)
    return rp.replace(os.sep, "/")

def parse_page_id_from_url(url: str) -> Optional[str]:
    u = urlparse(url)
    if u.path.endswith("/pages/viewpage.action"):
        q = parse_qs(u.query)
        pid = q.get("pageId", [None])[0]
        if pid:
            return str(pid)
    return None

# =========================
# Graph model
# =========================
@dataclass
class PageNode:
    id: str
    space_key: str
    title: Optional[str] = None
    slug: Optional[str] = None
    hrefs: Set[str] = field(default_factory=set)
    parent_id: Optional[str] = None
    children: Set[str] = field(default_factory=set)

class SpaceGraph:
    def __init__(self, space_key: str):
        self.space_key = space_key
        self.nodes: Dict[str, PageNode] = {}
        self.root_id: Optional[str] = None
    def get_or_create(self, pid: str) -> PageNode:
        if pid not in self.nodes:
            self.nodes[pid] = PageNode(id=pid, space_key=self.space_key)
        return self.nodes[pid]
    def set_parent(self, child: str, parent: Optional[str]):
        n = self.get_or_create(child)
        if parent:
            p = self.get_or_create(parent)
            n.parent_id = parent
            p.children.add(child)
        else:
            self.root_id = child
            n.parent_id = None
    def all_ids(self) -> List[str]:
        return list(self.nodes.keys())

# =========================
# Login gate helpers
# =========================
def page_matches_start(url: str) -> bool:
    # Uses globals: SPACE_KEY, CURRENT_START_PAGE
    u = urlparse(url)
    if "/display/" in (u.path or ""):
        parts = [p for p in u.path.split("/") if p]
        try:
            i = parts.index("display")
            space = parts[i + 1]
            title = unquote(parts[i + 2]).replace("+", " ") if len(parts) > i + 2 else ""
            return normalize_text(space) == normalize_text(SPACE_KEY) and normalize_text(title) == normalize_text(CURRENT_START_PAGE)
        except Exception:
            pass
    if (u.path or "").endswith("/pages/viewpage.action"):
        q = parse_qs(u.query)
        if normalize_text(q.get("spaceKey", [""])[0]) == normalize_text(SPACE_KEY) and normalize_text(q.get("title", [""])[0]) == normalize_text(CURRENT_START_PAGE):
            return True
    try:
        h1 = WebDriverWait(driver, 2).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "#title-text a"))
        )
        title = (h1.text or "").strip()
        return normalize_text(title) == normalize_text(CURRENT_START_PAGE)
    except Exception:
        return False

def wait_for_page_identity(timeout_sec: int = 30) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    deadline = time.time() + timeout_sec
    while time.time() < deadline:
        try:
            cid = driver.execute_script("""
                var q = (n)=>{var el=document.querySelector('meta[name="'+n+'"]'); return el? el.getAttribute('content'): null;};
                var m = q('ajs-content-id') || q('ajs-page-id') || q('ajs-latest-page-id');
                return m;
            """)
            if cid:
                title = driver.execute_script("""
                    var t = document.querySelector('#title-text a');
                    if (t && t.textContent) return t.textContent.trim();
                    var m = document.querySelector('meta[name="ajs-page-title"]');
                    if (m && m.content) return m.content.trim();
                    return (document.title||'').replace(/\s*-\s*Confluence.*/,'').trim();
                """)
                parent = driver.execute_script("""
                    var p = document.querySelector('meta[name="ajs-parent-page-id"]');
                    return p && p.content ? p.content : null;
                """)
                return str(cid), (title or None), (str(parent) if parent else None)
        except Exception:
            pass
        time.sleep(0.25)
    return None, None, None

def wait_for_dom_ready(timeout_sec: int = 15) -> bool:
    deadline = time.time() + timeout_sec
    while time.time() < deadline:
        try:
            rs = driver.execute_script("return document.readyState")
            if rs in ("interactive", "complete"):
                return True
        except Exception:
            pass
        time.sleep(0.15)
    return False

def wait_for_main_content(timeout_sec: int = 20) -> bool:
    selectors = ["#main-content",".wiki-content","[data-testid='ak-renderer-root']","main","article"]
    deadline = time.time() + timeout_sec
    js = """
      const sels = arguments[0];
      for (const s of sels) { if (document.querySelector(s)) return true; }
      return false;
    """
    while time.time() < deadline:
        try:
            if driver.execute_script(js, selectors):
                return True
        except Exception:
            pass
        time.sleep(0.2)
    return False

def _best_effort_stop_page_load():
    try:
        driver.execute_script("window.stop();")
    except Exception:
        pass
    try:
        driver.execute_cdp_cmd("Page.stopLoading", {})
    except Exception:
        pass

# def navigate_and_wait(url: str, timeout_sec: int = 45) -> Tuple[Optional[str], Optional[str], Optional[str]]:
#     driver.get(url)
#     wait_for_dom_ready(min(10, timeout_sec))
#     cid, title, parent = wait_for_page_identity(max(0, timeout_sec - 10))
#     wait_for_main_content(15)
#     return cid, title, parent

def navigate_and_wait(url: str, timeout_sec: int = 45, allow_restart: bool = True) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    global visits, restarts

    # Allow restart of browser client if beyond limit
    if allow_restart:
        #print("ALLOWED TO RESTART")
        maybe_restart_driver()
    #else:
        #print("NOT ALLOWED TO RESTART")

    attempts = 2
    last_err = None

    for attempt in range(1, attempts + 1):
        try:
            visits += 1
            print(f"[Nav] GET {url} (attempt {attempt}/{attempts}, visit {visits}), restarts {restarts}")
            driver.get(url)

        except TimeoutException as e:
            print(f"[Nav] Page load timeout for {url}; forcing driver restart.")
            last_err = e
            #_best_effort_stop_page_load()
            # Force driver restart on timeout
            maybe_restart_driver(force=True)

        except WebDriverException as e:
            print(f"[Nav] WebDriver error on {url}: {e}")
            last_err = e
            continue

        # Wait for DOM ready, then get page info (title and parent)
        wait_for_dom_ready(min(10, timeout_sec))
        cid, title, parent = wait_for_page_identity(max(5, timeout_sec - 8))

        # Wait for page content
        try:
            wait_for_main_content(10)
        except Exception:
            pass

        if cid or title:
            return cid, title, parent

        cid, title, parent = read_dom_ids_titles_parent()
        if cid or title:
            return cid, title, parent

    print(f"[Nav] Failed to identify page: {url} ({last_err})")
    return None, None, None

# =========================
# DOM extraction helpers
# =========================
def read_dom_ids_titles_parent() -> Tuple[Optional[str], Optional[str], Optional[str]]:
    try:
        cid = driver.execute_script("""
            var q = (n)=>{var el=document.querySelector('meta[name="'+n+'"]'); return el? el.getAttribute('content'): null;};
            return q('ajs-content-id') || q('ajs-page-id') || q('ajs-latest-page-id');
        """)
        title = driver.execute_script("""
            var t = document.querySelector('#title-text a');
            if (t && t.textContent) return t.textContent.trim();
            var m = document.querySelector('meta[name="ajs-page-title"]');
            if (m && m.content) return m.content.trim();
            return (document.title||'').replace(/\s*-\s*Confluence.*/,'').trim();
        """)
        parent = driver.execute_script(
            "var p=document.querySelector('meta[name=\"ajs-parent-page-id\"]'); return p && p.content ? p.content : null;"
        )
        if not cid:
            cid = parse_page_id_from_url(driver.current_url)
        return (str(cid) if cid else None), (title or None), (str(parent) if parent else None)
    except Exception:
        return None, None, None

# =========================
# PageTree expansion (robust)
# =========================
def _visible_collapsed_toggles() -> List[Tuple[str, str, str]]:
    toggles = driver.find_elements(By.CSS_SELECTOR, ".plugin_pagetree a.plugin_pagetree_childtoggle")
    out: List[Tuple[str, str, str]] = []
    for t in toggles:
        try:
            if not t.is_displayed():
                continue
            ae = (t.get_attribute("aria-expanded") or "").strip().lower()
            expanded = (ae == "true")
            if not expanded:
                pid = t.get_attribute("data-page-id") or ""
                tid = t.get_attribute("data-tree-id") or "0"
                tid_attr = t.get_attribute("id") or f"plusminus{pid}-{tid}"
                out.append((tid_attr, pid, tid))
        except StaleElementReferenceException:
            continue
        except Exception:
            continue
    return out

def expand_full_pagetree(max_rounds: int = 200, per_click_wait: float = 1.2):
    print("[PageTree] Expanding tree...")
    
    if not ensure_sidebar_expanded():
        print("[PageTree] Sidebar appears collapsed and could not be expanded.")
        return
    
    # Lil extra time to process
    time.sleep(1.0)
    try:
        WebDriverWait(driver, 15).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, ".plugin_pagetree"))
        )
    except Exception:
        print("[PageTree] No page tree widget detected.")
        return

    rounds = 0
    while rounds < max_rounds:
        rounds += 1
        targets = _visible_collapsed_toggles()
        if not targets:
            print(f"[PageTree] Done (no more collapsed toggles after {rounds-1} rounds).")
            break

        print(f"[PageTree] Round {rounds}: expanding {len(targets)} toggle(s)...")

        for toggle_id, pid, tid in targets:
            try:
                t = driver.find_element(By.ID, toggle_id)
            except Exception:
                alt = driver.find_elements(
                    By.CSS_SELECTOR,
                    f"a.plugin_pagetree_childtoggle[data-page-id='{pid}'][data-tree-id='{tid}']"
                )
                if not alt:
                    continue
                t = alt[0]

            try:
                driver.execute_script("arguments[0].scrollIntoView({block:'center'});", t)
                try:
                    t.click()
                except Exception:
                    driver.execute_script("arguments[0].click()", t)
            except StaleElementReferenceException:
                continue
            except Exception:
                continue

            container_sel = f"#children{pid}-{tid}"
            child_ul_sel = f"#child_ul{pid}-{tid}"

            end = time.time() + per_click_wait
            while time.time() < end:
                try:
                    expanded = (t.get_attribute("aria-expanded") or "").strip().lower() == "true"
                except StaleElementReferenceException:
                    expanded = True
                except Exception:
                    expanded = False

                have_children = False
                try:
                    if driver.find_elements(By.CSS_SELECTOR, f"{child_ul_sel} > li"):
                        have_children = True
                except Exception:
                    pass
                if not have_children:
                    try:
                        if driver.find_elements(By.CSS_SELECTOR, f"{container_sel} a[href]"):
                            have_children = True
                    except Exception:
                        pass

                if expanded or have_children:
                    break
                time.sleep(0.05)
        time.sleep(1)

# # =========================
# # PageTree harvest (robust)
# # =========================
# def harvest_pagetree_nodes() -> List[Tuple[str, str, Optional[str], str]]:
#     results = []
#     items = driver.find_elements(By.CSS_SELECTOR, ".plugin_pagetree_children_list li")
#     for li in items:
#         try:
#             a = li.find_element(By.CSS_SELECTOR, ".plugin_pagetree_children_content a[href]")
#             href = a.get_attribute("href")
#             title = (a.text or "").strip()

#             page_id = None
#             try:
#                 tog = li.find_element(By.CSS_SELECTOR, ".plugin_pagetree_childtoggle")
#                 page_id = tog.get_attribute("data-page-id")
#             except Exception:
#                 page_id = None

#             if not page_id:
#                 try:
#                     span = li.find_element(By.CSS_SELECTOR, ".plugin_pagetree_children_span[id]")
#                     m = re.search(r"childrenspan(\d+)-", span.get_attribute("id") or "")
#                     if m:
#                         page_id = m.group(1)
#                 except Exception:
#                     page_id = None

#             if not page_id:
#                 page_id = parse_page_id_from_url(href)
#             if not page_id:
#                 continue

#             parent_id = None
#             try:
#                 parent_ul = li.find_element(By.XPATH, "ancestor::ul[1]")
#                 ul_id = parent_ul.get_attribute("id") or ""
#                 m = re.search(r"child_ul(\d+)-", ul_id)
#                 if m:
#                     parent_id = m.group(1)
#             except Exception:
#                 pass

#             results.append((str(page_id), title, parent_id, href))
#         except Exception:
#             continue
#     return results

def harvest_pagetree_nodes() -> List[Tuple[str, str, Optional[str], str]]:
    js = r"""
    function parsePageIdFromHref(href) {
        try {
            const u = new URL(href, document.baseURI);
            if (u.pathname.endsWith('/pages/viewpage.action')) {
                return u.searchParams.get('pageId');
            }
        } catch(e){}
        return null;
    }

    const results = [];
    const items = document.querySelectorAll(".plugin_pagetree_children_list li");

    for (const li of items) {
        try {
            const a = li.querySelector(".plugin_pagetree_children_content a[href]");
            if (!a) continue;

            const href = a.href;
            const title = (a.textContent || "").trim();

            let pageId = null;

            const tog = li.querySelector(".plugin_pagetree_childtoggle");
            if (tog) {
                pageId = tog.getAttribute("data-page-id");
            }

            if (!pageId) {
                const span = li.querySelector(".plugin_pagetree_children_span[id]");
                if (span) {
                    const m = span.id.match(/childrenspan(\d+)-/);
                    if (m) pageId = m[1];
                }
            }

            if (!pageId) {
                pageId = parsePageIdFromHref(href);
            }

            if (!pageId) continue;

            let parentId = null;
            const parentUl = li.closest("ul");
            if (parentUl) {
                const m = (parentUl.id || "").match(/child_ul(\d+)-/);
                if (m) parentId = m[1];
            }

            results.push([String(pageId), title, parentId ? String(parentId) : null, href]);
        } catch(e) {}
    }

    return results;
    """
    
    try:
        raw = driver.execute_script(js) or []
        return [(pid, title, parent_id, href) for pid, title, parent_id, href in raw]
    except Exception as e:
        print(f"[PageTree] harvest failed: {e}")
        return []

def ensure_sidebar_expanded(timeout_sec: int = 10) -> bool:
    js_is_collapsed = """
    const sb = document.querySelector('.ia-fixed-sidebar');
    if (!sb) return false;
    return sb.classList.contains('collapsed');
    """

    js_expand = """
    const sb = document.querySelector('.ia-fixed-sidebar');
    if (!sb) return false;
    if (!sb.classList.contains('collapsed')) return true;

    const btn =
        document.querySelector('.expand-collapse-trigger') ||
        document.querySelector('.ia-splitter-handle') ||
        document.querySelector('[aria-label*="Expand sidebar"]') ||
        document.querySelector('[data-tooltip*="Expand sidebar"]');

    if (!btn) return false;

    btn.click();
    return true;
    """

    js_tree_visible = """
    const sb = document.querySelector('.ia-fixed-sidebar');
    const tree = document.querySelector('.plugin_pagetree');
    if (!sb || !tree) return false;

    const collapsed = sb.classList.contains('collapsed');
    const style = window.getComputedStyle(tree);
    const visible = style && style.display !== 'none' && style.visibility !== 'hidden';
    const rect = tree.getBoundingClientRect();

    return !collapsed && visible && rect.width > 0 && rect.height > 0;
    """

    deadline = time.time() + timeout_sec

    while time.time() < deadline:
        try:
            collapsed = bool(driver.execute_script(js_is_collapsed))
            if not collapsed:
                if driver.execute_script(js_tree_visible):
                    return True
            else:
                clicked = driver.execute_script(js_expand)
                if clicked:
                    time.sleep(0.4)
        except Exception:
            pass
        time.sleep(0.2)

    return False

# =========================
# Content utils & downloader
# =========================
def extract_same_space_links_from_content() -> Set[str]:
    try:
        anchors = driver.execute_script("""
            const out = [];
            document.querySelectorAll('#main-content a[href], .wiki-content a[href]').forEach(a=>{
                try{ out.push(a.href); }catch(e){}
            });
            return out;
        """)
    except Exception:
        anchors = []
    found = set()
    for href in anchors:
        href0, _frag = urldefrag(href)
        cl = clean_url(href0)
        if cl and same_space(cl):
            found.add(cl)
    return found

def download_file(url: str, save_dir: str) -> Optional[str]:
    """
    Downloads a non-HTML asset to save_dir, skipping if an existing local file
    matches the remote Content-Length (checked via HEAD).
    """
    if not (url.startswith("http://") or url.startswith("https://")):
        url = urljoin(BASE_URL, url)

    def _sanitize_filename(fn: str) -> str:
        fn = unquote(fn.split("?")[0].split("/")[-1])
        fn = re.sub(r"[^A-Za-z0-9._\-]+", "_", fn)
        return fn or "file"

    filename = _sanitize_filename(url)
    ensure_dir(save_dir)
    file_path = os.path.join(save_dir, filename)

    #print(f"[download_file] url={url} filename={filename} path={file_path}")

    if os.path.exists(file_path):
        try:
            local_size = os.path.getsize(file_path)
            # These two are really annoying, hardcoded out
            if "batch" not in filename and "colors" not in filename:
                print(f"[Skip] {filename} (already downloaded, {local_size} bytes).")
            return filename
        except Exception:
            pass

    try:
        resp = session.get(url, stream=True, timeout=60)
        resp.raise_for_status()
        total = int(resp.headers.get("Content-Length", 0) or 0)
        with open(file_path, "wb") as f:
            if total > 0:
                with tqdm(total=total, unit="B", unit_scale=True, desc=filename) as pbar:
                    for chunk in resp.iter_content(8192):
                        if chunk:
                            f.write(chunk); pbar.update(len(chunk))
            else:
                for chunk in resp.iter_content(8192):
                    if chunk:
                        f.write(chunk)
        # These two are really annoying, hardcoded out
        if "batch" not in filename and "colors" not in filename:
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

# ---------- Offline UI (unchanged) ----------
OFFLINE_UI_STYLE = """
<style>
*{box-sizing:border-box}
body{margin:0;font-family:system-ui,-apple-system,Segoe UI,Roboto,Helvetica,Arial,sans-serif}
:root{--bar-h:96px; --side-w:300px;}
/* Top bar */
.ocv-topbar{height:auto; display:flex; align-items:flex-start; justify-content:space-between;
  padding:8px 10px 10px; border-bottom:1px solid #ddd; background:#fff}
.ocv-topbar-btn{border:0;background:transparent;font-size:18px;cursor:pointer;padding:8px;align-self:center;display:flex;align-items:center;justify-content:center;}
/* Branding & breadcrumbs */
.ocv-brandwrap{display:flex;flex-direction:column;align-items:center;margin:0 auto}
.ocv-brand{font-weight:700;font-size:18px;}
.ocv-author{font-size:12px;color:#888;font-style:italic;}
.ocv-breadcrumbs{font-size:14px; line-height:1.25; color:#333; margin-top:2px; margin-bottom:2px}
.ocv-breadcrumbs a{text-decoration:none;color:#1f4aa3}
.ocv-breadcrumbs .sep{padding:0 6px;color:#aaa}
/* Layout */
.ocv-layout{display:grid; grid-template-columns: var(--side-w) 1fr; min-height: calc(100vh - var(--bar-h));}
.ocv-sidebar{overflow:auto; border-right:1px solid #eee; background:#fafafa; padding:10px 12px;
  transition: width .22s ease, opacity .22s ease, padding .22s ease, border-width .22s ease}
.ocv-main{overflow:auto; padding:20px 16px 16px}
body.nav-collapsed .ocv-layout{grid-template-columns: 0 1fr}
body.nav-collapsed .ocv-sidebar{width:0; min-width:0; padding:0; border-width:0; opacity:0; pointer-events:none}
.ocv-info{position:fixed; right:8px; top:calc(var(--bar-h) + 8px); width:320px; max-height:60vh; overflow:auto;
  border:1px solid #ddd; background:#fff; box-shadow:0 6px 24px rgba(0,0,0,.12); padding:12px; display:none; z-index:1200;}
.ocv-info.open{display:block}
.meta-kv{font-size:14px;line-height:1.5}
.meta-kv dt{font-weight:600}
.meta-kv dd{margin:0 0 8px 0; word-break:break-all}
.ocv-tree{font-size:14px}
.ocv-tree a{color:#1f4aa3;text-decoration:none}
.ocv-tree details{margin:3px 0}
.ocv-tree details details{margin-left:14px}
.ocv-tree summary{cursor:pointer; list-style:none; display:flex; align-items:center; gap:4px}
.ocv-tree summary::-webkit-details-marker{display:none}
.ocv-tree summary .tw{display:inline-block; width:1em; text-align:center; transition: transform .15s ease}
.ocv-tree details[open] > summary .tw{transform:rotate(90deg)}
.ocv-tree .leaf{padding-left:1.4em; display:block; margin:2px 0}
.ocv-tree .active > a, .ocv-tree a.active{font-weight:700; text-decoration:underline}
</style>
"""

OFFLINE_UI_SCRIPT = """
<script>
(function(){
  const navBtn = document.getElementById('navToggle');
  const infoBtn = document.getElementById('infoToggle');
  const info = document.querySelector('.ocv-info');
  navBtn.addEventListener('click', ()=>{ document.body.classList.toggle('nav-collapsed'); });
  infoBtn.addEventListener('click', ()=>{ info.classList.toggle('open'); });
})();
</script>
"""

def render_breadcrumbs(current_id: str, id_to_path_html: Dict[str, str]) -> str:
    chain_ids = []
    cur = current_id
    while cur is not None:
        chain_ids.append(cur)
        cur = GRAPH.nodes[cur].parent_id
    chain_ids.reverse()

    kept = chain_ids[-4:] if len(chain_ids) > 4 else chain_ids
    out = []
    my_dir = os.path.dirname(id_to_path_html[current_id])
    if len(chain_ids) > 4:
        anc = chain_ids[-5]
        anc_href = rel_href(my_dir, id_to_path_html[anc])
        out.append(f'<a href="{anc_href}">...</a><span class="sep">→</span>')
    for i, nid in enumerate(kept):
        title = html.escape(GRAPH.nodes[nid].title or f"page-{nid}")
        href = rel_href(my_dir, id_to_path_html[nid])
        if nid == current_id:
            out.append(f"<span>{title}</span>")
        else:
            out.append(f'<a href="{href}">{title}</a>')
        if i < len(kept)-1:
            out.append('<span class="sep">→</span>')
    return "".join(out)

def build_tree_details_html(current_id: str, id_to_path_html: Dict[str, str]) -> str:
    def is_ancestor(anc: str, desc: str) -> bool:
        c = desc
        while c is not None:
            if c == anc: return True
            c = GRAPH.nodes[c].parent_id
        return False
    def children_sorted(pid: str) -> List[str]:
        kids = list(GRAPH.nodes[pid].children)
        kids.sort(key=lambda k: (GRAPH.nodes[k].title or "", k))
        return kids
    def render_node(nid: str) -> str:
        node = GRAPH.nodes[nid]
        label = html.escape(node.title or f"page-{nid}")
        me_html = id_to_path_html[nid]
        cur_dir = os.path.dirname(id_to_path_html[current_id])
        href_rel = rel_href(cur_dir, me_html)
        kids = children_sorted(nid)
        if not kids:
            anchor_cls_attr = ' class="active"' if nid == current_id else ""
            cls = "leaf active" if nid == current_id else "leaf"
            return f'<div class="{cls}"><a href="{href_rel}"{anchor_cls_attr}>{label}</a></div>'
        open_attr = " open" if (nid == current_id or is_ancestor(nid, current_id)) else ""
        active_cls_attr = ' class="active"' if nid == current_id else ""
        html_parts = [
            f'<details{open_attr}><summary><span class="tw">▶</span>'
            f'<a href="{href_rel}"{active_cls_attr}>{label}</a></summary>'
        ]
        for k in kids:
            html_parts.append(render_node(k))
        html_parts.append('</details>')
        return "".join(html_parts)
    if not GRAPH.root_id or GRAPH.root_id not in GRAPH.nodes:
        return ""
    return f'<div class="ocv-tree">{render_node(GRAPH.root_id)}</div>'

def wrap_offline_shell(page_title: str,
                       breadcrumbs_html: str,
                       sidebar_html: str,
                       metadata_html: str,
                       main_html_fragment: str) -> str:
    return f"""<!doctype html>
<html>
<head>
<meta charset="utf-8">
<title>{html.escape(page_title)}</title>
{OFFLINE_UI_STYLE}
</head>
<body>
  <header class="ocv-topbar">
    <button id="navToggle" class="ocv-topbar-btn" aria-label="Toggle navigation">☰</button>
    <div class="ocv-brandwrap">
      <div class="ocv-brand">OfflineConfluenceViewer</div>
      <div class="ocv-author">{html.escape(OFFLINE_AUTHOR)}</div>
      <nav class="ocv-breadcrumbs">{breadcrumbs_html}</nav>
    </div>
    <button id="infoToggle" class="ocv-topbar-btn" aria-label="Toggle info">ℹ️</button>
  </header>
  <section class="ocv-layout">
    <aside class="ocv-sidebar">
      {sidebar_html}
    </aside>
    <main class="ocv-main" id="ocv-content">
      {main_html_fragment}
    </main>
  </section>
  <aside class="ocv-info">
    <div class="ocv-title" style="font-weight:700;margin-bottom:8px;">Page metadata</div>
    {metadata_html}
  </aside>
{OFFLINE_UI_SCRIPT}
</body>
</html>"""

# =========================
# Saving with offline UI + link rewriting
# =========================
def save_page_html(node: PageNode,
                   id_to_folder: Dict[str, str],
                   id_to_path_html: Dict[str, str]) -> bool:
    try:
        href = next(iter(node.hrefs)) if node.hrefs else None
        if not href:
            print(f"[Skip] No URL known for page {node.id}")
            return False
        #print(">>> SAVE_PAGE_HTML")
        # Do allow restarts (test this behavior)
        cid, _, _ = navigate_and_wait(href, 40)
        if cid and cid != node.id:
            print(f"[Info] Page {node.id} resolved to canonical {cid} while saving.")
    except Exception as e:
        print(f"[Error] Navigating to {href}: {e}")
        return False

    page_source = driver.page_source
    soup = BeautifulSoup(page_source, "html.parser")

    main_content = (soup.find(id="main-content") or soup.find(class_="wiki-content") or
                    soup.find(attrs={"data-testid": "ak-renderer-root"}) or
                    soup.find("main") or soup.find("article"))
    if not main_content:
        main_content = soup.new_tag("div")

    if not main_content.get_text(strip=True):
        placeholder = soup.new_tag("p")
        placeholder.string = "(Page Content Empty)"
        main_content.append(placeholder)

    page_folder = id_to_folder[node.id]
    content_dir = os.path.join(page_folder, "content")
    ensure_dir(content_dir)

    head = soup.find("head") or soup.new_tag("head")
    for link in list(head.find_all("link", href=True)):
        rels = [r.lower() for r in (link.get("rel") or [])]
        if "stylesheet" in rels:
            url = link["href"]
            if not (url.startswith("http://") or url.startswith("https://")):
                url = urljoin(BASE_URL, url)
            local = download_file(url, content_dir)
            if local:
                link["href"] = f"./content/{local}"
            else:
                link.decompose()
    for script in list(head.find_all("script", src=True)):
        url = script["src"]
        if not (url.startswith("http://") or url.startswith("https://")):
            url = urljoin(BASE_URL, url)
        local = download_file(url, content_dir)
        if local:
            script["src"] = f"./content/{local}"
        else:
            script.decompose()

    def resolve_target_id(href0: str) -> Optional[str]:
        pid = parse_page_id_from_url(href0)
        if pid and pid in GRAPH.nodes:
            return pid
        sp, title = get_space_and_title_from_url(href0)
        if (sp or "").lower() == SPACE_KEY.lower() and title:
            norm = normalize_text(title)
            for nid, nd in GRAPH.nodes.items():
                if normalize_text(nd.title or "") == norm:
                    return nid
        return None

    for a in main_content.find_all("a", href=True):
        raw_href = a["href"]
        href0, frag = urldefrag(raw_href)

        try:
            absu = urljoin(BASE_URL + "/", href0)
            pu = urlparse(absu)
            data_user = (a.get("data-username") or "").strip()
            candidate = data_user if ("@" in data_user) else None
            if not candidate:
                m = re.search(r"/display/~([^/?#]+)", pu.path or "", flags=re.IGNORECASE)
                if m:
                    candidate = unquote(m.group(1))
            if candidate and "@" in candidate:
                a["href"] = f"mailto:{candidate}"
                continue
        except Exception:
            pass

        cl = clean_url(href0)
        if not cl:
            continue

        tid = resolve_target_id(cl)
        if not tid:
            if not same_space(cl):
                continue
            tid = resolve_target_id(cl)
            if not tid:
                continue

        target_html = id_to_path_html.get(tid)
        if not target_html:
            continue
        cur_dir = id_to_folder[node.id]
        rel = rel_href(cur_dir, target_html)
        a["href"] = rel + (("#" + frag) if frag else "")

    for img in main_content.find_all("img", src=True):
        src = img["src"]
        if not (src.startswith("http://") or src.startswith("https://")):
            src = urljoin(BASE_URL, src)
        local = download_file(src, content_dir)
        if local:
            img["src"] = f"./content/{local}"

    for a in main_content.find_all("a", href=True):
        h = a["href"]
        if "/download/attachments/" in h:
            url = h if (h.startswith("http://") or h.startswith("https://")) else urljoin(BASE_URL, h)
            local = download_file(url, content_dir)
            if local:
                a["href"] = f"./content/{local}"

    final_title = node.title or f"page-{node.id}"
    sidebar_html = build_tree_details_html(node.id, id_to_path_html)
    breadcrumbs_html = render_breadcrumbs(node.id, id_to_path_html)
    meta_html = (
        "<dl class='meta-kv'>"
        f"<dt>Saved at</dt><dd>{time.strftime('%Y-%m-%d %H:%M:%S')}</dd>"
        f"<dt>Local title</dt><dd>{html.escape(final_title)}</dd>"
        f"<dt>Page ID</dt><dd>{html.escape(node.id)}</dd>"
        f"<dt>Original URL</dt><dd>{html.escape(next(iter(node.hrefs)) if node.hrefs else '')}</dd>"
        "</dl>"
    )
    wrapped = wrap_offline_shell(final_title, breadcrumbs_html, sidebar_html, meta_html, str(main_content))
    out_html_path = id_to_path_html[node.id]
    ensure_dir(os.path.dirname(out_html_path))
    with open(out_html_path, "w", encoding="utf-8") as f:
        f.write(wrapped)
    print(f"[Saved] {out_html_path}")
    return True

# =========================
# Crawl orchestration
# =========================
def build_graph_and_titles():
    # 1) Preload cookies on origin BEFORE navigating to the start page
    driver.get(ORIGIN)
    cookies = read_cookies_from_pickle()
    if cookies:
        push_cookies_to_browser(cookies)
        push_cookies_to_requests(cookies)
        driver.get(ORIGIN)  # ensure cookies are applied

    # 2) Navigate to the start page
    # Do not allow restart
    driver.get(START_PAGE_URL)

    print("[Login] Waiting for successful login or cookie auth...")
    while True:
        time.sleep(1)
        if page_matches_start(driver.current_url):
            save_cookies()  # persist any refreshed or new tokens
            break
    print(f"[Login] Success. URL={driver.current_url}")

    # Identify start page
    # Do not allow restart
    #print(">>> BUILD_GRAPH")
    cid, title, parent = navigate_and_wait(START_PAGE_URL, 45, False)
    if not cid:
        driver.refresh()
        #print(">>> BUILD_GRAPH 2")
        cid, title, parent = navigate_and_wait(driver.current_url, 30, False)
    if not cid:
        cid = parse_page_id_from_url(driver.current_url)
    if not cid:
        raise RuntimeError("Could not read content-id for start page (after waits).")

    root_id = str(cid)
    n0 = GRAPH.get_or_create(root_id)
    n0.hrefs.add(driver.current_url)
    if title and not n0.title:
        n0.title = title
    GRAPH.set_parent(root_id, None)
    print(f"[Graph] Seed: {START_PAGE_URL}  (cid={root_id}, title={n0.title})")

    # Expand + Harvest
    expand_full_pagetree()
    print("[PageTree] Harvesting...")
    entries = harvest_pagetree_nodes()
    print(f"[PageTree] Found {len(entries)} entries.")

    # Restart driver just for good measure
    maybe_restart_driver(force=True)

    for pid, t, parent_id, href in entries:
        node = GRAPH.get_or_create(pid)
        node.hrefs.add(href)
        if t and not node.title:
            node.title = t
        if pid == root_id:
            GRAPH.set_parent(root_id, None)
        elif parent_id:
            GRAPH.set_parent(pid, parent_id)

    # BFS crawl
    visited_ids: Set[str] = set()
    queue: List[str] = [root_id]
    while queue:
        cur = queue.pop(0)
        if cur in visited_ids:
            total_now = len(visited_ids) + len(queue)
            pct = (len(visited_ids) / total_now * 100) if total_now else 100.0
            print(f"[Crawl] Progress: {pct:.1f}% ({len(visited_ids)}/{total_now} discovered)")
            continue

        visited_ids.add(cur)
        node = GRAPH.nodes[cur]
        href = next(iter(node.hrefs)) if node.hrefs else f"{BASE_URL}/pages/viewpage.action?pageId={cur}"
        print(f"[Crawl] Visiting id={cur}  title={node.title}  visited={len(visited_ids)}  queue={len(queue)}")
        # DO allow restarts. This is where most of the issues happen
        cid, t, parent = navigate_and_wait(href, 40)
        if cid and str(cid) != cur:
            cur = str(cid)
            node = GRAPH.get_or_create(cur)
        node.hrefs.add(driver.current_url)
        if t and not node.title:
            node.title = t
        if parent:
            GRAPH.set_parent(cur, str(parent))
        elif cur == root_id:
            GRAPH.set_parent(cur, None)

        extra_links = extract_same_space_links_from_content()
        if extra_links and len(extra_links) > 0:
            print(f"[Crawl]   Extracted {len(extra_links)} in-space links.")
        for link in extra_links:
            pid = parse_page_id_from_url(link)
            if pid:
                nn = GRAPH.get_or_create(pid)
                nn.hrefs.add(link)
                if pid not in visited_ids and pid not in queue:
                    queue.append(pid)
            else:
                try:
                    #print(">>> LINK")
                    ecid, etitle, eparent = navigate_and_wait(link, 10)
                    if ecid:
                        ecid = str(ecid)
                        nn = GRAPH.get_or_create(ecid)
                        nn.hrefs.add(link)
                        if etitle and not nn.title:
                            nn.title = etitle
                        if eparent:
                            GRAPH.set_parent(ecid, str(eparent))
                        if ecid not in visited_ids and ecid not in queue:
                            queue.append(ecid)
                except Exception:
                    pass

        total_now = len(visited_ids) + len(queue)
        pct = (len(visited_ids) / total_now * 100) if total_now else 100.0
        print(f"[Crawl] Progress: {pct:.1f}% ({len(visited_ids)}/{total_now} discovered)")

    # Slugs
    for nid, n in GRAPH.nodes.items():
        if not n.title:
            n.title = f"page-{nid}"
    by_parent: Dict[Optional[str], List[str]] = {}
    for nid, n in GRAPH.nodes.items():
        by_parent.setdefault(n.parent_id, []).append(nid)
    for parent_id, child_ids in by_parent.items():
        used: Set[str] = set()
        for nid in sorted(child_ids, key=lambda i: (GRAPH.nodes[i].title or "", i)):
            n = GRAPH.nodes[nid]
            base = sanitize_slug(n.title or f"page-{nid}", f"page-{nid}")
            slug = base if base.lower() not in used else f"{base}-{nid}"
            n.slug = slug
            used.add(slug.lower())

def materialize_folders() -> Tuple[Dict[str, str], Dict[str, str]]:
    """
    Create folders for every page but *omit* the root ("Home") directory level.
    - Root page: folder = ROOT_DIR, html = ROOT_DIR/<root-slug>.html
    - Children of root: folder = ROOT_DIR/<child>/..., html inside that folder
    """
    id_to_folder: Dict[str, str] = {}
    id_to_path_html: Dict[str, str] = {}

    def path_components(nid: str) -> List[str]:
        # Build components from root -> nid
        comps = []
        cur = nid
        while cur is not None:
            node = GRAPH.nodes[cur]
            comps.append(node.slug or f"page-{cur}")
            cur = node.parent_id
        comps.reverse()
        # Drop the root ("Home") component if present
        if comps:
            comps = comps[1:]
        return comps

    for nid in GRAPH.all_ids():
        comps = path_components(nid)

        # Root page sits directly under ROOT_DIR
        if GRAPH.root_id and nid == GRAPH.root_id:
            folder = ROOT_DIR
        else:
            folder = os.path.join(ROOT_DIR, *comps) if comps else ROOT_DIR

        ensure_dir(folder)
        id_to_folder[nid] = folder

        # Keep filename as <slug>.html (root becomes ROOT_DIR/<root-slug>.html)
        html_name = f"{GRAPH.nodes[nid].slug}.html"
        id_to_path_html[nid] = os.path.join(folder, html_name)

        # Per-page assets folder
        ensure_dir(os.path.join(folder, "content"))

    return id_to_folder, id_to_path_html

def save_all_pages(id_to_folder: Dict[str, str], id_to_path_html: Dict[str, str]):
    def dfs(nid: str, acc: List[str]):
        acc.append(nid)
        for c in sorted(GRAPH.nodes[nid].children, key=lambda k: (GRAPH.nodes[k].title or "", k)):
            dfs(c, acc)
    order: List[str] = []
    if GRAPH.root_id:
        dfs(GRAPH.root_id, order)
    else:
        order = GRAPH.all_ids()
    
    total = len(order)
    for idx, nid in enumerate(order, start=1):
        title = GRAPH.nodes[nid].title or f"page-{nid}"
        try:
            ok = save_page_html(GRAPH.nodes[nid], id_to_folder, id_to_path_html)
            status = "ok" if ok else "skip"
        except Exception as e:
            print(f"[Error] Saving {nid}: {e}")
            status = "error"
        percent = int((idx / total) * 100) if total else 100
        print(f"{idx}/{total} ({percent}%) {status}: {title[:40]}")
    with open(os.path.join(ROOT_DIR, "offline_graph.json"), "w", encoding="utf-8") as f:
        json.dump({
            "space": SPACE_KEY,
            "root_id": GRAPH.root_id,
            "nodes": {
                nid: {
                    "title": GRAPH.nodes[nid].title,
                    "slug": GRAPH.nodes[nid].slug,
                    "parent": GRAPH.nodes[nid].parent_id,
                    "hrefs": list(GRAPH.nodes[nid].hrefs),
                    "folder": id_to_folder.get(nid),
                    "html": id_to_path_html.get(nid),
                } for nid in GRAPH.nodes
            }
        }, f, indent=2)

# =========================
# Per-link runner
# =========================
def run_one_space(space_key: str, start_page: str):
    global SPACE_KEY, CURRENT_START_PAGE, START_PAGE_URL, ROOT_DIR, GRAPH
    SPACE_KEY = space_key
    CURRENT_START_PAGE = start_page
    START_PAGE_URL = f"{BASE_URL}/display/{SPACE_KEY}/{CURRENT_START_PAGE}"
    ROOT_DIR = f"{SPACE_KEY}_offline"
    ensure_dir(ROOT_DIR)
    GRAPH = SpaceGraph(SPACE_KEY)

    print("\n" + "="*80)
    print(f"[Run] Space={SPACE_KEY}  StartPage={CURRENT_START_PAGE}")
    print(f"[Run] Start URL: {START_PAGE_URL}")
    print(f"[Run] Output dir: {ROOT_DIR}")
    print("="*80)

    build_graph_and_titles()
    id_to_folder, id_to_path_html = materialize_folders()
    save_all_pages(id_to_folder, id_to_path_html)
    print(f"[Run] Finished dumping all files for {SPACE_KEY}.")
    #save_cookies()  # persist any refreshed tokens

# =========================
# Main
# =========================
if __name__ == "__main__":
    try:
        for sp_key, sp_start in LINKS:
            run_one_space(sp_key, sp_start)
        print("All runs complete.")
    finally:
        try:
            driver.quit()
        except Exception:
            pass
