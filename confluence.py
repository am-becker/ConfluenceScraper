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

"""
Confluence Space Downloader — ID-first, hierarchical, offline viewer

Key changes vs. your script:
- Build a full in-memory graph of the space by contentId (read from DOM),
  expanding the Page Tree UI (no REST), plus extra in-space links we discover.
- Materialize a folder hierarchy that mirrors the page tree:
    <SPACE_KEY>_offline/
      <SlugA>/
        SlugA.html
        content/  (all assets used by SlugA)
        <SlugChild1>/
          SlugChild1.html
          content/
        ...
- Rewrite internal same-space links using the graph so they always point to the
  correct relative file (handles /display/... and /pages/viewpage.action?pageId=...).
- Titles with "/" (e.g., "Compressor/Inlet") are sanitized to "CompressorInlet".
- Pages with unknown titles save as page-<ID>.html inside page-<ID>/, but we try to
  resolve the real title by actually visiting the page before writing anything.
- Inject an offline sidebar tree + metadata panel on every saved page.
"""

# =========================
# Load Configuration
# =========================
CONFIG_FILE = "config.json"
with open(CONFIG_FILE, "r", encoding="utf-8") as f:
    config = json.load(f)

BASE_URL = config["base_url"].rstrip("/")
SPACE_KEY = config["space_key"]
START_PAGE_URL = f"{BASE_URL}/display/{SPACE_KEY}/{config['start_page']}"
COOKIES_FILE = config["cookies_file"]

# =========================
# Output Directories (hierarchical)
# =========================
ROOT_DIR = f"{SPACE_KEY}_offline"           # e.g., MITSET_offline
os.makedirs(ROOT_DIR, exist_ok=True)

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
    if os.path.exists(COOKIES_FILE):
        driver.get(BASE_URL)
        with open(COOKIES_FILE, "rb") as f:
            cookies = pickle.load(f)
            for cookie in cookies:
                try:
                    driver.add_cookie(cookie)
                except Exception:
                    pass
                # domain-less is OK for our requests session
                session.cookies.set(cookie.get("name"), cookie.get("value"))

def save_cookies():
    cookies = driver.get_cookies()
    with open(COOKIES_FILE, "wb") as f:
        pickle.dump(cookies, f)
    for cookie in cookies:
        try:
            driver.add_cookie(cookie)
        except Exception:
            pass
        session.cookies.set(cookie.get("name"), cookie.get("value"))

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

def same_space(url: str) -> bool:
    space, _ = get_space_and_title_from_url(url)
    return (space or "").lower() == SPACE_KEY.lower()

def get_space_and_title_from_url(url: str) -> Tuple[Optional[str], Optional[str]]:
    u = urlparse(url)
    p = u.path or ""

    # /display/<SPACE>/<TITLE>
    if "/display/" in p:
        try:
            parts = [x for x in p.split("/") if x]
            i = parts.index("display")
            space = parts[i + 1] if len(parts) > i + 1 else None
            title = unquote(parts[i + 2]).replace("+", " ") if len(parts) > i + 2 else None
            return space, title
        except Exception:
            pass

    # /pages/viewpage.action?spaceKey=...&title=... OR ?pageId=...
    if p.endswith("/pages/viewpage.action"):
        q = parse_qs(u.query)
        space = q.get("spaceKey", [None])[0]
        title = q.get("title", [None])[0]
        if title:
            title = unquote(title).replace("+", " ")
        return space, title

    return None, None

def clean_url(url: str) -> Optional[str]:
    if not url:
        return None
    # make absolute
    if not (url.startswith("http://") or url.startswith("https://")):
        url = urljoin(BASE_URL, url)
    u = urlparse(url)
    if not (u.scheme and u.netloc):
        return None
    if not url.startswith(BASE_URL):
        return None
    if any(bad in url for bad in RESTRICTED_URLS) or "/label/" in url:
        return None
    # drop query/fragment for enqueuing; we use ID/title later
    cleaned = u._replace(fragment="", query="")
    return urlunparse(cleaned)

def sanitize_slug(name: str, fallback: str) -> str:
    """
    Make a filesystem-safe slug for folder and html filename.
    - Remove '/', '\' entirely (so 'Compressor/Inlet' -> 'CompressorInlet')
    - Keep letters/digits/space/_/-, collapse whitespace to ''
    - Enforce length and reserved names safety (Windows)
    """
    if not name:
        base = fallback
    else:
        s = unquote(name)
        s = s.replace("/", "").replace("\\", "")
        s = re.sub(r"[^A-Za-z0-9 _\-.]+", "", s)
        s = re.sub(r"\s+", "", s)  # spaces -> nothing (Compressor Inlet -> CompressorInlet)
        base = s.strip() or fallback

    base = base.strip(". ") or fallback  # avoid trailing dots/spaces (Windows)
    low = base.lower()
    if low in WINDOWS_RESERVED:
        base = f"{base}_page"
    # keep it human-readable but within limits
    return base[:120] or fallback  # conservative length

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
    title: Optional[str] = None           # becomes final after we visit
    slug: Optional[str] = None            # filesystem-safe (finalized before write)
    hrefs: Set[str] = field(default_factory=set)  # known URLs that load this page
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

GRAPH = SpaceGraph(SPACE_KEY)

# =========================
# Login gate helpers
# =========================
def page_matches_start(url: str) -> bool:
    """
    Are we at the intended start page? Compare normalized SPACE + title when present,
    or fall back to content-id check after DOM is ready.
    """
    # Quick URL heuristic
    u = urlparse(url)
    if "/display/" in (u.path or ""):
        parts = [p for p in u.path.split("/") if p]
        try:
            i = parts.index("display")
            space = parts[i + 1]
            title = unquote(parts[i + 2]).replace("+", " ") if len(parts) > i + 2 else ""
            return normalize_text(space) == normalize_text(SPACE_KEY) and normalize_text(title) == normalize_text(config["start_page"])
        except Exception:
            pass
    if (u.path or "").endswith("/pages/viewpage.action"):
        q = parse_qs(u.query)
        if normalize_text(q.get("spaceKey", [""])[0]) == normalize_text(SPACE_KEY) and normalize_text(q.get("title", [""])[0]) == normalize_text(config["start_page"]):
            return True
    # DOM fallback
    try:
        h1 = WebDriverWait(driver, 2).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "#title-text a"))
        )
        title = (h1.text or "").strip()
        return normalize_text(title) == normalize_text(config["start_page"])
    except Exception:
        return False
    
def wait_for_page_identity(timeout_sec: int = 30) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    """
    Polls until the page exposes content-id & (title|parent) via meta/DOM.
    Returns (content_id, title, parent_id) or (None, None, None) on timeout.
    """
    deadline = time.time() + timeout_sec
    while time.time() < deadline:
        try:
            cid = driver.execute_script(
                """
                var q = (n)=>{var el=document.querySelector('meta[name="'+n+'"]'); return el? el.getAttribute('content'): null;};
                var m = q('ajs-content-id') || q('ajs-page-id') || q('ajs-latest-page-id');
                return m;
                """
            )
            if cid:
                # title via <h1 id="title-text"> or meta
                title = driver.execute_script(
                    """
                    var t = document.querySelector('#title-text a');
                    if (t && t.textContent) return t.textContent.trim();
                    var m = document.querySelector('meta[name="ajs-page-title"]');
                    if (m && m.content) return m.content.trim();
                    return (document.title||'').replace(/\s*-\s*Confluence.*/,'').trim();
                    """
                )
                parent = driver.execute_script(
                    """
                    var p = document.querySelector('meta[name="ajs-parent-page-id"]');
                    return p && p.content ? p.content : null;
                    """
                )
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
    # Any one of these showing up means “content is there”
    selectors = [
        "#main-content",
        ".wiki-content",
        "[data-testid='ak-renderer-root']",
        "main",
        "article",
    ]
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

def navigate_and_wait(url: str, timeout_sec: int = 45) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    driver.get(url)
    wait_for_dom_ready(min(10, timeout_sec))
    cid, title, parent = wait_for_page_identity(max(0, timeout_sec - 10))
    # Don’t fail if meta’s slow — at least wait for visible content.
    wait_for_main_content(15)
    return cid, title, parent

# =========================
# DOM extraction helpers
# =========================

def read_dom_ids_titles_parent() -> Tuple[Optional[str], Optional[str], Optional[str]]:
    """
    From the current page DOM, return (contentId, title, parentId) if available.
    """
    try:
        cid = driver.execute_script(
            """
            var q = (n)=>{var el=document.querySelector('meta[name="'+n+'"]'); return el? el.getAttribute('content'): null;};
            return q('ajs-content-id') || q('ajs-page-id') || q('ajs-latest-page-id');
            """
        )
        title = driver.execute_script(
            """
            var t = document.querySelector('#title-text a');
            if (t && t.textContent) return t.textContent.trim();
            var m = document.querySelector('meta[name="ajs-page-title"]');
            if (m && m.content) return m.content.trim();
            return (document.title||'').replace(/\s*-\s*Confluence.*/,'').trim();
            """
        )
        parent = driver.execute_script(
            "var p=document.querySelector('meta[name=\"ajs-parent-page-id\"]'); return p && p.content ? p.content : null;"
        )
        if not cid:
            # last-ditch: infer from URL if we're on viewpage.action
            cid = parse_page_id_from_url(driver.current_url)
        return (str(cid) if cid else None), (title or None), (str(parent) if parent else None)
    except Exception:
        return None, None, None

def expand_full_pagetree():
    """
    Expand the sidebar Page Tree by clicking all collapsed toggles.
    """
    try:
        # Sidebar might be collapsed; try to open it by clicking the page-tree header
        WebDriverWait(driver, 5).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, ".plugin_pagetree"))
        )
    except Exception:
        return

    for _ in range(60):
        toggles = driver.find_elements(By.CSS_SELECTOR, "a.plugin_pagetree_childtoggle[aria-expanded='false']")
        if not toggles:
            break
        for t in toggles:
            try:
                driver.execute_script("arguments[0].scrollIntoView({block:'center'});", t)
                t.click()
                pid = t.get_attribute("data-page-id") or ""
                container_id = f"children{pid}-0"
                # wait briefly for child container to populate (best-effort without REST)
                try:
                    WebDriverWait(driver, 4).until(
                        EC.presence_of_element_located((By.ID, container_id))
                    )
                except Exception:
                    pass
                time.sleep(0.15)
            except Exception:
                continue

def harvest_pagetree_nodes() -> List[Tuple[str, str, Optional[str], str]]:
    """
    Return list of (pageId, title, parentId, href) from the expanded Page Tree.
    """
    results = []
    lis = driver.find_elements(By.CSS_SELECTOR, ".plugin_pagetree_children_list li")
    for li in lis:
        try:
            # Find the anchor + toggle (has data-page-id)
            a = li.find_element(By.CSS_SELECTOR, ".plugin_pagetree_children_content a[href]")
            href = a.get_attribute("href")
            title = (a.text or "").strip()
            page_id = None
            try:
                tog = li.find_element(By.CSS_SELECTOR, ".plugin_pagetree_childtoggle")
                page_id = tog.get_attribute("data-page-id")
            except Exception:
                page_id = parse_page_id_from_url(href)

            # parent comes from nearest ancestor UL id child_ul<parent>-*
            parent_id = None
            try:
                parent_ul = li.find_element(By.XPATH, "ancestor::ul[1]")
                ul_id = parent_ul.get_attribute("id") or ""  # e.g., child_ul282896951-0
                m = re.search(r"child_ul(\d+)-", ul_id)
                if m:
                    parent_id = m.group(1)
            except Exception:
                pass

            if page_id:
                results.append((str(page_id), title, parent_id, href))
        except Exception:
            continue
    return results

def extract_same_space_links_from_content() -> Set[str]:
    """
    From current page main content, collect same-space anchors (absolute URLs).
    """
    try:
        anchors = driver.execute_script(
            """
            const out = [];
            document.querySelectorAll('#main-content a[href], .wiki-content a[href]').forEach(a=>{
                try{ out.push(a.href); }catch(e){}
            });
            return out;
            """
        )
    except Exception:
        anchors = []

    found = set()
    for href in anchors:
        href0, _frag = urldefrag(href)
        cl = clean_url(href0)
        if cl and same_space(cl):
            found.add(cl)
    return found

# =========================
# Download (per-page content folder)
# =========================

def download_file(url: str, save_dir: str) -> Optional[str]:
    if not (url.startswith("http://") or url.startswith("https://")):
        url = urljoin(BASE_URL, url)

    # filename from URL
    def _sanitize_filename(fn: str) -> str:
        fn = unquote(fn.split("?")[0].split("/")[-1])
        fn = re.sub(r"[^A-Za-z0-9._\-]+", "_", fn)
        return fn or "file"

    filename = _sanitize_filename(url)
    ensure_dir(save_dir)
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
# Saving with offline UI + link rewriting
# =========================

def build_tree_html(current_id: str, id_to_path_html: Dict[str, str]) -> str:
    """
    Build a nested UL of the page tree. Links are relative to the *current* page.
    """
    # Build children mapping (already present in GRAPH), but we need deterministic order
    def children_sorted(pid: str) -> List[str]:
        kids = list(GRAPH.nodes[pid].children)
        kids.sort(key=lambda k: (GRAPH.nodes[k].title or "", k))
        return kids

    def li_for(pid: str) -> str:
        n = GRAPH.nodes[pid]
        # compute relative href from current to this node
        target_html = id_to_path_html[pid]
        cur_dir = os.path.dirname(id_to_path_html[current_id])
        href_rel = rel_href(cur_dir, target_html)
        cls = " class=\"active\"" if pid == current_id else ""
        label = html.escape(n.title or f"page-{n.id}")
        s = f"<li{cls}><a href=\"{href_rel}\">{label}</a>"
        kids = children_sorted(pid)
        if kids:
            s += "<ul>"
            for c in kids:
                s += li_for(c)
            s += "</ul>"
        s += "</li>"
        return s

    if not GRAPH.root_id or GRAPH.root_id not in GRAPH.nodes:
        return ""

    return "<ul class=\"tree\">" + li_for(GRAPH.root_id) + "</ul>"

OFFLINE_UI_STYLE = """
<style>
body { margin:0; font-family: system-ui, -apple-system, Segoe UI, Roboto, Helvetica, Arial, sans-serif; }
.ocv-container { display: grid; grid-template-columns: 300px 1fr 300px; min-height: 100vh; }
.ocv-sidebar { border-right: 1px solid #e2e2e2; padding: 12px; overflow:auto; }
.ocv-main { padding: 0; }
.ocv-metadata { border-left: 1px solid #e2e2e2; padding: 12px; overflow:auto; }
.ocv-header { display:flex; align-items:center; justify-content: space-between; padding: 8px 12px; border-bottom: 1px solid #e2e2e2; background:#fafafa; position:sticky; top:0; z-index:5;}
.ocv-title { font-weight:700; }
button.ocv-toggle { border:1px solid #ccc; background:#fff; padding:6px 10px; border-radius:6px; cursor:pointer; }
ul.tree { list-style:none; padding-left: 0; }
ul.tree li { margin:2px 0; }
ul.tree li > a { text-decoration:none; color:#1f4aa3; }
ul.tree li.active > a { font-weight:700; text-decoration:underline; }
.ocv-collapsed .ocv-sidebar { display:none; }
.ocv-collapsed { grid-template-columns: 1fr 300px; }
.ocv-meta-collapsed .ocv-metadata { display:none; }
.ocv-meta-collapsed { grid-template-columns: 300px 1fr; }
.ocv-collapsed.ocv-meta-collapsed { grid-template-columns: 1fr; }
#ocv-content { padding: 16px; }
.meta-kv { font-size: 14px; line-height:1.5; }
.meta-kv dt { font-weight:600; }
.meta-kv dd { margin:0 0 8px 0; word-break:break-all; }
</style>
"""

OFFLINE_UI_SCRIPT = """
<script>
(function(){
  const root = document.querySelector('.ocv-container');
  document.querySelector('#toggle-nav').addEventListener('click', ()=> {
    root.classList.toggle('ocv-collapsed');
  });
  document.querySelector('#toggle-meta').addEventListener('click', ()=> {
    root.classList.toggle('ocv-meta-collapsed');
  });
})();
</script>
"""

def wrap_offline_shell(page_title: str,
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
<div class="ocv-container ocv-meta-collapsed">
  <div class="ocv-sidebar">
    <div class="ocv-header">
      <div class="ocv-title">OfflineConfluenceViewer</div>
      <button class="ocv-toggle" id="toggle-nav" aria-label="Hide/Show page tree">☰</button>
    </div>
    {sidebar_html}
  </div>
  <div class="ocv-main">
    <div class="ocv-header">
      <div class="ocv-title">{html.escape(page_title)}</div>
      <button class="ocv-toggle" id="toggle-meta" aria-label="Show/Hide metadata">ℹ</button>
    </div>
    <div id="ocv-content">{main_html_fragment}</div>
  </div>
  <aside class="ocv-metadata">
    <div class="ocv-title" style="margin-bottom:8px;">Page metadata</div>
    {metadata_html}
  </aside>
</div>
{OFFLINE_UI_SCRIPT}
</body>
</html>"""

def save_page_html(node: PageNode,
                   id_to_folder: Dict[str, str],
                   id_to_path_html: Dict[str, str]) -> bool:
    """
    - Downloads head CSS/JS into ./content/
    - Rewrites internal links to relative HTML paths
    - Downloads images/attachments into ./content/
    - Wraps content in offline viewer shell
    """
    try:
        # Load primary href
        href = next(iter(node.hrefs)) if node.hrefs else None
        if not href:
            print(f"[Skip] No URL known for page {node.id}")
            return False
        cid, _, _ = navigate_and_wait(href, 40)
        # (Optional) Assert we landed on the intended node; if not, adjust:
        if cid and cid != node.id:
            # We followed an alias URL; re-point to canonical node id
            print(f"[Info] Page {node.id} resolved to canonical {cid} while saving.")

    except Exception as e:
        print(f"[Error] Navigating to {href}: {e}")
        return False

    # Parse DOM
    page_source = driver.page_source
    soup = BeautifulSoup(page_source, "html.parser")

    # Main content detection (classic + newer)
    main_content = soup.find(id="main-content") or soup.find(class_="wiki-content")
    if not main_content:
        main_content = soup.find(attrs={"data-testid": "ak-renderer-root"}) or soup.find("main") or soup.find("article")
    if not main_content:
        main_content = soup.new_tag("div")

    # Per-page content dir
    page_folder = id_to_folder[node.id]
    content_dir = os.path.join(page_folder, "content")
    ensure_dir(content_dir)

    # ---------- Head assets ----------
    head = soup.find("head") or soup.new_tag("head")

    # Stylesheets
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

    # Scripts
    for script in list(head.find_all("script", src=True)):
        url = script["src"]
        if not (url.startswith("http://") or url.startswith("https://")):
            url = urljoin(BASE_URL, url)
        local = download_file(url, content_dir)
        if local:
            script["src"] = f"./content/{local}"
        else:
            script.decompose()

    # ---------- Rewrite internal links in main content ----------
    def resolve_target_id(href0: str) -> Optional[str]:
        # Prefer pageId in URL
        pid = parse_page_id_from_url(href0)
        if pid and pid in GRAPH.nodes:
            return pid
        # Try title mapping inside same space
        sp, title = get_space_and_title_from_url(href0)
        if (sp or "").lower() == SPACE_KEY.lower() and title:
            norm = normalize_text(title)
            # Find by normalized title
            for nid, nd in GRAPH.nodes.items():
                if normalize_text(nd.title or "") == norm:
                    return nid
        return None

    # Links
    for a in main_content.find_all("a", href=True):
        href0, frag = urldefrag(a["href"])
        cl = clean_url(href0)
        if not cl:
            continue
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

    # Images
    for img in main_content.find_all("img", src=True):
        src = img["src"]
        if not (src.startswith("http://") or src.startswith("https://")):
            src = urljoin(BASE_URL, src)
        local = download_file(src, content_dir)
        if local:
            img["src"] = f"./content/{local}"

    # Attachments (download/attachments)
    for a in main_content.find_all("a", href=True):
        h = a["href"]
        if "/download/attachments/" in h:
            url = h if (h.startswith("http://") or h.startswith("https://")) else urljoin(BASE_URL, h)
            local = download_file(url, content_dir)
            if local:
                a["href"] = f"./content/{local}"

    # Title + metadata
    final_title = node.title or f"page-{node.id}"

    # Build sidebar HTML (relative links) + metadata
    sidebar_html = build_tree_html(node.id, id_to_path_html)
    meta_html = (
        "<dl class='meta-kv'>"
        f"<dt>Saved at</dt><dd>{time.strftime('%Y-%m-%d %H:%M:%S')}</dd>"
        f"<dt>Local title</dt><dd>{html.escape(final_title)}</dd>"
        f"<dt>Page ID</dt><dd>{html.escape(node.id)}</dd>"
        f"<dt>Original URL</dt><dd>{html.escape(next(iter(node.hrefs)) if node.hrefs else '')}</dd>"
        "</dl>"
    )

    # Replace page body with our shell, embedding the (rewritten) main_content
    wrapped = wrap_offline_shell(final_title, sidebar_html, meta_html, str(main_content))

    # Write to <folder>/<slug>.html
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
    """
    Phase 1: login, get start page, expand page tree, harvest nodes.
    Phase 2: visit every discovered page to collect contentId, title, parentId,
             and discover extra in-space links. Avoid cycles by ID.
    """
    load_cookies()

    # Navigate to start page (user may need to log in manually)
    driver.get(START_PAGE_URL)
    print("Waiting for successful login...")
    while True:
        time.sleep(1)
        if page_matches_start(driver.current_url):
            save_cookies()
            break
    print("Login successful. Building space graph...")

    # Read start page id/title/parent (and actually wait)
    cid, title, parent = navigate_and_wait(START_PAGE_URL, 45)
    if not cid:
        driver.refresh()
        cid, title, parent = navigate_and_wait(driver.current_url, 30)
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

    # Expand and harvest Page Tree data
    print("PRE EXPAND")
    time.sleep(5)
    expand_full_pagetree()
    print("POST EXPAND")
    entries = harvest_pagetree_nodes()  # (pid, title, parentId, href)
    print("ENTRIES",entries)
    for pid, t, parent_id, href in entries:
        node = GRAPH.get_or_create(pid)
        node.hrefs.add(href)
        if t and not node.title:
            node.title = t
        # Parent inferred from UI nesting (may be None for root; keep previous if known)
        if pid == root_id:
            GRAPH.set_parent(root_id, None)
        elif parent_id:
            GRAPH.set_parent(pid, parent_id)

    # BFS visit to finalize titles/parents and find extra links
    visited_ids: Set[str] = set()
    queue: List[str] = [root_id]

    while queue:
        cur = queue.pop(0)
        if cur in visited_ids:
            continue
        visited_ids.add(cur)

        node = GRAPH.nodes[cur]
        # Choose a URL to load this page
        href = next(iter(node.hrefs)) if node.hrefs else f"{BASE_URL}/pages/viewpage.action?pageId={cur}"
        cid, t, parent = navigate_and_wait(href, 40)
        
        if cid and str(cid) != cur:
            # If Confluence redirected to the canonical page for this ID, reconcile
            # We'll relabel: keep 'cur' node but ensure cid matches (shouldn't happen often)
            cur = str(cid)
            node = GRAPH.get_or_create(cur)

        # Keep URL
        node.hrefs.add(driver.current_url)

        # Title
        if t and not node.title:
            node.title = t

        # Parent from meta (more authoritative than UI nesting)
        if parent:
            GRAPH.set_parent(cur, str(parent))
        elif cur == root_id:
            GRAPH.set_parent(cur, None)

        # Discover extra same-space links in content (not only in Page Tree)
        extra_links = extract_same_space_links_from_content()
        for link in extra_links:
            # Try to parse pageId without loading
            pid = parse_page_id_from_url(link)
            if pid:
                nn = GRAPH.get_or_create(pid)
                nn.hrefs.add(link)
                if pid not in visited_ids and pid not in queue:
                    queue.append(pid)
            else:
                # We'll visit the URL to read its actual content-id
                try:
                    ecid, etitle, eparent = navigate_and_wait(link, 35)
                    if ecid:
                        nn = GRAPH.get_or_create(str(ecid))
                        nn.hrefs.add(link)
                        if etitle and not nn.title:
                            nn.title = etitle
                        if eparent:
                            GRAPH.set_parent(str(ecid), str(eparent))
                        if str(ecid) not in visited_ids and str(ecid) not in queue:
                            queue.append(str(ecid))
                except Exception:
                    pass

    # Finalize slugs per parent to be unique and stable
    # First, ensure every node at least has a fallback title
    for nid, n in GRAPH.nodes.items():
        if not n.title:
            n.title = f"page-{nid}"

    # For each parent, ensure unique slugs among its children
    by_parent: Dict[Optional[str], List[str]] = {}
    for nid, n in GRAPH.nodes.items():
        by_parent.setdefault(n.parent_id, []).append(nid)

    for parent_id, child_ids in by_parent.items():
        used: Set[str] = set()
        for nid in sorted(child_ids, key=lambda i: (GRAPH.nodes[i].title or "", i)):
            n = GRAPH.nodes[nid]
            base = sanitize_slug(n.title or f"page-{nid}", f"page-{nid}")
            slug = base
            k = 2
            while slug.lower() in used:
                slug = f"{base}-{nid}"
                break  # disambiguate using id; one pass is enough
            n.slug = slug
            used.add(slug.lower())

def materialize_folders() -> Tuple[Dict[str, str], Dict[str, str]]:
    """
    Create folders for every page and return:
      - id_to_folder: pageId -> folder path
      - id_to_path_html: pageId -> full path to HTML file
    """
    id_to_folder: Dict[str, str] = {}
    id_to_path_html: Dict[str, str] = {}

    # Compute path by ascending to root
    def path_components(nid: str) -> List[str]:
        comps = []
        cur = nid
        while cur is not None:
            node = GRAPH.nodes[cur]
            comps.append(node.slug or f"page-{cur}")
            cur = node.parent_id
        comps.reverse()
        return comps

    for nid in GRAPH.all_ids():
        comps = path_components(nid)
        folder = os.path.join(ROOT_DIR, *comps)
        ensure_dir(folder)
        id_to_folder[nid] = folder
        id_to_path_html[nid] = os.path.join(folder, f"{GRAPH.nodes[nid].slug}.html")

        # Ensure per-page content directory exists
        ensure_dir(os.path.join(folder, "content"))

    return id_to_folder, id_to_path_html

def save_all_pages(id_to_folder: Dict[str, str], id_to_path_html: Dict[str, str]):
    """
    Save every page’s HTML with rewritten links and per-page assets.
    """
    # Save visit order root-first depth-first for nicer progress
    def dfs(nid: str, acc: List[str]):
        acc.append(nid)
        for c in sorted(GRAPH.nodes[nid].children, key=lambda k: (GRAPH.nodes[k].title or "", k)):
            dfs(c, acc)
    order: List[str] = []
    if GRAPH.root_id:
        dfs(GRAPH.root_id, order)
    else:
        order = GRAPH.all_ids()

    for nid in order:
        try:
            save_page_html(GRAPH.nodes[nid], id_to_folder, id_to_path_html)
        except Exception as e:
            print(f"[Error] Saving {nid}: {e}")

    # Persist crawl state & map
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
# Main
# =========================

if __name__ == "__main__":
    try:
        build_graph_and_titles()
        id_to_folder, id_to_path_html = materialize_folders()
        save_all_pages(id_to_folder, id_to_path_html)
        print("Finished dumping all files.")
    finally:
        try:
            driver.quit()
        except Exception:
            pass
