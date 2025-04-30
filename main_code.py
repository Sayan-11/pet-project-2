# ░░░  GMAT-Club Scraper  ░░░  (CR, DS, RC, …)

from __future__ import annotations
import json, logging, os, pickle, random, re, subprocess, sys, time
from contextlib import contextmanager
from enum  import Enum
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, TypedDict, Union
from bs4 import BeautifulSoup, NavigableString,Tag

import undetected_chromedriver as uc
from selenium.webdriver            import Chrome
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by   import By
from selenium.webdriver.support.ui  import WebDriverWait
from selenium.webdriver.support     import expected_conditions as EC
from selenium.common.exceptions     import TimeoutException
from openai import OpenAI, OpenAIError

# ─── logging ───────────────────────────────────────────────────────
LOG = logging.getLogger("gmat.scraper")
LOG.setLevel(logging.INFO)
if not LOG.handlers:
    h = logging.StreamHandler()
    h.setFormatter(logging.Formatter("%(asctime)s | %(levelname)8s | %(message)s"))
    LOG.addHandler(h)

# ─── enums / TypedDicts  (unchanged) ───────────────────────────────
class QuestionType(str, Enum):
    CR="cr"; DS="ds"; RC="rc"; PS="ps"; TPA="tpa"; MSR="msr"; GRAPHS="graphs"; TABLES="tables"

# …  all your CRDict / RCDict / GraphDict / …   definitions unchanged …

# ─── detect local Chrome major version ─────────────────────────────
def _detect_chrome_major() -> int:
    """Return the installed Chrome major version (135, 136, …)."""
    if sys.platform.startswith("darwin"):
        out = subprocess.check_output(
            ["/Applications/Google Chrome.app/Contents/MacOS/Google Chrome", "--version"],
            text=True
        )
    elif sys.platform.startswith("win"):
        out = subprocess.check_output(
            r'reg query "HKCU\Software\Google\Chrome\BLBeacon" /v version',
            shell=True, text=True
        )
    else:  # linux
        out = subprocess.check_output(["google-chrome", "--version"], text=True)
    return int(re.search(r"(\d+)\.", out).group(1))

CHROME_MAJOR = _detect_chrome_major()

# ─── driver helper ─────────────────────────────────────────────────
@contextmanager
def get_driver(headless: bool = True):
    opts = Options()
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--window-size=1920,1080")
    opts.add_argument("--disable-blink-features=AutomationControlled")
    if headless:
        opts.add_argument("--headless=new")        # Chrome ≥ 109
    drv: Chrome = uc.Chrome(options=opts, version_main=CHROME_MAJOR)
    try:
        yield drv
    finally:
        drv.quit()

# ─── cookies / login  (single fixed file) ──────────────────────────
COOKIE_FILE = (Path(__file__).resolve().parent if "__file__" in globals() else Path.cwd()) \
              / "emmarose0012_gmail_com.pkl"

def _is_logged_in(drv: Chrome) -> bool:
    return "logout" in drv.page_source.lower()

def _load_cookies(drv: Chrome) -> bool:
    """Return True ⇢ cookies existed *and* we end up logged-in."""
    if not COOKIE_FILE.exists():
        return False
    # 1️⃣ open the domain first so add_cookie will accept them
    drv.get("https://gmatclub.com/forum/")
    time.sleep(1.2)
    for c in pickle.loads(COOKIE_FILE.read_bytes()):
        try:
            drv.add_cookie(c)
        except Exception:
            pass    # skip expired / incompatible cookies
    drv.refresh()
    time.sleep(1.5)
    ok = _is_logged_in(drv)
    LOG.info("cookies loaded -> logged-in: %s", ok)
    return ok

def _save_cookies(drv: Chrome):
    COOKIE_FILE.write_bytes(pickle.dumps(drv.get_cookies()))
    LOG.info("✅ cookies saved → %s", COOKIE_FILE)

def _login(drv: Chrome, email: str, pw: str, timeout: int = 30):
    """Manual login (one-time); saves cookies for future runs."""
    drv.get("https://gmatclub.com/forum/ucp.php?mode=login")
    WebDriverWait(drv, timeout).until(EC.presence_of_element_located((By.NAME, "username")))
    drv.find_element(By.NAME, "username").send_keys(email)
    drv.find_element(By.NAME, "password").send_keys(pw)
    drv.find_element(By.NAME, "login").click()
    time.sleep(4)                          # Cloudflare / redirect

    if "just a moment" in drv.title.lower():
        input("⚠️  Solve CAPTCHA in the browser, then press <ENTER> here…")

    if not WebDriverWait(drv, timeout).until(lambda d: _is_logged_in(d)):
        raise ScrapeError("Login failed – check credentials")
    _save_cookies(drv)


# ─── basic cleaner (whitespace only) ────────────────────────────────
def basic_clean(t:str)->str: return re.sub(r"\\s+"," ", t.replace("\\r"," ").replace("\\n"," ")).strip()

# ─── optional GPT polish (spacing) ─────────────────────────────────
_client = OpenAI(api_key=os.getenv("OPENAI_API_KEY")) if os.getenv("OPENAI_API_KEY") else None
def _polish(d:QuestionData,q:QuestionType)->QuestionData:
    if not _client: return d
    try:
        r=_client.chat.completions.create(
            model=os.getenv("GPT_POLISH_MODEL","gpt-4o-mini"),
            response_format={"type":"json_object"},
            messages=[{"role":"system","content":"Fix spacing only."},{"role":"user","content":json.dumps({"type":q.value,"data":d})}],
            max_tokens=2048)
        return json.loads(r.choices[0].message.content)
    except (OpenAIError,json.JSONDecodeError):
        LOG.warning("Polish failed"); return d

# ─── helpers --------------------------------------------------------
def _split_opts(raw: str) -> tuple[str, dict[str, str]]:
    """
    Works for
        (A) text   •  A- text   •  A) text   •  (A). text
    Returns (stem, {"A": ..., "B": ...})
    """
    pattern = r"""
        (?:^|\n|\r)            # start of line
        [\s\(\[]*              # optional whitespace / opening bracket
        (?P<lbl>[A-E])         # capture A-E
        [\)\].\-–:]?           # closing ) ] . : - –
        \s+                    # at least one space before the real text
    """
    matches = list(re.finditer(pattern, raw, re.I | re.VERBOSE))
    if not matches:
        return raw.strip(), {}

    stem = raw[: matches[0].start()].strip()
    opts = {}

    for i, m in enumerate(matches):
        start = m.end()                              # ← begin **after** label/sep
        end   = matches[i + 1].start() if i + 1 < len(matches) else len(raw)
        label = m.group("lbl").upper()
        opts[label] = raw[start:end].strip()

    return stem, opts



def _open_spoiler(d:Chrome):
    try: d.find_element(By.CSS_SELECTOR,".upRow a").click(); time.sleep(1)
    except: pass

def _get_difficulty(d:Chrome):
    try: return d.find_element(By.CLASS_NAME,"tag_css_link").text.strip()
    except: return ""

# --- put near the other regex helpers ---------------------------------
_OA_RE = re.compile(
    r"""\b          # word-boundary
        (?:OA|Answer|Correct)   # usual keywords
        [\s\:\-\–]*             # :, -, – or just spaces
        ([A-E])                 # capture A … E
        \b""",
    re.I | re.X,
)

def _first_choice(txt: str) -> str:
    """
    Extract the FIRST A-E choice from a spoiler block.
    Falls back to the first stand-alone capital A-E if no keyword present.
    """
    m = _OA_RE.search(txt)
    if not m:
        # fall-back: any isolated capital A-E
        m = re.search(r"\b([A-E])\b", txt)
    return m.group(1).upper() if m else ""



def _get_answers(d: Chrome) -> List[str]:
    answers = []
    for blk in d.find_elements(By.CLASS_NAME, "answer-block"):
        try:                                   # open the spoiler
            blk.find_element(By.CLASS_NAME, "btn-show-answer").click()
            time.sleep(0.4)
        except Exception:
            pass
        try:
            raw = blk.find_element(By.CLASS_NAME, "downRow").text
            answers.append(_first_choice(raw))
        except Exception:
            answers.append("")
    return answers


def _parse_di_grid(rows, yes_is_first: bool, labels=("Supported",
                                                     "Not Supported")):
    """Return list of {"text": str, "official": label} for one grid."""
    out = []
    yes_lbl, no_lbl = labels
    for tr in rows:
        tds = tr.find_elements(By.TAG_NAME, "td")
        if len(tds) < 3:
            continue
        stmt = tds[2].text.strip()
        cls_yes, cls_no = tds[0].get_attribute("class"), tds[1].get_attribute("class")
        if yes_is_first:
            official = yes_lbl if "official_answer" in cls_yes else no_lbl
        else:
            official = yes_lbl if "official_answer" in cls_no else no_lbl
        out.append((stmt, official))
    return out

def _rows(el):  # yield <tr> skipping header if there is one
    trs = el.find_elements(By.TAG_NAME, "tr")
    return trs[1:] if len(trs) and "Yes" in trs[0].text and "No" in trs[0].text else trs

def _parse_binary_grid(tbl) -> List[Dict[str, str]]:
    out = []
    for tr in _rows(tbl):
        tds = tr.find_elements(By.TAG_NAME, "td")
        if len(tds) < 3:
            continue
        stmt = tds[2].text.strip()
        # “Yes” radio first, “No” second
        off = "Yes" if "official_answer" in tds[0].get_attribute("class") else "No"
        out.append({"statement": stmt, "official": off})
    return out



def _parse_multichoice(tbl) -> Dict[str, Any]:
    choices, official = [], None
    for tr in _rows(tbl):
        tds = tr.find_elements(By.TAG_NAME, "td")
        if len(tds) < 2:
            continue
        txt = tds[1].text.strip()
        choices.append(txt)
        if "official_answer" in tds[0].get_attribute("class"):
            official = txt
    return {"choices": choices, "official": official}

def _clean_opt(txt: str) -> str:
    """
    Strip GMAT-Club boiler-plate that sometimes gets appended
    to the last answer choice.
    """
    # anything that starts a spoiler / footer
    cut_marks = [r"\nShow\s*Answer",         #  Show\nAnswer
                 r"\n[_]{5,}",               #  _____
                 r"\nNew to the GMAT Club\?"]
    for mark in cut_marks:
        m = re.search(mark, txt, flags=re.I)
        if m:
            txt = txt[: m.start()]
            break
    return txt.strip()

# ─── CR-specific option splitter ────────────────────────────────────
def _split_opts_cr(raw: str):
    """
    Accepts answer choices written as:
       A- text
       B- text
       C- text   …   (also tolerates lowercase a-, b-, etc.)
    Returns:  stem_text,  {"A":choiceA, "B":choiceB, ...}
    """
    # Split the blob the *first* time a line starts with  A- / B- / ...
    parts = re.split(r"(?m)^[A-Ea-e]-\s*", raw.strip())
    if len(parts) == 1:                      # no choices found
        return raw.strip(), {}

    stem = parts[0].strip()
    choices = parts[1:]
    letters = list("ABCDE")[: len(choices)]
    return stem, {ltr: txt.strip() for ltr, txt in zip(letters, choices)}


# ─── RC noise-filter helpers ────────────────────────────────────────
_RC_NOISE_PAT = re.compile(
    r"""
    ^\s*(?:                       # entire line contains ONLY …
        \d{2}:\d{2} |                #   00:00 timers
        Show\s+Answer | Hide | Show\s+Spoiler |
        History | Add\s+Mistake |
        Difficulty: | Question\s+Stats: |
        Date | Time | Result | not\s+attempted\s+yet
    )\s*$""",
    re.I | re.X
)

def _strip_rc_noise(block: str) -> str:
    """Remove GC timer / statistics garbage from a question-stem block."""
    return "\n".join(
        ln for ln in block.splitlines() if not _RC_NOISE_PAT.match(ln)
    ).strip()
def _remove_timer(html: str) -> str:
    """Delete every <div id="rc_timer_placeholder_* … </div> block."""
    # non-greedy so we kill exactly one widget at a time
    return re.sub(r'<div id="rc_timer_placeholder_.*?</div>\s*</div>', '',
                  html, flags=re.S|re.I)

_QA_LINE_RE = re.compile(r"\b(\d+)\s*[\.\:\-]?\s*([A-E])\b", re.I)

def _explode_answer_blob(blob: str, n_q: int) -> list[str]:
    """
    If `blob` contains several '1. A  2.B' style answers, return them as
    a list of length `n_q`.  Otherwise return [first_choice(blob)].
    """
    found = _QA_LINE_RE.findall(blob)
    if len(found) >= n_q:                     # looks like a combined list
        # place by question number (1-based in blob)
        out = [""] * n_q
        for num, letter in found:
            idx = int(num) - 1
            if 0 <= idx < n_q:
                out[idx] = letter.upper()
        return out
    # fallback – treat blob as a single spoiler (old behaviour)
    return [_first_choice(blob)]



# ─── CR parser (fixed) ─────────────────────────────────────────────
def _parse_cr(d: Chrome) -> CRDict:
    WebDriverWait(d, 15).until(
        EC.presence_of_element_located((By.CSS_SELECTOR, "div.item.text"))
    )
    raw = d.find_element(By.CSS_SELECTOR, "div.item.text").text

    # 1️⃣ cut everything that follows the first “Show Spoiler”
    raw = re.split(r"Show\s+Spoiler", raw, 1)[0]
    raw = basic_clean(raw)

    # 2️⃣ try the A- / B- splitter first; if it fails → generic splitter
    stem, opts = _split_opts_cr(raw)
    if not opts:
        stem, opts = _split_opts(raw)
        opts = {k: _clean_opt(v) for k, v in opts.items()}
    # 3️⃣ OA & metadata
    _open_spoiler(d)
    ans = (_get_answers(d) or [""])[0]

    return CRDict(
        prompt      = stem,
        options     = opts,
        answer      = ans,
        difficulty  = _get_difficulty(d),
        explanation = ""
    )


# ─── DS parser ─────────────────────────────────────────────────────
def _parse_ds(d: Chrome) -> DSDict:
    WebDriverWait(d, 20).until(
        EC.presence_of_element_located((By.CSS_SELECTOR, "div.item.text"))
    )
    raw = basic_clean(d.find_element(By.CSS_SELECTOR, "div.item.text").text)

    stem, opts = _split_opts(raw)

    # ── scrub each answer choice ────────────────────────────────
    opts = {k: _clean_opt(v) for k, v in opts.items()}

    _open_spoiler(d)
    answers = _get_answers(d)
    ans = answers[0] if answers else ""

    return DSDict(
        question   = stem,
        options    = opts,
        answer     = ans,
        difficulty = _get_difficulty(d),
    )


# ─── RC parser ─────────────────────────────────────────────────────
def _parse_rc(d: Chrome) -> RCDict:
    # A. locate the passage + question container
    wrapper = WebDriverWait(d, 20).until(
        EC.presence_of_element_located((By.CLASS_NAME, "bbcodeBoxOut"))
    )
    boxes = wrapper.find_elements(By.CSS_SELECTOR, ":scope > .bbcodeBoxIn")
    if len(boxes) < 2:
        raise ScrapeError("RC page did not expose passage + questions")

    # Passage text
    passage_text = basic_clean(boxes[0].text)

    # Raw HTML for Q&A block
    raw_html = boxes[1].get_attribute('innerHTML')
    # Strip out GMAT Timer placeholders
    raw_html = re.sub(r'<div id="rc_timer_placeholder_\d+">.*?</div>', '', raw_html, flags=re.DOTALL)
    soup = BeautifulSoup(raw_html, 'html.parser')

    # Difficulty and official answers
    difficulty = _get_difficulty(d)
    answers = _get_answers(d)

    # Find question stems and options
    qlist = []
    stems = soup.find_all('span', style=lambda v: v and 'font-weight: bold' in v)
    for idx, stem_tag in enumerate(stems):
        stem = basic_clean(stem_tag.get_text())
        opts = {}
        # Traverse siblings until next stem
        current = stem_tag.next_sibling
        while current:
            # Stop at next bold stem
            if isinstance(current, Tag) and current.name == 'span' and 'font-weight: bold' in current.get('style', ''):
                break
            # Text node containing options
            if isinstance(current, str) and current.strip():
                for line in current.splitlines():
                    m = re.match(r"\(?([A-E])\)?\.?\s*(.+)", line.strip())
                    if m:
                        opts[m.group(1)] = m.group(2).strip()
            current = current.next_sibling
        oa = answers[idx] if idx < len(answers) else ""
        qlist.append(RCQuestion(prompt=stem, options=opts, answer=oa))

    return RCDict(passage=passage_text, difficulty=difficulty, questions=qlist)

# ─── PS parser ──────────────────────────────────────────────────────
def _parse_ps(d: Chrome) -> DSDict:
    WebDriverWait(d, 20).until(
        EC.presence_of_element_located((By.CSS_SELECTOR, "div.item.text"))
    )
    raw = basic_clean(d.find_element(By.CSS_SELECTOR, "div.item.text").text)
    stem, opts = _split_opts(raw)

    # ── scrub each answer choice ────────────────────────────────
    opts = {k: _clean_opt(v) for k, v in opts.items()}

    _open_spoiler(d)
    answers = _get_answers(d)
    ans = answers[0] if answers else ""

    return DSDict(
        question   = stem,
        options    = opts,
        answer     = ans,
        difficulty = _get_difficulty(d),
    )


# ─── GRAPHS parser ─────────────────────────────────────────────────
def _parse_graphs(d: Chrome) -> GraphDict:
    """
    Handles GMAT-Club 'Graphs & Charts' questions that use a set of
    <select class="di_graph_dropdown"> … </select> widgets.

    Output shape matches the GraphDict TypedDict shown above.
    """
    # 1) wait until the body of the post is loaded
    WebDriverWait(d, 20).until(
        EC.presence_of_element_located((By.CSS_SELECTOR, "div.item.text"))
    )

    # 2) passage text & (first) image
    passage = basic_clean(d.find_element(By.CSS_SELECTOR, "div.item.text").text)
    try:
        image_url = d.find_element(By.CSS_SELECTOR, "img.reimg").get_attribute("src")
    except Exception:
        image_url = None

    # 3) collect each dropdown, its label, its answer options
    dropdowns = d.find_elements(By.CSS_SELECTOR, "select.di_graph_dropdown")
    q_list: List[GraphDropdown] = []

    for sel in dropdowns:
        # the text node immediately preceding the <select> element
        prompt = d.execute_script("""
            const s = arguments[0];
            let n = s.previousSibling;
            while (n && n.nodeType !== Node.TEXT_NODE) n = n.previousSibling;
            return n ? n.textContent.trim() : "";
        """, sel)

        opts = [
            o.text.strip()
            for o in sel.find_elements(By.TAG_NAME, "option")
            if o.get_attribute("value")          # skips the blank default
        ]

        q_list.append(GraphDropdown(prompt=prompt, options=opts, answer=""))

    # 4) open spoiler → grab OAs (format: "Drop-down 1:  D", …)
    _open_spoiler(d)
    oa_blob = (_get_answers(d) or [""])[0]           # one big string
    for line in oa_blob.split("\n"):
        m = re.match(r"\s*Drop[- ]?down\s+(\d+)\s*:\s*([A-Z])", line, re.I)
        if m:
            idx = int(m.group(1)) - 1                # 1-based in page ⇒ 0-based list
            if 0 <= idx < len(q_list):
                q_list[idx]["answer"] = m.group(2)

    return GraphDict(
        passage    = passage,
        image_url  = image_url,
        difficulty = _get_difficulty(d),
        questions  = q_list,
    )

# ─── TABLES parser ──────────────────────────────────────────────────
def _parse_tables(d: Chrome) -> TableDict:
    """
    Handles questions that show a sortable table followed by several
    Yes/No statements (GMAT-Club 'multi-source reasoning-style' tables).
    """
    WebDriverWait(d, 20).until(
        EC.presence_of_element_located((By.CSS_SELECTOR, "div.item.text"))
    )

    passage = basic_clean(d.find_element(By.CSS_SELECTOR, "div.item.text").text)

    # 1) MAIN DATA TABLE --------------------------------------------------
    headers, row_objs = [], []
    try:
        tbl = d.find_element(By.CSS_SELECTOR, "table.stoker.table-sortable")
        headers = [th.text.strip() for th in tbl.find_elements(By.TAG_NAME, "th")]
        for tr in tbl.find_elements(By.TAG_NAME, "tr")[1:]:
            cells = [td.text.strip() for td in tr.find_elements(By.TAG_NAME, "td")]
            if cells:
                row_objs.append(TableRow(cells=dict(zip(headers, cells))))
    except Exception:
        pass  # keep empty → caller can detect

    # 2) YES / NO  STATEMENTS  -------------------------------------------
    statement_list: List[TableStatement] = []
    try:
        diag = d.find_element(By.CSS_SELECTOR, "table.stoker.di")
        for tr in diag.find_elements(By.TAG_NAME, "tr")[1:]:
            tds = tr.find_elements(By.TAG_NAME, "td")
            if len(tds) != 3:
                continue
            prompt = tds[2].text.strip()
            if "official_answer" in tds[0].get_attribute("class"):
                answer = "Yes"
            elif "official_answer" in tds[1].get_attribute("class"):
                answer = "No"
            else:
                answer = None
            statement_list.append(TableStatement(prompt=prompt, answer=answer))
    except Exception:
        pass

    return TableDict(
        passage    = passage,
        headers    = headers,
        rows       = row_objs,
        statements = statement_list,
        difficulty = _get_difficulty(d),
    )
# ─── TPA parser ─────────────────────────────────────────────────────
def _parse_tpa(d: Chrome) -> TPADict:
    """
    GMAT-Club Two-Part-Analysis: stem + radio-grid with one column
    of row-labels and two answer columns.  The spoiler lists the
    official answers as:
        1 : <row-text>
        2 : <row-text>
    """
    WebDriverWait(d, 20).until(
        EC.presence_of_element_located((By.CSS_SELECTOR, "div.item.text"))
    )

    passage = basic_clean(d.find_element(By.CSS_SELECTOR, "div.item.text").text)

    # --- collect every row-label from the grid ----------------------
    try:
        grid = d.find_element(By.CSS_SELECTOR, "table.stoker.di")
        rows = grid.find_elements(By.TAG_NAME, "tr")[1:]          # skip header
        choices = [r.find_elements(By.TAG_NAME, "td")[2].text.strip()
                   for r in rows]
    except Exception:
        choices = []

    # --- open spoiler & grab official mappings ----------------------
    answer1 = answer2 = None
    try:
        d.find_element(By.CSS_SELECTOR, ".answer-block a").click()
        time.sleep(0.6)
        lines = (d.find_element(By.CSS_SELECTOR, ".answer-block .downRow")
                   .text.strip().split("\n"))
        mapping = {int(l.split(":", 1)[0]): l.split(":", 1)[1].strip()
                   for l in lines if ":" in l}
        answer1 = mapping.get(1)
        answer2 = mapping.get(2)
    except Exception:
        pass

    return TPADict(
        passage       = passage,
        choices       = choices,
        answer_blank1 = answer1,
        answer_blank2 = answer2,
        difficulty    = _get_difficulty(d),
    )

# ─── MSR parser ─────────────────────────────────────────────────────
def _parse_msr(d: Chrome) -> MSRDict:
    WebDriverWait(d, 25).until(
        EC.presence_of_element_located((By.CSS_SELECTOR, ".tab_di_ms_wrapper"))
    )

    # 1)  gather the three source tabs (unchanged) -------------------
    titles  = d.find_elements(By.CSS_SELECTOR, ".tablinks_di_ms")
    panes   = d.find_elements(By.CSS_SELECTOR, ".tabcontent_di_ms")
    sources = []
    for t, p in zip(titles, panes):
        block = p.find_element(By.CSS_SELECTOR, ".item.text")
        try:
            img = block.find_element(By.CSS_SELECTOR, "img.reimg").get_attribute("src")
        except:
            img = None
        sources.append({
            "source_title": t.text.strip(),
            "text": block.get_attribute("textContent").strip(),
            "image_url": img
        })

    # 2)  find every DI table that sits in the *right* column --------
    right_pane = d.find_element(By.CSS_SELECTOR, ".tabcontent_di_ms_right")
    di_tables  = right_pane.find_elements(By.CSS_SELECTOR, "table.stoker.di")

    binaries, impacts, mcq = [], [], None
    for tbl in di_tables:
        first_row_tds = tbl.find_elements(By.TAG_NAME, "tr")[1] \
                           .find_elements(By.TAG_NAME, "td")
        if len(first_row_tds) >= 3 and all(
                td.find_elements(By.TAG_NAME, "input") for td in first_row_tds[:2]):
            # radio in both first & second columns ⇒ binary Yes/No grid
            rows = _parse_binary_grid(tbl)

            # Decide which “kind” of binary grid it is by looking at headers
            hdrs = [c.text.strip().lower() for c in
                    tbl.find_elements(By.TAG_NAME, "tr")[0]
                       .find_elements(By.TAG_NAME, "td")[:2]]
            if "positive" in hdrs[0]:
                impacts = [{"factor": r["statement"],
                            "official": ("Positive Impact"
                                         if r["official"] == "Yes"
                                         else "No Clear Impact")}
                           for r in rows]
            else:
                binaries.extend([{"statement": r["statement"],
                                  "official": ("Supported"
                                               if r["official"] == "Yes"
                                               else "Not Supported")}
                                 for r in rows])
        else:
            # otherwise it is the multiple-choice list
            mcq = _parse_multichoice(tbl)

    return MSRDict(
        sources            = sources,
        support_statements = binaries,
        impact_factors     = impacts,
        mcq                = mcq,
        difficulty         = _get_difficulty(d)
    )
# ─── registry & scrape ─────────────────────────────────────────────
def _todo(l:str)->Callable[[Chrome],QuestionData]: return lambda d:{"html":d.page_source,"note":f"{l} TODO"}
PARSERS:Dict[QuestionType,Callable[[Chrome],QuestionData]]={
    QuestionType.CR:_parse_cr, QuestionType.DS:_parse_ds, QuestionType.RC:_parse_rc,
    QuestionType.PS:_parse_ps, QuestionType.TPA:_parse_tpa, QuestionType.MSR:_parse_msr,
    QuestionType.GRAPHS: _parse_graphs, QuestionType.TABLES:_parse_tables
}

def scrape(*, url: str, q_type: QuestionType,
           email: str, password: str,
           headless: bool = True, polish: bool = False, retries: int = 1
           ) -> QuestionData:
    for attempt in range(retries + 1):
        try:
            with get_driver(headless=headless) as drv:
                if not _load_cookies(drv):
                    _login(drv, email, password)
                time.sleep(random.uniform(1.5, 3.0))
                drv.get(url)
                data = PARSERS[q_type](drv)
                return _polish(data, q_type) if polish else data
        except (TimeoutException, ScrapeError) as e:
            if attempt == retries:
                raise
            LOG.warning("retry %s because %s", attempt + 1, e)

# ─── quick smoke-test ───────────────────────────────────────────────
if __name__ == "__main__":
    res = scrape(
        url="https://gmatclub.com/forum/the-majority-of-successful-senior-managers-do-not-closely-119740.html",
        q_type=QuestionType.RC,
        email="emmarose0012@gmail.com",
        password="Sayan@123",
        headless=False,
        polish=False,
    )
    print(json.dumps(res, indent=2, ensure_ascii=False))

# ░░░  end cell ░░░
