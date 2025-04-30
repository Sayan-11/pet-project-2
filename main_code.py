# ░░░  GMAT-Club Scraper  ░░░  (CR, DS, RC, …)

from __future__ import annotations
import json, logging, os, pickle, random, re, subprocess, sys, time
from contextlib import contextmanager
from enum  import Enum
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, TypedDict, Union

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

def _get_answers(d:Chrome):
    out=[]
    for ab in d.find_elements(By.CLASS_NAME,"answer-block"):
        try: ab.find_element(By.CLASS_NAME,"btn-show-answer").click(); time.sleep(0.5)
        except: pass
        try: out.append(ab.find_element(By.CLASS_NAME,"downRow").text.strip())
        except: out.append("")
    return out

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
    """
    Return a dict with: passage, difficulty, questions[ {prompt, options, answer} ]
    The page always renders the reading-comprehension passage in the first
    .bbcodeBoxIn and the question block in the second .bbcodeBoxIn – both are
    direct children of a single .bbcodeBoxOut wrapper.
    """
    # 1) make sure the wrapper is present
    wrapper = WebDriverWait(d, 20).until(
        EC.presence_of_element_located((By.CLASS_NAME, "bbcodeBoxOut"))
    )

    # 2) pull only the *direct* children so we ignore any nested boxes
    boxes = wrapper.find_elements(By.CSS_SELECTOR, ":scope > .bbcodeBoxIn")
    if len(boxes) < 2:
        raise ScrapeError("RC page did not expose passage + questions")

    passage        = basic_clean(boxes[0].text)
    questions_blob = boxes[1].text                  # already strips <script> etc.

    difficulty = _get_difficulty(d)
    answers    = _get_answers(d)                    # opens spoilers and collects OA's

    # 3) split the big blob into individual questions
    chunks  = questions_blob.split("Question ")[1:]  # drop the header part
    qlist: List[RCQuestion] = []

    for idx, ch in enumerate(chunks):
        # remove blank lines and the spoiler row
        lines = [l for l in ch.split("\n") if l.strip()]
        try:
            spoiler_idx = next(
                i for i, l in enumerate(lines) if "Show Spoiler" in l or "Hide Spoiler" in l
            )
        except StopIteration:
            continue  # skip malformed question

        stem_raw   = " ".join(lines[spoiler_idx + 1 :])
        stem, opts = _split_opts(stem_raw)
        ans        = answers[idx] if idx < len(answers) else ""

        qlist.append(RCQuestion(prompt=stem, options=opts, answer=ans))

    return RCDict(passage=passage, difficulty=difficulty, questions=qlist)

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
