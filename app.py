import streamlit as st
import pandas as pd
import io
import re
import json
import time
from datetime import date
from anthropic import Anthropic
from openpyxl import Workbook
from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
from openpyxl.utils import get_column_letter

# ── CONFIG ────────────────────────────────────────────────────────────────────
CLAUDE_MODEL  = "claude-haiku-4-5-20251001"
OVERLAP_ROWS  = 8
MAX_RETRIES   = 3
PROMPT_VERSION = "v4.0"

# Chunk size auto-scales based on file size
def get_chunk_size(total_rows: int) -> int:
    if total_rows < 100:  return total_rows   # single chunk
    if total_rows > 800:  return 40            # large file — smaller chunks
    return 60                                   # standard

st.set_page_config(
    page_title="RealVal · Rent Roll Standardizer",
    page_icon="🏢",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# ── CSS ───────────────────────────────────────────────────────────────────────
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&family=Sora:wght@600;700&display=swap');

html, body, [class*="css"] {
    font-family: 'Inter', sans-serif;
    background-color: #0f1923;
    color: #e2e8f0;
}
#MainMenu, footer, header { visibility: hidden; }
.block-container { padding: 0 3rem 4rem; max-width: 100%; background: #111c27; }

.rv-hero {
    background: linear-gradient(135deg, #0a1628 0%, #0d2137 40%, #0e3347 100%);
    border-bottom: 1px solid rgba(46,196,182,0.2);
    padding: 2.2rem 3rem 2rem;
    display: flex; align-items: center; justify-content: space-between;
    position: relative; overflow: hidden;
}
.rv-hero::before {
    content: ''; position: absolute; top: 0; right: 0; bottom: 0; width: 40%;
    background: radial-gradient(ellipse at 80% 50%, rgba(46,196,182,0.07) 0%, transparent 70%);
    pointer-events: none;
}
.rv-hero-left { display: flex; align-items: center; gap: 1.2rem; z-index: 1; }
.rv-logo {
    width: 56px; height: 56px;
    background: linear-gradient(135deg, #2ec4b6 0%, #1a9490 100%);
    border-radius: 12px; display: flex; align-items: center; justify-content: center;
    font-family: 'Sora', sans-serif; font-weight: 700; font-size: 1.4rem;
    color: white; letter-spacing: -0.03em; flex-shrink: 0;
    box-shadow: 0 4px 20px rgba(46,196,182,0.4);
}
.rv-hero-text h1 { font-family: 'Sora', sans-serif; font-size: 1.5rem; font-weight: 700; color: #fff; margin: 0; letter-spacing: -0.02em; }
.rv-hero-text p  { font-size: 0.75rem; color: rgba(255,255,255,0.45); margin: 0.25rem 0 0; letter-spacing: 0.08em; text-transform: uppercase; }
.rv-hero-right   { display: flex; align-items: center; gap: 1rem; z-index: 1; }
.rv-badge  { background: rgba(46,196,182,0.12); border: 1px solid rgba(46,196,182,0.35); border-radius: 20px; padding: 0.35rem 1rem; font-size: 0.7rem; font-weight: 600; color: #2ec4b6; letter-spacing: 0.08em; text-transform: uppercase; }
.rv-version{ font-size: 0.68rem; color: rgba(255,255,255,0.25); letter-spacing: 0.06em; }

.sec-label { font-size: 0.65rem; font-weight: 700; letter-spacing: 0.15em; text-transform: uppercase; color: #2ec4b6; display: flex; align-items: center; gap: 0.6rem; margin: 2rem 0 1rem; }
.sec-label::after { content: ''; flex: 1; height: 1px; background: linear-gradient(90deg, rgba(46,196,182,0.3), transparent); }

.upload-panel { background: linear-gradient(135deg, #0d2137, #0e2a40); border: 1px solid rgba(46,196,182,0.2); border-radius: 14px; padding: 1.8rem 2rem; position: relative; overflow: hidden; }
.upload-panel::before { content: ''; position: absolute; top: -40px; right: -40px; width: 160px; height: 160px; background: radial-gradient(circle, rgba(46,196,182,0.08), transparent 70%); pointer-events: none; }
.upload-panel h3 { font-family: 'Sora', sans-serif; font-size: 1rem; font-weight: 700; color: #fff; margin: 0 0 0.4rem; }
.upload-panel p  { font-size: 0.78rem; color: rgba(255,255,255,0.45); margin: 0 0 1.2rem; }

.hint-box { background: #0d1e2e; border: 1px solid rgba(46,196,182,0.15); border-radius: 10px; padding: 1rem 1.2rem; margin-top: 1rem; }
.hint-box label { font-size: 0.65rem; font-weight: 700; letter-spacing: 0.1em; text-transform: uppercase; color: #2ec4b6; display: block; margin-bottom: 0.5rem; }

.info-panel { background: #0d1e2e; border: 1px solid rgba(255,255,255,0.07); border-radius: 14px; padding: 1.5rem; }
.info-panel h4 { font-size: 0.72rem; font-weight: 700; letter-spacing: 0.1em; text-transform: uppercase; color: #2ec4b6; margin: 0 0 0.8rem; }
.info-row  { display: flex; align-items: flex-start; gap: 0.6rem; margin-bottom: 0.55rem; font-size: 0.78rem; color: rgba(255,255,255,0.55); }
.info-dot  { width: 6px; height: 6px; background: #2ec4b6; border-radius: 50%; margin-top: 5px; flex-shrink: 0; }

.file-pill { display: inline-flex; align-items: center; gap: 0.5rem; background: rgba(46,196,182,0.1); border: 1px solid rgba(46,196,182,0.3); border-radius: 20px; padding: 0.4rem 1rem; font-size: 0.78rem; color: #2ec4b6; font-weight: 500; margin-bottom: 0.6rem; }
.sheet-pill { display: inline-flex; align-items: center; gap: 0.5rem; background: rgba(96,165,250,0.1); border: 1px solid rgba(96,165,250,0.3); border-radius: 20px; padding: 0.3rem 0.8rem; font-size: 0.72rem; color: #93c5fd; font-weight: 500; margin-left: 0.5rem; }

.stButton > button { background: linear-gradient(135deg, #2ec4b6, #1a9490) !important; color: #0a1628 !important; font-family: 'Inter', sans-serif !important; font-weight: 700 !important; font-size: 0.85rem !important; letter-spacing: 0.04em !important; border: none !important; border-radius: 10px !important; padding: 0.7rem 2rem !important; width: 100% !important; box-shadow: 0 4px 20px rgba(46,196,182,0.35) !important; transition: all 0.2s !important; }
.stButton > button:hover { background: linear-gradient(135deg, #3dd9ca, #2ec4b6) !important; box-shadow: 0 6px 28px rgba(46,196,182,0.5) !important; transform: translateY(-1px) !important; }

.steps-wrap { background: #0d1e2e; border: 1px solid rgba(255,255,255,0.07); border-radius: 12px; padding: 1.2rem 1.8rem; display: flex; align-items: center; gap: 0; margin: 1rem 0 1.5rem; overflow-x: auto; }
.step { display: flex; align-items: center; gap: 0.55rem; flex-shrink: 0; }
.step-dot { width: 30px; height: 30px; border-radius: 50%; display: flex; align-items: center; justify-content: center; font-size: 0.72rem; font-weight: 700; flex-shrink: 0; }
.step-dot.done   { background: #2ec4b6; color: #0a1628; }
.step-dot.active { background: transparent; color: #2ec4b6; border: 2px solid #2ec4b6; box-shadow: 0 0 12px rgba(46,196,182,0.4); animation: pulse 1.5s infinite; }
.step-dot.wait   { background: rgba(255,255,255,0.06); color: rgba(255,255,255,0.25); }
@keyframes pulse { 0%,100%{box-shadow:0 0 8px rgba(46,196,182,0.3)} 50%{box-shadow:0 0 18px rgba(46,196,182,0.6)} }
.step-text { font-size: 0.77rem; white-space: nowrap; }
.step-text.done   { color: #2ec4b6; font-weight: 600; }
.step-text.active { color: #ffffff; font-weight: 600; }
.step-text.wait   { color: rgba(255,255,255,0.25); }
.step-sep { color: rgba(255,255,255,0.15); margin: 0 0.8rem; font-size: 0.9rem; flex-shrink: 0; }

.kpi-grid { display: grid; grid-template-columns: repeat(5,1fr); gap: 1rem; margin: 0.5rem 0 1.5rem; }
.kpi-card { background: #0d1e2e; border: 1px solid rgba(255,255,255,0.07); border-radius: 12px; padding: 1.1rem 1.3rem; border-top: 2px solid #1e3a4a; transition: border-color 0.2s, transform 0.2s; }
.kpi-card:hover { border-top-color: #2ec4b6; transform: translateY(-2px); }
.kpi-card.hi { border-top-color: #2ec4b6; }
.kpi-label { font-size: 0.62rem; font-weight: 700; letter-spacing: 0.12em; text-transform: uppercase; color: rgba(255,255,255,0.35); margin-bottom: 0.45rem; }
.kpi-value { font-family: 'Sora', sans-serif; font-size: 1.75rem; font-weight: 700; color: #ffffff; line-height: 1.1; }
.kpi-sub   { font-size: 0.68rem; color: rgba(255,255,255,0.3); margin-top: 0.3rem; }

.recon-grid { display: grid; grid-template-columns: repeat(2,1fr); gap: 0.85rem; margin-bottom: 1.5rem; }
.recon-card { background: #0d1e2e; border: 1px solid rgba(255,255,255,0.07); border-radius: 12px; padding: 1rem 1.2rem; display: flex; align-items: center; gap: 1rem; }
.r-icon { width: 38px; height: 38px; border-radius: 9px; display: flex; align-items: center; justify-content: center; font-size: 1.05rem; flex-shrink: 0; }
.r-icon.pass { background: rgba(34,197,94,0.15); }
.r-icon.warn { background: rgba(245,158,11,0.15); }
.r-icon.fail { background: rgba(239,68,68,0.15); }
.r-body  { flex: 1; }
.r-title { font-size: 0.78rem; font-weight: 600; color: #e2e8f0; margin-bottom: 0.1rem; }
.r-detail{ font-size: 0.7rem; color: rgba(255,255,255,0.4); }
.r-badge { font-size: 0.62rem; font-weight: 700; letter-spacing: 0.07em; text-transform: uppercase; padding: 0.2rem 0.6rem; border-radius: 20px; flex-shrink: 0; }
.rb-pass { background: rgba(34,197,94,0.15); color: #4ade80; }
.rb-warn { background: rgba(245,158,11,0.15); color: #fbbf24; }
.rb-fail { background: rgba(239,68,68,0.15); color: #f87171; }

.flag-banner { background: rgba(245,158,11,0.1); border: 1px solid rgba(245,158,11,0.3); border-radius: 10px; padding: 0.8rem 1.2rem; margin-bottom: 1rem; display: flex; align-items: center; gap: 0.7rem; font-size: 0.8rem; color: #fbbf24; }

.tbl-wrap { background: #0d1e2e; border: 1px solid rgba(255,255,255,0.07); border-radius: 12px; overflow: hidden; margin-bottom: 1.5rem; }
.tbl-header { background: linear-gradient(135deg, #0a1f32, #0d2840); padding: 0.75rem 1.2rem; display: flex; align-items: center; justify-content: space-between; border-bottom: 1px solid rgba(46,196,182,0.15); }
.tbl-header-left { font-size: 0.72rem; font-weight: 600; letter-spacing: 0.08em; text-transform: uppercase; color: rgba(255,255,255,0.5); }
.legend { display: flex; gap: 1.2rem; }
.leg-item { display: flex; align-items: center; gap: 0.4rem; font-size: 0.67rem; color: rgba(255,255,255,0.4); }
.leg-dot  { width: 8px; height: 8px; border-radius: 2px; }

.rv-tbl { width: 100%; border-collapse: collapse; font-size: 0.79rem; }
.rv-tbl th { background: rgba(255,255,255,0.03); color: rgba(255,255,255,0.45); font-weight: 600; font-size: 0.68rem; text-align: left; letter-spacing: 0.05em; padding: 0.65rem 1rem; border-bottom: 1px solid rgba(255,255,255,0.06); white-space: nowrap; text-transform: uppercase; }
.rv-tbl td { padding: 0.52rem 1rem; border-bottom: 1px solid rgba(255,255,255,0.04); color: #cbd5e1; white-space: nowrap; }
.rv-tbl tr:last-child td { border-bottom: none; }
.rv-tbl tr.occ:hover td { background: rgba(255,255,255,0.025); }
.rv-tbl tr.vac td { background: rgba(245,158,11,0.05); color: #fcd34d; }
.rv-tbl tr.nr  td { background: rgba(34,197,94,0.05);  color: #86efac; font-style: italic; }
.rv-tbl tr.flagged td { background: rgba(245,158,11,0.08); }
.rv-tbl td.mono  { font-family: 'SF Mono','Fira Code',monospace; font-size: 0.73rem; color: #7dd3fc; }
.rv-tbl td.right { text-align: right; }
.rv-tbl td.green { color: #4ade80; font-weight: 600; }
.rv-tbl .tag { display: inline-block; font-size: 0.6rem; font-weight: 700; letter-spacing: 0.06em; text-transform: uppercase; padding: 0.12rem 0.45rem; border-radius: 20px; }
.tag-occ  { background: rgba(96,165,250,0.15);  color: #93c5fd; }
.tag-vac  { background: rgba(245,158,11,0.15);  color: #fcd34d; }
.tag-nr   { background: rgba(34,197,94,0.15);   color: #86efac; }
.tag-warn { background: rgba(245,158,11,0.2);   color: #f59e0b; }

/* Streamlit tab styling */
.stTabs [data-baseweb="tab-list"] { background: #0d1e2e; border-radius: 10px; padding: 0.3rem; gap: 0.3rem; border: 1px solid rgba(255,255,255,0.07); }
.stTabs [data-baseweb="tab"] { background: transparent; color: rgba(255,255,255,0.4); border-radius: 8px; font-size: 0.82rem; font-weight: 500; padding: 0.5rem 1.2rem; }
.stTabs [aria-selected="true"] { background: rgba(46,196,182,0.15) !important; color: #2ec4b6 !important; font-weight: 600; }
.stTabs [data-baseweb="tab-panel"] { padding: 1.2rem 0 0; }

.stDownloadButton > button { background: linear-gradient(135deg, #0e4a5c, #1a6b7a) !important; color: #ffffff !important; font-weight: 600 !important; border: 1px solid rgba(46,196,182,0.3) !important; border-radius: 10px !important; font-size: 0.85rem !important; padding: 0.7rem 2rem !important; width: 100% !important; box-shadow: 0 4px 20px rgba(14,74,92,0.4) !important; }
.stDownloadButton > button:hover { background: linear-gradient(135deg, #2ec4b6, #1a9490) !important; color: #0a1628 !important; border-color: transparent !important; }

.stProgress > div > div > div > div { background: #2ec4b6 !important; }

[data-testid="stFileUploader"] section { background: rgba(255,255,255,0.03) !important; border: 1.5px dashed rgba(46,196,182,0.3) !important; border-radius: 10px !important; }
[data-testid="stFileUploader"] section:hover { border-color: rgba(46,196,182,0.6) !important; }
[data-testid="stFileUploader"] section p { color: rgba(255,255,255,0.45) !important; }

.stTextArea textarea { background: #0a1628 !important; color: #e2e8f0 !important; border: 1px solid rgba(46,196,182,0.2) !important; border-radius: 8px !important; font-size: 0.82rem !important; }
.stTextArea textarea:focus { border-color: rgba(46,196,182,0.5) !important; box-shadow: 0 0 0 2px rgba(46,196,182,0.1) !important; }
.stSelectbox > div > div { background: #0a1628 !important; border: 1px solid rgba(46,196,182,0.2) !important; color: #e2e8f0 !important; border-radius: 8px !important; }

/* Format library */
.lib-card { background: #0d1e2e; border: 1px solid rgba(255,255,255,0.07); border-radius: 10px; padding: 1rem 1.2rem; margin-bottom: 0.6rem; }
.lib-card h5 { font-size: 0.78rem; font-weight: 600; color: #e2e8f0; margin: 0 0 0.3rem; }
.lib-card p  { font-size: 0.72rem; color: rgba(255,255,255,0.4); margin: 0; }
.lib-meta { font-size: 0.65rem; color: rgba(46,196,182,0.7); margin-top: 0.3rem; }

.rv-footer { border-top: 1px solid rgba(255,255,255,0.06); margin-top: 4rem; padding-top: 1.2rem; display: flex; justify-content: space-between; align-items: center; font-size: 0.68rem; color: rgba(255,255,255,0.2); }
.rv-footer a { color: #2ec4b6; text-decoration: none; }
</style>
""", unsafe_allow_html=True)


# ── PASSWORD GATE ─────────────────────────────────────────────────────────────
def check_password():
    if st.session_state.get("authenticated"):
        return True
    st.markdown("""
    <div class="rv-hero">
      <div class="rv-hero-left">
        <div class="rv-logo">RV</div>
        <div class="rv-hero-text"><h1>Rent Roll Standardizer</h1>
        <p>RealVal · Multifamily Underwriting Intelligence</p></div>
      </div>
      <div class="rv-badge">Secure Access</div>
    </div>""", unsafe_allow_html=True)
    col1, col2, col3 = st.columns([1.2, 1, 1.2])
    with col2:
        st.markdown("""
        <div style="background:#0d1e2e;border:1px solid rgba(46,196,182,0.2);border-radius:14px;
        padding:2rem 1.8rem;text-align:center;margin-top:2rem;">
        <div style="width:52px;height:52px;background:linear-gradient(135deg,#2ec4b6,#1a9490);
        border-radius:12px;display:flex;align-items:center;justify-content:center;
        font-family:Sora,sans-serif;font-weight:700;font-size:1.3rem;color:white;
        margin:0 auto 1.2rem;box-shadow:0 4px 20px rgba(46,196,182,0.4);">RV</div>
        <div style="font-family:Sora,sans-serif;font-size:1.15rem;font-weight:700;color:#fff;margin-bottom:0.3rem;">
        Internal Access Only</div>
        <div style="font-size:0.78rem;color:rgba(255,255,255,0.35);margin-bottom:1.5rem;">
        Authorized RealVal analysts only</div></div>""", unsafe_allow_html=True)
        pwd = st.text_input("Password", type="password", label_visibility="collapsed", placeholder="Enter password…")
        if st.button("Sign In →"):
            if pwd == st.secrets.get("password", ""):
                st.session_state["authenticated"] = True
                st.rerun()
            else:
                st.error("Incorrect password.")
    st.markdown("&nbsp;", unsafe_allow_html=True)
    return False


# ── MULTI-SHEET PRE-PROCESSOR ─────────────────────────────────────────────────
def detect_rent_roll_sheet(file_bytes: bytes) -> tuple[pd.DataFrame, str]:
    """
    Reads all sheets, scores each by rent-roll signal words,
    returns (dataframe_of_best_sheet, sheet_name).
    """
    xl      = pd.ExcelFile(io.BytesIO(file_bytes))
    sheets  = xl.sheet_names

    if len(sheets) == 1:
        df = pd.read_excel(io.BytesIO(file_bytes), sheet_name=sheets[0], header=None)
        return df, sheets[0]

    SIGNALS = ["unit","tenant","lease","rent","sqft","sq ft","market","charge",
               "resident","move","vacant","expir","deposit","occupan"]
    EXCLUDE = ["summary","totals","cover","index","toc","contents","chart","graph",
               "pivot","dashboard","notes","readme"]

    scores = {}
    for name in sheets:
        lname = name.lower()
        # Hard-exclude obvious non-data sheets
        if any(x in lname for x in EXCLUDE):
            scores[name] = -1
            continue
        try:
            sample = pd.read_excel(io.BytesIO(file_bytes), sheet_name=name, header=None, nrows=20)
            text   = " ".join(str(v).lower() for v in sample.values.flatten() if pd.notna(v))
            score  = sum(text.count(s) for s in SIGNALS)
            score += sample.shape[0] * 0.5  # reward row count
            scores[name] = score
        except Exception:
            scores[name] = 0

    best = max(scores, key=scores.get)
    df   = pd.read_excel(io.BytesIO(file_bytes), sheet_name=best, header=None)
    return df, best


# ── FORMAT LIBRARY ────────────────────────────────────────────────────────────
def init_library():
    if "format_library" not in st.session_state:
        st.session_state["format_library"] = []

def save_to_library(broker_name: str, hint: str, raw_df: pd.DataFrame, result_df: pd.DataFrame):
    """Save a 5-row anonymized sample to the in-session format library."""
    sample_raw    = raw_df.head(5).to_csv(index=False)
    sample_output = result_df.head(5).to_dict(orient="records")
    entry = {
        "broker":    broker_name,
        "hint":      hint,
        "date":      date.today().strftime("%m/%d/%Y"),
        "raw_sample": sample_raw,
        "out_sample": sample_output,
        "unit_count": len(result_df),
    }
    st.session_state["format_library"].append(entry)

def build_library_context() -> str:
    """Format saved examples as few-shot context for the Claude prompt."""
    lib = st.session_state.get("format_library", [])
    if not lib:
        return ""
    lines = ["\nFORMAT LIBRARY — examples from previously processed files (use as reference):"]
    for e in lib[-3:]:  # last 3 entries
        lines.append(f"\nBroker: {e['broker']} | Hint: {e['hint']} | Date: {e['date']}")
        lines.append(f"Raw sample (first 5 rows):\n{e['raw_sample']}")
        lines.append(f"Output sample: {json.dumps(e['out_sample'][:2], indent=2)}")
    return "\n".join(lines)


# ── STEPS ─────────────────────────────────────────────────────────────────────
STEPS = ["Reading File", "Detecting Sheet", "Sending to Claude", "Building Output", "Complete"]

def render_steps(active: int) -> str:
    html = "<div class='steps-wrap'>"
    for i, label in enumerate(STEPS):
        if i < active:   dc, tc, icon = "done",   "done",   "✓"
        elif i == active:dc, tc, icon = "active", "active", str(i+1)
        else:            dc, tc, icon = "wait",   "wait",   str(i+1)
        html += f"<div class='step'><div class='step-dot {dc}'>{icon}</div><span class='step-text {tc}'>{label}</span></div>"
        if i < len(STEPS)-1: html += "<span class='step-sep'>›</span>"
    html += "</div>"
    return html


# ── CLAUDE CALL ───────────────────────────────────────────────────────────────
def call_claude(client, chunk_text: str, chunk_num: int, total_chunks: int,
                analyst_hint: str, library_ctx: str) -> list:

    hint_section = f"\nANALYST NOTE: {analyst_hint.strip()}" if analyst_hint.strip() else ""

    prompt = f"""You are an expert multifamily real estate underwriting analyst. ({PROMPT_VERSION})
Standardize the rent roll excerpt below into a clean JSON array.

OUTPUT COLUMNS (exactly these 7 + optional flag):
Unit No | Unit Size (SF) | Market Rent (Monthly) | Effective Rent (Monthly) | Lease Start Date | Lease End Date | Tenant Name

RULES — follow precisely:
1. ONE ROW PER UNIT. Merge all charge sub-rows for the same unit into a single output row.
2. Effective Rent = base rent charge ("rent" line) ONLY.
   ADD: housing subsidy (rentsub, hap, subsidy) if present.
   DO NOT ADD OR SUBTRACT anything else — no deposits, no utilities, no pet fees,
   no parking, no amenity fees, no trash, no employee discounts, no concessions,
   no late fees. Nothing. Only rent + subsidy.
3. Annual rents → divide by 12. Rent/SF given → multiply by SF for monthly rent.
4. Dates → MM/DD/YYYY. Missing date → null.
5. VACANT units: include if market rent shown. Effective Rent = null. Tenant Name = "VACANT".
6. ADMIN / MODEL units: include. Effective Rent = null. Tenant Name = "ADMIN" or "MODEL".
7. Future leases (no active rent charge, future move-in date): EXCLUDE entirely.
8. Section headers, subtotal rows, summary rows, footer rows: EXCLUDE entirely.
9. Duplicate unit numbers: keep only the first occurrence (current tenant).
10. Monetary values: round to 2 decimal places.
11. CONFIDENCE FLAG: Add "flag": true to any row you are uncertain about (ambiguous format,
    missing key data, unusual values). Omit "flag" or set false for confident rows.

IMPORTANT: Do not skip any unit that has a unit number. Missing units cause reconciliation failures.
{hint_section}
{library_ctx}

Return ONLY a raw JSON array — no markdown fences, no commentary.
Start with [ and end with ].

Rent roll data (chunk {chunk_num} of {total_chunks}):
{chunk_text}"""

    resp = client.messages.create(
        model=CLAUDE_MODEL, max_tokens=4096,
        messages=[{"role": "user", "content": prompt}]
    )
    raw = resp.content[0].text.strip()
    raw = re.sub(r"^```(?:json)?\s*\n?", "", raw, flags=re.MULTILINE)
    raw = re.sub(r"\n?```\s*$",           "", raw, flags=re.MULTILINE)
    return json.loads(raw.strip())


# ── POST-PROCESSING VALIDATION ────────────────────────────────────────────────
def validate_rows(df: pd.DataFrame) -> pd.DataFrame:
    """Flag rows where Effective Rent looks wrong."""
    mkt = pd.to_numeric(df["Market Rent (Monthly)"],    errors="coerce")
    eff = pd.to_numeric(df["Effective Rent (Monthly)"], errors="coerce")

    # Already-flagged by Claude
    if "flag" not in df.columns:
        df["flag"] = False
    df["flag"] = df["flag"].fillna(False).astype(bool)

    # Python-level checks
    occupied_mask = ~df["Tenant Name"].str.upper().isin(["VACANT","ADMIN","MODEL"]) & df["Tenant Name"].notna()
    too_high  = occupied_mask & eff.notna() & mkt.notna() & (eff > mkt * 2)
    too_low   = occupied_mask & eff.notna() & (eff < 100)

    df.loc[too_high | too_low, "flag"] = True
    return df


# ── STANDARDIZE ───────────────────────────────────────────────────────────────
def standardize_rent_roll(df, step_ph, prog_ph, status_ph, analyst_hint, library_ctx):
    client      = Anthropic(api_key=st.secrets["anthropic_api_key"])
    total_rows  = len(df)
    chunk_size  = get_chunk_size(total_rows)
    all_results = []

    step_ph.markdown(render_steps(2), unsafe_allow_html=True)

    # Build chunks with overlap
    chunks, start = [], 0
    while start < total_rows:
        end = min(start + chunk_size, total_rows)
        chunks.append(df.iloc[start:end])
        if end == total_rows: break
        start += chunk_size - OVERLAP_ROWS

    num_chunks = len(chunks)
    for i, chunk in enumerate(chunks):
        status_ph.markdown(
            f"<small style='color:rgba(255,255,255,0.4);'>Chunk {i+1}/{num_chunks} · "
            f"{len(chunk)} rows · chunk_size={chunk_size}</small>", unsafe_allow_html=True)
        last_error = None
        for attempt in range(MAX_RETRIES):
            try:
                rows = call_claude(client, chunk.to_csv(index=False),
                                   i+1, num_chunks, analyst_hint, library_ctx)
                all_results.extend(rows)
                last_error = None
                break
            except json.JSONDecodeError as e:
                last_error = f"JSON error chunk {i+1} (attempt {attempt+1}/{MAX_RETRIES}): {e}"
                time.sleep(1.5)
            except Exception as e:
                last_error = f"Error chunk {i+1} (attempt {attempt+1}/{MAX_RETRIES}): {e}"
                time.sleep(1.5)
        if last_error:
            status_ph.empty(); st.error(last_error)
            return pd.DataFrame()
        prog_ph.progress((i+1)/num_chunks)

    status_ph.empty()
    step_ph.markdown(render_steps(3), unsafe_allow_html=True)

    if not all_results:
        return pd.DataFrame()

    result_df = pd.DataFrame(all_results)
    COLS = ["Unit No","Unit Size (SF)","Market Rent (Monthly)",
            "Effective Rent (Monthly)","Lease Start Date","Lease End Date","Tenant Name"]
    for col in COLS:
        if col not in result_df.columns: result_df[col] = None
    result_df = result_df[COLS + (["flag"] if "flag" in result_df.columns else [])]

    for dc in ["Lease Start Date","Lease End Date"]:
        result_df[dc] = pd.to_datetime(result_df[dc], errors="coerce").dt.strftime("%m/%d/%Y")

    result_df.drop_duplicates(subset=["Unit No"], keep="first", inplace=True)
    result_df.reset_index(drop=True, inplace=True)

    # Post-processing validation
    result_df = validate_rows(result_df)

    step_ph.markdown(render_steps(4), unsafe_allow_html=True)
    return result_df


# ── COLOR-CODED TABLE ─────────────────────────────────────────────────────────
def render_table(df: pd.DataFrame) -> str:
    COLS = ["Unit No","Unit Size (SF)","Market Rent (Monthly)",
            "Effective Rent (Monthly)","Lease Start Date","Lease End Date","Tenant Name"]
    hdr  = "".join(f"<th>{c}</th>" for c in COLS) + "<th>Status</th>"
    body = ""
    flag_col = "flag" in df.columns

    for _, row in df.iterrows():
        name     = str(row.get("Tenant Name","") or "").strip().upper()
        is_flag  = bool(row.get("flag", False)) if flag_col else False

        if name == "VACANT":
            tr, status_tag = "vac", "<span class='tag tag-vac'>Vacant</span>"
            name_tag = "<span class='tag tag-vac'>Vacant</span>"
        elif name in ("ADMIN","MODEL"):
            tr, status_tag = "nr", "<span class='tag tag-nr'>Non-Rev</span>"
            name_tag = f"<span class='tag tag-nr'>{name.title()}</span>"
        else:
            tr = "occ"
            name_tag = "<span class='tag tag-occ'>Occupied</span>"
            status_tag = "<span class='tag tag-occ'>OK</span>"

        if is_flag:
            tr += " flagged"
            status_tag = "<span class='tag tag-warn'>⚠ Review</span>"

        def fmt(col, val):
            if val is None or (isinstance(val, float) and pd.isna(val)):
                return "<span style='color:rgba(255,255,255,0.2);'>—</span>"
            if col in ("Market Rent (Monthly)","Effective Rent (Monthly)"):
                try:    return f"${float(val):,.2f}"
                except: return str(val)
            if col == "Unit Size (SF)":
                try:    return f"{int(float(val)):,}"
                except: return str(val)
            return str(val)

        cells = ""
        for col in COLS:
            val = row.get(col)
            cls = "mono" if col=="Unit No" else \
                  "right green" if col=="Effective Rent (Monthly)" else \
                  "right" if col in ("Market Rent (Monthly)","Unit Size (SF)") else ""
            display = name_tag if col == "Tenant Name" else fmt(col, val)
            cells += f"<td class='{cls}'>{display}</td>"
        cells += f"<td>{status_tag}</td>"
        body  += f"<tr class='{tr}'>{cells}</tr>"

    flag_count = int(df["flag"].sum()) if "flag" in df.columns else 0
    legend_extra = f'<div class="leg-item"><div class="leg-dot" style="background:#f59e0b;border:1px solid #fbbf24;"></div>⚠ Flagged ({flag_count})</div>' if flag_count else ""

    return f"""
    <div class='tbl-wrap'>
      <div class='tbl-header'>
        <span class='tbl-header-left'>{len(df)} units · standardized output</span>
        <div class='legend'>
          <div class='leg-item'><div class='leg-dot' style='background:#fcd34d;'></div>Vacant</div>
          <div class='leg-item'><div class='leg-dot' style='background:#86efac;'></div>Admin/Model</div>
          <div class='leg-item'><div class='leg-dot' style='background:#93c5fd;'></div>Occupied</div>
          {legend_extra}
        </div>
      </div>
      <div style='overflow-x:auto;max-height:500px;overflow-y:auto;'>
        <table class='rv-tbl'><thead><tr>{hdr}</tr></thead><tbody>{body}</tbody></table>
      </div>
    </div>"""


# ── RAW FILE TABLE ────────────────────────────────────────────────────────────
def render_raw_table(df: pd.DataFrame, sheet_name: str) -> str:
    df_show = df.head(200)
    hdr  = "".join(f"<th>{i}</th>" for i in range(df_show.shape[1]))
    body = ""
    for _, row in df_show.iterrows():
        cells = "".join(
            f"<td style='max-width:180px;overflow:hidden;text-overflow:ellipsis;'>"
            f"{'' if pd.isna(v) else str(v)}</td>"
            for v in row
        )
        body += f"<tr class='occ'>{cells}</tr>"

    return f"""
    <div class='tbl-wrap'>
      <div class='tbl-header'>
        <span class='tbl-header-left'>Raw file · sheet: "{sheet_name}" · {len(df)} rows × {df.shape[1]} cols (showing first 200)</span>
      </div>
      <div style='overflow-x:auto;max-height:500px;overflow-y:auto;'>
        <table class='rv-tbl'><thead><tr>{hdr}</tr></thead><tbody>{body}</tbody></table>
      </div>
    </div>"""


# ── KPI CARDS ─────────────────────────────────────────────────────────────────
def render_kpis(df: pd.DataFrame) -> str:
    occ   = df[~df["Tenant Name"].str.upper().isin(["VACANT","ADMIN","MODEL"]) & df["Tenant Name"].notna()]
    vac   = df[df["Tenant Name"].str.upper()=="VACANT"]
    total = len(df)
    occ_pct = (len(occ)/total*100) if total else 0
    avg_mkt = pd.to_numeric(df["Market Rent (Monthly)"],    errors="coerce").mean()
    avg_eff = pd.to_numeric(occ["Effective Rent (Monthly)"], errors="coerce").mean()
    tpgi    = pd.to_numeric(df["Market Rent (Monthly)"],    errors="coerce").sum()
    def fc(v): return f"${v:,.0f}" if pd.notna(v) and v > 0 else "—"
    cards = [
        ("Total Units",        f"{total}",         "All incl. vacant & non-rev", ""),
        ("Occupancy Rate",     f"{occ_pct:.1f}%",  f"{len(occ)} occ / {len(vac)} vac", "hi"),
        ("Avg Market Rent",    fc(avg_mkt),          "Monthly · all units", ""),
        ("Avg Effective Rent", fc(avg_eff),           "Monthly · occupied only", "hi"),
        ("Total Potential GI", fc(tpgi),              "Sum of all market rents / mo", ""),
    ]
    html = "<div class='kpi-grid'>"
    for label, value, sub, cls in cards:
        html += f"<div class='kpi-card {cls}'><div class='kpi-label'>{label}</div><div class='kpi-value'>{value}</div><div class='kpi-sub'>{sub}</div></div>"
    return html + "</div>"


# ── RECONCILIATION ────────────────────────────────────────────────────────────
def render_recon(df: pd.DataFrame, orig_rows: int) -> str:
    occ        = df[~df["Tenant Name"].str.upper().isin(["VACANT","ADMIN","MODEL"]) & df["Tenant Name"].notna()]
    vac        = df[df["Tenant Name"].str.upper()=="VACANT"]
    clean_rows = len(df)
    clean_mkt  = pd.to_numeric(df["Market Rent (Monthly)"], errors="coerce").sum()
    missing    = [c for c in ["Unit No","Unit Size (SF)","Market Rent (Monthly)",
                               "Effective Rent (Monthly)","Lease Start Date","Lease End Date","Tenant Name"]
                  if c not in df.columns]
    size_warn  = clean_rows < orig_rows * 0.25
    flag_count = int(df["flag"].sum()) if "flag" in df.columns else 0

    checks = [
        {"title":"All Required Columns Present",
         "detail":"7 of 7 columns found" if not missing else f"Missing: {', '.join(missing)}",
         "st":"pass" if not missing else "fail","icon":"✓" if not missing else "✗","badge":"Pass" if not missing else "Fail"},
        {"title":"Row Count vs Raw File",
         "detail":f"{clean_rows} standardized rows from {orig_rows} raw rows",
         "st":"warn" if size_warn else "pass","icon":"⚠" if size_warn else "✓","badge":"Review" if size_warn else "Pass"},
        {"title":"Market Rent Populated",
         "detail":f"Total market rent: ${clean_mkt:,.2f}",
         "st":"pass" if clean_mkt>0 else "fail","icon":"✓" if clean_mkt>0 else "✗","badge":"Pass" if clean_mkt>0 else "Fail"},
        {"title":"Occupied Tenant Count",
         "detail":f"{len(occ)} occupied · {len(vac)} vacant",
         "st":"pass" if len(occ)>0 else "warn","icon":"✓" if len(occ)>0 else "⚠","badge":"Pass" if len(occ)>0 else "Review"},
        {"title":"Flagged Rows",
         "detail":f"{flag_count} rows need analyst review" if flag_count else "No rows flagged",
         "st":"warn" if flag_count else "pass","icon":"⚠" if flag_count else "✓","badge":f"{flag_count} Flags" if flag_count else "Clean"},
        {"title":"Effective Rent Validation",
         "detail":"All effective rents within expected range" if flag_count==0 else f"{flag_count} outliers detected (>2× market or <$100)",
         "st":"warn" if flag_count else "pass","icon":"⚠" if flag_count else "✓","badge":"Review" if flag_count else "Pass"},
    ]
    html = "<div class='recon-grid'>"
    for c in checks:
        rb = {"pass":"rb-pass","warn":"rb-warn","fail":"rb-fail"}[c["st"]]
        html += f"""<div class='recon-card'>
          <div class='r-icon {c["st"]}'>{c["icon"]}</div>
          <div class='r-body'><div class='r-title'>{c["title"]}</div><div class='r-detail'>{c["detail"]}</div></div>
          <span class='r-badge {rb}'>{c["badge"]}</span>
        </div>"""
    return html + "</div>"


# ── EXCEL EXPORT ──────────────────────────────────────────────────────────────
def build_excel(df: pd.DataFrame) -> bytes:
    wb=Workbook(); ws=wb.active; ws.title="Standardized Rent Roll"
    navy="0A2E3D"; thin=Side(style="thin",color="E2E8F0")
    bdr=Border(left=thin,right=thin,top=thin,bottom=thin)
    COLS=["Unit No","Unit Size (SF)","Market Rent (Monthly)",
          "Effective Rent (Monthly)","Lease Start Date","Lease End Date","Tenant Name"]
    for col,h in enumerate(COLS,1):
        c=ws.cell(row=1,column=col,value=h)
        c.font=Font(name="Calibri",bold=True,color="FFFFFF",size=10)
        c.fill=PatternFill("solid",start_color=navy)
        c.alignment=Alignment(horizontal="center",vertical="center",wrap_text=True)
        c.border=bdr
    ws.row_dimensions[1].height=28
    flag_col="flag" in df.columns
    for ri,rec in enumerate(df.itertuples(index=False),2):
        nm=str(rec[6] or "").upper()
        iv=nm=="VACANT"; nr=nm in("ADMIN","MODEL")
        is_flagged=bool(getattr(rec,"flag",False)) if flag_col else False
        fill=(PatternFill("solid",start_color="FFFBEB") if iv else
              PatternFill("solid",start_color="F0FDF4") if nr else
              PatternFill("solid",start_color="FFF9EB") if is_flagged else
              PatternFill("solid",start_color="FFFFFF") if ri%2 else
              PatternFill("solid",start_color="F8FAFC"))
        for col,val in enumerate(rec[:7],1):
            c=ws.cell(row=ri,column=col,value=val); c.border=bdr; c.fill=fill
            c.font=Font(name="Calibri",size=10,italic=(iv or nr),
                        color=("92400E" if iv else "166534" if nr else "854D0E" if is_flagged else "1E293B"))
            if col==2:        c.number_format="#,##0";     c.alignment=Alignment(horizontal="right")
            elif col in(3,4): c.number_format="$#,##0.00"; c.alignment=Alignment(horizontal="right")
            elif col in(5,6): c.alignment=Alignment(horizontal="center")
            else:             c.alignment=Alignment(horizontal="left")
        if is_flagged:
            ws.cell(row=ri,column=7).comment = None
    last=len(df)+1; tot=last+1
    for col in range(1,8):
        c=ws.cell(row=tot,column=col)
        c.fill=PatternFill("solid",start_color=navy)
        c.font=Font(name="Calibri",bold=True,color="FFFFFF",size=10)
        c.border=bdr; c.alignment=Alignment(horizontal="right")
    ws.cell(row=tot,column=1,value="TOTALS / AVERAGES").alignment=Alignment(horizontal="left")
    ws.cell(row=tot,column=2,value=f"=SUM(B2:B{last})").number_format="#,##0"
    ws.cell(row=tot,column=3,value=f"=AVERAGE(C2:C{last})").number_format="$#,##0.00"
    ws.cell(row=tot,column=4,value=f'=AVERAGEIF(D2:D{last},"<>",D2:D{last})').number_format="$#,##0.00"
    nr2=tot+2
    for i,note in enumerate(["Notes:",
        "• Yellow rows = Vacant units","• Green rows = Non-revenue (Admin/Model)",
        "• Orange rows = ⚠ Flagged for review (unusual effective rent or low confidence)",
        "• Effective Rent = base rent + housing subsidy only",
        "• Deposits, utilities, discounts, fees excluded"]):
        c=ws.cell(row=nr2+i,column=1,value=note)
        c.font=Font(name="Calibri",bold=(i==0),size=9,
                    color=("166534" if "Green" in note else "92400E" if "Yellow" in note
                           else "854D0E" if "Orange" in note else "595959"))
    for i,w in enumerate([14,14,22,24,16,16,26],1):
        ws.column_dimensions[get_column_letter(i)].width=w
    ws.freeze_panes="A2"; ws.auto_filter.ref=f"A1:G{last}"
    buf=io.BytesIO(); wb.save(buf); buf.seek(0)
    return buf.getvalue()


# ── MAIN ──────────────────────────────────────────────────────────────────────
init_library()

if not check_password():
    st.stop()

st.markdown("""
<div class="rv-hero">
  <div class="rv-hero-left">
    <div class="rv-logo">RV</div>
    <div class="rv-hero-text">
      <h1>Rent Roll Standardizer</h1>
      <p>RealVal · Multifamily Underwriting Intelligence</p>
    </div>
  </div>
  <div class="rv-hero-right">
    <span class="rv-version">v4.0</span>
    <div class="rv-badge">⚡ Claude AI</div>
  </div>
</div>
""", unsafe_allow_html=True)

# ── TABS: Main tool | Format Library ──────────────────────────────────────────
main_tab, library_tab = st.tabs(["📋  Standardize Rent Roll", "📚  Format Library"])

with main_tab:
    st.markdown("<div class='sec-label'>Upload Rent Roll</div>", unsafe_allow_html=True)
    col_up, col_info = st.columns([2, 1])

    with col_up:
        st.markdown("<div class='upload-panel'><h3>Upload Your Excel File</h3><p>Any broker format accepted — multi-sheet workbooks supported</p>", unsafe_allow_html=True)
        uploaded_file = st.file_uploader("", type=["xlsx"], label_visibility="collapsed")

        # Format hint box
        st.markdown("<div class='hint-box'><label>Format Hint (optional)</label>", unsafe_allow_html=True)
        analyst_hint = st.text_area(
            "", height=68, label_visibility="collapsed",
            placeholder='e.g. "This broker uses annual rents" · "Charge codes in column H" · "Yardi export format"',
            key="analyst_hint"
        )
        st.markdown("</div></div>", unsafe_allow_html=True)

    with col_info:
        st.markdown("""
        <div class='info-panel'>
          <h4>What's new in v4.0</h4>
          <div class='info-row'><div class='info-dot'></div>Auto-detects correct sheet in multi-tab workbooks</div>
          <div class='info-row'><div class='info-dot'></div>Side-by-side raw vs. standardized preview</div>
          <div class='info-row'><div class='info-dot'></div>Format hint passes context to Claude</div>
          <div class='info-row'><div class='info-dot'></div>⚠ Flags rows with unusual effective rents</div>
          <div class='info-row'><div class='info-dot'></div>Claude flags low-confidence rows automatically</div>
          <div class='info-row'><div class='info-dot'></div>Chunk size auto-scales with file size</div>
          <div class='info-row'><div class='info-dot'></div>Format library saves examples for future files</div>
        </div>
        """, unsafe_allow_html=True)

    if uploaded_file:
        file_bytes = uploaded_file.read()
        st.markdown(
            f"<div class='file-pill'>📄 &nbsp;{uploaded_file.name}&nbsp; · &nbsp;{len(file_bytes)/1024:.1f} KB</div>",
            unsafe_allow_html=True
        )

        col_btn, col_broker = st.columns([1, 1])
        with col_btn:
            run = st.button("⚡  Standardize Rent Roll")
        with col_broker:
            broker_name = st.text_input("Broker / Source name (for library)",
                                        placeholder="e.g. CBRE, Marcus & Millichap, Yardi…",
                                        label_visibility="visible")

        if run:
            st.markdown("<div class='sec-label'>Processing</div>", unsafe_allow_html=True)
            step_ph   = st.empty()
            step_ph.markdown(render_steps(0), unsafe_allow_html=True)
            prog_ph   = st.progress(0)
            status_ph = st.empty()

            # Step 0 → Step 1: Read file
            step_ph.markdown(render_steps(1), unsafe_allow_html=True)
            try:
                original_df, sheet_name = detect_rent_roll_sheet(file_bytes)
            except Exception as e:
                st.error(f"Could not read file: {e}"); st.stop()

            status_ph.markdown(
                f"<small style='color:rgba(255,255,255,0.4);'>Sheet detected: <strong style='color:#93c5fd;'>'{sheet_name}'</strong> · {len(original_df)} raw rows</small>",
                unsafe_allow_html=True
            )
            time.sleep(0.4)

            # Build library context
            library_ctx = build_library_context()

            # Step 2 → processing
            standardized_df = standardize_rent_roll(
                original_df, step_ph, prog_ph, status_ph, analyst_hint, library_ctx
            )

            if standardized_df.empty:
                st.error("Standardization failed or returned no data. Please check the file and try again.")
                st.stop()

            prog_ph.empty(); status_ph.empty()

            # Auto-save to format library
            if broker_name.strip():
                save_to_library(broker_name.strip(), analyst_hint, original_df, standardized_df)
                st.success(f"✅ Format saved to library as **{broker_name.strip()}**")

            # Flag banner
            flag_count = int(standardized_df["flag"].sum()) if "flag" in standardized_df.columns else 0
            if flag_count:
                st.markdown(f"""
                <div class='flag-banner'>
                  ⚠ &nbsp;<strong>{flag_count} row{'s' if flag_count>1 else ''} flagged for review</strong>
                  &nbsp;— effective rent appears unusual (>2× market or <$100). Check these rows before using.
                </div>""", unsafe_allow_html=True)

            # KPIs
            st.markdown("<div class='sec-label'>Underwriting Summary</div>", unsafe_allow_html=True)
            st.markdown(render_kpis(standardized_df), unsafe_allow_html=True)

            # Reconciliation
            st.markdown("<div class='sec-label'>Reconciliation Checks</div>", unsafe_allow_html=True)
            st.markdown(render_recon(standardized_df, len(original_df)), unsafe_allow_html=True)

            # Side-by-side tabs
            st.markdown("<div class='sec-label'>Preview</div>", unsafe_allow_html=True)
            tab_raw, tab_clean = st.tabs([f"📄  Raw File  ({sheet_name})", "✅  Standardized Output"])
            with tab_raw:
                st.markdown(render_raw_table(original_df, sheet_name), unsafe_allow_html=True)
            with tab_clean:
                st.markdown(render_table(standardized_df), unsafe_allow_html=True)

            # Download
            st.markdown("<div class='sec-label'>Download</div>", unsafe_allow_html=True)
            today     = date.today().strftime("%m%d%Y")
            base_name = uploaded_file.name.replace(".xlsx","")
            col_dl, col_hint_dl = st.columns([1, 2])
            with col_dl:
                st.download_button(
                    label="⬇  Download Standardized Rent Roll",
                    data=build_excel(standardized_df),
                    file_name=f"{base_name}_standardized_{today}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                )
            with col_hint_dl:
                if flag_count:
                    st.markdown(f"<div style='padding-top:0.7rem;font-size:0.78rem;color:#fbbf24;'>⚠ {flag_count} flagged rows included — review before use</div>", unsafe_allow_html=True)

    else:
        st.markdown("""
        <div style='background:#0d1e2e;border:1px solid rgba(255,255,255,0.06);border-radius:14px;
        padding:3rem;text-align:center;margin-top:1rem;'>
          <div style='font-size:2.5rem;margin-bottom:1rem;'>📂</div>
          <div style='font-family:Sora,sans-serif;font-size:1rem;font-weight:700;color:#fff;margin-bottom:0.4rem;'>No file uploaded yet</div>
          <div style='font-size:0.8rem;color:rgba(255,255,255,0.3);'>Upload an Excel rent roll above to get started</div>
        </div>""", unsafe_allow_html=True)

# ── FORMAT LIBRARY TAB ────────────────────────────────────────────────────────
with library_tab:
    st.markdown("<div class='sec-label'>Saved Format Examples</div>", unsafe_allow_html=True)
    lib = st.session_state.get("format_library", [])

    if not lib:
        st.markdown("""
        <div style='background:#0d1e2e;border:1px solid rgba(255,255,255,0.06);border-radius:14px;
        padding:2.5rem;text-align:center;'>
          <div style='font-size:2rem;margin-bottom:0.8rem;'>📚</div>
          <div style='font-family:Sora,sans-serif;font-size:0.95rem;font-weight:700;color:#fff;margin-bottom:0.4rem;'>No formats saved yet</div>
          <div style='font-size:0.78rem;color:rgba(255,255,255,0.3);'>
          Process a rent roll and enter a broker name to save it here.<br>
          Saved formats are used as examples to improve future processing.
          </div>
        </div>""", unsafe_allow_html=True)
    else:
        st.markdown(f"<div style='font-size:0.78rem;color:rgba(255,255,255,0.4);margin-bottom:1rem;'>{len(lib)} format{'s' if len(lib)>1 else ''} saved this session · used as context for future files</div>", unsafe_allow_html=True)
        for i, entry in enumerate(reversed(lib)):
            with st.expander(f"📄 {entry['broker']}  ·  {entry['unit_count']} units  ·  {entry['date']}"):
                if entry["hint"]:
                    st.markdown(f"**Hint used:** `{entry['hint']}`")
                st.markdown("**Raw sample (first 5 rows):**")
                st.code(entry["raw_sample"], language="text")
                st.markdown("**Standardized output sample:**")
                st.json(entry["out_sample"][:2])
        if st.button("🗑  Clear Format Library"):
            st.session_state["format_library"] = []
            st.rerun()

st.markdown("""
<div class='rv-footer'>
  <span>© 2026 <a href='https://therealval.com' target='_blank'>RealVal</a> · Internal Tool · Confidential</span>
  <span>Powered by Claude AI (Haiku) · Prompt {PROMPT_VERSION} · Authorized analyst use only</span>
</div>
""".replace("{PROMPT_VERSION}", PROMPT_VERSION), unsafe_allow_html=True)
