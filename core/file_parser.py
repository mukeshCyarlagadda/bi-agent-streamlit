"""
Parse uploaded files into a user-scoped temporary SQLite database.

Supported inputs
----------------
Digital formats (no OCR):
  .csv, .xlsx, .xls       → pandas
  .pdf (digital)          → pdfplumber table extraction

OCR fallback (OpenAI vision, gpt-4o-mini):
  .pdf (scanned/no tables) → render pages → GPT-4o vision
  .jpg / .jpeg / .png / .webp / .gif  → GPT-4o vision

Data isolation
--------------
Every temp SQLite file is written to:
  /tmp/bi_agent_uploads/<user_id_prefix>/<uuid>.db
A user can only access their own session (enforced in api/dependencies.py),
so even if two users upload identical files they each get separate DB files.

Returns (db_path, [table_name], human_readable_message).
"""
from __future__ import annotations

import base64
import io
import json
import logging
import re
import tempfile
import uuid
from pathlib import Path
from typing import List, Tuple

import pandas as pd
import sqlalchemy

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

MAX_BYTES = 50 * 1024 * 1024   # 50 MB
DIGITAL_SUFFIXES = {'.csv', '.xlsx', '.xls', '.pdf'}
IMAGE_SUFFIXES   = {'.jpg', '.jpeg', '.png', '.webp', '.gif'}
ALL_SUFFIXES     = DIGITAL_SUFFIXES | IMAGE_SUFFIXES
OCR_MODEL        = 'gpt-4o-mini'
OCR_MAX_PAGES    = 10          # cap pages sent to vision API


# ---------------------------------------------------------------------------
# Column normalisation + type coercion
# ---------------------------------------------------------------------------

def _clean_df(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    # Normalise column names → snake_case, alphanumeric only
    df.columns = [
        re.sub(r'[^a-z0-9_]', '', c.lower().strip().replace(' ', '_').replace('-', '_')) or f'col_{i}'
        for i, c in enumerate(df.columns)
    ]

    # Drop fully-empty rows / columns
    df = df.dropna(how='all').dropna(axis=1, how='all').reset_index(drop=True)

    # Coerce date-like columns → ISO string
    date_kw = ('date', 'time', 'dt', 'posted', 'trans', 'period', 'value_date')
    for col in df.columns:
        if any(kw in col for kw in date_kw):
            try:
                parsed = pd.to_datetime(df[col], infer_datetime_format=True, errors='coerce')
                if parsed.notna().sum() > len(df) * 0.4:
                    df[col] = parsed.dt.strftime('%Y-%m-%d').where(parsed.notna(), None)
            except Exception:
                pass

    # Coerce amount-like columns: strip £$€ / commas, handle (100.00) → -100
    amount_kw = ('amount', 'debit', 'credit', 'balance', 'payment', 'charge', 'total', 'value')
    for col in df.columns:
        if df[col].dtype == object and any(kw in col for kw in amount_kw):
            try:
                cleaned = (
                    df[col].astype(str)
                    .str.replace(r'[£$€,\s]', '', regex=True)
                    .str.replace(r'^\((.+)\)$', r'-\1', regex=True)
                )
                coerced = pd.to_numeric(cleaned, errors='coerce')
                if coerced.notna().sum() > len(df) * 0.4:
                    df[col] = coerced
            except Exception:
                pass

    return df


# ---------------------------------------------------------------------------
# Digital parsers
# ---------------------------------------------------------------------------

def _parse_csv(content: bytes) -> pd.DataFrame:
    for enc in ('utf-8-sig', 'utf-8', 'latin-1', 'cp1252'):
        try:
            df = pd.read_csv(io.BytesIO(content), encoding=enc, thousands=',', skipinitialspace=True)
            if not df.empty:
                return _clean_df(df)
        except Exception:
            continue
    raise ValueError('Could not parse CSV — try saving as UTF-8 CSV from your bank portal.')


def _parse_excel(content: bytes) -> pd.DataFrame:
    df = pd.read_excel(io.BytesIO(content), thousands=',')
    return _clean_df(df)


def _collapse_multiline_rows(df: pd.DataFrame) -> pd.DataFrame:
    """
    Bank statements often wrap long descriptions across rows.
    A continuation row has no date and no amount — just a description fragment.
    We detect these and append them to the previous real row's description.

    Also drops summary/total rows (e.g. "Total ATM and debit card subtractions").
    """
    if df.empty:
        return df

    # Find the date and amount column by name heuristic
    cols = list(df.columns)
    date_col   = next((c for c in cols if 'date' in c), cols[0] if cols else None)
    amount_col = next((c for c in cols if any(k in c for k in ('amount', 'debit', 'credit', 'balance'))), cols[-1] if cols else None)
    desc_col   = next((c for c in cols if any(k in c for k in ('desc', 'narr', 'detail', 'merchant', 'transaction'))), None)

    if date_col is None or amount_col is None:
        return df

    # If no description column found, use the middle column
    if desc_col is None:
        middle = [c for c in cols if c not in (date_col, amount_col)]
        desc_col = middle[0] if middle else None

    if desc_col is None:
        return df

    result_rows: list[dict] = []
    for _, row in df.iterrows():
        raw_date   = str(row.get(date_col, '') or '').strip()
        raw_amount = str(row.get(amount_col, '') or '').strip()
        raw_desc   = str(row.get(desc_col, '') or '').strip()

        # Skip pure summary/total rows
        if any(kw in raw_desc.lower() for kw in ('total ', 'subtotal', 'continued on')):
            continue

        has_date   = bool(re.search(r'\d{1,2}[/\-]\d{1,2}', raw_date))
        has_amount = bool(re.search(r'[\d]', raw_amount))

        if has_date or has_amount:
            # Real transaction row
            result_rows.append({c: row[c] for c in cols})
        elif raw_desc and result_rows:
            # Continuation: append description to previous row
            prev = result_rows[-1]
            prev[desc_col] = f"{prev.get(desc_col, '')} {raw_desc}".strip()

    if not result_rows:
        return df  # fallback: return original if we stripped everything

    return pd.DataFrame(result_rows, columns=cols).reset_index(drop=True)


def _parse_pdf_digital(content: bytes) -> pd.DataFrame:
    """Extract tables from a digital (non-scanned) PDF using pdfplumber."""
    try:
        import pdfplumber
    except ImportError:
        raise ImportError('pdfplumber is required: pip install pdfplumber')

    all_tables: list[pd.DataFrame] = []
    with pdfplumber.open(io.BytesIO(content)) as pdf:
        for page in pdf.pages:
            for raw in (page.extract_tables() or []):
                if not raw or len(raw) < 2:
                    continue
                headers = [str(c).strip() if c else f'col_{j}' for j, c in enumerate(raw[0])]
                try:
                    tbl = pd.DataFrame(raw[1:], columns=headers)
                    all_tables.append(tbl)
                except Exception:
                    pass

    if not all_tables:
        return pd.DataFrame()   # signal to caller: no tables found, try OCR

    df = pd.concat(all_tables, ignore_index=True)
    df = _collapse_multiline_rows(df)
    return _clean_df(df)


# ---------------------------------------------------------------------------
# OCR via OpenAI vision
# ---------------------------------------------------------------------------

# Summary/total row keywords — these are account-level aggregates, not transactions
_SUMMARY_KEYWORDS = (
    'beginning balance', 'ending balance', 'starting balance',
    'deposits and other additions', 'atm and debit card subtractions',
    'other subtractions', 'total deposits', 'total withdrawals',
    'total subtractions', 'total atm', 'total other', 'total service',
    'continued on', 'this page intentionally',
)


def _make_ocr_prompt(hint_year: int) -> str:
    return (
        'Extract all data rows from this document image into a JSON array. '
        'Look at the column headers visible in the image and use them as JSON keys (snake_case). '
        'Include every data row — skip section headers, totals, subtotals, and summary lines. '
        'For date values: use the full 4-digit year visible in the document header or context '
        f'(filename suggests year {hint_year} — use this if no 4-digit year is visible). '
        'Convert all dates to YYYY-MM-DD format. '
        'For numeric columns strip currency symbols and return plain numbers. '
        'Return ONLY a raw JSON array, no markdown, no commentary.'
    )


def _detect_statement_year(content: bytes, filename: str) -> int:
    """Extract the statement year from filename or first-page PDF text."""
    # Try filename first (e.g. eStmt_2026-05-07.pdf)
    m = re.search(r'\b(20\d{2})\b', filename)
    if m:
        return int(m.group(1))
    # Try reading text from first PDF page
    try:
        import pdfplumber
        with pdfplumber.open(io.BytesIO(content)) as pdf:
            if pdf.pages:
                text = pdf.pages[0].extract_text() or ''
                m = re.search(r'\b(20\d{2})\b', text)
                if m:
                    return int(m.group(1))
    except Exception:
        pass
    import datetime
    return datetime.datetime.now().year


def _fix_ocr_years(df: pd.DataFrame, target_year: int) -> pd.DataFrame:
    """
    GPT sometimes misreads 2-digit years (04/22/26 → 2022 instead of 2026).
    Post-process: any date with a year outside [target_year-1, target_year+1]
    gets its year forced to target_year.
    """
    date_col = next((c for c in df.columns if 'date' in c), None)
    if date_col is None:
        return df

    def fix(val: object) -> object:
        s = str(val or '').strip()
        # Already YYYY-MM-DD
        m = re.match(r'(\d{4})-(\d{2})-(\d{2})$', s)
        if m:
            yr = int(m.group(1))
            if abs(yr - target_year) > 1:
                return f'{target_year}-{m.group(2)}-{m.group(3)}'
            return s
        # MM/DD/YY or MM/DD/YYYY still raw
        m = re.match(r'(\d{1,2})/(\d{1,2})/(\d{2,4})$', s)
        if m:
            return f'{target_year}-{int(m.group(1)):02d}-{int(m.group(2)):02d}'
        return val

    df[date_col] = df[date_col].apply(fix)
    return df


def _filter_summary_rows(rows: list[dict]) -> list[dict]:
    """Drop header/total rows that GPT included despite the prompt."""
    out = []
    for row in rows:
        # Drop bank-statement summary lines by keyword (safe for non-financial docs too)
        all_text = ' '.join(str(v) for v in row.values()).lower()
        if any(kw in all_text for kw in _SUMMARY_KEYWORDS):
            continue
        # Drop rows where every value is empty/null
        values = [v for v in row.values() if v not in (None, '', 'None', 'null', 'N/A')]
        if not values:
            continue
        out.append(row)
    return out


def _vision_extract(image_b64: str, mime: str, api_key: str, prompt: str) -> list[dict]:
    """Send one base64 image to GPT-4o-mini, return parsed transaction rows."""
    from openai import OpenAI
    client = OpenAI(api_key=api_key)
    resp = client.chat.completions.create(
        model=OCR_MODEL,
        messages=[{
            'role': 'user',
            'content': [
                {'type': 'text', 'text': prompt},
                {'type': 'image_url', 'image_url': {
                    'url': f'data:{mime};base64,{image_b64}',
                    'detail': 'high',
                }},
            ],
        }],
        max_tokens=4096,
    )
    text = (resp.choices[0].message.content or '').strip()
    m = re.search(r'\[.*\]', text, re.DOTALL)
    if not m:
        return []
    try:
        rows = json.loads(m.group())
        return _filter_summary_rows(rows)
    except json.JSONDecodeError:
        logger.warning('Vision JSON parse failed: %s', text[:200])
        return []


def _parse_pdf_ocr(content: bytes, api_key: str, statement_year: int) -> pd.DataFrame:
    """Render PDF pages → PNG → GPT-4o-mini vision, with year-aware prompt."""
    try:
        import fitz  # PyMuPDF
    except ImportError:
        raise ImportError('PyMuPDF is required for scanned PDF OCR: pip install PyMuPDF')

    prompt = _make_ocr_prompt(statement_year)
    doc = fitz.open(stream=content, filetype='pdf')
    n_pages = min(len(doc), OCR_MAX_PAGES)
    all_rows: list[dict] = []

    for i in range(n_pages):
        pix = doc[i].get_pixmap(dpi=150)
        b64 = base64.b64encode(pix.tobytes('png')).decode()
        rows = _vision_extract(b64, 'image/png', api_key, prompt)
        all_rows.extend(rows)
        logger.debug('PDF OCR page %d/%d → %d rows', i + 1, n_pages, len(rows))

    if not all_rows:
        raise ValueError(
            'No transactions could be extracted from this PDF via OCR. '
            'The document may be encrypted or contain no financial tables.'
        )

    df = pd.DataFrame(all_rows)
    df = _fix_ocr_years(df, statement_year)
    return _clean_df(df)


def _parse_image_ocr(content: bytes, filename: str, api_key: str, statement_year: int = 0) -> pd.DataFrame:
    """Send an image file directly to GPT-4o-mini vision."""
    import datetime
    if not statement_year:
        statement_year = datetime.datetime.now().year
    suffix = Path(filename).suffix.lower().lstrip('.')
    mime_map = {'jpg': 'image/jpeg', 'jpeg': 'image/jpeg', 'png': 'image/png',
                'webp': 'image/webp', 'gif': 'image/gif'}
    mime = mime_map.get(suffix, 'image/jpeg')
    b64 = base64.b64encode(content).decode()
    prompt = _make_ocr_prompt(statement_year)
    rows = _vision_extract(b64, mime, api_key, prompt)
    if not rows:
        raise ValueError('No transactions found in the image.')
    df = pd.DataFrame(rows)
    df = _fix_ocr_years(df, statement_year)
    return _clean_df(df)


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

def file_to_sqlite(
    content: bytes,
    filename: str,
    user_id: str = 'anonymous',
    table_name: str = 'transactions',
) -> Tuple[str, List[str], str]:
    """
    Parse *content* → SQLite temp DB scoped to *user_id*.

    Returns (db_path, [table_name], human_readable_message).
    Raises ValueError for bad inputs, ImportError for missing optional deps.
    """
    if len(content) > MAX_BYTES:
        mb = len(content) // 1_048_576
        raise ValueError(f'File too large ({mb} MB). Maximum is 50 MB.')

    suffix = Path(filename).suffix.lower()
    if suffix not in ALL_SUFFIXES:
        raise ValueError(
            f"Unsupported file type '{suffix}'. "
            f"Accepted: {', '.join(sorted(ALL_SUFFIXES))}"
        )

    # --- parse ---
    if suffix == '.csv':
        df = _parse_csv(content)

    elif suffix in ('.xlsx', '.xls'):
        df = _parse_excel(content)

    elif suffix in IMAGE_SUFFIXES:
        from core.config import settings
        if not settings.openai_api_key:
            raise ValueError('OCR requires OPENAI_API_KEY to be set.')
        stmt_year = _detect_statement_year(content, filename)
        df = _parse_image_ocr(content, filename, settings.openai_api_key, stmt_year)

    else:
        # PDF — try digital first, fall back to OCR
        stmt_year = _detect_statement_year(content, filename)
        df = _parse_pdf_digital(content)
        if df.empty:
            logger.info('%s: no tables found digitally, trying OCR (year=%d)', filename, stmt_year)
            from core.config import settings
            if not settings.openai_api_key:
                raise ValueError(
                    'This PDF appears to be scanned (no digital text). '
                    'OCR requires OPENAI_API_KEY to be set.'
                )
            df = _parse_pdf_ocr(content, settings.openai_api_key, stmt_year)

    if df.empty:
        raise ValueError('The file is empty or contains no data rows.')

    # --- write to user-scoped SQLite ---
    # safe prefix: first 8 chars of user_id (UUID), alphanumeric only
    safe_uid = re.sub(r'[^a-zA-Z0-9]', '', user_id)[:16] or 'anon'
    # Persistent storage so sessions survive server restarts
    tmp_dir = Path.home() / '.bi_agent_uploads' / safe_uid
    tmp_dir.mkdir(parents=True, exist_ok=True)

    db_path = str(tmp_dir / f'{uuid.uuid4().hex}.db')
    engine = sqlalchemy.create_engine(f'sqlite:///{db_path}')
    try:
        df.to_sql(table_name, engine, index=False, if_exists='replace')
    finally:
        engine.dispose()

    rows, cols = df.shape
    msg = f'Loaded {rows:,} rows × {cols} columns from {filename}'
    logger.info('%s | user=%s | %s', filename, safe_uid, msg)
    return db_path, [table_name], msg
