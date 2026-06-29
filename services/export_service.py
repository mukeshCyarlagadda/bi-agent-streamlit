"""
Export service — generates a PDF from session chat history.

Zero Streamlit imports here. This is pure business logic.
The PDF is returned as bytes; the caller (router) wraps it in a Response.
"""
from __future__ import annotations

import base64
import io
import logging
import tempfile
from datetime import datetime
from typing import List

from fpdf import FPDF

from api.session_store import ChatEntry

logger = logging.getLogger(__name__)

_MAX_ROWS_IN_PDF = 50       # truncate large DataFrames to keep PDFs manageable
_MAX_CELL_CHARS = 25        # truncate long cell values


class _BIPdf(FPDF):
    """Thin FPDF subclass with helper methods for our layout."""

    def header(self) -> None:
        self.set_font("Helvetica", "B", 10)
        self.set_text_color(100, 100, 100)
        self.cell(0, 8, "Business Intelligence Session Summary", align="C", new_x="LMARGIN", new_y="NEXT")
        self.set_text_color(0, 0, 0)

    def footer(self) -> None:
        self.set_y(-12)
        self.set_font("Helvetica", "I", 8)
        self.set_text_color(150, 150, 150)
        self.cell(0, 8, f"Page {self.page_no()}", align="C")
        self.set_text_color(0, 0, 0)

    def section_title(self, text: str) -> None:
        self.set_font("Helvetica", "B", 11)
        self.set_fill_color(230, 240, 255)
        self.cell(0, 8, text, fill=True, new_x="LMARGIN", new_y="NEXT")
        self.ln(2)

    def body_text(self, text: str) -> None:
        self.set_font("Helvetica", "", 10)
        self.multi_cell(0, 6, text)
        self.ln(2)

    def add_dataframe(self, rows: List[dict]) -> None:
        if not rows:
            self.body_text("(empty result)")
            return

        cols = list(rows[0].keys())
        col_w = min(180 / len(cols), 50)

        self.set_font("Helvetica", "B", 8)
        self.set_fill_color(200, 220, 255)
        for col in cols:
            self.cell(col_w, 6, str(col)[:_MAX_CELL_CHARS], border=1, fill=True)
        self.ln()

        self.set_font("Helvetica", "", 8)
        display_rows = rows[:_MAX_ROWS_IN_PDF]
        for row in display_rows:
            for col in cols:
                self.cell(col_w, 6, str(row.get(col, ""))[:_MAX_CELL_CHARS], border=1)
            self.ln()

        if len(rows) > _MAX_ROWS_IN_PDF:
            self.set_font("Helvetica", "I", 8)
            self.cell(0, 6, f"... and {len(rows) - _MAX_ROWS_IN_PDF} more rows", new_x="LMARGIN", new_y="NEXT")

        self.ln(4)

    def add_chart(self, chart_base64: str) -> None:
        try:
            img_bytes = base64.b64decode(chart_base64)
            with tempfile.NamedTemporaryFile(suffix=".png", delete=False) as tmp:
                tmp.write(img_bytes)
                tmp_path = tmp.name
            self.image(tmp_path, x=15, w=180)
            import os
            os.remove(tmp_path)
        except Exception as exc:
            logger.warning("Could not add chart to PDF: %s", exc)
            self.body_text(f"[Chart unavailable: {exc}]")
        self.ln(4)


def generate_session_pdf(chat_history: List[ChatEntry]) -> bytes:
    """
    Convert the session's chat history into a PDF and return raw bytes.
    """
    pdf = _BIPdf()
    pdf.set_auto_page_break(auto=True, margin=15)
    pdf.add_page()

    # Cover block
    pdf.set_font("Helvetica", "B", 14)
    pdf.cell(0, 10, "BI Agent — Session Export", align="C", new_x="LMARGIN", new_y="NEXT")
    pdf.set_font("Helvetica", "", 9)
    pdf.cell(0, 6, f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}", align="C", new_x="LMARGIN", new_y="NEXT")
    pdf.ln(8)

    for idx, entry in enumerate(chat_history, start=1):
        pdf.section_title(f"Q{idx}: {entry.query}")

        if entry.sql_query:
            pdf.set_font("Courier", "", 8)
            pdf.set_fill_color(245, 245, 245)
            pdf.multi_cell(0, 5, entry.sql_query, fill=True)
            pdf.ln(3)

        if entry.error:
            pdf.body_text(f"Error: {entry.error}")

        elif entry.result_type == "multi_table" and entry.multi_table_data:
            for table in entry.multi_table_data:
                pdf.set_font("Helvetica", "B", 9)
                pdf.cell(0, 6, f"Table: {table.get('table_name','')}", new_x="LMARGIN", new_y="NEXT")
                if table.get("error"):
                    pdf.body_text(f"Error: {table['error']}")
                elif table.get("dataframe"):
                    rows = [
                        {col: vals[i] for col, vals in table["dataframe"].items()}
                        for i in range(len(next(iter(table["dataframe"].values()))))
                    ]
                    pdf.add_dataframe(rows)

        elif entry.dataframe:
            pdf.add_dataframe(entry.dataframe)

        if entry.chart_base64:
            pdf.add_chart(entry.chart_base64)

        pdf.ln(4)

    return bytes(pdf.output())
