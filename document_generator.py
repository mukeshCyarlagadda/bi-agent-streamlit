import pandas as pd
import streamlit as st
from PIL import Image
import os
from datetime import datetime
from fpdf import FPDF

class SessionDocumentGenerator:
    def __init__(self):
        self.pdf = FPDF()
        self.pdf.set_auto_page_break(auto=True, margin=15)
        
    def generate_session_summary(self, chat_history):
        """
        Generate a PDF summary of the chat session
        """
        self.pdf.add_page()
        self._add_header()
        
        for idx, chat in enumerate(chat_history, 1):
            self._add_chat_entry(idx, chat)
            
        # Return the PDF as bytes
        return self.pdf.output(dest='S').encode('latin-1')
    
    def _add_header(self):
        """Add header to the PDF"""
        self.pdf.set_font('Arial', 'B', 16)
        self.pdf.cell(0, 10, 'Business Intelligence Session Summary', 0, 1, 'C')
        self.pdf.cell(0, 10, f'Generated on: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}', 0, 1, 'C')
        self.pdf.ln(10)
    
    def _add_chat_entry(self, idx, chat):
        """Add a single chat entry to the PDF"""
        # Add query
        self.pdf.set_font('Arial', 'B', 12)
        self.pdf.cell(0, 10, f'Query {idx}: {chat["query"]}', 0, 1)
        self.pdf.ln(5)
        
        result = chat["result"]
        
        # Handle different types of results
        if isinstance(result, dict):
            self._add_dict_result(result)
        elif isinstance(result, pd.DataFrame):
            self._add_dataframe(result)
        elif isinstance(result, str) and result.endswith('.png'):
            self._add_chart(result)
        elif isinstance(result, str):
            self._add_text_response(result)
            
        self.pdf.ln(10)
    
    def _add_dict_result(self, result):
        """Handle dictionary results containing multiple types of data"""
        if "dataframe" in result:
            self._add_dataframe(result["dataframe"])
        
        if "chart" in result and isinstance(result["chart"], str) and result["chart"].endswith('.png'):
            self._add_chart(result["chart"])
    
    def _add_dataframe(self, df):
        """Add DataFrame to PDF"""
        self.pdf.set_font('Arial', 'B', 10)
        
        # Add column headers
        col_width = 180 / len(df.columns)  # Distribute columns evenly
        for col in df.columns:
            self.pdf.cell(col_width, 10, str(col)[:20], 1)
        self.pdf.ln()
        
        # Add rows
        self.pdf.set_font('Arial', '', 10)
        for _, row in df.head(50).iterrows():  # Limit to first 50 rows
            for item in row:
                self.pdf.cell(col_width, 10, str(item)[:20], 1)
            self.pdf.ln()
        
        if len(df) > 50:
            self.pdf.cell(0, 10, f"... and {len(df) - 50} more rows", 0, 1, 'C')
        self.pdf.ln(5)
    
    def _add_chart(self, chart_path):
        """Add chart image to PDF"""
        if os.path.exists(chart_path):
            try:
                # Set a reasonable image width while maintaining aspect ratio
                img_width = 180  # Maximum width in mm
                self.pdf.image(chart_path, x=15, w=img_width)
                self.pdf.ln(5)
            except Exception as e:
                self.pdf.set_text_color(255, 0, 0)
                self.pdf.cell(0, 10, f'Error adding chart: {str(e)}', 0, 1)
                self.pdf.set_text_color(0, 0, 0)
    
    def _add_text_response(self, text):
        """Add text response to PDF"""
        self.pdf.set_font('Arial', '', 11)
        self.pdf.multi_cell(0, 10, str(text))

def create_download_button(chat_history):
    """
    Create a download button for the session summary
    """
    try:
        # Generate PDF
        generator = SessionDocumentGenerator()
        pdf_data = generator.generate_session_summary(chat_history)
        
        # Create download button
        st.download_button(
            label="📥 Download Session Summary",
            data=pdf_data,
            file_name=f"bi_session_summary_{datetime.now().strftime('%Y%m%d_%H%M%S')}.pdf",
            mime="application/pdf"
        )
        
    except Exception as e:
        st.error(f"Error generating PDF: {str(e)}")