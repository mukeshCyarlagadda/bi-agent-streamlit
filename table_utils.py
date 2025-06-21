import streamlit as st
import pandas as pd
import io

def create_individual_download_button(dataframe, table_name, file_format="CSV"):
    """
    Create download button for individual table.
    
    Args:
        dataframe (pd.DataFrame): The dataframe to download
        table_name (str): Name of the table for filename
        file_format (str): Format for download (CSV, Excel)
    """
    if dataframe.empty:
        st.warning(f"No data available for {table_name}")
        return
    
    if file_format.upper() == "CSV":
        # Convert DataFrame to CSV
        csv_buffer = io.StringIO()
        dataframe.to_csv(csv_buffer, index=False)
        csv_data = csv_buffer.getvalue()
        
        st.download_button(
            label="CSV ↓",
            data=csv_data,
            file_name=f"{table_name}_data.csv",
            mime="text/csv",
            key=f"download_{table_name}_csv",
            help=f"Download {table_name} as CSV file"
        )
    
    elif file_format.upper() == "EXCEL":
        try:
            # Check if openpyxl is available
            import openpyxl
            
            # Convert DataFrame to Excel
            excel_buffer = io.BytesIO()
            with pd.ExcelWriter(excel_buffer, engine='openpyxl') as writer:
                dataframe.to_excel(writer, sheet_name=table_name[:31], index=False)  # Excel sheet name limit
            excel_data = excel_buffer.getvalue()
            
            st.download_button(
                label="Excel ↓",
                data=excel_data,
                file_name=f"{table_name}_data.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                key=f"download_{table_name}_excel",
                help=f"Download {table_name} as Excel file"
            )
        except ImportError:
            # Fallback to CSV if openpyxl is not available
            st.info(f"Excel format not available. Install 'openpyxl' for Excel downloads.")
            csv_buffer = io.StringIO()
            dataframe.to_csv(csv_buffer, index=False)
            csv_data = csv_buffer.getvalue()
            
            st.download_button(
                label="CSV ↓",
                data=csv_data,
                file_name=f"{table_name}_data.csv",
                mime="text/csv",
                key=f"download_{table_name}_excel_fallback",
                help=f"Download {table_name} as CSV file (Excel unavailable)"
            )

def display_multi_table_results(multi_table_data, show_sql=False):
    """
    Display multiple tables with individual download buttons.
    
    Args:
        multi_table_data (list): List of table data dictionaries
        show_sql (bool): Whether to show SQL queries
    """
    st.subheader(f"📊 Query Results ({len(multi_table_data)} tables)")
    
    for i, table_data in enumerate(multi_table_data):
        table_name = table_data.get("table_name", f"Table {i+1}")
        
        # Create expandable section for each table
        with st.expander(f"🔍 {table_name}", expanded=True):
            
            # Show SQL query if requested
            if show_sql and "sql_statement" in table_data:
                st.code(table_data["sql_statement"], language="sql")
            
            # Check for errors
            if "error" in table_data:
                st.error(f"Error executing query: {table_data['error']}")
                if "sql_statement" in table_data:
                    st.code(table_data["sql_statement"], language="sql")
                continue
            
            # Display dataframe
            if "dataframe" in table_data:
                try:
                    df = pd.DataFrame(table_data["dataframe"])
                    if not df.empty:
                        st.dataframe(df, use_container_width=True)
                        
                        # Add compact download buttons at bottom right
                        try:
                            import openpyxl
                            # Both CSV and Excel available
                            col1, col2, col3 = st.columns([3, 1, 1])
                            with col1:
                                pass  # Empty space to push buttons to the right
                            with col2:
                                create_individual_download_button(df, table_name, "CSV")
                            with col3:
                                create_individual_download_button(df, table_name, "EXCEL")
                        except ImportError:
                            # Only CSV available
                            col1, col2 = st.columns([4, 1])
                            with col1:
                                pass  # Empty space to push button to the right
                            with col2:
                                create_individual_download_button(df, table_name, "CSV")
                            st.info("💡 Install 'openpyxl' to enable Excel downloads: `pip install openpyxl`")
                    else:
                        st.info(f"No data returned for {table_name}")
                except Exception as e:
                    st.error(f"Error displaying table {table_name}: {str(e)}")