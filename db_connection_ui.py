# db_connection_ui.py
import streamlit as st
import os
from database_manager import DatabaseManager

def initialize_connection_state():
    """Initialize session state variables for database connection."""
    if "db_manager" not in st.session_state:
        st.session_state["db_manager"] = DatabaseManager()
    if "is_connected" not in st.session_state:
        st.session_state["is_connected"] = False
    if "connection_error" not in st.session_state:
        st.session_state["connection_error"] = ""
    if "db_info" not in st.session_state:
        st.session_state["db_info"] = {}

def render_sqlite_form():
    """Render SQLite connection form."""
    st.subheader("SQLite Connection")
    
    db_path = st.text_input(
        "Database File Path", 
        value= r"C:\BI_database\dental_clinic.db",  # 
        help="Path to the SQLite database file"
    )
    
    return {
        "db_path": db_path
    }


def render_mysql_form():
    """Render MySQL connection form."""
    st.subheader("MySQL Connection")
    
    cols = st.columns(2)
    with cols[0]:
        host = st.text_input("Host", value="localhost")
        user = st.text_input("Username")
        database = st.text_input("Database Name")
    
    with cols[1]:
        port = st.number_input("Port", value=3306)
        password = st.text_input("Password", type="password")
    
    return {
        "host": host,
        "port": port,
        "user": user,
        "password": password,
        "database": database
    }

def render_postgres_form():
    """Render PostgreSQL connection form."""
    st.subheader("PostgreSQL Connection")
    
    cols = st.columns(2)
    with cols[0]:
        host = st.text_input("Host", value="localhost", key="pg_host")
        user = st.text_input("Username", key="pg_user")
        database = st.text_input("Database Name", key="pg_db")
    
    with cols[1]:
        port = st.number_input("Port", value=5432, key="pg_port")
        password = st.text_input("Password", type="password", key="pg_pwd")
    
    return {
        "host": host,
        "port": port,
        "user": user,
        "password": password,
        "database": database
    }

def render_mssql_form():
    """Render Microsoft SQL Server connection form."""
    st.subheader("Microsoft SQL Server Connection")
    
    cols = st.columns(2)
    with cols[0]:
        host = st.text_input("Server", key="mssql_host")
        user = st.text_input("Username", key="mssql_user")
        database = st.text_input("Database Name", key="mssql_db")
    
    with cols[1]:
        port = st.number_input("Port", value=1433, key="mssql_port")
        password = st.text_input("Password", type="password", key="mssql_pwd")
    
    return {
        "host": host,
        "port": port,
        "user": user,
        "password": password,
        "database": database
    }

def render_snowflake_form():
    """Render Snowflake connection form."""
    st.subheader("Snowflake Connection")
    
    cols = st.columns(2)
    with cols[0]:
        account = st.text_input("Account", placeholder="orgname-accountname", key="sf_account")
        user = st.text_input("Username", key="sf_user")
        database = st.text_input("Database", key="sf_db")
    
    with cols[1]:
        warehouse = st.text_input("Warehouse", key="sf_wh")
        password = st.text_input("Password", type="password", key="sf_pwd")
        schema = st.text_input("Schema (default: PUBLIC)", value="PUBLIC", key="sf_schema")
    
    return {
        "account": account,
        "user": user,
        "password": password,
        "database": database,
        "schema": schema,
        "warehouse": warehouse
    }

def render_bigquery_form():
    """Render BigQuery connection form."""
    st.subheader("Google BigQuery Connection")
    
    project = st.text_input("Project ID", key="bq_project")
    dataset = st.text_input("Dataset", key="bq_dataset")
    
    # Option to upload credentials file
    credentials_file = st.file_uploader("Service Account JSON Key", type=['json'], key="bq_creds")
    
    # If file is uploaded, save it temporarily
    credentials_path = None
    if credentials_file:
        temp_dir = os.path.join(os.getcwd(), "temp")
        os.makedirs(temp_dir, exist_ok=True)
        credentials_path = os.path.join(temp_dir, "bigquery_credentials.json")
        with open(credentials_path, "wb") as f:
            f.write(credentials_file.getvalue())
        st.success("Credentials file uploaded successfully!")
    
    return {
        "project": project,
        "dataset": dataset,
        "credentials_path": credentials_path
    }

def render_oracle_form():
    """Render Oracle connection form."""
    st.subheader("Oracle Connection")
    
    cols = st.columns(2)
    with cols[0]:
        host = st.text_input("Host", key="ora_host")
        user = st.text_input("Username", key="ora_user")
    
    with cols[1]:
        port = st.number_input("Port", value=1521, key="ora_port")
        password = st.text_input("Password", type="password", key="ora_pwd")
    
    sid = st.text_input("SID", key="ora_sid")
    
    return {
        "host": host,
        "port": port,
        "user": user,
        "password": password,
        "sid": sid
    }

def render_duckdb_form():
    """Render DuckDB connection form."""
    st.subheader("DuckDB Connection")
    
    db_path = st.text_input(
        "Database File Path", 
        value="database/analytics.duckdb",
        help="Path to the DuckDB database file",
        key="duck_path")
    
    return {
        "db_path": db_path
    }

def render_database_selector():
    """Render database connection UI in the sidebar."""
    initialize_connection_state()
    
    st.sidebar.markdown("<h2 class='sidebar-header'>Database Connection</h2>", unsafe_allow_html=True)
    
    # Database type selection
    db_type = st.sidebar.selectbox(
        "Select Database Type",
        ["SQLite", "PostgreSQL", "MySQL", "Microsoft SQL Server", "Snowflake", 
         "BigQuery", "Oracle", "DuckDB"],
        key="db_type"
    )
    
    # Connection parameters based on database type
    if db_type == "SQLite":
        params = render_sqlite_form()
        db_key = "sqlite"
    elif db_type == "PostgreSQL":
        params = render_postgres_form()
        db_key = "postgres"
    elif db_type == "MySQL":
        params = render_mysql_form()
        db_key = "mysql"
    elif db_type == "Microsoft SQL Server":
        params = render_mssql_form()
        db_key = "mssql"
    elif db_type == "Snowflake":
        params = render_snowflake_form()
        db_key = "snowflake"
    elif db_type == "BigQuery":
        params = render_bigquery_form()
        db_key = "bigquery"
    elif db_type == "Oracle":
        params = render_oracle_form()
        db_key = "oracle"
    elif db_type == "DuckDB":
        params = render_duckdb_form()
        db_key = "duckdb"
    
    # Connect button
    if st.sidebar.button("Connect to Database"):
        try:
            # Attempt connection
            success, message = st.session_state["db_manager"].connect(db_key, **params)
            
            if success:
                st.session_state["is_connected"] = True
                st.session_state["connection_error"] = ""
                st.session_state["db_info"] = {
                    "type": db_type,
                    "params": params,
                    "uri": st.session_state["db_manager"].get_connection_uri()
                }
                st.sidebar.success(f"✅ {message}")
                
                # Get tables for display
                tables = st.session_state["db_manager"].get_tables()
                if tables:
                    st.session_state["db_info"]["tables"] = tables
                
                return True, st.session_state["db_manager"].get_connection_uri()
            else:
                st.session_state["is_connected"] = False
                st.session_state["connection_error"] = message
                st.sidebar.error(f"❌ {message}")
        except Exception as e:
            st.session_state["is_connected"] = False
            st.session_state["connection_error"] = str(e)
            st.sidebar.error(f"❌ Connection Error: {e}")
    
    # Show connection status
    if st.session_state["is_connected"]:
        st.sidebar.markdown("---")
        st.sidebar.markdown("### Connection Info")
        st.sidebar.write(f"**Type:** {st.session_state['db_info']['type']}")
        
        if "tables" in st.session_state["db_info"]:
            tables = st.session_state["db_info"]["tables"]
            if tables:
                with st.sidebar.expander("Available Tables"):
                    st.write(tables)
    
    # If there's a connection error, display it
    if st.session_state["connection_error"]:
        st.sidebar.error(st.session_state["connection_error"])
    
    return False, None

def get_database_connection():
    """
    Get current database connection.
    Returns:
        Tuple: (engine, connection, connection_uri)
    """
    if "db_manager" in st.session_state and st.session_state["is_connected"]:
        return (
            st.session_state["db_manager"].get_engine(),
            st.session_state["db_manager"].get_connection(),
            st.session_state["db_manager"].get_connection_uri()
        )
    return None, None, None