# database_manager.py
import os
import sqlalchemy
from typing import Dict, Tuple, Optional, Any
from sqlalchemy import create_engine, inspect

class DatabaseManager:
    """
    A modular database connection manager supporting multiple database types.
    """
    
    def __init__(self):
        self.current_engine = None
        self.current_conn = None
        self.current_db_type = None
        self.current_db_uri = None
        self.inspector = None
    
    def connect(self, db_type: str, **connection_params) -> Tuple[bool, str]:
        """
        Connect to a database and return success status and message.
        
        Args:
            db_type: Type of database (sqlite, mysql, postgres, snowflake, bigquery, etc.)
            **connection_params: Parameters specific to the database type
            
        Returns:
            Tuple[bool, str]: (success, message)
        """
        try:
            # Close any existing connections
            self.close_connection()
            
            # Create connection URI based on database type
            db_uri = self._create_connection_uri(db_type, **connection_params)
            
            if not db_uri:
                return False, f"Unsupported database type: {db_type}"
            
            # Create SQLAlchemy engine
            self.current_engine = create_engine(db_uri)
            
            # Test connection
            self.current_conn = self.current_engine.connect()
            self.current_conn.execute(sqlalchemy.text("SELECT 1"))
            
            # Store current database info
            self.current_db_type = db_type
            self.current_db_uri = db_uri
            
            # Create inspector for schema introspection
            self.inspector = inspect(self.current_engine)
            
            return True, f"Connected successfully to {db_type} database"
            
        except Exception as e:
            self.close_connection()
            return False, f"Connection error: {str(e)}"
    
    def close_connection(self) -> None:
        """Close the current database connection if it exists."""
        if self.current_conn:
            self.current_conn.close()
            self.current_conn = None
        
        if self.current_engine:
            self.current_engine.dispose()
            self.current_engine = None
    
    def get_engine(self) -> Optional[sqlalchemy.engine.Engine]:
        """Return the current SQLAlchemy engine."""
        return self.current_engine
    
    def get_connection(self) -> Optional[sqlalchemy.engine.Connection]:
        """Return the current database connection."""
        return self.current_conn
    
    def get_connection_uri(self) -> Optional[str]:
        """Return the current connection URI."""
        return self.current_db_uri
    
    def get_tables(self) -> list:
        """Get list of tables in the current database."""
        if self.inspector:
            return self.inspector.get_table_names()
        return []
    
    def get_table_schema(self, table_name: str) -> list:
        """Get schema information for a specific table."""
        if self.inspector:
            return self.inspector.get_columns(table_name)
        return []
    
    def execute_query(self, query: str) -> Tuple[bool, Any]:
        """
        Execute a SQL query on the current connection.
        
        Args:
            query: SQL query to execute
            
        Returns:
            Tuple[bool, Any]: (success, result or error message)
        """
        if not self.current_conn:
            return False, "No active database connection"
        
        try:
            result = self.current_conn.execute(sqlalchemy.text(query))
            return True, result
        except Exception as e:
            return False, f"Query execution error: {str(e)}"

    def _create_connection_uri(self, db_type: str, **params) -> Optional[str]:
        """
        Create a database connection URI based on the database type and parameters.
        
        Args:
            db_type: Type of database
            **params: Connection parameters
            
        Returns:
            Optional[str]: Connection URI or None if database type is not supported
        """
        db_type = db_type.lower()
        
        if db_type == "sqlite":
            db_path = params.get("db_path", ":memory:")
            return f"sqlite:///{db_path}"
            
        elif db_type == "mysql":
            host = params.get("host", "localhost")
            port = params.get("port", 3306)
            user = params.get("user", "")
            password = params.get("password", "")
            database = params.get("database", "")
            return f"mysql+pymysql://{user}:{password}@{host}:{port}/{database}"
            
        elif db_type in ["postgres", "postgresql"]:
            host = params.get("host", "localhost")
            port = params.get("port", 5432)
            user = params.get("user", "")
            password = params.get("password", "")
            database = params.get("database", "")
            return f"postgresql://{user}:{password}@{host}:{port}/{database}"
            
        elif db_type == "mssql":
            host = params.get("host", "localhost")
            port = params.get("port", 1433)
            user = params.get("user", "")
            password = params.get("password", "")
            database = params.get("database", "")
            return f"mssql+pyodbc://{user}:{password}@{host}:{port}/{database}?driver=ODBC+Driver+17+for+SQL+Server"
            
        elif db_type == "oracle":
            host = params.get("host", "localhost")
            port = params.get("port", 1521)
            user = params.get("user", "")
            password = params.get("password", "")
            sid = params.get("sid", "")
            return f"oracle+cx_oracle://{user}:{password}@{host}:{port}/{sid}"
            
        elif db_type == "snowflake":
            account = params.get("account", "")
            user = params.get("user", "")
            password = params.get("password", "")
            database = params.get("database", "")
            schema = params.get("schema", "")
            warehouse = params.get("warehouse", "")
            return f"snowflake://{user}:{password}@{account}/{database}/{schema}?warehouse={warehouse}"
            
        elif db_type == "bigquery":
            project = params.get("project", "")
            dataset = params.get("dataset", "")
            credentials_path = params.get("credentials_path", "")
            os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = credentials_path
            return f"bigquery://{project}/{dataset}"
            
        elif db_type == "duckdb":
            db_path = params.get("db_path", ":memory:")
            return f"duckdb:///{db_path}"
            
        return None