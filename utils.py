def extract_sql_code(response):
    """
    Extracts SQL code from an AI response.
    The function looks for SQL queries typically wrapped in markdown code blocks
    or specific patterns indicating SQL code.
    
    Args:
        response: The response from the LLM containing SQL code
        
    Returns:
        str: The extracted SQL query
    """
    # If response is an object with content attribute (typical for LLM responses)
    if hasattr(response, 'content'):
        text = response.content
    else:
        text = str(response)
    
    # Remove SQLQuery: prefix if exists
    text = text.replace("SQLQuery:", "").strip()
    
    # Replace placeholder table name
    text = text.replace("your_table_name", "store_sales_data")
    
    # Common patterns to look for:
    # 1. SQL between markdown code blocks ```sql and ```
    # 2. SQL between backticks `SELECT...`
    # 3. Direct SQL starting with SELECT, WITH, etc.
    
    # Try to find SQL between markdown blocks first
    if '```sql' in text.lower():
        # Split by SQL code block marker and take the content
        sql = text.lower().split('```sql')[1].split('```')[0].strip()
        return sql.replace("your_table_name", "store_sales_data")
    
    # Look for SQL between backticks
    if '`' in text:
        # Get content between backticks that looks like SQL
        parts = text.split('`')
        for part in parts:
            if any(keyword in part.upper() for keyword in ['SELECT', 'WITH', 'UPDATE', 'DELETE', 'INSERT']):
                return part.strip().replace("your_table_name", "store_sales_data")
    
    # Direct SQL detection
    # Split into lines and look for SQL keywords
    lines = text.split('\n')
    for line in lines:
        if any(line.upper().strip().startswith(keyword) for keyword in ['SELECT', 'WITH', 'UPDATE', 'DELETE', 'INSERT']):
            return line.strip().replace("your_table_name", "store_sales_data")
            
    # If no SQL found
    return text.strip().replace("your_table_name", "store_sales_data") 

'''import re

def extract_sql_code(response):
    """
    Extracts SQL code from an AI response using pattern matching.
    
    Args:
        response (str or object): The response from the LLM containing SQL code.
        
    Returns:
        str: The extracted SQL query or an empty string if no SQL found.
    """
    # Ensure response is a string
    if hasattr(response, 'content'):
        text = response.content.strip()
    else:
        text = str(response).strip()
    
    # Remove SQLQuery: prefix
    text = text.replace("SQLQuery:", "").strip()
    
    # Replace placeholder table name
    text = text.replace("your_table_name", "store_sales_data")
    
    # **1. Extract SQL inside triple backticks ```sql ... ```**
    sql_match = re.search(r"```sql\s*([\s\S]+?)\s*```", text, re.IGNORECASE)
    if sql_match:
        return sql_match.group(1).strip()
    
    # **2. Extract SQL inside backticks ` ... `**
    sql_backtick_match = re.search(r"`([^`]+)`", text)
    if sql_backtick_match:
        return sql_backtick_match.group(1).strip()
    
    # **3. Direct SQL detection (starting with common SQL keywords)**
    sql_lines = []
    for line in text.split("\n"):
        stripped_line = line.strip()
        if re.match(r"^\s*(SELECT|WITH|INSERT|UPDATE|DELETE)\s+", stripped_line, re.IGNORECASE):
            sql_lines.append(stripped_line)
    
    if sql_lines:
        return " ".join(sql_lines)  # Combine SQL lines into a single query
    
    # If nothing matches, return the cleaned text as a fallback
    return text
'''

def tag_question_type(question: str, llm) -> str:
    """
    Uses LLM to classify the question as either 'chart' or 'table'.
    """
    prompt = f"""
    Classify the following user query as either 'chart' or 'table'. 
    If the user is asking for numerical trends, visualizations, or distributions, return 'chart'. 
    Otherwise, return 'table'.
    
    User query: "{question}"
    Output:
    """
    
    response = llm.invoke(prompt).content.strip().lower()
    
    # Ensure the output is valid
    if response not in ["chart", "table"]:
        response = "table"  # Default fallback
    
    return response



import re

def extract_python_code(response):
    """
    Extracts Python code from an AI-generated response.
    The function looks for Python code typically wrapped in markdown code blocks
    or specific patterns indicating Python code.

    Args:
        response (str or object): The response from the LLM containing Python code.

    Returns:
        str: The extracted Python script.
    """
    # If response is an object with a 'content' attribute (typical for LLM responses)
    if hasattr(response, 'content'):
        text = response.content
    else:
        text = str(response)

    # Remove any "Python Script:" prefix if it exists
    text = text.replace("Python Script:", "").strip()

    # Try to extract Python code from markdown-style code blocks
    match = re.search(r"```python\n(.*?)\n```", text, re.DOTALL)
    if match:
        return match.group(1).strip()  # Extract the Python code inside triple backticks

    # If no markdown blocks, look for direct Python function definitions
    python_keywords = ['def ', 'import ', 'plt.', 'sns.', 'px.', 'fig.']
    lines = text.split("\n")
    extracted_code = []
    
    for line in lines:
        if any(keyword in line for keyword in python_keywords):
            extracted_code.append(line)
    
    print(text.strip()) # If no valid Python code found, return raw text as fallback
    if extracted_code:
        return "\n".join(extracted_code).strip()
        
    return text.strip()

def split_sql_statements(sql_text):
    """
    Split multiple SQL statements from a single text block.
    
    Args:
        sql_text (str): Text containing one or more SQL statements
        
    Returns:
        list: List of individual SQL statements
    """
    if not sql_text:
        return []
    
    # Remove comments and split by semicolons
    lines = sql_text.split('\n')
    cleaned_lines = []
    
    for line in lines:
        # Remove comment lines (starting with --)
        if line.strip().startswith('--'):
            continue
        cleaned_lines.append(line)
    
    # Join back and split by semicolon
    cleaned_sql = '\n'.join(cleaned_lines)
    statements = [stmt.strip() for stmt in cleaned_sql.split(';') if stmt.strip()]
    
    return statements

def is_multi_statement_sql(sql_query):
    """
    Check if SQL query contains multiple statements.
    
    Args:
        sql_query (str): SQL query string
        
    Returns:
        bool: True if multiple statements detected
    """
    if not sql_query:
        return False
    
    statements = split_sql_statements(sql_query)
    return len(statements) > 1

def extract_table_name_from_sql(sql_statement):
    """
    Extract table name from a SQL statement for labeling purposes.
    
    Args:
        sql_statement (str): Single SQL statement
        
    Returns:
        str: Extracted table name or 'Unknown Table'
    """
    import re
    
    # Look for FROM clause
    from_match = re.search(r'FROM\s+(\w+)', sql_statement, re.IGNORECASE)
    if from_match:
        return from_match.group(1)
    
    # Look for table name in comments
    comment_match = re.search(r'--.*?(\w+)\s+table', sql_statement, re.IGNORECASE)
    if comment_match:
        return comment_match.group(1)
    
    return "Unknown Table"
