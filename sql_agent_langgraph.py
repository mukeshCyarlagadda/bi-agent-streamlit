# sql_agent_langgraph.py
# At the very top of sql_agent_langgraph.py
import os
from dotenv import load_dotenv
load_dotenv()  # <-- This will load the .env file with your OPENAI_API_KEY

print("Working directory:", os.getcwd())
print("OPENAI_API_KEY:", os.getenv("OPENAI_API_KEY"))
print("🔑 Using API Key:", os.getenv("OPENAI_API_KEY"))

import os
import pandas as pd
import sqlalchemy as sql
import matplotlib.pyplot as plt
import plotly.express as px
from typing import TypedDict
from PIL import Image
from langchain_openai import ChatOpenAI
from langchain_core.prompts import PromptTemplate
from langchain_community.utilities import SQLDatabase
from langchain.chains import create_sql_query_chain
from langgraph.graph import StateGraph, END

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt


from sql_agent.utils import (
    extract_sql_code,
    tag_question_type,
    extract_python_code,
    split_sql_statements,
    is_multi_statement_sql,
    extract_table_name_from_sql
)

# ------------------------------
# 1. Define the State structure
# ------------------------------
class GraphState(TypedDict):
    question: str
    sql_query: str
    data: dict
    tag: str  # "chart" or "table"
    chart_script: str
    chart_output: object
    final_output: object

# ------------------------------
# 2. Node Functions
# ------------------------------
def process_question(state: GraphState, llm) -> GraphState:
    """
    Determines whether the question is for a chart or a table.
    """
    question = state["question"]
    tag = tag_question_type(question, llm)
    return {**state, "tag": tag}

def generate_sql(state: GraphState, sql_generator):
    """
    Uses the sql_generator chain to produce an SQL query.
    """
    question = state["question"]
    raw_sql = sql_generator.invoke({"question": question})
    sql_query = extract_sql_code(raw_sql)
    return {**state, "sql_query": sql_query}

def convert_dataframe(state: GraphState, conn):
    """
    Executes the SQL query and returns a DataFrame.
    Handles both single and multi-statement SQL queries.
    """
    sql_query = state["sql_query"]
    
    # Check if this is a multi-statement SQL
    if is_multi_statement_sql(sql_query):
        print("DEBUG: Multi-statement SQL detected")
        statements = split_sql_statements(sql_query)
        
        # Execute each statement and collect results
        multi_table_data = []
        for i, statement in enumerate(statements):
            try:
                print(f"DEBUG: Executing statement {i+1}: {statement[:50]}...")
                df = pd.read_sql(statement, conn)
                table_name = extract_table_name_from_sql(statement)
                
                multi_table_data.append({
                    "table_name": table_name,
                    "dataframe": {col: df[col].tolist() for col in df.columns},
                    "sql_statement": statement
                })
            except Exception as e:
                print(f"DEBUG: Error executing statement {i+1}: {e}")
                multi_table_data.append({
                    "table_name": f"Error in Statement {i+1}",
                    "error": str(e),
                    "sql_statement": statement
                })
        
        return {**state, "data": {"multi_table": True, "tables": multi_table_data}}
    
    else:
        # Single statement - existing logic
        df = pd.read_sql(sql_query, conn)
        return {**state, "data": {col: df[col].tolist() for col in df.columns}}

def conditional_router(state: GraphState):
    """
    Reads state["tag"] and returns state updates with routing decision
    """
    tag = state.get("tag", "table")
    return {**state, "next": tag}


def generate_chart_instructions(state: GraphState, llm):
    """
    Calls an LLM to generate Python script for visualizing data.
    """
    print("\nDEBUG: Entering generate_chart_instructions")
    question = state["question"]
    data = state.get("data")

    if data is None:
        print("DEBUG: No data found in state")
        return {**state, "chart_script": "Error: No DataFrame in state."}

    # Prompt to determine chart type and generate appropriate visualization
    prompt = f"""
    Analyze the following question and data visualization requirements:
    Question: "{question}"
    Data Columns: {list(data.keys())}

    Determine the most appropriate chart type from the "{question}"
    - Bar Chart: Compare quantities across different categories
    - Pie Chart: Show proportional distribution of a whole
    - Line Chart: Display trends over time
    - Histogram: Show distribution of numerical data
    - Scatter Plot: Explore relationship between two variables
    -desnisty line graph: line graph to show the how the distribution is spread.

    Generate a Python visualization script using matplotlib that:
    1. Chooses the most suitable chart type based on the question
    2. Uses clear, descriptive labels
    3. Adds a meaningful title
    4. Ensures readability
    5. Highlights key insights from the data

    Provide ONLY the pure Python code for visualization.
    Directly use the DataFrame {data} for plotting.
    Do NOT wrap the code in a function.
    """

    # Generate chart code using LLM
    response = llm.invoke(prompt)
    
    # Extract Python code using utility function
    chart_script = extract_python_code(response)
    
    print("DEBUG: Generated Chart Script")
    print(f"DEBUG: Chart script length: {len(chart_script)}")
    print(f"DEBUG: Chart script:\n{chart_script}")

    return {**state, "chart_script": chart_script}


def execute_chart_code(state: GraphState):
    """
    Executes the generated Python script to create a chart.
    """
    print("\nDEBUG: Entering execute_chart_code")
    chart_script = state.get("chart_script", "")
    
    if not chart_script or chart_script.startswith("Error"):
        print("DEBUG: No valid chart script found")
        return {**state, "chart_output": "Error: No chart script available"}

    df_data = state.get("data", {})
    
    # Convert dict back to DataFrame
    df = pd.DataFrame(df_data)
    print(f"DEBUG: DataFrame shape: {df.shape}")
    
    # Clear any existing plots
    plt.close('all')
    
    try:
        print("DEBUG: Executing chart generation code")
        
        # Create a new figure
        plt.figure(figsize=(12, 6))
        
        # Create a local namespace with the DataFrame
        local_namespace = {
            'df': df,
            'plt': plt,
            'pd': pd,
            'np': np
        }
        
        # Execute the chart generation code
        exec(chart_script, local_namespace)
        
        # Ensure the output directory exists
        output_dir = 'generated_charts'
        os.makedirs(output_dir, exist_ok=True)
        
        # Generate unique filename using timestamp and random string
        import time
        import random
        import string
        
        timestamp = int(time.time())
        random_str = ''.join(random.choices(string.ascii_lowercase + string.digits, k=6))
        filename = f'chart_{timestamp}_{random_str}.png'
        output_path = os.path.join(output_dir, filename)
        
        # Save the figure
        print(f"DEBUG: Saving chart to {output_path}")
        plt.savefig(output_path, 
                   bbox_inches='tight', 
                   dpi=300, 
                   pad_inches=0.1,
                   facecolor='white')
        
        plt.close('all')
        
        print(f"DEBUG: Chart saved successfully to {output_path}")
        
        return {**state, "chart_output": output_path}
        
    except Exception as e:
        print(f"DEBUG: Error in execute_chart_code: {str(e)}")
        import traceback
        traceback.print_exc()
        plt.close('all')
        return {**state, "chart_output": f"Error executing chart code: {e}"}
        
    finally:
        plt.close('all')
       
def state_printer(state: GraphState):
    """
    Final node that returns both DataFrame and chart if they exist.
    Handles both single and multi-table results.
    """
    outputs = {}
    
    # Check for DataFrame data
    if "data" in state and isinstance(state["data"], dict):
        try:
            # Check if this is multi-table data
            if state["data"].get("multi_table", False):
                outputs["multi_table_data"] = state["data"]["tables"]
                print(f"DEBUG: Processing {len(state['data']['tables'])} tables")
            else:
                # Single table - existing logic
                outputs["dataframe"] = pd.DataFrame(state["data"])
        except Exception as e:
            outputs["error"] = f"Error creating DataFrame: {str(e)}"
    
    # Check for chart independently
    if "chart_output" in state:
        outputs["chart"] = state["chart_output"]
    
    if not outputs:
        outputs["message"] = "No valid output found."
        
    # Return the complete state with all outputs
    return {**state, "final_output": outputs} 

def debug_state(state: GraphState):
    """
    Helper function to print the current state for debugging.
    """
    print("\nDEBUG State Contents:")
    for key, value in state.items():
        print(f"{key}: {type(value)}")
        if isinstance(value, dict):
            print(f"  {value}")
        elif isinstance(value, pd.DataFrame):
            print(f"  Shape: {value.shape}")
            print(f"  Columns: {value.columns.tolist()}")
        else:
            print(f"  {value}")
    print("-" * 50)

# ------------------------------
# 3. Build & Compile the DAG
# ------------------------------
def build_dag(sql_generator, conn):
    """
    Constructs the DAG using the node functions above,
    passing in references to sql_generator & conn where needed.
    """
    workflow = StateGraph(GraphState)

    llm_for_questions = ChatOpenAI(model="gpt-4o-mini")

    workflow.add_node("process_question", 
        lambda s: process_question(s, llm_for_questions))

    workflow.add_node("generate_sql",
        lambda s: generate_sql(s, sql_generator))

    workflow.add_node("convert_dataframe",
        lambda s: convert_dataframe(s, conn))

    workflow.add_node("conditional_router", conditional_router)

    workflow.add_node("generate_chart_instructions",
        lambda s: generate_chart_instructions(s, llm_for_questions))

    workflow.add_node("execute_chart_code", execute_chart_code)
    workflow.add_node("state_printer", state_printer)

    # DAG Edges
    workflow.set_entry_point("process_question")
    workflow.add_edge("process_question", "generate_sql")
    workflow.add_edge("generate_sql", "convert_dataframe")
    workflow.add_edge("convert_dataframe", "conditional_router")

    # Modified conditional edges to use the 'next' key from router
    workflow.add_conditional_edges(
        "conditional_router",
        lambda x: x["next"],  # Use the 'next' key from router's output
        {
            "table": "state_printer",
            "chart": "generate_chart_instructions"
        }
    )

    workflow.add_edge("generate_chart_instructions", "execute_chart_code")
    workflow.add_edge("execute_chart_code", "state_printer")
    workflow.add_edge("state_printer", END)
   
    return workflow.compile()

# ------------------------------
# 4. initialize_dag(db_uri)
# ------------------------------
def initialize_dag(db_uri: str):
    """
    Connects to DB, sets up sql_generator, and compiles the DAG. 
    Returns the compiled DAG (app).
    """
    print("✅ initialize_dag: Connecting to DB & building DAG")

    # 1. Create DB engine & conn
    sql_engine = sql.create_engine(db_uri)
    conn = sql_engine.connect()

    # 2. Create the SQLDatabase & sql_generator
    db = SQLDatabase.from_uri(db_uri)
    template = """
    when you consume any contents of prompts to use in the WHERE clause, use them as is because they are case sensitive.
    Do not change the capitalization of any words used in conditions.

    User Query: {input}
    Table Information: {table_info}
    Retrieve Top {top_k} results.

    SQL Query:
    """
    chain_prompt = PromptTemplate.from_template(template)

    llm_for_sql = ChatOpenAI(
        model="gpt-4o-mini"
    )

    sql_generator = create_sql_query_chain(
        llm=llm_for_sql,
        db=db,
        k=int(1e7),
        prompt=chain_prompt
    )

    # 3. Build the DAG
    app = build_dag(sql_generator, conn)
    return app


'''if __name__ == "__main__":
    # 1. Provide a db_uri
    db_uri =db_uri = "sqlite:///C:\\BI_database\\dental_clinic.db"
  # or your actual path

    # 2. Initialize the DAG
    app = initialize_dag(db_uri)

    # 3. Test a question
    QUESTION = """
      give the details of all patients 
    """
    inputs = {"question": QUESTION}

    print("=== MANUAL TEST: DAG Invoking with question ===")
    # 4. Invoke the DAG and collect states
    results = []
    for step_output in app.stream(inputs):
        print(step_output)
        results.append(step_output)
    
    # 5. After execution, check if chart was generated
    final_output = results[-1].get('state_printer', {}).get('final_output')
    if isinstance(final_output, str) and final_output.endswith('.png'):
        print(f"\nChart has been saved to: {final_output}")"""
       
'''




