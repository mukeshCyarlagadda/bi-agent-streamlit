# sql_agent_langgraph.py
import os
import logging
import numpy as np
import pandas as pd
import sqlalchemy as sql
import matplotlib.pyplot as plt
from typing import TypedDict
from dotenv import load_dotenv
from langchain_openai import ChatOpenAI
from langchain_core.prompts import PromptTemplate
from langchain_community.utilities import SQLDatabase
from langchain_classic.chains.sql_database.query import create_sql_query_chain
from langgraph.graph import StateGraph, END

from utils import (
    extract_sql_code,
    tag_question_type,
    extract_python_code,
    split_sql_statements,
    is_multi_statement_sql,
    extract_table_name_from_sql
)

load_dotenv()

logger = logging.getLogger(__name__)

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

    if is_multi_statement_sql(sql_query):
        logger.debug("Multi-statement SQL detected")
        statements = split_sql_statements(sql_query)
        multi_table_data = []
        for i, statement in enumerate(statements):
            try:
                df = pd.read_sql(statement, conn)
                table_name = extract_table_name_from_sql(statement)
                multi_table_data.append({
                    "table_name": table_name,
                    "dataframe": {col: df[col].tolist() for col in df.columns},
                    "sql_statement": statement
                })
            except Exception as e:
                logger.error("Error executing statement %d: %s", i + 1, e)
                multi_table_data.append({
                    "table_name": f"Error in Statement {i + 1}",
                    "error": str(e),
                    "sql_statement": statement
                })
        return {**state, "data": {"multi_table": True, "tables": multi_table_data}}
    else:
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
    question = state["question"]
    data = state.get("data")

    if data is None:
        return {**state, "chart_script": "Error: No DataFrame in state."}

    # Multi-table data cannot be charted as a single frame; flatten to first table
    if isinstance(data, dict) and data.get("multi_table"):
        tables = data.get("tables", [])
        if not tables or "dataframe" not in tables[0]:
            return {**state, "chart_script": "Error: No suitable data for chart."}
        data = tables[0]["dataframe"]

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

    response = llm.invoke(prompt)
    chart_script = extract_python_code(response)
    logger.debug("Generated chart script (%d chars)", len(chart_script))
    return {**state, "chart_script": chart_script}


def execute_chart_code(state: GraphState):
    """
    Executes the generated Python script to create a chart.
    """
    import time
    import uuid

    chart_script = state.get("chart_script", "")
    if not chart_script or chart_script.startswith("Error"):
        return {**state, "chart_output": "Error: No chart script available"}

    df_data = state.get("data", {})
    # Multi-table: use first table's dataframe dict
    if isinstance(df_data, dict) and df_data.get("multi_table"):
        tables = df_data.get("tables", [])
        df_data = tables[0]["dataframe"] if tables and "dataframe" in tables[0] else {}

    df = pd.DataFrame(df_data)
    plt.close("all")

    try:
        plt.figure(figsize=(12, 6))
        local_namespace = {"df": df, "plt": plt, "pd": pd, "np": np}
        exec(chart_script, local_namespace)  # noqa: S102 — LLM output, sandboxed namespace

        output_dir = "generated_charts"
        os.makedirs(output_dir, exist_ok=True)
        filename = f"chart_{int(time.time())}_{uuid.uuid4().hex[:6]}.png"
        output_path = os.path.join(output_dir, filename)

        plt.savefig(output_path, bbox_inches="tight", dpi=150, pad_inches=0.1, facecolor="white")
        logger.info("Chart saved to %s", output_path)
        return {**state, "chart_output": output_path}

    except Exception as e:
        logger.error("Chart execution failed: %s", e)
        return {**state, "chart_output": f"Error executing chart code: {e}"}

    finally:
        plt.close("all")
       
def state_printer(state: GraphState):
    """
    Final node that returns both DataFrame and chart if they exist.
    Handles both single and multi-table results.
    """
    outputs = {}
    
    if "data" in state and isinstance(state["data"], dict):
        try:
            if state["data"].get("multi_table", False):
                outputs["multi_table_data"] = state["data"]["tables"]
                logger.debug("Processing %d tables", len(state["data"]["tables"]))
            else:
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
    for key, value in state.items():
        if isinstance(value, pd.DataFrame):
            logger.debug("%s: DataFrame shape=%s cols=%s", key, value.shape, value.columns.tolist())
        else:
            logger.debug("%s (%s): %s", key, type(value).__name__, value)

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
    logger.info("Connecting to DB and building DAG")

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
        k=500,
        prompt=chain_prompt
    )

    # 3. Build the DAG
    app = build_dag(sql_generator, conn)
    return app






