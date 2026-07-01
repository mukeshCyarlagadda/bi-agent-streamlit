"""
All prompt templates in one place.
Edit here — no need to touch node logic to tune LLM behaviour.
"""
from langchain_core.prompts import PromptTemplate

# ── SQL generation ────────────────────────────────────────────────────────────
# {input}, {table_info}, {top_k} are filled by LangChain's create_sql_query_chain.
# {retry_context} is injected by our node when retrying after a failed attempt.
SQL_GENERATION_TEMPLATE = """\
Rules:
- Use ONLY tables and columns that exist in the schema below.
- Preserve the exact case of all string values in WHERE clauses.
- When JOINing multiple tables, assign a short alias to each table and ALWAYS qualify
  every column reference with the correct alias for the table that actually owns that column.
  Wrong: SELECT a.AlbumId FROM Artist a JOIN Album al ...  (AlbumId is on Album, not Artist)
  Right: SELECT al.AlbumId FROM Artist a JOIN Album al ...
- For aggregations across JOINs, use COUNT(DISTINCT alias.PrimaryKey) to avoid row inflation.

User Query: {input}
Table Information: {table_info}
Retrieve at most {top_k} rows.

SQL Query:\
"""
# Note: retry context is prepended to the question string inside generate_sql(),
# not here — create_sql_query_chain only fills {input}/{table_info}/{top_k}.

sql_generation_prompt = PromptTemplate.from_template(SQL_GENERATION_TEMPLATE)


# ── Intent + visualisation classifier ────────────────────────────────────────
# Single LLM call that classifies BOTH dimensions to save a round-trip.
# Returns two lines: INTENT and TAG.
CLASSIFY_TEMPLATE = """\
You are classifying a user message for a Business Intelligence assistant.

Respond with exactly two lines — no other text:
INTENT: <data|chitchat>
TAG: <chart|table>

Rules:
- INTENT=data      → the user wants information from a database (query, count, list, trend)
- INTENT=chitchat  → greeting, thanks, general conversation, or anything not data-related
- TAG=chart        → user asks for a graph, plot, trend, distribution, or visualisation
- TAG=table        → everything else (TAG is ignored when INTENT=chitchat)

User message: "{question}"\
"""


# ── Chitchat response ─────────────────────────────────────────────────────────
CHITCHAT_TEMPLATE = """\
{system_instructions}

The user sent a message that is not a data question. Reply in 1-2 sentences.
Briefly acknowledge them and invite a data question about the connected database.

Recent conversation:
{history}

User: {question}
Assistant:\
"""


# ── Chart code generation (Plotly) ───────────────────────────────────────────
# The execution context pre-injects: df, px, go, pd, np, COLORS, THEME.
# The LLM only needs to write the logic and assign the result to `fig`.
CHART_GENERATION_TEMPLATE = """\
Generate a Plotly Python script to visualise the query result below.

Question: "{question}"
Column names: {columns}
Sample rows (first 5): {sample}

Available in scope (do NOT import or redefine):
  df      — pandas DataFrame with the full result
  px      — plotly.express
  go      — plotly.graph_objects
  pd, np  — pandas, numpy
  COLORS  — list of 8 modern accent colours (use for explicit colouring)
  THEME   — pre-built dark Plotly template name; pass as template=THEME

Rules:
1. Choose the best chart type for the question:
   - px.bar()        → compare quantities across categories
   - px.line()       → trends over time or ordered sequences
   - px.pie()        → proportional share (only when ≤ 8 slices)
   - px.scatter()    → relationship between two numeric columns; for bubble charts
                       add size=<col>, color=<col>, hover_name=<label_col>
   - px.histogram()  → distribution of a single numeric column
   - px.area()       → cumulative trends / stacked areas
   - go.Figure()     → only for custom/combined charts
2. Always pass template=THEME to px calls or fig.update_layout(template=THEME).
3. Always pass color_discrete_sequence=COLORS for categorical color (bar, line, scatter, pie).
   For single-series charts use color_discrete_sequence=[COLORS[0]].
   Never use default Plotly colors.
4. Set a clear, concise title. Label axes when using go directly.
5. For bar charts with many categories, sort by value descending.
6. For pie charts, add hole=0.35 for a modern donut style (px.pie supports this).
7. The LAST statement must assign the final figure to a variable named exactly `fig`.
   This is mandatory — the runtime reads `fig` from the local namespace after exec().
   Wrong: `chart = px.bar(...)` — Right: `fig = px.bar(...)`
   Do NOT call fig.show() or fig.write_html().
8. Do NOT wrap code in a function, class, or if-block.
9. Do NOT use any import statements — all libraries are already in scope.
10. For go.Figure() charts, set each trace's marker_color or line_color explicitly from COLORS.

Return ONLY the Python code — no explanations, no markdown fences, no backticks.\
"""
