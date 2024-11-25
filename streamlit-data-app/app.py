import os
from databricks import sql
from databricks.sdk.core import Config
import streamlit as st
import pandas as pd

# Ensure environment variable is set correctly
assert os.getenv('DATABRICKS_WAREHOUSE_ID'), "DATABRICKS_WAREHOUSE_ID must be set in app.yaml."

def sql_query(query: str) -> pd.DataFrame:
    cfg = Config() # Pull environment variables for auth
    with sql.connect(
        server_hostname=cfg.host,
        http_path=f"/sql/1.0/warehouses/{os.getenv('DATABRICKS_WAREHOUSE_ID')}",
        credentials_provider=lambda: cfg.authenticate
    ) as connection:
        with connection.cursor() as cursor:
            cursor.execute(query)
            return cursor.fetchall_arrow().to_pandas()

# Cache results for performance
@st.cache_data(ttl=60)
def get_catalogs():
    return sql_query("SHOW CATALOGS") #catalog

@st.cache_data(ttl=60)
def get_schemas(catalog: str):
    return sql_query(f"SHOW SCHEMAS IN {catalog}") #databaseName 

@st.cache_data(ttl=60)
def get_tables(catalog: str, schema: str):
    return sql_query(f"SHOW TABLES IN {catalog}.{schema}") #database #tableName #isTemporary

@st.cache_data(ttl=60)
def get_table_data(catalog: str, schema: str, table: str, limit: int = 10):
    return sql_query(f"SELECT * FROM {catalog}.{schema}.{table} LIMIT {limit}")

# Streamlit Page Configuration
st.set_page_config(
    page_title="Databricks Metadata Explorer",
    page_icon=":bar_chart:",
    layout="wide",
)

# --- Custom CSS Styling ---
st.markdown("""
    <style>
        /* Header styling */
        h1, h2, h3 {
            color: #FF5733;
            font-family: 'Arial Black', sans-serif;
        }
        .css-18e3th9 {
            padding-top: 1rem;
        }
        /* Sidebar background */
        [data-testid="stSidebar"] {
            background-color: #79b1db;
        }
        /* Highlighted table rows */
        tbody tr:hover {
            background-color: #f5f5f5;
        }
        /* Align DataFrame */
        .dataframe {
            text-align: left;
        }
    </style>
""", unsafe_allow_html=True)

# App Title
st.title("🚀 **Databricks Metadata Explorer**")
st.caption("Browse catalogs, schemas, and tables interactively with Databricks SQL.")

# Sidebar
st.sidebar.header("🔍 Navigation Panel")
st.sidebar.markdown("Use the selectors below to explore the metadata.")

# Step 1: Select Catalog
catalogs = get_catalogs()
catalog_list = catalogs['catalog'].tolist()
selected_catalog = st.sidebar.selectbox("Select a Catalog", catalog_list)

# Step 2: Select Schema
if selected_catalog:
    schemas = get_schemas(selected_catalog)
    schema_list = schemas['databaseName'].tolist()
    selected_schema = st.sidebar.selectbox("Select a Schema", schema_list)

# Step 3: Select Table
if selected_catalog and selected_schema:
    tables = get_tables(selected_catalog, selected_schema)
    table_list = tables['tableName'].tolist()
    selected_table = st.sidebar.selectbox("Select a Table", table_list)

# Step 4: Display Data
st.divider()
if selected_catalog and selected_schema and selected_table:
    st.subheader(f"📊 Data Preview: `{selected_catalog}.{selected_schema}.{selected_table}`")
    data = get_table_data(selected_catalog, selected_schema, selected_table)
    st.dataframe(data, height=400, use_container_width=True)

# Additional Metadata Display
col1, col2 = st.columns(2)

if st.sidebar.checkbox("Show Schemas Metadata"):
    with col1:
        st.subheader(f"📂 Schemas in `{selected_catalog}`")
        st.dataframe(schemas, height=300, use_container_width=True)

if st.sidebar.checkbox("Show Tables Metadata"):
    with col2:
        st.subheader(f"📑 Tables in `{selected_catalog}.{selected_schema}`")
        st.dataframe(tables, height=300, use_container_width=True)

# Footer
st.divider()
st.markdown(
    """
    <footer style="text-align: center; font-size: 0.9rem; color: #888;">
        DataTribe Collective | <a href="https://www.dataribe.fi">datatribe.fi</a>
    </footer>
    """, unsafe_allow_html=True
)
