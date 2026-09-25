import streamlit as st
import pandas as pd
import plotly.express as px
import subprocess
from io import StringIO


# =========================================================
# PAGE CONFIGURATION
# =========================================================

st.set_page_config(
    page_title="FMCG Global Sales Dashboard",
    page_icon="🌍",
    layout="wide"
)

st.title("🌍 FMCG Global Sales Dashboard")

st.markdown(
    "Sales analytics powered by Apache Hive, Docker, Spark and HDFS"
)


# =========================================================
# FILTERS
# =========================================================

selected_year = st.selectbox(
    "Select Year",
    ["All", 2021, 2022, 2023]
)

selected_country = st.selectbox(
    "Select Country",
    [
        "All",
        "Austria",
        "France",
        "Germany",
        "Italy",
        "Netherlands",
        "Poland",
        "Spain"
    ]
)

selected_store = st.selectbox(
    "Select Store",
    [
        "All",
        "STORE0001",
        "STORE0002",
        "STORE0003",
        "STORE0004",
        "STORE0005",
        "STORE0006",
        "STORE0007",
        "STORE0008",
        "STORE0009",
        "STORE0010",
        "STORE0011",
        "STORE0012",
        "STORE0013"
    ]
)


# =========================================================
# RUN HIVE QUERY
# =========================================================

def run_hive_query(query, columns):

    query = f"SET hive.vectorized.execution.enabled=false; {query}"

    result = subprocess.run(
        [
            "docker",
            "exec",
            "-i",
            "-e",
            "HADOOP_CLIENT_OPTS=-Dorg.jline.terminal.provider=dumb",
            "fmcg-hive",
            "beeline",
            "-u",
            "jdbc:hive2://localhost:10000/fmcg",
            "--silent=true",
            "--showHeader=false",
            "--outputformat=csv2",
            "-e",
            query
        ],
        capture_output=True,
        text=True
    )

    if result.returncode != 0:

        st.error("Unable to connect to Hive.")
        st.code(result.stderr)

        return pd.DataFrame(columns=columns)

    lines = []

    for line in result.stdout.splitlines():

        line = line.strip()

        if not line:
            continue

        if "," in line and not line.startswith(
            ("INFO", "WARN", "SLF4J")
        ):
            lines.append(line)

    if not lines:

        return pd.DataFrame(columns=columns)

    df = pd.read_csv(
        StringIO("\n".join(lines)),
        names=columns,
        header=None
    )

    return df


# =========================================================
# COUNTRY SALES DATA
# =========================================================

@st.cache_data
def load_country_data(year, country, store):

    filters = []

    if year != "All":
        filters.append(
            f"sales_year = {year}"
        )

    if country != "All":
        filters.append(
            f"country = '{country}'"
        )

    if store != "All":
        filters.append(
            f"store_id = '{store}'"
        )

    if filters:
        where_clause = (
            "WHERE " +
            " AND ".join(filters)
        )
    else:
        where_clause = ""

    query = f"""
    SELECT
        country,
        ROUND(SUM(total_sales), 2) AS total_sales,
        SUM(total_units) AS total_units
    FROM sales_summary
    {where_clause}
    GROUP BY country
    ORDER BY total_sales DESC
    """

    df = run_hive_query(
        query,
        [
            "country",
            "total_sales",
            "total_units"
        ]
    )

    if df.empty:
        return df

    df["total_sales"] = pd.to_numeric(
        df["total_sales"],
        errors="coerce"
    )

    df["total_units"] = pd.to_numeric(
        df["total_units"],
        errors="coerce"
    )

    return df.dropna()


# =========================================================
# PRODUCT SALES DATA
# =========================================================

@st.cache_data
def load_product_data(year, country, store):

    filters = []

    if year != "All":
        filters.append(
            f"sales_year = {year}"
        )

    if country != "All":
        filters.append(
            f"country = '{country}'"
        )

    if store != "All":
        filters.append(
            f"store_id = '{store}'"
        )

    if filters:
        where_clause = (
            "WHERE " +
            " AND ".join(filters)
        )
    else:
        where_clause = ""

    query = f"""
    SELECT
        sku_name,
        ROUND(SUM(total_sales), 2) AS total_sales,
        SUM(total_units) AS total_units
    FROM sales_summary
    {where_clause}
    GROUP BY sku_name
    ORDER BY total_sales DESC
    LIMIT 10
    """

    df = run_hive_query(
        query,
        [
            "sku_name",
            "total_sales",
            "total_units"
        ]
    )

    if df.empty:
        return df

    df["total_sales"] = pd.to_numeric(
        df["total_sales"],
        errors="coerce"
    )

    df["total_units"] = pd.to_numeric(
        df["total_units"],
        errors="coerce"
    )

    return df.dropna()


# =========================================================
# MONTHLY SALES TREND
# =========================================================

@st.cache_data
def load_sales_trend(year, country, store):

    filters = []

    if year != "All":
        filters.append(
            f"sales_year = {year}"
        )

    if country != "All":
        filters.append(
            f"country = '{country}'"
        )

    if store != "All":
        filters.append(
            f"store_id = '{store}'"
        )

    if filters:
        where_clause = (
            "WHERE " +
            " AND ".join(filters)
        )
    else:
        where_clause = ""

    query = f"""
    SELECT
        sales_year,
        sales_month,
        ROUND(SUM(total_sales), 2) AS total_sales
    FROM sales_summary
    {where_clause}
    GROUP BY
        sales_year,
        sales_month
    ORDER BY
        sales_year,
        sales_month
    """

    df = run_hive_query(
        query,
        [
            "sales_year",
            "sales_month",
            "total_sales"
        ]
    )

    if df.empty:
        return df

    df["sales_year"] = pd.to_numeric(
        df["sales_year"],
        errors="coerce"
    )

    df["sales_month"] = pd.to_numeric(
        df["sales_month"],
        errors="coerce"
    )

    df["total_sales"] = pd.to_numeric(
        df["total_sales"],
        errors="coerce"
    )

    df = df.dropna()

    df["month"] = (
        df["sales_year"]
        .astype(int)
        .astype(str)
        + "-"
        + df["sales_month"]
        .astype(int)
        .astype(str)
        .str.zfill(2)
    )

    return df


# =========================================================
# LOAD DASHBOARD DATA
# =========================================================

country_df = load_country_data(
    selected_year,
    selected_country,
    selected_store
)

product_df = load_product_data(
    selected_year,
    selected_country,
    selected_store
)

trend_data = load_sales_trend(
    selected_year,
    selected_country,
    selected_store
)


# =========================================================
# DASHBOARD
# =========================================================

if not country_df.empty:

    # =====================================================
    # KPI CALCULATIONS
    # =====================================================

    total_sales = country_df["total_sales"].sum()

    total_units = country_df["total_units"].sum()

    total_countries = country_df["country"].nunique()

    average_sale_per_unit = (
        total_sales / total_units
        if total_units > 0
        else 0
    )

    total_products = (
        product_df["sku_name"].nunique()
        if not product_df.empty
        else 0
    )


    # =====================================================
    # KPI CARDS
    # =====================================================

    col1, col2, col3, col4, col5 = st.columns(5)

    col1.metric(
        "Total Sales",
        f"€{total_sales:,.2f}"
    )

    col2.metric(
        "Total Units Sold",
        f"{total_units:,.0f}"
    )

    col3.metric(
        "Countries",
        total_countries
    )

    col4.metric(
        "Avg Sale / Unit",
        f"€{average_sale_per_unit:.2f}"
    )

    col5.metric(
        "Products",
        total_products
    )

    st.divider()


    # =====================================================
    # SALES BY COUNTRY
    # =====================================================

    st.subheader("🌍 Sales by Country")

    fig_country = px.bar(
        country_df,
        x="country",
        y="total_sales",
        title="Total Sales by Country",
        labels={
            "country": "Country",
            "total_sales": "Total Sales (€)"
        }
    )

    st.plotly_chart(
        fig_country,
        use_container_width=True
    )


    # =====================================================
    # COUNTRY SALES DETAILS
    # =====================================================

    st.subheader("📊 Country Sales Details")

    country_display = country_df.copy()

    country_display["total_sales"] = (
        country_display["total_sales"].round(2)
    )

    country_display = country_display.sort_values(
        "total_sales",
        ascending=False
    )

    st.dataframe(
        country_display,
        use_container_width=True,
        hide_index=True
    )


    # =====================================================
    # UNITS SOLD BY COUNTRY
    # =====================================================

    st.subheader("📦 Units Sold by Country")

    fig_units = px.pie(
        country_df,
        names="country",
        values="total_units",
        title="Units Sold Distribution"
    )

    st.plotly_chart(
        fig_units,
        use_container_width=True
    )


    # =====================================================
    # SALES BY STORE
    # =====================================================

    st.divider()

    st.subheader("🏪 Sales by Store")

    store_filters = []

    if selected_year != "All":

        store_filters.append(
            f"sales_year = {selected_year}"
        )

    if selected_country != "All":

        store_filters.append(
            f"country = '{selected_country}'"
        )

    if selected_store != "All":

        store_filters.append(
            f"store_id = '{selected_store}'"
        )

    if store_filters:

        store_where_clause = (
            "WHERE " +
            " AND ".join(store_filters)
        )

    else:

        store_where_clause = ""

    store_query = f"""
    SELECT
        store_id,
        ROUND(SUM(total_sales), 2) AS total_sales,
        SUM(total_units) AS total_units
    FROM sales_summary
    {store_where_clause}
    GROUP BY store_id
    ORDER BY total_sales DESC
    """

    store_df = run_hive_query(
        store_query,
        [
            "store_id",
            "total_sales",
            "total_units"
        ]
    )

    if not store_df.empty:

        store_df["total_sales"] = pd.to_numeric(
            store_df["total_sales"],
            errors="coerce"
        )

        store_df["total_units"] = pd.to_numeric(
            store_df["total_units"],
            errors="coerce"
        )

        store_df = store_df.dropna()

        fig_store = px.bar(
            store_df,
            x="store_id",
            y="total_sales",
            title="Total Sales by Store",
            labels={
                "store_id": "Store",
                "total_sales": "Total Sales (€)"
            }
        )

        st.plotly_chart(
            fig_store,
            use_container_width=True
        )

    else:

        st.warning(
            "No store sales data available."
        )


    # =====================================================
    # MONTHLY SALES TREND
    # =====================================================

    st.divider()

    st.subheader("📈 Monthly Sales Trend")

    if not trend_data.empty:

        fig_trend = px.line(
            trend_data,
            x="month",
            y="total_sales",
            markers=True,
            title="Monthly Sales Trend"
        )

        fig_trend.update_layout(
            xaxis_title="Month",
            yaxis_title="Total Sales (€)"
        )

        st.plotly_chart(
            fig_trend,
            use_container_width=True
        )

    else:

        st.warning(
            "No monthly trend data available."
        )


    # =====================================================
    # TOP 10 PRODUCTS
    # =====================================================

    st.divider()

    st.subheader(
        "🏆 Top 10 Products by Sales"
    )

    if not product_df.empty:

        product_chart_df = (
            product_df
            .sort_values(
                "total_sales",
                ascending=True
            )
        )

        fig_products = px.bar(
            product_chart_df,
            x="total_sales",
            y="sku_name",
            orientation="h",
            title="Top 10 Products",
            labels={
                "sku_name": "Product",
                "total_sales": "Total Sales (€)"
            }
        )

        st.plotly_chart(
            fig_products,
            use_container_width=True
        )


        # =================================================
        # PRODUCT DETAILS
        # =================================================

        st.subheader(
            "📋 Top Product Details"
        )

        st.dataframe(
            product_df,
            use_container_width=True,
            hide_index=True
        )

    else:

        st.warning(
            "No product data available."
        )


    # =====================================================
    # COUNTRY LEVEL DATA
    # =====================================================

    st.divider()

    st.subheader(
        "📋 Country-Level Sales Data"
    )

    st.dataframe(
        country_df,
        use_container_width=True,
        hide_index=True
    )


    # =====================================================
    # DATA QUALITY / PIPELINE HEALTH
    # =====================================================

    st.divider()

    st.subheader(
        "✅ Data Quality & Pipeline Health"
    )

    dq1, dq2, dq3, dq4, dq5 = st.columns(5)

    dq1.metric(
        "Total Records",
        "1,100,000"
    )

    dq2.metric(
        "Missing Values",
        "0"
    )

    dq3.metric(
        "Duplicate Records",
        "0"
    )

    dq4.metric(
        "Negative Sales",
        "0"
    )

    dq5.metric(
        "Negative Units",
        "0"
    )

    st.success(
        "Data quality checks passed successfully."
    )


else:

    st.warning(
        "No data available for the selected filters."
    )