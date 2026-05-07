"""Streamlit dashboard for the books ETL pipeline."""
import os

import pandas as pd
import plotly.express as px
import streamlit as st
from pymongo import MongoClient

MONGO_HOST = os.getenv("MONGO_HOST", "mongodb:27017")
DB_NAME = os.getenv("MONGO_DB", "books_db")

st.set_page_config(page_title="Books Pipeline Dashboard", layout="wide")


@st.cache_resource
def get_client() -> MongoClient:
    return MongoClient(f"mongodb://{MONGO_HOST}", serverSelectionTimeoutMS=10000)


@st.cache_data(ttl=60)
def load_collection(name: str) -> pd.DataFrame:
    client = get_client()
    db = client[DB_NAME]
    docs = list(db[name].find({}, {"_id": 0}))
    return pd.DataFrame(docs)


@st.cache_data(ttl=60)
def load_metrics() -> dict:
    client = get_client()
    db = client[DB_NAME]
    doc = db["books_metricas"].find_one({}, {"_id": 0})
    return doc or {}


def render_header():
    st.title("Books Pipeline Dashboard")
    st.caption(
        "Datos consolidados desde Open Library API + Books to Scrape, "
        "orquestados por Apache Airflow."
    )


def render_kpis(metrics: dict, scraped_df: pd.DataFrame, api_df: pd.DataFrame):
    price_stats = metrics.get("price_stats", {})
    cols = st.columns(4)
    cols[0].metric("Libros (API)", len(api_df))
    cols[1].metric("Libros (scraping)", len(scraped_df))
    cols[2].metric("Precio promedio", f"£{price_stats.get('avg_price', 0):.2f}")
    cols[3].metric("Precio máximo", f"£{price_stats.get('max_price', 0):.2f}")
    if metrics.get("generated_at"):
        st.caption(f"Métricas generadas: {metrics['generated_at']}")


def render_price_histogram(scraped_df: pd.DataFrame):
    if scraped_df.empty or "price" not in scraped_df:
        st.info("No hay datos de precios disponibles.")
        return
    fig = px.histogram(scraped_df, x="price", nbins=30, title="Histograma de precios")
    fig.update_layout(xaxis_title="Precio (£)", yaxis_title="Frecuencia")
    st.plotly_chart(fig, use_container_width=True)


def render_category_bar(scraped_df: pd.DataFrame):
    if scraped_df.empty or "category" not in scraped_df:
        st.info("No hay datos de categorías disponibles.")
        return
    counts = scraped_df["category"].value_counts().reset_index()
    counts.columns = ["category", "count"]
    fig = px.bar(counts, x="category", y="count", title="Libros por categoría")
    fig.update_layout(xaxis_title="Categoría", yaxis_title="Cantidad", xaxis_tickangle=-45)
    st.plotly_chart(fig, use_container_width=True)


def render_rating_distribution(scraped_df: pd.DataFrame):
    if scraped_df.empty or "rating" not in scraped_df:
        st.info("No hay datos de ratings disponibles.")
        return
    counts = scraped_df["rating"].value_counts().sort_index().reset_index()
    counts.columns = ["rating", "count"]
    fig = px.pie(counts, names="rating", values="count", title="Distribución de ratings")
    st.plotly_chart(fig, use_container_width=True)


def render_top_books(scraped_df: pd.DataFrame, by: str, title: str):
    if scraped_df.empty or by not in scraped_df:
        st.info(f"No hay datos para {title}.")
        return
    top = scraped_df.nlargest(10, by)[["title", by, "category"]]
    fig = px.bar(
        top.sort_values(by),
        x=by,
        y="title",
        orientation="h",
        title=title,
        color="category",
    )
    fig.update_layout(xaxis_title=by.capitalize(), yaxis_title="Título")
    st.plotly_chart(fig, use_container_width=True)


def render_filtered_table(scraped_df: pd.DataFrame, api_df: pd.DataFrame):
    st.subheader("Catálogo con filtros")
    if scraped_df.empty and api_df.empty:
        st.info("No hay libros para mostrar.")
        return

    merged = scraped_df.copy()
    if not api_df.empty and "title" in api_df.columns:
        api_lookup = api_df.set_index("title")["author"].to_dict() if "author" in api_df else {}
        if not merged.empty:
            merged["author"] = merged["title"].map(api_lookup).fillna("-")

    col1, col2, col3 = st.columns(3)
    title_filter = col1.text_input("Filtrar por título")
    author_filter = col2.text_input("Filtrar por autor")
    category_options = ["(todas)"] + sorted(merged["category"].dropna().unique().tolist()) if "category" in merged else ["(todas)"]
    category_filter = col3.selectbox("Filtrar por categoría", category_options)

    filtered = merged
    if title_filter and "title" in filtered:
        filtered = filtered[filtered["title"].str.contains(title_filter, case=False, na=False)]
    if author_filter and "author" in filtered:
        filtered = filtered[filtered["author"].str.contains(author_filter, case=False, na=False)]
    if category_filter and category_filter != "(todas)" and "category" in filtered:
        filtered = filtered[filtered["category"] == category_filter]

    st.dataframe(filtered, use_container_width=True, hide_index=True)


def main():
    render_header()

    try:
        scraped_df = load_collection("books_scraped")
        api_df = load_collection("books_api")
        metrics = load_metrics()
    except Exception as exc:
        st.error(f"No se pudo conectar a MongoDB: {exc}")
        return

    render_kpis(metrics, scraped_df, api_df)

    st.divider()
    col_left, col_right = st.columns(2)
    with col_left:
        render_price_histogram(scraped_df)
        render_top_books(scraped_df, "price", "Top 10 libros más caros")
    with col_right:
        render_category_bar(scraped_df)
        render_top_books(scraped_df, "rating", "Top 10 libros mejor calificados")

    render_rating_distribution(scraped_df)

    st.divider()
    render_filtered_table(scraped_df, api_df)


if __name__ == "__main__":
    main()
