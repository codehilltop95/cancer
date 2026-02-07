import streamlit as st
import pandas as pd
import plotly.express as px
from snowflake.snowpark.context import get_active_session

st.set_page_config(page_title="Oncology KPI Dashboard", layout="wide")
session = get_active_session()

def load_table(name):
    df = session.table(name).to_pandas()
    date_cols = ["SERVICE_DATE", "DOB", "UPDATED_AT"]
    for c in df.columns:
        if c in date_cols:
            df[c] = pd.to_datetime(df[c], format="mixed", dayfirst=True, errors="coerce")
    return df

provider_df = load_table("PROVIDER")
patient_df = load_table("PATIENT")
cancer_df = load_table("CANCER_CATALOG")
enc_df = load_table("ONCOLOGY_ENCOUNTERS")

def bg(url, opacity=0.78):
    safe = url.replace('"','%22')
    st.markdown(
        f"""
        <style>
        .stApp {{
            background: linear-gradient(rgba(255,255,255,{opacity}), rgba(255,255,255,{opacity})),
            url("{safe}") no-repeat center center fixed;
            background-size: cover;
        }}
        </style>
        """,
        unsafe_allow_html=True
    )

bg("https://images.pexels.com/photos/4966406/pexels-photo-4966406.jpeg")

st.sidebar.title("Navigation")
page = st.sidebar.radio(
    "",
    [
        "Overview",
        "Encounter Trends",
        "Cancer Insights",
        "Provider Insights",
        "Patient Insights",
        "Financial Metrics",
        "AI Search Assistant"
    ]
)

st.sidebar.subheader("Filters")
status_vals = session.sql("SELECT DISTINCT ENCOUNTER_STATUS FROM ONCOLOGY_ENCOUNTERS").to_pandas()["ENCOUNTER_STATUS"].tolist()
sel_status = st.sidebar.multiselect("Encounter Status", status_vals)

if sel_status:
    txt = ",".join([f"'{x}'" for x in sel_status])
    where_status = f"AND ENCOUNTER_STATUS IN ({txt})"
    label = txt
else:
    where_status = ""
    label = "ALL"

date_df = session.sql("SELECT SERVICE_DATE FROM ONCOLOGY_ENCOUNTERS").to_pandas()
date_df["SERVICE_DATE"] = pd.to_datetime(date_df["SERVICE_DATE"], format="mixed", dayfirst=True)

min_d = date_df["SERVICE_DATE"].dt.date.min()
max_d = date_df["SERVICE_DATE"].dt.date.max()

d1 = st.sidebar.date_input("Start Date", min_d, min_value=min_d, max_value=max_d)
d2 = st.sidebar.date_input("End Date", max_d, min_value=min_d, max_value=max_d)

enc_filter = enc_df[
    (enc_df["SERVICE_DATE"].dt.date >= d1) &
    (enc_df["SERVICE_DATE"].dt.date <= d2)
]

st.title("ONCOLOGY OVERVIEW ⚕️")
st.caption("A real-time analytics dashboard for oncology performance metrics, encounter trends, cancer insights, providers, patients, and financial KPIs.")

if page == "Overview":
    c1, c2, c3, c4 = st.columns(4)

    total_enc = session.sql(f"SELECT COUNT(*) AS T FROM ONCOLOGY_ENCOUNTERS {where_status}").collect()[0]["T"]
    c1.metric("Total Encounters", total_enc)

    billed = session.sql(f"SELECT SUM(BILLED_AMOUNT) AS A FROM ONCOLOGY_ENCOUNTERS {where_status}").collect()[0]["A"]
    c2.metric("Total Billed Amount", round(billed,2))

    unique_pat = session.sql("SELECT COUNT(DISTINCT PATIENT_ID) AS C FROM ONCOLOGY_ENCOUNTERS").collect()[0]["C"]
    c3.metric("Unique Patients", unique_pat)

    avg_bill = session.sql("SELECT AVG(BILLED_AMOUNT) AS A FROM ONCOLOGY_ENCOUNTERS").collect()[0]["A"]
    c4.metric("Avg Bill Amount", round(avg_bill,2))

    daily = (
        enc_filter.groupby(enc_filter["SERVICE_DATE"].dt.date)["ENCOUNTER_ID"]
        .count().reset_index()
    )

    st.subheader("Daily Encounter Trend")
    fig = px.area(daily, x="SERVICE_DATE", y="ENCOUNTER_ID")
    st.plotly_chart(fig, use_container_width=True)

    s = session.sql("""
        SELECT DISEASE_STAGE, COUNT(*) AS C
        FROM ONCOLOGY_ENCOUNTERS
        GROUP BY DISEASE_STAGE
    """).to_pandas()

    fig2 = px.bar(s, x="DISEASE_STAGE", y="C", color="DISEASE_STAGE")
    st.plotly_chart(fig2, use_container_width=True)

if page == "Encounter Trends":
    st.header("Encounter Trends")

    daily = (
        enc_filter.groupby(enc_filter["SERVICE_DATE"].dt.date)["ENCOUNTER_ID"]
        .count().reset_index()
    )

    fig = px.line(daily, x="SERVICE_DATE", y="ENCOUNTER_ID", markers=True)
    st.plotly_chart(fig, use_container_width=True)

    fig2 = px.scatter(enc_df, x="BILLED_AMOUNT", y="SERVICE_DATE", color="ENCOUNTER_STATUS")
    st.plotly_chart(fig2, use_container_width=True)

if page == "Cancer Insights":
    st.header("Cancer Insights")

    cancers = session.sql("SELECT DISTINCT CANCER_NAME FROM CANCER_CATALOG").to_pandas()["CANCER_NAME"].tolist()
    csel = st.selectbox("Select Cancer", cancers)

    bar = session.sql(f"""
        SELECT COUNT(*) AS TOTAL, E.ENCOUNTER_STATUS
        FROM ONCOLOGY_ENCOUNTERS E
        JOIN CANCER_CATALOG C ON C.CANCER_CODE = E.CANCER_CODE
        WHERE C.CANCER_NAME = '{csel}'  {where_status}
        GROUP BY E.ENCOUNTER_STATUS
    """).to_pandas()

    fig = px.bar(bar, x="ENCOUNTER_STATUS", y="TOTAL", color="ENCOUNTER_STATUS")
    st.plotly_chart(fig, use_container_width=True)

    pie = session.sql(f"""
        SELECT COUNT(*) AS TOTAL, P.PROVIDER_NAME
        FROM PROVIDER P
        JOIN ONCOLOGY_ENCOUNTERS E ON P.PROVIDER_ID = E.PROVIDER_ID
        JOIN CANCER_CATALOG C ON C.CANCER_CODE = E.CANCER_CODE
        WHERE C.CANCER_NAME = '{csel}'
        GROUP BY PROVIDER_NAME
    """).to_pandas()
    st.subheader(f"""BEST HOSPITALS IN INDIA FOR {csel}""")

    fig2 = px.pie(pie, names="PROVIDER_NAME", values="TOTAL", hole=0.4)
    st.plotly_chart(fig2, use_container_width=True)

    pt = session.sql(f"""
        SELECT CONCAT(P.FIRST_NAME,' ',P.LAST_NAME) AS NAME,
        P.GENDER, P.DOB, P.CITY, P.STATE, P.COUNTRY, P.PAYER_ID
        FROM PATIENT P
        JOIN ONCOLOGY_ENCOUNTERS E ON P.PATIENT_ID = E.PATIENT_ID
        JOIN CANCER_CATALOG C ON C.CANCER_CODE = E.CANCER_CODE
        WHERE C.CANCER_NAME = '{csel}'
    """).to_pandas()

    st.subheader("Patients with this Cancer")
    st.dataframe(pt, use_container_width=True)

if page == "Provider Insights":
    st.header("Provider Insights")

    prov = session.sql("""
        SELECT P.PROVIDER_NAME, COUNT(*) AS TOTAL
        FROM PROVIDER P
        JOIN ONCOLOGY_ENCOUNTERS E ON P.PROVIDER_ID = E.PROVIDER_ID
        GROUP BY PROVIDER_NAME
    """).to_pandas()

    fig = px.bar(
        prov.sort_values("TOTAL", ascending=False).head(20),
        x="PROVIDER_NAME",
        y="TOTAL",
        color="PROVIDER_NAME"
    )
    st.plotly_chart(fig, use_container_width=True)

if page == "Patient Insights":
    st.header("Patient Insights")

    gen = session.sql("SELECT GENDER, COUNT(*) AS C FROM PATIENT GROUP BY GENDER").to_pandas()
    fig = px.pie(gen, names="GENDER", values="C", hole=0.45)
    st.plotly_chart(fig, use_container_width=True)

    cities = session.sql("""
        SELECT CITY, COUNT(*) AS C
        FROM PATIENT
        GROUP BY CITY
    """).to_pandas().sort_values("C", ascending=False).head(20)

    fig2 = px.bar(cities, x="CITY", y="C")
    st.plotly_chart(fig2, use_container_width=True)

if page == "Financial Metrics":
    st.header("Financial Metrics")

    fin = session.sql("""
        SELECT SERVICE_DATE, SUM(BILLED_AMOUNT) AS AMT
        FROM ONCOLOGY_ENCOUNTERS
        GROUP BY SERVICE_DATE
    """).to_pandas()

    fig = px.area(fin, x="SERVICE_DATE", y="AMT")
    st.plotly_chart(fig, use_container_width=True)

    fig2 = px.scatter(enc_df, x="BILLED_AMOUNT", y="ENCOUNTER_STATUS", color="DISEASE_STAGE")
    st.plotly_chart(fig2, use_container_width=True)

if page == "AI Search Assistant":
    st.header("AI Search Assistant")

    q = st.text_input("Ask your question about oncology data")

    if q:
        sql = f"""
        SELECT snowflake.cortex.complete(
    	'mistral-large',
    	'You are an Oncology Data Assistant.

    	STRICT RULES:
    	- Use only the data found in ONCOLOGY_ENCOUNTERS, PATIENT, PROVIDER, CANCER_CATALOG
    	- NEVER say you cannot access the tables (you CAN)
    	- NEVER output SQL queries
    	- NEVER explain how you got the answer
    	- ONLY output the final answer to the user concisely

    	User question: {q}
    	'
	) AS A
        """
        ans = session.sql(sql).to_pandas()["A"][0]
        st.success(ans)
