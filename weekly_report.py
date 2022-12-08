import streamlit as st
import numpy as np
import pandas as pd
import plotly.express as pe
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import psycopg as pc
import warnings
from credentials import DB_USER, DB_PW


cn = pc.connect(
    host="sculptor.stat.cmu.edu", dbname=DB_USER,
    user=DB_USER, password=DB_PW
)

st.set_page_config(layout="wide")

with warnings.catch_warnings():
    warnings.simplefilter('ignore')
    dates_to_select = pd.read_sql_query(
        """SELECT DISTINCT collection_week FROM hospital_beds
        ORDER BY collection_week DESC;""",
        cn
    )

st.sidebar.header('Input')
as_of_date = st.sidebar.selectbox(
    'Report Date',
    dates_to_select
)

with warnings.catch_warnings():

    warnings.simplefilter('ignore')

    # QUERY
    # A summary of how many hospital records were loaded
    # in the most recent week, and how that compares to previous weeks.
    record_counts = pd.read_sql_query(
        """SELECT collection_week AS Date, COUNT(1) as "Records Loaded"
        FROM hospital_beds
        WHERE collection_week <= %(as_of_date)s
        GROUP BY collection_week
        ORDER BY collection_week;""",
        cn,
        params={'as_of_date': as_of_date}
    )

    # QUERY
    # A table summarizing the number of adult and pediatric beds available
    # this week, the number used, and the number used by patients with COVID,
    # compared to the 4 most recent weeks.
    bed_counts = pd.read_sql_query(
        """SELECT collection_week AS Date,
        ROUND(SUM(all_adult_hospital_beds_7_day_avg)) AS "Adult Beds Available",
        ROUND(SUM(all_pediatric_inpatient_beds_7_day_avg)) AS "Pediatric Beds Available",
        ROUND(SUM(all_adult_hospital_inpatient_bed_occupied_7_day_avg)) AS "Adult Beds Used",
        ROUND(SUM(all_pediatric_inpatient_bed_occupied_7_day_avg)) AS "Pediatric Beds Used",
        ROUND(SUM(inpatient_beds_used_covid_7_day_avg)) AS "Confirmed COVID (All)"
        FROM hospital_beds
        WHERE collection_week <= %(as_of_date)s
        AND collection_week >= (%(as_of_date)s - INTERVAL '3 weeks')
        GROUP BY collection_week
        ORDER BY collection_week DESC;""",
        cn,
        index_col='date',
        params={'as_of_date': as_of_date},
        dtype={
            'Adult Beds Available': 'int',
            'Pediatric Beds Available': 'int',
            'Adult Beds Used': 'int',
            'Pediatric Beds Used': 'int',
            'Confirmed COVID (All)': 'int'
        }
    )

    # QUERY
    # A graph or table summarizing the fraction of beds in use by hospital
    # quality rating, so we can compare high-quality and low-quality hospitals.
    beds_by_quality = pd.read_sql_query(
        """
        WITH most_recent_ratings AS (
            -- use DISTINCT + ORDER BY to get most recent rating.
            SELECT DISTINCT ON (hospital_pk) hospital_pk,
            quality_rating
            FROM hospital_quality
            WHERE record_date <= %(as_of_date)s 
            ORDER BY hospital_pk, record_date DESC
        )
        SELECT (all_adult_hospital_inpatient_bed_occupied_7_day_avg + all_pediatric_inpatient_bed_occupied_7_day_avg) / (all_adult_hospital_inpatient_bed_occupied_7_day_avg + all_pediatric_inpatient_bed_occupied_7_day_avg + all_adult_hospital_beds_7_day_avg + all_pediatric_inpatient_beds_7_day_avg) AS "Beds In Use",
        mrr.quality_rating AS "Quality Rating",
        h.hospital_pk
        FROM hospitals h
        JOIN hospital_beds hb on h.hospital_pk = hb.hospital_pk
        JOIN most_recent_ratings mrr on h.hospital_pk = mrr.hospital_pk
        WHERE collection_week = %(as_of_date)s
        AND COALESCE(all_adult_hospital_beds_7_day_avg, 0) > 0;""",
        cn,
        params={'as_of_date': as_of_date}
    )

    # QUERY
    # A plot of the total number of hospital beds used per week, over all time,
    # split into all cases and COVID cases.
    beds_used_plot = pd.read_sql_query(
        """
        SELECT collection_week,
        SUM(all_adult_hospital_inpatient_bed_occupied_7_day_avg) + SUM(all_pediatric_inpatient_bed_occupied_7_day_avg) AS "Beds Used",
        SUM(inpatient_beds_used_covid_7_day_avg) AS "COVID Beds Used"
        FROM hospitals h
        JOIN hospital_beds hb ON h.hospital_pk = hb.hospital_pk
        GROUP BY collection_week
        ORDER BY collection_week;""",
        cn
    )

    # QUERY
    # Heatmap of COVID cases as of current date.
    covid_heatmap_data = pd.read_sql_query(
        """
        SELECT AVG(lat) AS lat,
        AVG(long) AS long,
        city,
        state,
        SUM(inpatient_beds_used_covid_7_day_avg) AS "COVID Cases"
        FROM hospitals h
        JOIN hospital_beds hb ON h.hospital_pk = hb.hospital_pk
        WHERE collection_week = %(as_of_date)s
        AND inpatient_beds_used_covid_7_day_avg IS NOT NULL
        GROUP BY state, city;""",
        cn,
        params={'as_of_date': as_of_date}
    )

    # QUERY
    # A table of hospitals that did not report any data in the past week, their 
    # names, and the date they most recently reported data.
    nonreporting_hospitals = pd.read_sql_query(
        """
        WITH _recently_reported AS (
            SELECT DISTINCT hospital_pk, 1 AS dummy
            FROM hospital_beds hb
            WHERE collection_week = %(as_of_date)s
        )
        SELECT MAX(h.hospital_name) AS "Hospital Name", MAX(hb.collection_week) AS "Last Reported"
        FROM hospitals h
        FULL JOIN _recently_reported rr ON rr.hospital_pk = h.hospital_pk
        JOIN hospital_beds hb ON hb.hospital_pk = h.hospital_pk
        WHERE rr.dummy IS NULL
        GROUP BY h.hospital_pk
        ORDER BY "Last Reported" DESC, "Hospital Name";
        """,
        cn,
        params={'as_of_date': as_of_date},
        index_col='Hospital Name'
    )

    # QUERY
    # Graphs of hospital utilization (the percent of available beds being used) 
    # by state, over time.
    hosp_util_plot = pd.read_sql_query(
        """
        SELECT collection_week AS "Date", state, city, AVG(lat) AS lat, AVG(long) AS long,
        ( SUM(all_adult_hospital_inpatient_bed_occupied_7_day_avg) + SUM(all_pediatric_inpatient_bed_occupied_7_day_avg)) / (SUM(all_adult_hospital_inpatient_bed_occupied_7_day_avg) + SUM(all_pediatric_inpatient_bed_occupied_7_day_avg) + SUM(all_adult_hospital_beds_7_day_avg) + SUM(all_pediatric_inpatient_beds_7_day_avg)) AS "Utilization"
        FROM hospitals h
        JOIN hospital_beds hb ON h.hospital_pk = hb.hospital_pk
        GROUP BY state, city, collection_week
        HAVING SUM(all_adult_hospital_inpatient_bed_occupied_7_day_avg) + SUM(all_pediatric_inpatient_bed_occupied_7_day_avg) + SUM(all_adult_hospital_beds_7_day_avg) + SUM(all_pediatric_inpatient_beds_7_day_avg) > 0
        ORDER BY collection_week;""",
        cn
    )


# PLOT
# A summary of how many hospital records were loaded
# in the most recent week, and how that compares to previous weeks.
st.header('Records Loaded By Date')
st.plotly_chart(
    pe.line(
        record_counts,
        x='date',
        y='Records Loaded',
        labels={'date': 'Date'}
    ),
    use_container_width=True
)


# TABLE
# A table summarizing the number of adult and pediatric beds available
# this week, the number used, and the number used by patients with COVID,
# compared to the 4 most recent weeks.
st.header('Hospital Beds Available')
st.dataframe(bed_counts, use_container_width=True)


# PLOT
# A graph or table summarizing the fraction of beds in use by hospital
# quality rating, so we can compare high-quality and low-quality hospitals.
st.header('Beds In Use By Hospital Quality')
st.plotly_chart(
    pe.box(
        beds_by_quality,
        x='Quality Rating',
        y='Beds In Use',
        labels={'hospital_pk': 'PK', 'Beds In Use': '% Beds In Use'}
    ),
    use_container_width=True
)


# PLOT
# A plot of the total number of hospital beds used per week, over all time,
# split into all cases and COVID cases.
beds_used_fig = make_subplots(specs=[[{"secondary_y": True}]])

beds_used_fig.add_trace(
    go.Scatter(
        x=beds_used_plot['collection_week'],
        y=beds_used_plot['Beds Used'],
        name='Beds Used'
    ),
    secondary_y=False,
)
beds_used_fig.add_trace(
    go.Scatter(
        x=beds_used_plot['collection_week'],
        y=beds_used_plot['COVID Beds Used'],
        name='COVID Beds Used'
    ),
    secondary_y=True,
)

beds_used_fig.update_xaxes(title_text="Date")
beds_used_fig.update_yaxes(title_text="Beds Used", secondary_y=False)
beds_used_fig.update_yaxes(title_text="COVID Beds Used", secondary_y=True)

st.header('Beds Used Over Time')
st.plotly_chart(beds_used_fig, use_container_width=True)


# PLOT
# Heatmap of COVID cases as of current date.
covid_heatmap_data['label'] = covid_heatmap_data['city'] + ', ' + \
    covid_heatmap_data['state'] + '<br>' + 'COVID Cases: ' + \
    covid_heatmap_data['COVID Cases'].astype(str) 

heatmap=go.Figure(
    data=go.Scattergeo(
        lon=covid_heatmap_data['long'],
        lat=covid_heatmap_data['lat'],
        text=covid_heatmap_data['label'],
        # <extra></extra> removes the trace label.
        hovertemplate='%{text}<extra></extra>',
        marker = dict(
            # scaling the sizes to be more reasonable.
            size = np.sqrt(covid_heatmap_data['COVID Cases'])*1.5,
            color = covid_heatmap_data['COVID Cases'], 
            colorscale = [[0, 'rgb(255,255,0)'], [1, 'rgb(255,0,0)']],
            opacity = 0.6
        )
    )
)
heatmap.update_layout(
    geo_scope='usa',
    width=1000, 
    height=800
)

st.header('COVID Cases by City')
st.plotly_chart(heatmap, use_container_width=True)


# TABLE
# A table of hospitals that did not report any data in the past week, their 
# names, and the date they most recently reported data.
st.header('No Data Reported This Week')
st.dataframe(nonreporting_hospitals,  use_container_width=True)


# PLOT
# Graphs of hospital utilization (the percent of available beds being used) 
# by state, over time.
hosp_util_plot['addr'] = hosp_util_plot['city'] + ', ' + \
    hosp_util_plot['state']
# scaling the size a bit so we aren't overwhelmed with points.
hosp_util_plot['size'] = np.power(hosp_util_plot['Utilization'], 3)

util_over_time = pe.scatter_geo(
    hosp_util_plot, 
    lat='lat', 
    lon='long', 
    scope='usa',
    color="Utilization",
    color_continuous_scale=[[0, 'rgb(255,255,0)'], [1, 'rgb(255,0,0)']],
    opacity = 0.6,
    size='size',
    hover_name="addr",
    hover_data={
        'lat': False,
        'long': False,
        'Date': False,
        'size': False,
        'Utilization':':.2f'
    },
    projection="albers usa", 
    animation_frame="Date", 
    width=1000, 
    height=800
)

st.header('Hospital Utilization By City, Over Time')
st.plotly_chart(util_over_time, use_container_width=True)