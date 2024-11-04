import os
import sys
import json
import datetime
import plotly.graph_objs as go
from copy import deepcopy

# Streamlit utils
import streamlit as st
import altair as alt
import hydralit_components as hc

# App utils
from newmind_fresh.preprocess.segment_agg import compare_segment_durations
from utils import fetch_filtered_segment_data, plot_NewMindFresh
from newmind_fresh.config import FRESHBOARD_BUS_IMG

st.set_page_config(
        page_title="GPS Dashboard",
        page_icon="🚌",
        layout="wide",
    )
# Dark Theme
alt.themes.enable("dark")

@st.cache_data
def fetch_filtered_segment_data_cached():
    return fetch_filtered_segment_data()

initial_dataset = fetch_filtered_segment_data_cached()

@st.cache_data
def load_segment_data(filter_1, filter_2):
    segment_data = compare_segment_durations(df=initial_dataset, filter1=filter_1, filter2=filter_2)
    return segment_data

@st.cache_data
def load_lottiefile(filepath: str):
    with open(filepath,"r") as f:
        return json.load(f)

days_of_week_mapping = {
    "Montag": "Monday", 
    "Dienstag": "Tuesday", 
    "Mittwoch": "Wednesday", 
    "Donnerstag": "Thursday", 
    "Freitag": "Friday", 
    "Samstag": "Saturday", 
    "Sonntag": "Sunday"
}

severity_mapping = {
    "Starke Verzögerung": "Intense Delay",
    "Mäßige Verzögerung": "Moderate Delay", 
    "Minimale Verzögerung": "Minimal Delay"
}

def set_segment_type(segment):
    if segment.startswith('haltestelle'):
        return 'bus_stop'
    elif segment.startswith('stop_lines'):
        return 'stop_lines'
    else:
        return 'road_path'

def fetch_days_of_week_mapped(days_of_week):
    return_days_of_week = []
    for day_value in days_of_week:
        return_days_of_week.append(days_of_week_mapping.get(day_value))
    return return_days_of_week


def fetch_severity_mapped(severity):
    return_severity = []
    for severity_value in severity:
        return_severity.append(severity_mapping.get(severity_value))
    return return_severity


def main():
    # specify the primary menu definition
    menu_data = [
            {'icon': "far fa-chart-bar", 'label':"Visualisierung"},
            {'icon': "fas fa-tachometer-alt", 'label':"Armaturenbrett",'ttip':"I'm the Dashboard tooltip!"}, #can add a tooltip message
            {'icon': "far fa-envelope", 'label':"Rückmeldung"}
    ]
    # we can override any part of the primary colors of the menu
    # over_theme = {'txc_inactive': '#FFFFFF','menu_background':'red','txc_active':'yellow','option_active':'blue'}
    over_theme = {'txc_inactive': '#FFFFFF'}
    menu_id = hc.nav_bar(menu_definition=menu_data,home_name='Startseite',override_theme=over_theme)

    if menu_id == "Startseite":
        st.title("Über das Projekt")
        st.markdown("""Als integraler Bestandteil von AIMotion Bayern konzentriert sich unsere Initiative auf die Verbesserung des öffentlichen Nahverkehrs durch die Anwendung von Methoden des maschinellen Lernens und Simulationen, wobei authentische GPS-Daten von Bussen, die in Ingolstadt verkehren, genutzt werden.
                    \\
                    \\
                    Werfen Sie einen Blick in:[INVG](https://www.invg.de/)
                    """)
        st.image(FRESHBOARD_BUS_IMG)
    elif menu_id == "Visualisierung":
        st.title("Ingolstadt Bus GPS-Daten")

        # Calendar-like selection box
        col1, col2, col3, col4, col5 = st.columns(5)
        with col1:
            filter_1_start_date = st.date_input("Startdatum", datetime.date(2023, 10, 10), format="DD.MM.YYYY")
        with col2:
            filter_1_end_date = st.date_input("Enddatum", datetime.date(2023, 11, 20), format="DD.MM.YYYY")
        with col3:
            # start_time = st.number_input("Start Time", min_value=0, max_value=23, value=0)
            filter_1_start_time = st.time_input("Startzeit", datetime.time(12, 0), step=3600) # 300 sec = 5min)
        with col4:
            # end_time = st.number_input("End Time", min_value=0, max_value=23, value=23)
            filter_1_end_time = st.time_input("Endzeit", datetime.time(23, 00), step=3600)
        with col5:
            filter_1_days_of_week = st.multiselect("Wochentage", ["Montag", "Dienstag", "Mittwoch", "Donnerstag", "Freitag", "Samstag", "Sonntag"], default=["Mittwoch"])
            filter_1_days_of_week = fetch_days_of_week_mapped(filter_1_days_of_week)

        # Segment data filter
        dt, l1, l2, f1, f2 = compare_segment_durations(df=initial_dataset, filter1=(filter_1_start_date,filter_1_end_date,filter_1_days_of_week,filter_1_start_time,None), 
                                                       filter2=(filter_1_start_date,filter_1_end_date,filter_1_days_of_week,None,None), for_dashboard=True)
        # Plot map
        plot_NewMindFresh(gdf_list=initial_dataset.loc[l1], df_deviation=dt, lw=13)
    elif menu_id == "Armaturenbrett":
        st.title("Verkehrsanalyse")

        ''' Per Segment - Bar Plot '''
        avg_duration_per_segment = deepcopy(initial_dataset).groupby('segment')['duration'].mean().reset_index() # average duration for each segment        
        avg_duration_per_segment['duration'] = avg_duration_per_segment['duration'].round(2)
        # Create a Plotly bar trace
        bar_trace = go.Bar(
            x=avg_duration_per_segment['duration'],
            y=avg_duration_per_segment['segment'],
            orientation='h',
            marker=dict(color='skyblue'),  # Set bar color
            text=avg_duration_per_segment.apply(lambda row: f"{row['segment']}<br>{row['duration']} seconds", axis=1),  # Customize hover text,       # Display the average duration as text on the bars
            textposition='auto',           # Automatically position the text on the bars
        )
        # Create a Plotly layout
        layout = go.Layout(
            title='Average Duration By Segment',
            xaxis=dict(title='Average Duration (seconds)'),
            yaxis=dict(title='Segment', tickmode='array', dtick=1),  # Display every tick),
            height=1500,  # Adjust the height of the chart
            width=2000,  # Adjust the width of the chart
        )
        # Create a Plotly figure
        fig = go.Figure(data=[bar_trace], layout=layout)
        # Display the Plotly figure using Streamlit
        st.plotly_chart(fig)

        ''' Per Segment Type - Bar Plot''' 
        avg_duration_per_segment['segment_type'] = avg_duration_per_segment['segment'].apply(set_segment_type)
        segment_type_avg_duration = avg_duration_per_segment.groupby('segment_type')['duration'].mean().reset_index()
        segment_type_avg_duration['duration'] = segment_type_avg_duration['duration'].round(2)
        # Create a Plotly bar trace
        bar_trace = go.Bar(
            x=segment_type_avg_duration['duration'],
            y=segment_type_avg_duration['segment_type'],
            orientation='h',
            marker=dict(color='skyblue'),  # Set bar color
            text=segment_type_avg_duration.apply(lambda row: f"{row['segment_type']}<br>{row['duration']} seconds", axis=1),  # Customize hover text,       # Display the average duration as text on the bars
            textposition='auto',           # Automatically position the text on the bars
        )
        # Create a Plotly layout
        layout = go.Layout(
            title='Average Duration By Segment Type',
            xaxis=dict(title='Average Duration (seconds)'),
            yaxis=dict(title='Segment', tickmode='array', dtick=1),  # Display every tick),
            height=300,  # Adjust the height of the chart
            width=1500,  # Adjust the width of the chart
        )
        # Create a Plotly figure
        fig = go.Figure(data=[bar_trace], layout=layout)
        # Display the Plotly figure using Streamlit
        st.plotly_chart(fig)

        ''' Per Segment Type - PI Chart'''
        # Create a Plotly pie chart trace
        pie_trace = go.Pie(
            labels=segment_type_avg_duration['segment_type'],
            values=segment_type_avg_duration['duration'],
        )
        # Create a Plotly layout
        layout = go.Layout(
            title='Average Duration By Segment Type',
        )
        # Create a Plotly figure
        fig = go.Figure(data=[pie_trace], layout=layout)
        # Display the Plotly figure using Streamlit
        st.plotly_chart(fig)


if __name__ == "__main__":
    main()
