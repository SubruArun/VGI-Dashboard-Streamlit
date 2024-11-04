import os
import warnings
import numpy as np
import pandas as pd
import geopandas as gpd
from shapely.geometry import LineString
import plotly.graph_objects as go
from typing import List, Union, Optional
from shapely import geometry
import geopandas as gpd
import streamlit as st

# config
from newmind_fresh.config import CRS_O, CRS,CRS, SAHPE_FILE_PATH, SPLIT_ROUTE_101_PATH, GPS_DATA_PATH, GPS_DATA_LABELED, SEGMENT_DURATION_PATH


bus_stop_mapping = {
        'haltestelle_geibelstrase': ['Geibelstraße,2', 'Geibelstraße,1'],
        'haltestelle_kurt-huber-strase': ['Kurt-Huber-Straße,1', 'Kurt-Huber-Straße,2', 'Kurt-Huber-Straße,4'],
        'haltestelle_uhlandstrase': ['Uhlandstraße,1', 'Uhlandstraße,2'],
        'haltestelle_stadtwerk-continental': ['Stadtwerke / Continental,1', 'Stadtwerke / Continental,2', 'Stadtwerke / Continental,3', 'Stadtwerke / Continental,4'],
        'haltestelle_kornerstrase': ['Körnerstraße,2'],
        'haltestelle_marienplatz-ersatz': ['Marienplatz,1', 'Marienplatz,2'],
        'haltestelle_feldschlosl': ['Feldschlößl,1', 'Feldschlößl,2'],
        'haltestelle_theodor-heuss-brucke': ['Theodor-Heuss-Brücke,1', 'Theodor-Heuss-Brücke,2'],
        'haltestelle_theodor-heuss-strase': ['Theodor-Heuss-Straße,1', 'Theodor-Heuss-Straße,2'],
        'haltestelle_gutenbergstrase': ['Gutenbergstraße,99'],
        'haltestelle_marienplatz': [],
        'haltestelle_schellingstrase': ['Schellingstraße,1', 'Schellingstraße,1']
    }

bus_stop_reverse_mapping = {
    'Geibelstraße,2': 'haltestelle_geibelstrase',
    'Geibelstraße,1': 'haltestelle_geibelstrase',
    'Kurt-Huber-Straße,1': 'haltestelle_kurt-huber-strase',
    'Kurt-Huber-Straße,2': 'haltestelle_kurt-huber-strase',
    'Kurt-Huber-Straße,4': 'haltestelle_kurt-huber-strase',
    'Uhlandstraße,1': 'haltestelle_uhlandstrase',
    'Uhlandstraße,2': 'haltestelle_uhlandstrase',
    'Stadtwerke / Continental,1': 'haltestelle_stadtwerk-continental',
    'Stadtwerke / Continental,2': 'haltestelle_stadtwerk-continental',
    'Stadtwerke / Continental,3': 'haltestelle_stadtwerk-continental',
    'Stadtwerke / Continental,4': 'haltestelle_stadtwerk-continental',
    'Körnerstraße,2': 'haltestelle_kornerstrase',
    'Marienplatz,1': 'haltestelle_marienplatz-ersatz',
    'Marienplatz,2': 'haltestelle_marienplatz-ersatz',
    'Feldschlößl,1': 'haltestelle_feldschlosl',
    'Feldschlößl,2': 'haltestelle_feldschlosl',
    'Theodor-Heuss-Brücke,1': 'haltestelle_theodor-heuss-brucke',
    'Theodor-Heuss-Brücke,2': 'haltestelle_theodor-heuss-brucke',
    'Theodor-Heuss-Straße,1': 'haltestelle_theodor-heuss-strase',
    'Theodor-Heuss-Straße,2': 'haltestelle_theodor-heuss-strase',
    'Gutenbergstraße,99': 'haltestelle_gutenbergstrase',
    'Schellingstraße,1': 'haltestelle_schellingstrase'
    }

def load_gdf_net(path:str=SAHPE_FILE_PATH)->gpd.GeoDataFrame:
    gdf_net = gpd.read_file(path)
    gdf_net = gdf_net.loc[:, (~gdf_net.agg(["nunique"]).isin([0,1])).values[0]]
    gdf_net = gdf_net.to_crs(CRS)
    return gdf_net

def list_of_geometries_to_single_list(geo_df):
    lats = []
    lons = []
    names = []
    if "name" not in geo_df.columns:
        geo_df = geo_df.assign(name = geo_df.index)
    for feature, name in zip(geo_df.to_crs(CRS_O).geometry, geo_df.name):
        if isinstance(feature, geometry.linestring.LineString):
            linestrings = [feature]
        elif isinstance(feature, geometry.multilinestring.MultiLineString):
            linestrings = feature.geoms
        else:
            continue
        for linestring in linestrings:
            x, y = linestring.xy
            lats = np.append(lats, y)
            lons = np.append(lons, x)
            names = np.append(names, [name]*len(y))
            lats = np.append(lats, None)
            lons = np.append(lons, None)
            names = np.append(names, None)


    return lats,lons,names

def get_unique_values(df_list: List[pd.DataFrame],
                      col:str = "link") -> List:

    if not isinstance(df_list,list):
        df_list = [df_list]

    l = []
    for df in df_list:
        l.extend(df[col].unique())

    return list(set(l))

def load_dataset():
    gdf = gpd.read_parquet([GPS_DATA_PATH + s for s in os.listdir(GPS_DATA_PATH) if ".parquet" in s])
    gdf.drop_duplicates(inplace = True)
    drop_col = [s for s in ["longitude","latitude"] if s in gdf.columns]
    gdf.drop(drop_col,axis=1,inplace=True)
    gdf.dropna(inplace=True)
    #filter to route 101
    gdf = gdf[gdf["route"] == "101"]
    return gdf

def add_segment_col(gdf:gpd.GeoDataFrame,
                    split_route_path:gpd.GeoDataFrame,
                    distance_threshold:int= 0.01)->gpd.GeoDataFrame:
    gdf = gdf.assign(segment = None)
    for index,row in split_route_path.iterrows():
        row.geometry 
        gdf.loc[gdf[gdf.distance(row.geometry) < distance_threshold].index,"segment"] = index
    if gdf["segment"].isna().any():
        warnings.warn("At least one of the Points is outside of all segments")
    return gdf

def fetch_filtered_segment_data():
    # Gps dataset
    gdf = gpd.read_parquet(GPS_DATA_LABELED)
    gdf = gdf.reset_index()

    # duration in each segment
    df_segment = pd.read_parquet(SEGMENT_DURATION_PATH)
    df_segment = df_segment.reset_index()
    bus_stop_mapping = {
            'haltestelle_geibelstrase': ['Geibelstraße,2', 'Geibelstraße,1'],
            'haltestelle_kurt-huber-strase': ['Kurt-Huber-Straße,1', 'Kurt-Huber-Straße,2', 'Kurt-Huber-Straße,4'],
            'haltestelle_uhlandstrase': ['Uhlandstraße,1', 'Uhlandstraße,2'],
            'haltestelle_stadtwerk-continental': ['Stadtwerke / Continental,1', 'Stadtwerke / Continental,2', 'Stadtwerke / Continental,3', 'Stadtwerke / Continental,4'],
            'haltestelle_kornerstrase': ['Körnerstraße,2'],
            'haltestelle_marienplatz-ersatz': ['Marienplatz,1', 'Marienplatz,2'],
            'haltestelle_feldschlosl': ['Feldschlößl,1', 'Feldschlößl,2'],
            'haltestelle_theodor-heuss-brucke': ['Theodor-Heuss-Brücke,1', 'Theodor-Heuss-Brücke,2'],
            'haltestelle_theodor-heuss-strase': ['Theodor-Heuss-Straße,1', 'Theodor-Heuss-Straße,2'],
            'haltestelle_gutenbergstrase': ['Gutenbergstraße,99'],
            'haltestelle_marienplatz': [],
            'haltestelle_schellingstrase': ['Schellingstraße,1', 'Schellingstraße,1']
        }

    # map segment values to the respective key in value_map_dict
    segment_mapping_dict = {value: key for key, values in bus_stop_mapping.items() for value in values}
    df_segment['segment'] = df_segment['segment'].replace(segment_mapping_dict)
    merged_df = pd.merge(df_segment, gdf[['run', 'segment', 'route', 'geometry', 'utcTime', 'speed']], on=['run', 'segment', 'route'])

    df_segment['geometry'] = merged_df['geometry']
    df_segment['utcTime'] = merged_df['utcTime']
    df_segment['speed'] = merged_df['speed']

    return df_segment


def create_custom_stop_lines_angle(stop_line_name):
    custom_angle = 90
    line_length = 0.0005

    if stop_line_name in ["stop_lines_6", "stop_lines_7", "stop_lines_9", "stop_lines_10"]:
        custom_angle = 80
    elif stop_line_name in ["stop_lines_1", "stop_lines_4", "stop_lines_15"]:
        custom_angle = 100
    elif stop_line_name in ["stop_lines_2", "stop_lines_3", "stop_lines_8", ""]:
        custom_angle = 110
    elif stop_line_name in ["stop_lines_5"]:
        custom_angle = 85
    elif stop_line_name in ["stop_lines_11", "stop_lines_12", "stop_lines_13"]:
        custom_angle = 90
    elif stop_line_name in ["stop_lines_14"]:
        custom_angle = 28

    if stop_line_name in ["stop_lines_14", "stop_lines_13"]:
        line_length = 0.00015
    return custom_angle, line_length


def custom_haltestelle_text_location(stop_name, mid_x, mid_y):
    if stop_name == "haltestelle_stadtwerk-continental":
        mid_x = mid_x - 0.0005
        mid_y = mid_y + 0.0003
    elif stop_name == "haltestelle_uhlandstrase":
        mid_x = mid_x - 0.0007
        mid_y = mid_y + 0.0003
    elif stop_name == "haltestelle_kornerstrase":
        mid_x = mid_x - 0.0007
        mid_y = mid_y + 0.0003
    elif stop_name == "haltestelle_gutenbergstrase":
        mid_x = mid_x + 0.0009
    else:
        mid_y = mid_y + 0.0003

    return mid_x ,mid_y


def split_linestring_at_midpoint(linestring):
    coords = list(linestring.coords)
    if len(coords) < 2:
        raise ValueError("LineString must contain at least 2 points to be split")
    mid_idx = len(coords) // 2
    if len(coords) == 2:  # Special case: LineString has exactly two points
        left_part = LineString(coords)
        return left_part, None
    else:
        left_coords = coords[:mid_idx + 1]
        right_coords = coords[mid_idx:]
        if len(left_coords) < 1 or len(right_coords) < 1:
            raise ValueError("Invalid split resulting in non-LineString parts")
        left_part = LineString(left_coords)
        right_part = LineString(right_coords)
    return left_part, right_part


def process_dataframe(df):
    new_geometries = df['geometry'].copy()
    for i, idx in enumerate(df.index):
        if "haltestelle" in idx or "stop_line" in idx:
            linestring = df.at[idx, 'geometry']
            try:
                left_part, right_part = split_linestring_at_midpoint(linestring)
            except ValueError as e:
                print(f"Skipping index {idx} due to error: {e}")
                continue
            if left_part is not None and i > 0:
                prev_idx = df.index[i - 1]
                new_geometries.at[prev_idx] = LineString(
                    list(new_geometries.at[prev_idx].coords) + list(left_part.coords)
                )
            if right_part is not None and i < len(df.index) - 1:
                next_idx = df.index[i + 1]
                new_geometries.at[next_idx] = LineString(
                    list(right_part.coords) + list(new_geometries.at[next_idx].coords)
                )
            # update the current row with the same geometry
            new_geometries.at[idx] = linestring
    df['geometry'] = new_geometries
    return df


def calculate_midpoint(x, y):
    midpoint_index = len(x) // 2
    return x[midpoint_index], y[midpoint_index]


def rotate_vector(dx, dy, angle):
    angle_rad = np.deg2rad(angle)
    rotated_dx = dx * np.cos(angle_rad) - dy * np.sin(angle_rad)
    rotated_dy = dx * np.sin(angle_rad) + dy * np.cos(angle_rad)
    return rotated_dx, rotated_dy


def create_angle_line(x, y, angle, line_length=0.001):
    # Calculate the midpoint of the path
    mid_x, mid_y = calculate_midpoint(x, y)
    midpoint_index = len(x) // 2

    # Calculate the direction of the original path at the midpoint
    if midpoint_index == 0:
        dx = x[midpoint_index + 1] - x[midpoint_index]
        dy = y[midpoint_index + 1] - y[midpoint_index]
    elif midpoint_index == len(x) - 1:
        dx = x[midpoint_index] - x[midpoint_index - 1]
        dy = y[midpoint_index] - y[midpoint_index - 1]
    else:
        dx = x[midpoint_index + 1] - x[midpoint_index - 1]
        dy = y[midpoint_index + 1] - y[midpoint_index - 1]

    # Rotate the direction vector by the specified angle
    rotated_dx, rotated_dy = rotate_vector(dx, dy, angle)

    # Normalize the rotated direction to the desired line length
    length = np.sqrt(rotated_dx ** 2 + rotated_dy ** 2)
    rotated_dx = (rotated_dx / length) * line_length / 2
    rotated_dy = (rotated_dy / length) * line_length / 2

    # Create the line segment
    line_x = [mid_x - rotated_dx, mid_x + rotated_dx]
    line_y = [mid_y - rotated_dy, mid_y + rotated_dy]
    return line_x, line_y


''' PLOT FUNCTIONS '''

def plot_NewMindFresh(gdf_list, 
                      df_deviation:Optional[pd.DataFrame] = None,
                      split_path:Union[str,gpd.GeoDataFrame] = SPLIT_ROUTE_101_PATH,
                      lw:int = 20
                      ):
    fig = go.Figure()

    if not isinstance(gdf_list,list):
        gdf_list = [gdf_list]
    # print(gdf_list[0].head(10))
    ##### Plot the split path
    if split_path is not None:
        plot_add_split_path(fig,gdf_list,df_deviation,split_path,lw)

    fig.update_layout(width=None,
                        # autosize=True,
                        height=1200,
                        margin={'l': 0, 't': 0, 'b': 0, 'r': 0},
                        legend = dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
                        # annotations=annotations,
                        mapbox={
                            'style': "carto-positron", # "white-bg", # "open-street-map",
                            'center': {'lon': 11.441815, 'lat': 48.772619},
                            'zoom': 13.5}, showlegend=False)

    # Display the plot within Streamlit
    st.plotly_chart(fig, use_container_width=True)

def plot_add_split_path(fig:go.Figure,
                        gdf_list:gpd.GeoDataFrame,
                        df_deviation:Optional[pd.DataFrame],
                        split_path:Union[str,gpd.GeoDataFrame] = SPLIT_ROUTE_101_PATH,
                        lw:int=20,#line width
                        )->None:
    ##### Plot the split path
    if isinstance(split_path,str):
        gdf_route101_path = gpd.read_parquet(split_path)
    else:
        gdf_route101_path = split_path

    segments = list(set([segment for gdf in gdf_list for segment in gdf["segment"].unique()]))
    gdf_route101_path = gdf_route101_path.rename(index=bus_stop_reverse_mapping)
    gdf_route101_path.drop([segment for segment in gdf_route101_path.index if segment not in segments],inplace=True)
    gdf_route101_path = process_dataframe(gdf_route101_path)

    # for proper display in map, we need in a sorted index
    # indices starting with "road_path", "stop_lines", and "haltestelle"
    road_path_indices = [idx for idx in gdf_route101_path.index if idx.startswith('road_path')]
    stop_lines_indices = [idx for idx in gdf_route101_path.index if idx.startswith('stop_lines')]
    haltestelle_indices = [idx for idx in gdf_route101_path.index if idx.startswith('haltestelle')]
    sorted_indices = road_path_indices + stop_lines_indices + haltestelle_indices
    gdf_route101_path = gdf_route101_path.reindex(sorted_indices)

    for index,row in gdf_route101_path.to_crs(CRS_O).iterrows():
        x, y = row.geometry.xy
        midpoint_x = (x[0] + x[-1]) / 2
        midpoint_y = (y[0] + y[-1]) / 2

        r = df_deviation.loc[index]
        
        if "road_path" in index.lower():
            fig = fig.add_trace(go.Scattermapbox(
                    name= index,
                    mode = "lines",
                    lon = list(x),
                    lat = list(y),
                    text = f"Deviation: {r.deviation:.2f}",
                    line=dict(width=lw,color = f"rgb{r.rgba[:-1]}"),
                    hoverinfo="text"))
        elif 'stop_lines' in index.lower():
            custom_angle, line_length = create_custom_stop_lines_angle(index)
            line_x, line_y = create_angle_line(x, y, custom_angle, line_length)
            fig.add_trace(go.Scattermapbox(
                name=index,
                mode="lines",
                lon=line_x,
                lat=line_y,
                text=f"Deviation: {r.deviation:.2f}",
                hoverinfo="text",
                line=dict(width=lw, color=f"rgb{r.rgba[:-1]}"))
            )
        elif 'haltestelle' in index.lower():
            updated_lat, updated_lon = custom_haltestelle_text_location(index, midpoint_x, midpoint_y)
            name = bus_stop_mapping.get(index.lower(), [])
            if len(name) > 0:
                name = name[0]
            else:
                name = ""
            # Add the bus stop marker with hover text
            fig.add_trace(go.Scattermapbox(
                name=name,
                mode="markers",
                lon=[midpoint_x],
                lat=[midpoint_y],
                marker=dict(size=30, color = f"rgb{r.rgba[:-1]}", symbol="circle"),
                text=f"Deviation: {r.deviation:.2f}",
                hoverinfo="text"
            ))
            fig.add_trace(go.Scattermapbox(
                name=f"text_{name}",
                mode="text",
                lon=[updated_lat],
                lat=[updated_lon],  # Slightly offset to place text above the circle
                text=[f"{name}\n\u200b"],  # Use Unicode for vertical orientation
                textfont=dict(size=12, color="black"),  # Customize the text font
                showlegend=False  # Hide legend for text
            ))

