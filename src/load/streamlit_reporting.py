import os
import csv
import sys
import streamlit as st
import matplotlib.pyplot as plt
from datetime import datetime
import pandas as pd
import numpy as np
import json
import altair as alt
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots

current_script_path = os.path.dirname(os.path.abspath(__file__))

DATA_DIR = os.path.join(current_script_path, "reporting_data")
LOAD_DATA_DIR = os.path.join(current_script_path, "..", "load", "reporting_data")

try:
    sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
    from src.logging.logger import setup_logger
    from src.util.config import Config
    from src.error_handling.error_handling import (
        ErrorManager, ETLError, ErrorSeverity, ErrorCode
    )
    logger = setup_logger()
    error_manager = ErrorManager(logger=logger)
except ImportError:

    class DummyLogger:
        def info(self, msg): print(f"INFO: {msg}")
        def error(self, msg): print(f"ERROR: {msg}")
        def warning(self, msg): print(f"WARNING: {msg}")

    class DummyErrorManager:
        def create_error(self, **kwargs): 
            print(f"ERROR: {kwargs.get('message')}")

    logger = DummyLogger()
    error_manager = DummyErrorManager()
    
    class ErrorSeverity:
        ERROR = "ERROR"
        WARNING = "WARNING"
        CRITICAL = "CRITICAL"
        
    class ErrorCode:
        FILE_NOT_FOUND = "FILE_NOT_FOUND"
        FILE_CORRUPTED = "FILE_CORRUPTED"
        DATA_OUT_OF_RANGE = "DATA_OUT_OF_RANGE"
        INVALID_DATA_TYPE = "INVALID_DATA_TYPE"

st.set_page_config(layout="wide", page_title="📊 ETL Reporting", page_icon="📈")

st.markdown("""
<style>
    .main {
        background-color: #f5f7f9;
    }
   
    h1, h2, h3 {
        color: #0f4c75;
    }
            
    .stTabs [data-baseweb="tab-list"] {
        flex-wrap: wrap;
        justify-content: space-between;
    }
    .stTabs [data-baseweb="tab"] {
        flex-grow: 1;
        text-align: center;
        min-width: 120px;
        background-color: #e8f1f5;
        border-radius: 4px 4px 0 0;
    }

    .stTabs [aria-selected="true"] {
        background-color: #3282b8;
        color: white;
    }
</style>
""", unsafe_allow_html=True)

st.title("ETL Process Data Analytics Dashboard")
st.markdown("Visualization and analysis of data from ETL pipeline")

def load_csv(filepath):
    try:
        df = pd.read_csv(filepath, encoding="utf-8")
        return df
    except FileNotFoundError:
        error_manager.create_error(
            code=ErrorCode.FILE_NOT_FOUND,
            message=f"File {filepath} not found",
            severity=ErrorSeverity.ERROR,
            component="streamlit_reporting",
            source_file=filepath
        )
        st.error(f"File {filepath} not found")
        return pd.DataFrame()
    except Exception as e:
        error_manager.create_error(
            code=ErrorCode.FILE_CORRUPTED,
            message=str(e),
            severity=ErrorSeverity.ERROR,
            component="streamlit_reporting",
            source_file=filepath
        )
        st.error(f"Error reading file {filepath}: {str(e)}")
        return pd.DataFrame()

def load_json(filepath):
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            data = json.load(f)
        return pd.DataFrame(data)
    except FileNotFoundError:
        error_manager.create_error(
            code=ErrorCode.FILE_NOT_FOUND,
            message=f"File {filepath} not found",
            severity=ErrorSeverity.ERROR,
            component="streamlit_reporting",
            source_file=filepath
        )
        st.error(f"File {filepath} not found")
        return pd.DataFrame()
    except Exception as e:
        error_manager.create_error(
            code=ErrorCode.FILE_CORRUPTED,
            message=str(e),
            severity=ErrorSeverity.ERROR,
            component="streamlit_reporting",
            source_file=filepath
        )
        st.error(f"Error reading file {filepath}: {str(e)}")
        return pd.DataFrame()

def find_file(filename, extensions=['.csv', '.json']):
    search_dirs = [DATA_DIR, LOAD_DATA_DIR]
    
    if os.path.splitext(filename)[1] in extensions:
        for directory in search_dirs:
            filepath = os.path.join(directory, filename)
            if os.path.isfile(filepath):
                return filepath
                
        for directory in search_dirs:
            if os.path.exists(directory):
                for subdir in os.listdir(directory):
                    subdir_path = os.path.join(directory, subdir)
                    if os.path.isdir(subdir_path):
                        filepath = os.path.join(subdir_path, filename)
                        if os.path.isfile(filepath):
                            return filepath
    else:
        for ext in extensions:
            file_with_ext = filename + ext
            for directory in search_dirs:
                filepath = os.path.join(directory, file_with_ext)
                if os.path.isfile(filepath):
                    return filepath
                    
                if os.path.exists(directory):
                    for subdir in os.listdir(directory):
                        subdir_path = os.path.join(directory, subdir)
                        if os.path.isdir(subdir_path):
                            filepath = os.path.join(subdir_path, file_with_ext)
                            if os.path.isfile(filepath):
                                return filepath
                                
                table_dir = os.path.join(directory, filename)
                if os.path.isdir(table_dir):
                    filepath = os.path.join(table_dir, file_with_ext)
                    if os.path.isfile(filepath):
                        return filepath
    
    return None

def find_and_load_dataframe(table_name):
    file_path = find_file(f"{table_name}.csv")
    if file_path:
        return load_csv(file_path)
    
    file_path = find_file(f"{table_name}.json")
    if file_path:
        return load_json(file_path)
    
    if table_name == "reporting_weather_data":
        file_path = find_file("weather_data_import.csv")
    elif table_name == "reporting_covid_19_data":
        file_path = find_file("covid_19_data_import.csv")
    elif table_name == "reporting_import_log":
        file_path = find_file("import_log.csv")
    
    if file_path:
        return load_csv(file_path)
    
    return pd.DataFrame()

def load_all_data():
    data = {}
    
    tables = [
        "reporting_weather_data",
        "reporting_covid_19_data",
        "reporting_transform_log",
        "reporting_import_log",
        "reporting_api_import_log"
    ]
    
    for table in tables:
        df = find_and_load_dataframe(table)
        if not df.empty:
            data[table] = df
    
    if not data:
        st.warning("No data files found. Creating demo data for demonstration.")
        return create_demo_data()
    
    return data

def create_demo_data():
    demo_data = {}
    
    import_log_data = {
        "id": range(1, 6),
        "batch_date": [f"2023-01-0{i}" for i in range(1, 6)],
        "country_id": [1, 1, 1, 1, 1],
        "import_directory_name": ["api"] * 5,
        "import_file_name": [f"batch_{i}.csv" for i in range(1, 6)],
        "file_created_date": [f"2023-01-0{i} 10:00:00" for i in range(1, 6)],
        "file_last_modified_date": [f"2023-01-0{i} 11:00:00" for i in range(1, 6)],
        "row_count": [100, 120, 95, 110, 105]
    }
    demo_data["reporting_import_log"] = pd.DataFrame(import_log_data)
    
    transform_log_data = {
        "id": range(1, 6),
        "batch_date": [f"2023-01-0{i}" for i in range(1, 6)],
        "country_id": [1, 1, 1, 1, 1],
        "processed_directory_name": ["processed"] * 5,
        "processed_file_name": [f"processed_batch_{i}.csv" for i in range(1, 6)],
        "row_count": [90, 110, 85, 100, 95],
        "status": ["success", "success", "warning", "success", "success"]
    }
    demo_data["reporting_transform_log"] = pd.DataFrame(transform_log_data)
    
    api_log_data = {
        "id": range(1, 6),
        "country_id": [1, 1, 1, 1, 1],
        "api_id": [1, 1, 1, 1, 1],
        "start_time": [f"2023-01-0{i} 09:00:00" for i in range(1, 6)],
        "end_time": [f"2023-01-0{i} 09:15:00" for i in range(1, 6)],
        "code_response": [200, 200, 404, 200, 200],
        "error_messages": ["", "", "Not found", "", ""]
    }
    demo_data["reporting_api_import_log"] = pd.DataFrame(api_log_data)
    
    weather_data = {
        "country_id": [1] * 5,
        "date": [f"2023-01-0{i}" for i in range(1, 6)],
        "tavg": [5.2, 6.1, 4.8, 3.9, 5.7],
        "prcp": [0.0, 1.2, 2.5, 0.7, 0.0],
        "pres": [1013, 1010, 1008, 1015, 1012],
        "tsun": [4.5, 3.2, 1.5, 2.8, 5.0]
    }
    demo_data["reporting_weather_data"] = pd.DataFrame(weather_data)
    
    covid_data = {
        "country_id": [1] * 5,
        "date": [f"2023-01-0{i}" for i in range(1, 6)],
        "cases": [150, 165, 180, 172, 190],
        "deaths": [2, 3, 2, 4, 3],
        "recovered": [120, 130, 145, 135, 150]
    }
    demo_data["reporting_covid_19_data"] = pd.DataFrame(covid_data)
    
    return demo_data

def render_import_logs(data):
    st.header("Data import logs")
    
    col1, col2 = st.columns([3, 1])
    
    with col1:
        if st.checkbox("Show import log data", value=True):
            df = data["reporting_import_log"].copy()
            
            for col in ['batch_date', 'file_created_date', 'file_last_modified_date']:
                if col in df.columns:
                    try:
                        df[col] = pd.to_datetime(df[col])
                    except:
                        pass
            
            if 'batch_date' in df.columns and not df['batch_date'].empty:
                try:
                    min_date = df['batch_date'].min()
                    max_date = df['batch_date'].max()
                    date_range = st.date_input("Select date range:", 
                                               (min_date.date() if isinstance(min_date, pd.Timestamp) else min_date,
                                                max_date.date() if isinstance(max_date, pd.Timestamp) else max_date))
                    
                    if isinstance(date_range, tuple) and len(date_range) == 2:
                        df = df[(df['batch_date'].dt.date >= date_range[0]) & 
                               (df['batch_date'].dt.date <= date_range[1])]
                except Exception as e:
                    st.error(f"Error filtering by date: {e}")
            
            st.dataframe(df)
    
    with col2:
        st.subheader("Statistics")
        df = data["reporting_import_log"]
        
        total_imports = len(df)
        total_rows = df['row_count'].sum() if 'row_count' in df.columns else 0
        
        st.metric("Total imports", total_imports)
        st.metric("Total lines", int(total_rows))
        
        if 'import_directory_name' in df.columns:
            source_counts = df['import_directory_name'].value_counts()
            source_data = pd.DataFrame({
                'Source': source_counts.index,
                'Quantity': source_counts.values
            })
            
            if not source_data.empty:
                st.subheader("Data sources")
                st.bar_chart(source_data.set_index('Source'))
    
    st.subheader("Dynamics of data import")
    try:
        df = data["reporting_import_log"].copy()
        
        if 'batch_date' in df.columns and 'row_count' in df.columns:
            df['batch_date'] = pd.to_datetime(df['batch_date'])
            df['date'] = df['batch_date'].dt.date
            
            daily_imports = df.groupby('date')['row_count'].sum().reset_index()
            
            fig = px.line(
                daily_imports, 
                x='date', 
                y='row_count',
                title='Number of rows by days',
                labels={'date': 'Date', 'row_count': 'Number of rows'},
                markers=True
            )
            
            fig.update_layout(
                xaxis_title='Date',
                yaxis_title='Number of rows',
                hovermode='x unified'
            )
            
            st.plotly_chart(fig, use_container_width=True)
    except Exception as e:
        st.error(f"Error while plotting import graph: {e}")

def render_transform_logs(data):
    st.header("Data transformation logs")
    
    col1, col2 = st.columns([3, 1])
    
    with col1:
        if st.checkbox("Show transformation log data", value=True):
            df = data["reporting_transform_log"].copy()
            
            for col in ['batch_date']:
                if col in df.columns:
                    try:
                        df[col] = pd.to_datetime(df[col])
                    except:
                        pass
            
            if 'status' in df.columns:
                statuses = df['status'].unique()
                selected_status = st.multiselect("Status:", 
                                                statuses, 
                                                default=list(statuses))
                if selected_status:
                    df = df[df['status'].isin(selected_status)]
            
            st.dataframe(df)
    
    with col2:
        st.subheader("Statistics")
        df = data["reporting_transform_log"]
        
        total_transforms = len(df)
        total_rows = df['row_count'].sum() if 'row_count' in df.columns else 0
        
        st.metric("Total transformations", total_transforms)
        st.metric("Total lines processed", int(total_rows))
        
        if 'status' in df.columns:
            status_counts = df['status'].value_counts()
            
            fig = px.pie(
                values=status_counts.values,
                names=status_counts.index,
                title="Distribution by status",
                color_discrete_sequence=px.colors.qualitative.Bold
            )
            st.plotly_chart(fig, use_container_width=True)
    
    if "reporting_import_log" in data:
        st.subheader("Comparison of import and transformation processes")
        try:
            import_df = data["reporting_import_log"].copy()
            import_df['batch_date'] = pd.to_datetime(import_df['batch_date'])
            import_df['date'] = import_df['batch_date'].dt.date
            import_daily = import_df.groupby('date')['row_count'].sum().reset_index()
            import_daily.columns = ['date', 'import_count']
            
            transform_df = data["reporting_transform_log"].copy()
            transform_df['batch_date'] = pd.to_datetime(transform_df['batch_date'])
            transform_df['date'] = transform_df['batch_date'].dt.date
            transform_daily = transform_df.groupby('date')['row_count'].sum().reset_index()
            transform_daily.columns = ['date', 'transform_count']
            
            merged_df = pd.merge(import_daily, transform_daily, on='date', how='outer').fillna(0)
            
            fig = go.Figure()
            
            fig.add_trace(go.Bar(
                x=merged_df['date'],
                y=merged_df['import_count'],
                name='Imported',
                marker_color='#3282B8'
            ))
            
            fig.add_trace(go.Bar(
                x=merged_df['date'],
                y=merged_df['transform_count'],
                name='Transformed',
                marker_color='#0F4C75'
            ))
            
            fig.update_layout(
                title='Comparison of the number of rows during import and transformation',
                xaxis_title='Date',
                yaxis_title='Number of rows',
                barmode='group',
                hovermode='x unified'
            )
            
            st.plotly_chart(fig, use_container_width=True)
            
            if not merged_df.empty:
                merged_df['loss'] = merged_df['import_count'] - merged_df['transform_count']
                merged_df['loss_percent'] = (merged_df['loss'] / merged_df['import_count'] * 100).fillna(0)
                
                avg_loss = merged_df['loss_percent'].mean()
                
                st.metric(
                    "Average data loss during transformation",
                    f"{avg_loss:.2f}%",
                    delta=None
                )
                
                loss_df = merged_df[['date', 'import_count', 'transform_count', 'loss', 'loss_percent']]
                loss_df.columns = ['Date', 'Imported', 'Transformed', 'Loss', '%\ loss']
                loss_df['%\ loss'] = loss_df['%\ loss'].round(2)
                
                st.dataframe(loss_df)
                
        except Exception as e:
            st.error(f"Error while comparing processes: {e}")

def render_api_logs(data):
    st.header("API import logs")
    
    if st.checkbox("API import logs", value=True):
        df = data["reporting_api_import_log"].copy()
        
        for col in ['start_time', 'end_time']:
            if col in df.columns:
                try:
                    df[col] = pd.to_datetime(df[col])
                except:
                    pass
        
        if 'start_time' in df.columns and 'end_time' in df.columns:
            df['execution_time'] = (df['end_time'] - df['start_time']).dt.total_seconds()
        
        st.dataframe(df)
    
    if 'code_response' in data["reporting_api_import_log"].columns:
        st.subheader("API Response Code Statistics")
        
        code_stats = data["reporting_api_import_log"]['code_response'].value_counts().reset_index()
        code_stats.columns = ['Response code', 'Quantity']
        
        def get_color(code):
            if 200 <= code < 300:
                return 'green'
            elif 300 <= code < 400:
                return 'blue'
            elif 400 <= code < 500:
                return 'orange'
            else:
                return 'red'
        
        code_stats['Color'] = code_stats['Response code'].apply(get_color)
        
        # Создаем график
        fig = px.bar(
            code_stats, 
            x='Response code', 
            y='Quantity',
            color='Color',
            color_discrete_map={
                'green': '#4CAF50', 
                'blue': '#2196F3', 
                'orange': '#FF9800', 
                'red': '#F44336'
            },
            title='Distribution of API response codes'
        )
        
        st.plotly_chart(fig, use_container_width=True)
    
    if 'start_time' in data["reporting_api_import_log"].columns and 'end_time' in data["reporting_api_import_log"].columns:
        st.subheader("API Request Execution Time")
        
        try:
            df = data["reporting_api_import_log"].copy()
            df['start_time'] = pd.to_datetime(df['start_time'])
            df['end_time'] = pd.to_datetime(df['end_time'])
            df['execution_time'] = (df['end_time'] - df['start_time']).dt.total_seconds()
            df['date'] = df['start_time'].dt.date
            
            fig = px.line(
                df, 
                x='start_time', 
                y='execution_time',
                markers=True,
                title='API Request Execution Time',
                labels={'start_time': 'Request time', 'execution_time': 'Execution time (sec)'}
            )
            
            st.plotly_chart(fig, use_container_width=True)
            
            avg_time = df['execution_time'].mean()
            max_time = df['execution_time'].max()
            min_time = df['execution_time'].min()
            
            col1, col2, col3 = st.columns(3)
            col1.metric("Average time (sec)", f"{avg_time:.2f}")
            col2.metric("Maximum time (sec)", f"{max_time:.2f}")
            col3.metric("Minimum time (sec)", f"{min_time:.2f}")
            
        except Exception as e:
            st.error(f"Error while analyzing execution time: {e}")

def render_weather_data(data):
    st.header("Weather data")
    
    df = data["reporting_weather_data"].copy()
    
    if 'date' in df.columns:
        df['date'] = pd.to_datetime(df['date'])
    
    col1, col2 = st.columns([3, 1])
    
    with col1:
        if st.checkbox("Show weather data", value=True):
            if 'country_id' in df.columns and df['country_id'].nunique() > 1:
                countries = df['country_id'].unique()
                selected_country = st.selectbox("Select country:", countries, key="weather_country_selector")
                filtered_df = df[df['country_id'] == selected_country]
            else:
                filtered_df = df
            
            if 'date' in filtered_df.columns:
                min_date = filtered_df['date'].min()
                max_date = filtered_df['date'].max()
                date_range = st.date_input(
                    "Select date range:", 
                    (min_date.date() if isinstance(min_date, pd.Timestamp) else min_date,
                     max_date.date() if isinstance(max_date, pd.Timestamp) else max_date)
                )
                
                if isinstance(date_range, tuple) and len(date_range) == 2:
                    filtered_df = filtered_df[
                        (filtered_df['date'].dt.date >= date_range[0]) & 
                        (filtered_df['date'].dt.date <= date_range[1])
                    ]
            
            st.dataframe(filtered_df)
    
    with col2:
        st.subheader("Statistics")
        
        if 'tavg' in df.columns:
            avg_temp = df['tavg'].mean()
            max_temp = df['tavg'].max()
            min_temp = df['tavg'].min()
            
            st.metric("Average temperature", f"{avg_temp:.1f}°C")
            st.metric("Maximum temperature", f"{max_temp:.1f}°C") 
            st.metric("Minimum temperature", f"{min_temp:.1f}°C")
        
        if 'prcp' in df.columns:
            total_precip = df['prcp'].sum()
            max_precip = df['prcp'].max()
            
            st.metric("Total precipitation", f"{total_precip:.1f} мм")
            st.metric("Maximum amount of precipitation", f"{max_precip:.1f} мм")
    
    st.subheader("Temperature and precipitation dynamics")
    
    try:
        if 'date' in df.columns and ('tavg' in df.columns or 'prcp' in df.columns):
            plot_df = df.sort_values('date')
            
            fig = make_subplots(specs=[[{"secondary_y": True}]])
            
            if 'tavg' in df.columns:
                fig.add_trace(
                    go.Scatter(
                        x=plot_df['date'], 
                        y=plot_df['tavg'], 
                        name="Temperature",
                        line=dict(color="#FF5722", width=2),
                        mode='lines+markers'
                    ),
                    secondary_y=False
                )
            
            if 'prcp' in df.columns:
                fig.add_trace(
                    go.Bar(
                        x=plot_df['date'], 
                        y=plot_df['prcp'], 
                        name="Precipitation",
                        marker_color='rgba(0, 119, 182, 0.6)'
                    ),
                    secondary_y=True
                )
                
            fig.update_layout(
                title_text="Temperature and Precipitation",
                hovermode="x unified"
            )
            fig.update_xaxes(title_text="Date")
            fig.update_yaxes(title_text="Temperature (°C)", secondary_y=False)
            fig.update_yaxes(title_text="Precipitation (мм)", secondary_y=True)
            
            st.plotly_chart(fig, use_container_width=True)
            
            if 'tavg' in df.columns and len(df) > 30:
                st.subheader("Heat map of temperature by month")
                
                plot_df['month'] = plot_df['date'].dt.month
                plot_df['day'] = plot_df['date'].dt.day
                plot_df['year'] = plot_df['date'].dt.year
                
                if plot_df['year'].nunique() > 1:
                    pivot_df = plot_df.pivot_table(
                        index='month', 
                        columns='year', 
                        values='tavg', 
                        aggfunc='mean'
                    )
                    
                    month_names = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun',
                                    'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
                    
                    fig = px.imshow(
                        pivot_df,
                        labels=dict(x="Year", y="Month", color="Temperature"),
                        y=[month_names[i-1] for i in pivot_df.index],
                        x=pivot_df.columns,
                        color_continuous_scale='RdBu_r',
                        aspect="auto"
                    )
                    
                    fig.update_layout(title="Average temperature by month and year")
                    st.plotly_chart(fig, use_container_width=True)
        
    except Exception as e:
        st.error(f"Error while plotting weather chart: {e}")
    
    if 'pres' in df.columns or 'tsun' in df.columns:
        st.subheader("Additional meteorological parameters")
        
        tabs = st.tabs(["Pressure", "Solar Time", "Correlations"])
        
        with tabs[0]:
            if 'pres' in df.columns:
                st.caption("Atmospheric pressure")
                
                fig = px.line(
                    df, 
                    x='date', 
                    y='pres',
                    title='Dynamics of atmospheric pressure',
                    labels={'date': 'Date', 'pres': 'Pressure (hPa)'},
                    markers=True
                )
                
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("Atmospheric pressure data is not available")
        
        with tabs[1]:
            if 'tsun' in df.columns:
                st.caption("Time for sunshine")
                
                fig = px.area(
                    df, 
                    x='date', 
                    y='tsun',
                    title='Sunshine duration',
                    labels={'date': 'Date', 'tsun': 'Hours'},
                    color_discrete_sequence=['#FFC107']
                )
                
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("No sunshine hours data available")
        
        with tabs[2]:
            st.caption("Correlation of meteorological parameters")
            
            numeric_cols = df.select_dtypes(include=['float64', 'int64']).columns.tolist()
            
            if len(numeric_cols) > 1:
                corr_matrix = df[numeric_cols].corr()
                
                fig = px.imshow(
                    corr_matrix,
                    text_auto=True,
                    aspect="auto",
                    color_continuous_scale='RdBu_r'
                )
                
                fig.update_layout(title="Correlation matrix of meteorological parameters")
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("Not enough numerical parameters for correlation analysis")

def render_covid_data(data):
    st.header("COVID-19 data")
    
    df = data["reporting_covid_19_data"].copy()
    
    if 'date' in df.columns:
        df['date'] = pd.to_datetime(df['date'])
    
    col1, col2 = st.columns([3, 1])
    
    with col1:
        if st.checkbox("Show COVID-19 data", value=True):
            if 'country_id' in df.columns and df['country_id'].nunique() > 1:
                countries = df['country_id'].unique()
                selected_country = st.selectbox("Select country:", countries, key="covid_country_selector")
                filtered_df = df[df['country_id'] == selected_country]
            else:
                filtered_df = df
            
            if 'date' in filtered_df.columns:
                min_date = filtered_df['date'].min()
                max_date = filtered_df['date'].max()
                date_range = st.date_input(
                    "Select date range for COVID-19:", 
                    (min_date.date() if isinstance(min_date, pd.Timestamp) else min_date,
                     max_date.date() if isinstance(max_date, pd.Timestamp) else max_date),
                    key="covid_date_range"
                )
                
                if isinstance(date_range, tuple) and len(date_range) == 2:
                    filtered_df = filtered_df[
                        (filtered_df['date'].dt.date >= date_range[0]) & 
                        (filtered_df['date'].dt.date <= date_range[1])
                    ]
            
            st.dataframe(filtered_df)
    
    with col2:
        st.subheader("Statistics")
        
        if 'cases' in df.columns:
            total_cases = df['cases'].sum()
            max_daily_cases = df['cases'].max()
            
            st.metric("Total cases", f"{int(total_cases):,}")
            st.metric("Maximum per day", f"{int(max_daily_cases):,}")
        
        if 'deaths' in df.columns:
            total_deaths = df['deaths'].sum()
            max_daily_deaths = df['deaths'].max()
            
            st.metric("Total deaths", f"{int(total_deaths):,}")
            st.metric("Maximum deaths per day", f"{int(max_daily_deaths):,}")
            
            if 'cases' in df.columns and total_cases > 0:
                mortality_rate = (total_deaths / total_cases) * 100
                st.metric("Mortality", f"{mortality_rate:.2f}%")
    
    st.subheader("Dynamics of COVID-19 cases")
    
    try:
        if 'date' in df.columns and 'cases' in df.columns:
            plot_df = df.sort_values('date')
            
            fig = go.Figure()
            
            fig.add_trace(go.Scatter(
                x=plot_df['date'],
                y=plot_df['cases'],
                name='Cases',
                mode='lines',
                line=dict(color='#0077B6', width=2)
            ))
            
            if 'deaths' in df.columns:
                fig.add_trace(go.Scatter(
                    x=plot_df['date'],
                    y=plot_df['deaths'],
                    name='Deaths',
                    mode='lines',
                    line=dict(color='#D62828', width=2)
                ))
            
            if 'recovered' in df.columns:
                fig.add_trace(go.Scatter(
                    x=plot_df['date'],
                    y=plot_df['recovered'],
                    name='Recovered',
                    mode='lines',
                    line=dict(color='#2A9D8F', width=2)
                ))
            
            fig.update_layout(
                title='COVID-19 Dynamics',
                xaxis_title='Date',
                yaxis_title='Quantity',
                hovermode='x unified',
                legend=dict(
                    orientation="h",
                    yanchor="bottom",
                    y=1.02,
                    xanchor="center",
                    x=0.5
                )
            )
            
            st.plotly_chart(fig, use_container_width=True)
            
            st.subheader("Rate of growth of morbidity")
            
            plot_df['growth_rate'] = plot_df['cases'].pct_change() * 100
            
            fig = px.bar(
                plot_df, 
                x='date', 
                y='growth_rate',
                title='Daily increase in COVID-19 cases (%)',
                labels={'date': 'Date', 'growth_rate': 'Increase (%)'},
                color='growth_rate',
                color_continuous_scale=['green', 'yellow', 'red']
            )
            
            fig.update_layout(hovermode='x unified')
            st.plotly_chart(fig, use_container_width=True)
            
    except Exception as e:
        st.error(f"Error in plotting COVID-19 chart: {e}")
    
    st.subheader("Disease trends (moving average)")
    
    try:
        if 'date' in df.columns and 'cases' in df.columns:
            plot_df = df.sort_values('date').copy()
            
            window_size = st.slider("Moving Average Period (days)", 3, 14, 7)
            
            plot_df['cases_ma'] = plot_df['cases'].rolling(window=window_size).mean()
            
            if 'deaths' in plot_df.columns:
                plot_df['deaths_ma'] = plot_df['deaths'].rolling(window=window_size).mean()
            
            fig = go.Figure()
            
            fig.add_trace(go.Scatter(
                x=plot_df['date'],
                y=plot_df['cases'],
                name='Cases (fact)',
                mode='markers',
                marker=dict(color='rgba(0, 119, 182, 0.3)', size=5)
            ))
            
            fig.add_trace(go.Scatter(
                x=plot_df['date'],
                y=plot_df['cases_ma'],
                name=f'cases (СС-{window_size})',
                mode='lines',
                line=dict(color='#0077B6', width=3)
            ))
            
            if 'deaths' in plot_df.columns and 'deaths_ma' in plot_df.columns:
                fig.add_trace(go.Scatter(
                    x=plot_df['date'],
                    y=plot_df['deaths'],
                    name='Deaths (fact)',
                    mode='markers',
                    marker=dict(color='rgba(214, 40, 40, 0.3)', size=5)
                ))
                
                fig.add_trace(go.Scatter(
                    x=plot_df['date'],
                    y=plot_df['deaths_ma'],
                    name=f'Deaths (СС-{window_size})',
                    mode='lines',
                    line=dict(color='#D62828', width=3)
                ))
            
            fig.update_layout(
                title=f'Moving average for {window_size} days',
                xaxis_title='Date',
                yaxis_title='Quantity',
                hovermode='x unified',
                legend=dict(
                    orientation="h",
                    yanchor="bottom",
                    y=1.02,
                    xanchor="center",
                    x=0.5
                )
            )
            
            st.plotly_chart(fig, use_container_width=True)
            
    except Exception as e:
        st.error(f"Error while plotting trend chart: {e}")

def render_correlation_analysis(weather_data, covid_data):
    st.header("Correlation analysis of data")
    
    weather_df = weather_data.copy()
    covid_df = covid_data.copy()
    
    if 'date' not in weather_df.columns or 'date' not in covid_df.columns:
        st.error("The necessary data for correlation analysis are missing")
        return
    
    weather_df['date'] = pd.to_datetime(weather_df['date'])
    covid_df['date'] = pd.to_datetime(covid_df['date'])
    
    merged_df = pd.merge(weather_df, covid_df, on=['date', 'country_id'] if 'country_id' in weather_df.columns and 'country_id' in covid_df.columns else 'date', suffixes=('_weather', '_covid'))
    
    if merged_df.empty:
        st.warning("No overlapping data for correlation analysis")
        return
    
    st.subheader("Selecting parameters for analysis")
    
    col1, col2 = st.columns(2)
    
    with col1:
        weather_numeric_cols = weather_df.select_dtypes(include=['float64', 'int64']).columns.tolist()
        if 'country_id' in weather_numeric_cols:
            weather_numeric_cols.remove('country_id')
        
        weather_param = st.selectbox("Weather parameter:", weather_numeric_cols)
    
    with col2:
        covid_numeric_cols = covid_df.select_dtypes(include=['float64', 'int64']).columns.tolist()
        if 'country_id' in covid_numeric_cols:
            covid_numeric_cols.remove('country_id')
            
        covid_param = st.selectbox("COVID-19 parameter:", covid_numeric_cols)
    
    min_date = merged_df['date'].min()
    max_date = merged_df['date'].max()
    
    date_range = st.date_input(
        "Select a date range to analyze:", 
        (min_date.date() if isinstance(min_date, pd.Timestamp) else min_date,
         max_date.date() if isinstance(max_date, pd.Timestamp) else max_date),
        key="correlation_date_range"
    )
    
    if isinstance(date_range, tuple) and len(date_range) == 2:
        merged_df = merged_df[
            (merged_df['date'].dt.date >= date_range[0]) & 
            (merged_df['date'].dt.date <= date_range[1])
        ]
    
    if merged_df.empty:
        st.warning("No data for the selected date range")
        return
    
    weather_col = weather_param
    covid_col = covid_param
    
    correlation = merged_df[weather_col].corr(merged_df[covid_col])
    
    st.subheader("Results of correlation analysis")
    
    corr_color = 'green' if abs(correlation) > 0.7 else ('orange' if abs(correlation) > 0.3 else 'red')
    
    st.markdown(f"""
    <div style="
        padding: 20px; 
        border-radius: 10px; 
        background-color: #f1f1f1; 
        margin-bottom: 20px;
        text-align: center;
    ">
        <h3>Correlation coefficient</h3>
        <div style="
            font-size: 36px; 
            font-weight: bold;
            color: {corr_color};
        ">
            {correlation:.3f}
        </div>
        <p>Между {weather_col} и {covid_col}</p>
    </div>
    """, unsafe_allow_html=True)
    
    st.subheader("Interpretation of correlation")
    
    if abs(correlation) > 0.7:
        interpret = "Strong"
        desc = "There is a strong relationship between the selected parameters."
    elif abs(correlation) > 0.5:
        interpret = "Moderate"
        desc = "There is a moderate relationship between the selected parameters."
    elif abs(correlation) > 0.3:
        interpret = "Weak"
        desc = "There is a weak relationship between the selected parameters."
    else:
        interpret = "Very weak or absent"
        desc = "The relationship between the selected parameters is very weak or absent."
    
    direction = "positive" if correlation > 0 else "negative"
    
    st.info(f"**{interpret} {direction} correlation**. {desc}")
    
    st.subheader("Visualizing relationships between parameters")
    
    fig = px.scatter(
        merged_df, 
        x=weather_col, 
        y=covid_col,
        trendline="ols",
        labels={weather_col: weather_col, covid_col: covid_col},
        title=f"Dependence of {covid_col} on {weather_col}"
    )
    
    fig.update_layout(
        xaxis_title=weather_col,
        yaxis_title=covid_col
    )
    
    st.plotly_chart(fig, use_container_width=True)
    
    st.subheader("Dynamics of indicators over time")
    
    fig = make_subplots(specs=[[{"secondary_y": True}]])
    
    fig.add_trace(
        go.Scatter(
            x=merged_df['date'],
            y=merged_df[weather_col],
            name=weather_col,
            line=dict(color="#009688", width=2)
        ),
        secondary_y=False
    )
    
    fig.add_trace(
        go.Scatter(
            x=merged_df['date'],
            y=merged_df[covid_col],
            name=covid_col,
            line=dict(color="#E91E63", width=2)
        ),
        secondary_y=True
    )
    
    fig.update_layout(
        title_text=f"Dynamics of {weather_col} and {covid_col} over time",
        hovermode="x unified"
    )
    fig.update_xaxes(title_text="Date")
    fig.update_yaxes(title_text=weather_col, secondary_y=False)
    fig.update_yaxes(title_text=covid_col, secondary_y=True)
    
    st.plotly_chart(fig, use_container_width=True)
    
    st.subheader("Additional statistics")
    
    st.dataframe(merged_df[['date', weather_col, covid_col]])
    
    stats_df = pd.DataFrame({
        "Parameter": [weather_col, covid_col],
        "Minimum": [merged_df[weather_col].min(), merged_df[covid_col].min()],
        "Maximum": [merged_df[weather_col].max(), merged_df[covid_col].max()],
        "Average": [merged_df[weather_col].mean(), merged_df[covid_col].mean()],
        "Median": [merged_df[weather_col].median(), merged_df[covid_col].median()],
        "Standard deviation": [merged_df[weather_col].std(), merged_df[covid_col].std()]
    })
    
    st.dataframe(stats_df)

    st.subheader("Lag correlation")
    st.info("Correlation analysis taking into account time shift (lag)")
    
    max_lag = min(14, len(merged_df) // 2)
    lag_days = st.slider("Quantity of days of lag (shift):", 0, max_lag, 0)
    
    if lag_days > 0:
        lagged_df = merged_df.copy()
        lagged_df[f'{covid_col}_lagged'] = lagged_df[covid_col].shift(-lag_days)
        
        lagged_df = lagged_df.dropna()
        
        if not lagged_df.empty:
            lag_correlation = lagged_df[weather_col].corr(lagged_df[f'{covid_col}_lagged'])
            
            lag_corr_color = 'green' if abs(lag_correlation) > 0.7 else ('orange' if abs(lag_correlation) > 0.3 else 'red')
            
            st.markdown(f"""
            <div style="
                padding: 20px; 
                border-radius: 10px; 
                background-color: #f1f1f1; 
                margin-bottom: 20px;
                text-align: center;
            ">
                <h3>Lag correlation (shift {lag_days} days)</h3>
                <div style="
                    font-size: 36px; 
                    font-weight: bold;
                    color: {lag_corr_color};
                ">
                    {lag_correlation:.3f}
                </div>
                <p>Between {weather_col} and {covid_col} with a lag of {lag_days} days</p>
            </div>
            """, unsafe_allow_html=True)
            
            fig = px.scatter(
                lagged_df, 
                x=weather_col, 
                y=f'{covid_col}_lagged',
                trendline="ols",
                labels={weather_col: weather_col, f'{covid_col}_lagged': f'{covid_col} (lag {lag_days} days)'},
                title=f"Dependence of {covid_col} on {weather_col} with a lag of {lag_days} days"
            )
            
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.warning(f"Not enough data for analysis with a lag of {lag_days} days")

def render_prediction_interface():
    from src.predictor.covid_predictor import predict_cases
    st.header("Prediction of COVID-19 Cases Based on Weather")

    tavg = st.slider("Average Temperature (°C)", -30.0, 50.0, 15.0)
    prcp = st.slider("Precipitation (mm)", 0.0, 100.0, 10.0)
    pressure = st.slider("Pressure (hPa)", 800.0, 1100.0, 1013.25)

    if st.button("Predict"):
        result = predict_cases("covid_model.pkl", tavg, prcp, pressure)
        st.success(f"Predicted number of cases: {int(result)}")


# === MAIN APP ===
def main():
    st.sidebar.title("Settings")
    
    st.sidebar.subheader("Information about data:")
    st.sidebar.markdown(f"**Data folder 1:** `{DATA_DIR}`")
    st.sidebar.markdown(f"**Data folder 2:** `{LOAD_DATA_DIR}`")
    
    data = load_all_data()
    
    st.sidebar.subheader("Found datasets:")
    for name in data.keys():
        st.sidebar.markdown(f" `{name}`")
    
    tabs = st.tabs([
        "Overview",
        "Import",
        "Transformation",
        "API",
        "Weather",
        "COVID-19",
        "Correlation",
        "AI Prediction"
    ])
    
    with tabs[0]:
        st.header("General overview of the data")
        
        st.subheader("Statistics datasets")
        
        stats_cols = st.columns(len(data))
        
        for i, (name, df) in enumerate(data.items()):
            with stats_cols[i]:
                display_name = name.replace("reporting_", "").replace("_", " ").title()
                st.metric(display_name, f"{len(df)} records")
                
                if 'row_count' in df.columns:
                    total_rows = df['row_count'].sum()
                    st.metric("Data rows", f"{int(total_rows):,}")
        
        st.subheader("ETL process activity")
        
        activity_data = {}
        date_range = []
        
        for log_type in ["reporting_import_log", "reporting_transform_log", "reporting_api_import_log"]:
            if log_type in data:
                df = data[log_type].copy()
                if 'batch_date' in df.columns:
                    df['batch_date'] = pd.to_datetime(df['batch_date'])
                    df['date'] = df['batch_date'].dt.date
                    if 'row_count' in df.columns:
                        daily_counts = df.groupby('date')['row_count'].sum().reset_index()
                        daily_counts['log_type'] = log_type
                        activity_data[log_type] = daily_counts
                        date_range.extend(daily_counts['date'].tolist())
        
        if activity_data:
            activity_df = pd.concat(activity_data.values(), ignore_index=True)
            min_date = min(date_range)
            max_date = max(date_range)
            date_range_input = st.date_input(
                "Select a date range to analyze activities:", 
                (min_date, max_date),
                key="activity_date_range"
            )
            if isinstance(date_range_input, tuple) and len(date_range_input) == 2:
                activity_df = activity_df[
                    (activity_df['date'] >= date_range_input[0]) & 
                    (activity_df['date'] <= date_range_input[1])
                ]
            activity_summary = activity_df.groupby(['date', 'log_type'])['row_count'].sum().unstack(fill_value=0).reset_index()
            st.subheader("ETL Process Activity Graph")
            fig = px.line(
                activity_summary, 
                x='date', 
                y=activity_summary.columns[1:], 
                title='ETL process activity by day',
                labels={'date': 'Date', 'value': 'Rows quantity'},
                markers=True
            )
            fig.update_layout(
                xaxis_title='Date',
                yaxis_title='Rows quantity',
                hovermode='x unified'
            )
            st.plotly_chart(fig, use_container_width=True)
    
    with tabs[1]:
        render_import_logs(data)
    
    with tabs[2]:
        render_transform_logs(data)
    
    with tabs[3]:
        render_api_logs(data)
    
    with tabs[4]:
        render_weather_data(data)
    
    with tabs[5]:
        render_covid_data(data)
    
    with tabs[6]:
        if "reporting_weather_data" in data and "reporting_covid_19_data" in data:
            render_correlation_analysis(data["reporting_weather_data"], data["reporting_covid_19_data"])
        else:
            st.warning("Weather and COVID-19 data needed for correlation analysis")
    with tabs[7]:
        render_prediction_interface()

if __name__ == "__main__":
    main()