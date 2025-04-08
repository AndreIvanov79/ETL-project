import os
import csv
import json
import datetime
from src.db.db_manager import DBManager

OUTPUT_DIR = 'src/load/reporting_data'
os.makedirs(OUTPUT_DIR, exist_ok=True)

db = DBManager()

export_targets = {
    "reporting_weather_data": db.get_reporting_weather_data,
    "reporting_covid_19_data": db.get_reporting_covid_data,
    "reporting_transform_log": db.get_reporting_transform_log,
    "reporting_import_log": db.get_reporting_import_log,
    "reporting_api_import_log": db.get_reporting_api_import_log,
}

class DateTimeEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, (datetime.date, datetime.datetime)):
            return obj.isoformat()
        return super().default(obj)

def save_csv(filepath, headers, rows):
    with open(filepath, mode='w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        writer.writerows(rows)
    print(f"CSV file saved: {filepath}")

def save_json(filepath, headers, rows):
    data = [dict(zip(headers, row)) for row in rows]
    with open(filepath, mode='w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2, cls=DateTimeEncoder)
    print(f"JSON file saved: {filepath}")

def get_column_names(table_name):
    conn = db.get_connection()
    cursor = conn.cursor()
    
    try:
        db.execute_query(f"SELECT * FROM {table_name} LIMIT 1")
        return [desc[0] for desc in cursor.description]
    except Exception as e:
        print(f"Error getting table structure {table_name}: {e}")
        
        if table_name == "reporting_weather_data":
            return ["country_id", "date", "tavg", "prcp", "pres", "tsun"]
        elif table_name == "reporting_covid_19_data":
            return ["country_id", "date", "cases"]
        elif table_name == "reporting_transform_log":
            return ["id", "batch_date", "country_id", "processed_directory_name", 
                    "processed_file_name", "row_count", "status"]
        elif table_name == "reporting_import_log":
            return ["id", "batch_date", "country_id", "import_directory_name", 
                    "import_file_name", "file_created_date", "file_last_modified_date", "row_count"]
        elif table_name == "reporting_api_import_log":
            return ["id", "country_id", "api_id", "start_time", "end_time", 
                    "code_response", "error_messages"]
        else:
            return []

def process_table_data(table_name, rows):
    processed_rows = []
    
    for row in rows:
        processed_row = []
        for item in row:
            if isinstance(item, str) and (
                'date' in item or 'time' in item) and 'T' in item and ':' in item:
                try:
                    dt = datetime.datetime.fromisoformat(item)
                    processed_row.append(dt)
                except ValueError:
                    processed_row.append(item)
            else:
                processed_row.append(item)
        
        processed_rows.append(processed_row)
    
    return processed_rows

def save_table(table_name, rows):
    if not rows:
        print(f"No data in table: {table_name}")
        return

    headers = get_column_names(table_name)
    
    if not headers:
        print(f"Unable to determine table structure: {table_name}")
        return
    
    processed_rows = process_table_data(table_name, rows)
    
    table_dir = os.path.join(OUTPUT_DIR, table_name)
    os.makedirs(table_dir, exist_ok=True)
    
    csv_path = os.path.join(table_dir, f"{table_name}.csv")
    save_csv(csv_path, headers, processed_rows)

    json_path = os.path.join(table_dir, f"{table_name}.json")
    save_json(json_path, headers, processed_rows)

    print(f"Table data {table_name} is saved in CSV and JSON format")

def prepare_reporting_tables():
    try:
        print("Updating reporting tables...")
        result = db.prepare_reporting_tables()
        if result:
            print("Reporting tables updated successfully")
        else:
            print("Error updating reporting tables")
    except Exception as e:
        print(f"Unexpected error while preparing reporting tables: {e}")

if __name__ == "__main__":
    prepare_reporting_tables()
    
    for table, fetch_method in export_targets.items():
        print(f"Export table: {table}")
        try:
            rows = fetch_method()
            save_table(table, rows)
        except Exception as e:
            print(f"Export table {table} failed: {e}")
    
    print("Data export for reporting is complete.")
    