import requests
import pandas as pd
import time
from datetime import datetime, timedelta
import csv
import os
import json

class FloodDataExtractor:
    def __init__(self):
        self.base_url = "https://environment.data.gov.uk/flood-monitoring/id"
        self.data_dir = "flood_data"
        os.makedirs(self.data_dir, exist_ok=True)
    
    def extract_all_stations(self):
        """Extract all monitoring stations to CSV"""
        print("Extracting all monitoring stations...")
        all_stations = []
        offset = 0
        limit = 500
        
        while True:
            try:
                response = requests.get(
                    f"{self.base_url}/stations",
                    params={"_limit": limit, "_offset": offset},
                    timeout=30
                )
                response.raise_for_status()
                data = response.json()
                
                stations = data.get('items', [])
                if not stations:
                    break
                
                all_stations.extend(stations)
                offset += limit
                print(f"Extracted {len(stations)} stations...")
                
                time.sleep(0.1)
                
            except Exception as e:
                print(f"Error extracting stations: {e}")
                break
        
        # Save to CSV with dynamic headers
        if all_stations:
            # Get all unique fieldnames from all records
            all_fieldnames = set()
            for station in all_stations:
                all_fieldnames.update(station.keys())
            
            stations_csv_path = f"{self.data_dir}/stations.csv"
            with open(stations_csv_path, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=sorted(all_fieldnames))
                writer.writeheader()
                writer.writerows(all_stations)
            
            print(f"Saved {len(all_stations)} stations to {stations_csv_path}")
        
        return all_stations
    
    def extract_historical_readings(self, start_date="2010-01-01", end_date=None):
        """Extract historical readings to CSV"""
        if end_date is None:
            end_date = datetime.now().strftime('%Y-%m-%d')
        
        print(f"Extracting historical readings from {start_date} to {end_date}...")
        
        all_readings = []
        offset = 0
        limit = 1000
        
        while True:
            try:
                response = requests.get(
                    f"{self.base_url}/readings",
                    params={
                        "startdate": start_date,
                        "enddate": end_date,
                        "_limit": limit,
                        "_offset": offset,
                        "_sorted": "asc"
                    },
                    timeout=30
                )
                response.raise_for_status()
                data = response.json()
                
                readings = data.get('items', [])
                if not readings:
                    break
                
                all_readings.extend(readings)
                offset += limit
                print(f"Extracted {len(readings)} readings...")
                
                time.sleep(0.2)
                
            except Exception as e:
                print(f"Error extracting readings: {e}")
                break
        
        # Save to CSV with dynamic headers
        if all_readings:
            # Get all unique fieldnames from all records
            all_fieldnames = set()
            for reading in all_readings:
                all_fieldnames.update(reading.keys())
            
            readings_csv_path = f"{self.data_dir}/readings.csv"
            with open(readings_csv_path, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=sorted(all_fieldnames))
                writer.writeheader()
                writer.writerows(all_readings)
            
            print(f"Saved {len(all_readings)} readings to {readings_csv_path}")
        
        return all_readings
    
    def extract_flood_warnings(self):
        """Extract all flood warnings to CSV"""
        print("Extracting flood warnings...")
        
        all_floods = []
        offset = 0
        limit = 500
        
        while True:
            try:
                response = requests.get(
                    f"{self.base_url}/floods",
                    params={"_limit": limit, "_offset": offset},
                    timeout=30
                )
                response.raise_for_status()
                data = response.json()
                
                floods = data.get('items', [])
                if not floods:
                    break
                
                all_floods.extend(floods)
                offset += limit
                print(f"Extracted {len(floods)} flood warnings...")
                
                time.sleep(0.1)
                
            except Exception as e:
                print(f"Error extracting flood warnings: {e}")
                break
        
        # Save to CSV with dynamic headers
        if all_floods:
            # Get all unique fieldnames from all records
            all_fieldnames = set()
            for flood in all_floods:
                all_fieldnames.update(flood.keys())
            
            floods_csv_path = f"{self.data_dir}/flood_warnings.csv"
            with open(floods_csv_path, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=sorted(all_fieldnames))
                writer.writeheader()
                writer.writerows(all_floods)
            
            print(f"Saved {len(all_floods)} flood warnings to {floods_csv_path}")
        
        return all_floods

def run_full_extraction():
    """Run complete full load extraction"""
    print("🚀 Starting FULL LOAD extraction...")
    print("=" * 50)
    
    extractor = FloodDataExtractor()
    
    # Extract all data
    stations = extractor.extract_all_stations()
    readings = extractor.extract_historical_readings()
    floods = extractor.extract_flood_warnings()
    
    print("=" * 50)
    print("✅ Full load extraction complete!")
    print(f"🏞️  Stations: {len(stations)}")
    print(f"📊 Readings: {len(readings)}")
    print(f"⚠️   Flood warnings: {len(floods)}")
    
    # Save extraction timestamp
    with open(f"{extractor.data_dir}/last_full_extraction.txt", 'w') as f:
        f.write(datetime.now().isoformat())
    
    return stations, readings, floods

def run_incremental_extraction():
    """Run incremental extraction since last run"""
    print("🔄 Starting INCREMENTAL extraction...")
    print("=" * 50)
    
    extractor = FloodDataExtractor()
    
    # Get last extraction time
    last_extraction_file = f"{extractor.data_dir}/last_extraction.txt"
    try:
        with open(last_extraction_file, 'r') as f:
            last_extraction = f.read().strip()
    except FileNotFoundError:
        print("No previous extraction found. Running full load first...")
        return run_full_extraction()
    
    print(f"Extracting data since: {last_extraction}")
    
    # Extract new readings since last extraction
    new_readings = []
    offset = 0
    limit = 1000
    
    while True:
        try:
            response = requests.get(
                f"{extractor.base_url}/readings",
                params={
                    "since": last_extraction,
                    "_limit": limit,
                    "_offset": offset,
                    "_sorted": "asc"
                },
                timeout=30
            )
            response.raise_for_status()
            data = response.json()
            
            readings = data.get('items', [])
            if not readings:
                break
            
            new_readings.extend(readings)
            offset += limit
            print(f"Extracted {len(readings)} new readings...")
            
            time.sleep(0.2)
            
        except Exception as e:
            print(f"Error extracting incremental readings: {e}")
            break
    
    # Append to existing CSV with dynamic headers
    if new_readings:
        readings_csv_path = f"{extractor.data_dir}/readings.csv"
        
        # Read existing headers if file exists
        existing_headers = set()
        if os.path.exists(readings_csv_path):
            with open(readings_csv_path, 'r', encoding='utf-8') as f:
                reader = csv.reader(f)
                existing_headers = set(next(reader))
        
        # Get all fieldnames from new records
        new_fieldnames = set()
        for reading in new_readings:
            new_fieldnames.update(reading.keys())
        
        # Combine all fieldnames
        all_fieldnames = sorted(existing_headers.union(new_fieldnames))
        
        # Read existing data if any
        existing_data = []
        if os.path.exists(readings_csv_path):
            with open(readings_csv_path, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                existing_data = list(reader)
        
        # Write all data with updated headers
        with open(readings_csv_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=all_fieldnames)
            writer.writeheader()
            writer.writerows(existing_data + new_readings)
    
    # Update extraction timestamp
    with open(last_extraction_file, 'w') as f:
        f.write(datetime.now().isoformat())
    
    print("=" * 50)
    print(f"✅ Incremental extraction complete! Added {len(new_readings)} new readings")
    
    return new_readings

def check_data_status():
    """Check what data we have available"""
    extractor = FloodDataExtractor()
    
    print("📊 Data Status:")
    print("=" * 30)
    
    files = ['stations.csv', 'readings.csv', 'flood_warnings.csv']
    for file in files:
        filepath = f"{extractor.data_dir}/{file}"
        if os.path.exists(filepath):
            with open(filepath, 'r', encoding='utf-8') as f:
                reader = csv.reader(f)
                headers = next(reader)
                line_count = sum(1 for line in reader)
            print(f"{file}: {line_count} records, {len(headers)} columns")
        else:
            print(f"{file}: Not found")
    
    # Check last extraction time
    last_extraction_file = f"{extractor.data_dir}/last_extraction.txt"
    if os.path.exists(last_extraction_file):
        with open(last_extraction_file, 'r') as f:
            last_time = f.read().strip()
        print(f"Last extraction: {last_time}")

# Simple usage functions
def extract_data_once():
    """One-time function to extract all data (full load)"""
    return run_full_extraction()

def extract_new_data():
    """Function to extract only new data (incremental)"""
    return run_incremental_extraction()

# Main execution
if __name__ == "__main__":
    # Run full extraction (do this once)
    print("Running full data extraction...")
    stations, readings, floods = extract_data_once()
    
    # Check what we extracted
    check_data_status()