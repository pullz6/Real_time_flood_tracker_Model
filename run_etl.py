import requests
import pandas as pd
import json
from datetime import datetime, timedelta
import time
import logging
from pathlib import Path

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class FloodETL:
    def __init__(self):
        self.base_url = "https://environment.data.gov.uk/flood-monitoring/id"
        self.data_dir = Path("data")
        self.raw_dir = self.data_dir / "raw"
        self.processed_dir = self.data_dir / "processed"
        self.features_dir = self.data_dir / "features"
        
        # Create directories
        for dir_path in [self.raw_dir, self.processed_dir, self.features_dir]:
            dir_path.mkdir(parents=True, exist_ok=True)
    
    def extract(self):
        """Extract data from API"""
        logging.info("Starting extraction...")
        
        # Extract stations
        stations = self._extract_paginated_data("stations", limit=500)
        self._save_json(stations, self.raw_dir / "stations.json")
        
        # Extract readings (last 90 days)
        end_date = datetime.now()
        start_date = end_date - timedelta(days=90)
        readings = self._extract_readings(start_date, end_date)
        self._save_json(readings, self.raw_dir / "readings.json")
        
        # Extract floods
        floods = self._extract_paginated_data("floods", limit=500)
        self._save_json(floods, self.raw_dir / "floods.json")
        
        logging.info(f"Extraction complete: {len(stations)} stations, {len(readings)} readings, {len(floods)} floods")
        return stations, readings, floods
    
    def _extract_paginated_data(self, endpoint, limit=500):
        """Extract paginated data from API"""
        all_data = []
        offset = 0
        
        while True:
            try:
                response = requests.get(
                    f"{self.base_url}/{endpoint}",
                    params={"_limit": limit, "_offset": offset},
                    timeout=30
                )
                data = response.json()
                items = data.get('items', [])
                
                if not items:
                    break
                
                all_data.extend(items)
                offset += limit
                logging.info(f"Extracted {len(items)} {endpoint}...")
                time.sleep(0.1)
                
            except Exception as e:
                logging.error(f"Error extracting {endpoint}: {e}")
                break
        
        return all_data
    
    def _extract_readings(self, start_date, end_date):
        """Extract readings with date range"""
        all_readings = []
        offset = 0
        limit = 1000
        
        while True:
            try:
                response = requests.get(
                    f"{self.base_url}/readings",
                    params={
                        "startdate": start_date.strftime('%Y-%m-%d'),
                        "enddate": end_date.strftime('%Y-%m-%d'),
                        "_limit": limit,
                        "_offset": offset,
                        "_sorted": "asc"
                    },
                    timeout=30
                )
                data = response.json()
                readings = data.get('items', [])
                
                if not readings:
                    break
                
                all_readings.extend(readings)
                offset += limit
                logging.info(f"Extracted {len(readings)} readings...")
                time.sleep(0.2)
                
            except Exception as e:
                logging.error(f"Error extracting readings: {e}")
                break
        
        return all_readings
    
    def transform(self):
        """Transform raw data into structured format"""
        logging.info("Starting transformation...")
        
        # Load raw data
        stations = self._load_json(self.raw_dir / "stations.json")
        readings = self._load_json(self.raw_dir / "readings.json")
        floods = self._load_json(self.raw_dir / "floods.json")
        
        # Transform data
        stations_df = self._transform_stations(stations)
        readings_df = self._transform_readings(readings)
        floods_df = self._transform_floods(floods)
        
        # Save processed data
        stations_df.to_csv(self.processed_dir / "stations.csv", index=False)
        readings_df.to_csv(self.processed_dir / "readings.csv", index=False)
        floods_df.to_csv(self.processed_dir / "floods.csv", index=False)
        
        logging.info("Transformation complete")
        return stations_df, readings_df, floods_df
    
    def _transform_stations(self, raw_stations):
        """Transform stations data"""
        transformed = []
        for station in raw_stations:
            transformed.append({
                'station_id': station.get('@id', '').split('/')[-1],
                'label': station.get('label'),
                'river_name': station.get('riverName'),
                'town': station.get('town'),
                'lat': station.get('lat'),
                'long': station.get('long'),
                'status': station.get('status'),
                'last_updated': datetime.now()
            })
        return pd.DataFrame(transformed)
    
    def _transform_readings(self, raw_readings):
        """Transform readings data"""
        transformed = []
        for reading in raw_readings:
            transformed.append({
                'reading_id': reading.get('@id', '').split('/')[-1],
                'station_id': reading.get('station', '').split('/')[-1],
                'datetime': reading.get('dateTime'),
                'value': reading.get('value'),
                'unit': reading.get('unit'),
                'parameter': reading.get('parameter'),
                'qualifier': reading.get('qualifier'),
                'extracted_at': datetime.now()
            })
        return pd.DataFrame(transformed)
    
    def _transform_floods(self, raw_floods):
        """Transform floods data"""
        transformed = []
        for flood in raw_floods:
            transformed.append({
                'flood_id': flood.get('@id', '').split('/')[-1],
                'severity': flood.get('severity'),
                'description': flood.get('description'),
                'is_active': flood.get('isActive', False),
                'area_name': flood.get('floodArea', {}).get('name'),
                'time_changed': flood.get('timeMessageChanged'),
                'extracted_at': datetime.now()
            })
        return pd.DataFrame(transformed)
    
    def create_features(self):
        """Create features for ML model"""
        logging.info("Creating features...")
        
        # Load processed data
        stations_df = pd.read_csv(self.processed_dir / "stations.csv")
        readings_df = pd.read_csv(self.processed_dir / "readings.csv")
        
        # Merge data
        merged = pd.merge(readings_df, stations_df, on='station_id', how='left')
        
        # Create time features
        merged['datetime'] = pd.to_datetime(merged['datetime'])
        merged['hour'] = merged['datetime'].dt.hour
        merged['day_of_week'] = merged['datetime'].dt.dayofweek
        merged['month'] = merged['datetime'].dt.month
        merged['year'] = merged['datetime'].dt.year
        
        # Create lag features
        merged.sort_values(['station_id', 'datetime'], inplace=True)
        merged['value_lag_1'] = merged.groupby('station_id')['value'].shift(1)
        merged['value_lag_24'] = merged.groupby('station_id')['value'].shift(24)
        
        # Save features
        merged.to_csv(self.features_dir / "features.csv", index=False)
        
        logging.info("Features created")
        return merged
    
    def _save_json(self, data, filepath):
        """Save data as JSON"""
        with open(filepath, 'w') as f:
            json.dump(data, f, indent=2)
    
    def _load_json(self, filepath):
        """Load data from JSON"""
        with open(filepath, 'r') as f:
            return json.load(f)
    
    def run_pipeline(self):
        """Run complete ETL pipeline"""
        logging.info("🚀 Starting ETL Pipeline")
        
        # Run all steps
        self.extract()
        self.transform()
        features_df = self.create_features()
        
        logging.info("✅ ETL Pipeline Complete")
        return features_df

# For direct execution
if __name__ == "__main__":
    etl = FloodETL()
    etl.run_pipeline()