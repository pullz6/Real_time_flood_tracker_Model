import requests
import pandas as pd
from datetime import datetime
import json
import sqlite3
from typing import Dict, List, Optional
import time

# Base URL for the API
BASE_URL = "https://environment.data.gov.uk/flood-monitoring/id"

def extract_stations() -> List[Dict]:
    """Extract all monitoring stations"""
    url = f"{BASE_URL}/stations"
    try:
        response = requests.get(url, params={"_limit": 5000})
        response.raise_for_status()
        return response.json()['items']
    except requests.exceptions.RequestException as e:
        print(f"Error extracting stations: {e}")
        return []

def extract_station_measures(station_id: str) -> List[Dict]:
    """Extract measures for a specific station"""
    url = f"{BASE_URL}/stations/{station_id}/measures"
    try:
        response = requests.get(url)
        response.raise_for_status()
        return response.json()['items']
    except requests.exceptions.RequestException as e:
        print(f"Error extracting measures for station {station_id}: {e}")
        return []

def extract_latest_readings() -> List[Dict]:
    """Extract latest readings from all stations"""
    url = f"{BASE_URL}/readings"
    try:
        response = requests.get(url, params={
            "_limit": 1000,
            "_sorted": "desc",
            "today": datetime.now().date().isoformat()
        })
        response.raise_for_status()
        return response.json()['items']
    except requests.exceptions.RequestException as e:
        print(f"Error extracting latest readings: {e}")
        return []

def extract_historic_readings(station_id: str, measure_id: str, 
                            days: int = 7) -> List[Dict]:
    """Extract historic readings for a specific measure"""
    url = f"{BASE_URL}/readings"
    since_date = (datetime.now() - pd.Timedelta(days=days)).date().isoformat()
    
    try:
        response = requests.get(url, params={
            "station": station_id,
            "measure": measure_id,
            "since": since_date,
            "_sorted": "asc",
            "_limit": 10000
        })
        response.raise_for_status()
        return response.json()['items']
    except requests.exceptions.RequestException as e:
        print(f"Error extracting historic readings: {e}")
        return []