import requests
from datetime import datetime

def display_flood_data():
    """Display flood monitoring data from the UK Environment Agency"""
    
    print("UK Environment Agency Flood Monitoring Data")
    print("=" * 50)
    print(f"Date: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print()
    
    try:
        # Get monitoring stations
        print("🏞️  MONITORING STATIONS:")
        print("-" * 30)
        response = requests.get("https://environment.data.gov.uk/flood-monitoring/id/stations?&_limit=8", timeout=10)
        
        if response.status_code == 200:
            data = response.json()
            stations = data.get('items', [])
            
            for station in stations:
                name = station.get('label', 'Unknown')
                river = station.get('riverName', 'Unknown')
                town = station.get('town', '')
                status = station.get('status', 'Unknown')
                print(f"• {name}")
                print(f"  River: {river}, Town: {town}, Status: {status}")
                print()
        else:
            print("Could not fetch stations data")
            
    except Exception as e:
        print(f"Error fetching stations: {e}")
    
    try:
        # Get latest readings
        print("📊 LATEST READINGS:")
        print("-" * 30)
        response = requests.get("https://environment.data.gov.uk/flood-monitoring/id/readings?&_limit=10", timeout=10)
        
        if response.status_code == 200:
            data = response.json()
            readings = data.get('items', [])
            
            for reading in readings:
                station_id = reading.get('station', '').split('/')[-1]
                value = reading.get('value', 'N/A')
                unit = reading.get('unit', '')
                param = reading.get('parameterName', reading.get('parameter', 'Unknown'))
                time = reading.get('dateTime', '')[:16].replace('T', ' ') if reading.get('dateTime') else 'Unknown'
                print(f"• {station_id}: {value} {unit} ({param})")
                print(f"  Time: {time}")
                print()
        else:
            print("Could not fetch readings data")
            
    except Exception as e:
        print(f"Error fetching readings: {e}")
    
    try:
        # Get flood warnings
        print("⚠️  FLOOD WARNINGS:")
        print("-" * 30)
        response = requests.get("https://environment.data.gov.uk/flood-monitoring/id/floods?&_limit=5", timeout=10)
        
        if response.status_code == 200:
            data = response.json()
            floods = data.get('items', [])
            
            active_floods = [f for f in floods if f.get('isActive', False)]
            
            if not active_floods:
                print("No active flood warnings")
            else:
                for flood in active_floods:
                    severity = flood.get('severity', 'Unknown')
                    area = flood.get('floodArea', {}).get('name', 'Unknown area')
                    print(f"• {severity}: {area}")
        else:
            print("Could not fetch flood warnings")
            
    except Exception as e:
        print(f"Error fetching flood warnings: {e}")

# Run the function
display_flood_data()