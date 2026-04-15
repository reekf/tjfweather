import os
import json
import time
import requests
import datetime
import re

CITIES = [
    {"name": "New York", "lat": 40.71, "lon": -74.00, "rank": 1, "icao": "KLGA"},
    {"name": "Los Angeles", "lat": 34.05, "lon": -118.24, "rank": 1, "icao": "KLAX"},
    {"name": "Chicago", "lat": 41.87, "lon": -87.62, "rank": 1, "icao": "KORD"},
    {"name": "Houston", "lat": 29.76, "lon": -95.36, "rank": 1, "icao": "KIAH"},
    {"name": "Phoenix", "lat": 33.44, "lon": -112.07, "rank": 1, "icao": "KPHX"},
    {"name": "Philadelphia", "lat": 39.95, "lon": -75.16, "rank": 1, "icao": "KPHL"},
    {"name": "San Antonio", "lat": 29.42, "lon": -98.49, "rank": 1, "icao": "KSAT"},
    {"name": "San Diego", "lat": 32.71, "lon": -117.16, "rank": 1, "icao": "KSAN"},
    {"name": "Dallas", "lat": 32.77, "lon": -96.79, "rank": 1, "icao": "KDFW"},
    {"name": "Denver", "lat": 39.73, "lon": -104.99, "rank": 1, "icao": "KDEN"},
    {"name": "Seattle", "lat": 47.60, "lon": -122.33, "rank": 1, "icao": "KSEA"},
    {"name": "Miami", "lat": 25.76, "lon": -80.19, "rank": 1, "icao": "KMIA"},
    {"name": "Atlanta", "lat": 33.74, "lon": -84.38, "rank": 1, "icao": "KATL"},
    {"name": "Minneapolis", "lat": 44.97, "lon": -93.26, "rank": 1, "icao": "KMSP"},
    {"name": "Des Moines", "lat": 41.60, "lon": -93.60, "rank": 1, "icao": "KDSM"},
    {"name": "Omaha", "lat": 41.25, "lon": -95.93, "rank": 2, "icao": "KOMA"},
    {"name": "Kansas City", "lat": 39.09, "lon": -94.57, "rank": 2, "icao": "KMCI"},
    {"name": "St. Louis", "lat": 38.62, "lon": -90.19, "rank": 2, "icao": "KSTL"},
    {"name": "Milwaukee", "lat": 43.03, "lon": -87.90, "rank": 2, "icao": "KMKE"},
    {"name": "Madison", "lat": 43.07, "lon": -89.40, "rank": 2, "icao": "KMSN"},
    {"name": "Indianapolis", "lat": 39.76, "lon": -86.15, "rank": 2, "icao": "KIND"},
    {"name": "Columbus", "lat": 39.96, "lon": -82.99, "rank": 2, "icao": "KCMH"},
    {"name": "Wichita", "lat": 37.68, "lon": -97.33, "rank": 2, "icao": "KICT"},
    {"name": "Sioux Falls", "lat": 43.54, "lon": -96.72, "rank": 2, "icao": "KFSD"},
    {"name": "Austin", "lat": 30.26, "lon": -97.74, "rank": 2, "icao": "KAUS"},
    {"name": "San Francisco", "lat": 37.77, "lon": -122.41, "rank": 2, "icao": "KSFO"},
    {"name": "Charlotte", "lat": 35.22, "lon": -80.84, "rank": 2, "icao": "KCLT"},
    {"name": "Nashville", "lat": 36.16, "lon": -86.78, "rank": 2, "icao": "KBNA"},
    {"name": "Oklahoma City", "lat": 35.46, "lon": -97.51, "rank": 2, "icao": "KOKC"},
    {"name": "Portland", "lat": 45.52, "lon": -122.67, "rank": 2, "icao": "KPDX"},
    {"name": "Las Vegas", "lat": 36.16, "lon": -115.13, "rank": 2, "icao": "KLAS"},
    {"name": "Detroit", "lat": 42.33, "lon": -83.04, "rank": 2, "icao": "KDTW"},
    {"name": "Memphis", "lat": 35.14, "lon": -90.04, "rank": 2, "icao": "KMEM"},
    {"name": "Baltimore", "lat": 39.29, "lon": -76.61, "rank": 2, "icao": "KBWI"},
    {"name": "Salt Lake City", "lat": 40.76, "lon": -111.89, "rank": 2, "icao": "KSLC"},
    {"name": "New Orleans", "lat": 29.95, "lon": -90.07, "rank": 2, "icao": "KMSY"},
    {"name": "Ames", "lat": 42.03, "lon": -93.62, "rank": 3, "icao": "KAMW"},
    {"name": "Iowa City", "lat": 41.66, "lon": -91.53, "rank": 3, "icao": "KIOW"},
    {"name": "Cedar Rapids", "lat": 41.97, "lon": -91.66, "rank": 3, "icao": "KCID"},
    {"name": "Davenport", "lat": 41.52, "lon": -90.57, "rank": 3, "icao": "KDVN"},
    {"name": "Waterloo", "lat": 42.49, "lon": -92.33, "rank": 3, "icao": "KALO"},
    {"name": "Sioux City", "lat": 42.49, "lon": -96.40, "rank": 3, "icao": "KSUX"},
    {"name": "Council Bluffs", "lat": 41.26, "lon": -95.86, "rank": 3, "icao": "KCBF"},
    {"name": "Dubuque", "lat": 42.50, "lon": -90.66, "rank": 3, "icao": "KDBQ"},
    {"name": "Ankeny", "lat": 41.72, "lon": -93.60, "rank": 3, "icao": "KIKV"},
    {"name": "Naperville", "lat": 41.75, "lon": -88.15, "rank": 3, "icao": "KDPA"},
    {"name": "Aurora", "lat": 41.76, "lon": -88.31, "rank": 3, "icao": "KARR"},
    {"name": "Rockford", "lat": 42.27, "lon": -89.09, "rank": 3, "icao": "KRFD"},
    {"name": "Peoria", "lat": 40.69, "lon": -89.58, "rank": 3, "icao": "KPIA"},
    {"name": "Mason City", "lat": 43.15, "lon": -93.20, "rank": 3, "icao": "KMCW"},
    {"name": "Fort Dodge", "lat": 42.49, "lon": -94.16, "rank": 3, "icao": "KFOD"},
    {"name": "Ottumwa", "lat": 41.01, "lon": -92.41, "rank": 3, "icao": "KOTM"},
    {"name": "Burlington", "lat": 40.80, "lon": -91.10, "rank": 3, "icao": "KBRL"},
    {"name": "Fargo", "lat": 46.87, "lon": -96.78, "rank": 3, "icao": "KFAR"},
    {"name": "Bismarck", "lat": 46.80, "lon": -100.78, "rank": 3, "icao": "KBIS"},
    {"name": "Little Rock", "lat": 34.74, "lon": -92.28, "rank": 3, "icao": "KLIT"},
    {"name": "Boise", "lat": 45.61, "lon": -114.31, "rank": 3, "icao": "KBOI"},
    {"name": "Raleigh", "lat": 35.77, "lon": -78.63, "rank": 3, "icao": "KRDU"},
    {"name": "Lincoln", "lat": 40.82, "lon": -96.68, "rank": 3, "icao": "KLNK"},
    {"name": "South Bend", "lat": 41.67, "lon": -86.25, "rank": 3, "icao": "KSBN"},
    {"name": "Lansing", "lat": 42.73, "lon": -84.55, "rank": 3, "icao": "KLAN"},
    {"name": "Columbia", "lat": 34.00, "lon": -81.03, "rank": 3, "icao": "KCAE"},
    {"name": "Eugene", "lat": 44.05, "lon": -123.09, "rank": 3, "icao": "KEUG"},
    {"name": "Surprise", "lat": 33.63, "lon": -112.37, "rank": 3, "icao": "KLUF"}
]

def robust_nws_fetch_backend(url):
    """Crucial failover engine for the Python server to bypass NOAA IP blocking."""
    headers = {"User-Agent": "TJFWeather_Backend/1.0 (contact@tjfweather.com)", "Accept": "application/geo+json"}
    try:
        res = requests.get(url, headers=headers, timeout=10)
        if res.status_code == 200:
            data = res.json()
            if data.get('status') != 500: return data
    except: pass
    
    # Proxy Fallback
    try:
        import urllib.parse
        p_url = f"https://api.allorigins.win/raw?url={urllib.parse.quote(url)}"
        p_res = requests.get(p_url, timeout=10)
        if p_res.status_code == 200:
            data = p_res.json()
            if data.get('status') != 500: return data
    except: pass
    return None

def update_nws_alerts():
    print("\n[1/4] Fetching active NWS Severe Weather Alerts...")
    try:
        res = robust_nws_fetch_backend("https://api.weather.gov/alerts/active?status=actual")
        if res:
            os.makedirs('static', exist_ok=True)
            with open("static/nws_alerts.json", "w") as f:
                json.dump(res, f)
            print("  ✓ Alerts successfully updated!")
    except Exception as e:
        print(f"  [X] Error fetching alerts: {e}")

def fetch_asos_current_conditions():
    print("\n[2/4] Fetching live ASOS/METAR observations...")
    icaos = ",".join([c['icao'] for c in CITIES if 'icao' in c])
    awc_url = f"https://aviationweather.gov/api/data/metar?ids={icaos}&format=json"
    
    try:
        res = requests.get(awc_url, timeout=15)
        if res.status_code == 200:
            data = res.json()
            conditions = {}
            for obs in data:
                icao = obs.get('icaoId')
                temp_c = obs.get('temp')
                wind_spd = obs.get('wspd') 
                wx_string = obs.get('wxString', '')
                
                temp_f = round((temp_c * 9/5) + 32) if temp_c is not None else None
                wind_mph = round(wind_spd * 1.15078) if wind_spd is not None else None
                
                city_name = next((c['name'] for c in CITIES if c.get('icao') == icao), None)
                if city_name:
                    conditions[city_name] = {"temp": temp_f, "wind": wind_mph, "wx": wx_string}
            
            os.makedirs('static', exist_ok=True)
            with open("static/asos_conditions.json", "w") as f:
                json.dump(conditions, f)
            print("  ✓ Successfully cached current conditions!")
    except Exception as e:
        print(f"  [X] Failed to fetch METARs: {e}")

def cache_nws_point_forecasts():
    print("\n[3/4] Syncing human-made NWS point forecasts and NBM Consensus...")
    os.makedirs('static/nws', exist_ok=True)
    
    now = datetime.datetime.utcnow()
    c1_hour = (now.hour // 6) * 6
    c1_str = f"{now.strftime('%Y-%m-%d')}_{c1_hour:02d}"
    
    nbm_data = {}

    for city in CITIES:
        try:
            pt_data = robust_nws_fetch_backend(f"https://api.weather.gov/points/{city['lat']},{city['lon']}")
            if not pt_data or 'properties' not in pt_data: continue
                
            f_json = robust_nws_fetch_backend(pt_data['properties']['forecast'])
            h_json = robust_nws_fetch_backend(pt_data['properties']['forecastHourly'])
            
            if f_json and h_json:
                payload = {"daily": f_json, "hourly": h_json, "cached_at": datetime.datetime.utcnow().isoformat()}
                safe_name = city['name'].replace(" ", "_")
                with open(f"static/nws/{safe_name}.json", "w") as f:
                    json.dump(payload, f)
                
                nbm_temps = [None] * 49
                nbm_qpf = [0.0] * 49
                nbm_snow = [0.0] * 49
                
                if 'periods' in h_json.get('properties', {}):
                    base_time = now.replace(hour=c1_hour, minute=0, second=0, microsecond=0)
                    for p in h_json['properties']['periods']:
                        try:
                            p_time = datetime.datetime.strptime(p['startTime'][:19], "%Y-%m-%dT%H:%M:%S")
                            diff_hours = int((p_time - base_time).total_seconds() / 3600)
                            
                            if 0 <= diff_hours <= 48:
                                nbm_temps[diff_hours] = p['temperature']
                                sf = p.get('shortForecast', '').lower()
                                if 'rain' in sf or 'showers' in sf: nbm_qpf[diff_hours] += 0.05
                                if 'snow' in sf: nbm_snow[diff_hours] += 0.1
                        except: pass
                
                nbm_data[city['name']] = {
                    "lat": city['lat'], "lon": city['lon'], "fxx": list(range(49)),
                    "temp": nbm_temps, "qpf": nbm_qpf, "snow": nbm_snow, "wind": [None]*49
                }
            time.sleep(0.3) # Be nice to NOAA API
        except Exception as e:
            pass
            
    if nbm_data:
        try:
            with open(f"static/nbm_timeseries_{c1_str}.json", "w") as f: json.dump(nbm_data, f)
            # Write a generic 'latest' file for easy frontend access
            with open("static/nbm_timeseries_latest.json", "w") as f: json.dump(nbm_data, f)
            print("  ✓ NBM Base Generation Complete.")
        except: pass

def cache_mos_forecasts():
    print("\n[4/4] Fetching Statistical MOS Model Bulletins (Uncertainty Range)...")
    now = datetime.datetime.utcnow()
    c1_hour = (now.hour // 6) * 6
    c1_str = f"{now.strftime('%Y-%m-%d')}_{c1_hour:02d}"
    
    gfs_mos = {}
    nam_mos = {}

    def parse_mos_text(raw_text):
        lines = raw_text.split('\n')
        hr_line = next((l for l in lines if l.strip().startswith('HR ')), None)
        tmp_line = next((l for l in lines if l.strip().startswith('TMP')), None)
        
        if not hr_line or not tmp_line: return None
        
        hours = re.findall(r'\d+', hr_line[3:])
        temps = re.findall(r'-?\d+', tmp_line[3:])
        
        if len(hours) < 2 or len(temps) < 2: return None
        
        f_temps = [None] * 49
        try:
            for idx, val in enumerate(temps):
                f_idx = 6 + (idx * 3) # F06, F09, F12, F15...
                if f_idx <= 48:
                    f_temps[f_idx] = int(val)
            
            # Interpolate missing hours
            last_valid_idx = -1
            for i in range(49):
                if f_temps[i] is not None:
                    if last_valid_idx != -1 and (i - last_valid_idx) <= 3:
                        diff = f_temps[i] - f_temps[last_valid_idx]
                        steps = i - last_valid_idx
                        step_val = diff / steps
                        for j in range(1, steps):
                            f_temps[last_valid_idx + j] = int(f_temps[last_valid_idx] + (step_val * j))
                    last_valid_idx = i
            return f_temps
        except:
            return None

    for city in CITIES:
        icao = city.get('icao')
        if not icao: continue
        
        # GFS MOS
        try:
            data = robust_nws_fetch_backend(f"https://api.weather.gov/products/types/MAV/locations/{icao}")
            if data and data.get('@graph') and len(data['@graph']) > 0:
                raw_res = robust_nws_fetch_backend(data['@graph'][0]['@id'])
                if raw_res:
                    parsed_temps = parse_mos_text(raw_res.get('productText', ''))
                    if parsed_temps:
                        gfs_mos[city['name']] = {"lat": city['lat'], "lon": city['lon'], "fxx": list(range(49)), "temp": parsed_temps}
        except: pass
        
        # NAM MOS
        try:
            data = robust_nws_fetch_backend(f"https://api.weather.gov/products/types/MET/locations/{icao}")
            if data and data.get('@graph') and len(data['@graph']) > 0:
                raw_res = robust_nws_fetch_backend(data['@graph'][0]['@id'])
                if raw_res:
                    parsed_temps = parse_mos_text(raw_res.get('productText', ''))
                    if parsed_temps:
                        nam_mos[city['name']] = {"lat": city['lat'], "lon": city['lon'], "fxx": list(range(49)), "temp": parsed_temps}
        except: pass

        time.sleep(0.3)

    if gfs_mos:
        with open(f"static/gfsmos_timeseries_{c1_str}.json", "w") as f: json.dump(gfs_mos, f)
        with open("static/gfsmos_timeseries_latest.json", "w") as f: json.dump(gfs_mos, f)
    if nam_mos:
        with open(f"static/nammos_timeseries_{c1_str}.json", "w") as f: json.dump(nam_mos, f)
        with open("static/nammos_timeseries_latest.json", "w") as f: json.dump(nam_mos, f)
        
    print("  ✓ MOS Statistical Fetch Complete.")

if __name__ == '__main__':
    print("==================================================")
    print("   TJFWeather Github Actions Pipeline Started     ")
    print("==================================================")
    
    update_nws_alerts()
    fetch_asos_current_conditions()
    cache_nws_point_forecasts()
    cache_mos_forecasts()
    
    print("\nPipeline finished successfully. Exiting for GitHub commit.")
