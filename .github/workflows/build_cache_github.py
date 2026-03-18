import os
import json
import gc
import time
import glob
import requests
import multiprocessing
import threading
from herbie import Herbie
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.colors as mcolors
from scipy.ndimage import gaussian_filter
import numpy as np
import datetime
import re

# Gracefully import psutil for hardware throttling
try:
    import psutil
    HAS_PSUTIL = True
except ImportError:
    HAS_PSUTIL = False
    print("\n[WARNING] 'psutil' module not found. Hardware throttling disabled.")

CONUS = {'minLat': 24.0, 'maxLat': 50.0, 'minLon': -125.0, 'maxLon': -66.0}

# --- UPGRADED CITIES LIST WITH EUGENE & SURPRISE ---
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

RADAR_COLORS = [
    '#7CFC00', '#00FF00', '#00C800', '#009000', '#006400', 
    '#FFFF00', '#FFD700', '#FF8C00', '#FF4500', '#FF0000', 
    '#CC0000', '#990000', '#FF00FF', '#9900CC', '#FFFFFF',
]

def check_resources(model_name):
    """Monitors CPU/RAM to prevent kernel panic crashes"""
    if not HAS_PSUTIL: return
    try:
        mem_usage = psutil.virtual_memory().percent
        if mem_usage > 85.0:
            print(f"  [THROTTLE] {model_name.upper()} Worker paused! RAM critical ({mem_usage}%).")
            gc.collect()
            time.sleep(45)
    except Exception: pass

# --- CACHE ENGINE ---

def robust_nws_fetch_backend(url):
    """Crucial failover engine for the Python server to bypass NOAA IP blocking."""
    headers = {"User-Agent": "TJFWeather_Backend/1.0 (contact@tjfweather.local)", "Accept": "application/geo+json"}
    try:
        res = requests.get(url, headers=headers, timeout=10)
        if res.status_code == 200:
            data = res.json()
            if data.get('status') != 500: return data
    except: pass
    
    # Proxy Fallback to guarantee backend gets data
    try:
        import urllib.parse
        p_url = f"https://api.allorigins.win/raw?url={urllib.parse.quote(url)}"
        p_res = requests.get(p_url, timeout=10)
        if p_res.status_code == 200:
            data = p_res.json()
            if data.get('status') != 500: return data
    except: pass
    return None

def fetch_asos_current_conditions():
    print("\n  [ASOS] Fetching live airport observations...")
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
            print("  [ASOS] Successfully cached current conditions!")
    except Exception as e:
        print(f"  [ASOS ERROR] Failed to fetch METARs: {e}")

def cache_nws_point_forecasts():
    print("\n  [NWS/NBM] Syncing official point forecasts and generating NBM Consensus...")
    os.makedirs('static/nws', exist_ok=True)
    
    now = datetime.datetime.utcnow()
    c1_time = now
    c1_hour = (c1_time.hour // 6) * 6
    c1_str = f"{c1_time.strftime('%Y-%m-%d')}_{c1_hour:02d}"
    
    c2_time = now - datetime.timedelta(hours=6)
    c2_hour = (c2_time.hour // 6) * 6
    c2_str = f"{c2_time.strftime('%Y-%m-%d')}_{c2_hour:02d}"
    
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
            time.sleep(0.5) 
        except: pass
            
    if nbm_data:
        try:
            with open(f"static/nbm_timeseries_{c1_str}.json", "w") as f: json.dump(nbm_data, f)
            with open(f"static/nbm_timeseries_{c2_str}.json", "w") as f: json.dump(nbm_data, f)
            with open(f"static/nbm_status.json", "w") as sf: 
                json.dump({"is_building": False, "eta_seconds": 0, "fxx": 48}, sf)
            print("  [NBM] Model Output Generation Complete.")
        except: pass

def cache_mos_forecasts():
    print("\n  [MOS] Fetching Statistical Model Bulletins...")
    now = datetime.datetime.utcnow()
    c1_time = now
    c1_hour = (c1_time.hour // 6) * 6
    c1_str = f"{c1_time.strftime('%Y-%m-%d')}_{c1_hour:02d}"
    
    c2_time = now - datetime.timedelta(hours=6)
    c2_hour = (c2_time.hour // 6) * 6
    c2_str = f"{c2_time.strftime('%Y-%m-%d')}_{c2_hour:02d}"
    
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
        
        try:
            data = robust_nws_fetch_backend(f"https://api.weather.gov/products/types/MAV/locations/{icao}")
            if data and data.get('@graph') and len(data['@graph']) > 0:
                raw_res = robust_nws_fetch_backend(data['@graph'][0]['@id'])
                if raw_res:
                    parsed_temps = parse_mos_text(raw_res.get('productText', ''))
                    if parsed_temps:
                        gfs_mos[city['name']] = {"lat": city['lat'], "lon": city['lon'], "fxx": list(range(49)), "temp": parsed_temps, "qpf": [0.0]*49, "snow": [0.0]*49, "wind": [None]*49}
        except: pass
        
        try:
            data = robust_nws_fetch_backend(f"https://api.weather.gov/products/types/MET/locations/{icao}")
            if data and data.get('@graph') and len(data['@graph']) > 0:
                raw_res = robust_nws_fetch_backend(data['@graph'][0]['@id'])
                if raw_res:
                    parsed_temps = parse_mos_text(raw_res.get('productText', ''))
                    if parsed_temps:
                        nam_mos[city['name']] = {"lat": city['lat'], "lon": city['lon'], "fxx": list(range(49)), "temp": parsed_temps, "qpf": [0.0]*49, "snow": [0.0]*49, "wind": [None]*49}
        except: pass

        time.sleep(0.3)

    if gfs_mos:
        with open(f"static/gfsmos_timeseries_{c1_str}.json", "w") as f: json.dump(gfs_mos, f)
        with open(f"static/gfsmos_timeseries_{c2_str}.json", "w") as f: json.dump(gfs_mos, f)
        with open(f"static/gfsmos_status.json", "w") as f: json.dump({"is_building": False, "eta_seconds": 0, "fxx": 48}, f)
    if nam_mos:
        with open(f"static/nammos_timeseries_{c1_str}.json", "w") as f: json.dump(nam_mos, f)
        with open(f"static/nammos_timeseries_{c2_str}.json", "w") as f: json.dump(nam_mos, f)
        with open(f"static/nammos_status.json", "w") as f: json.dump({"is_building": False, "eta_seconds": 0, "fxx": 48}, f)

def backend_services_loop():
    while True:
        update_nws_alerts()
        fetch_asos_current_conditions()
        cache_nws_point_forecasts()
        cache_mos_forecasts()
        time.sleep(900) 

def update_nws_alerts():
    try:
        print("\n  [NWS] Fetching active Severe Weather Alerts...")
        res = robust_nws_fetch_backend("https://api.weather.gov/alerts/active?status=actual")
        if res:
            os.makedirs('static', exist_ok=True)
            with open("static/nws_alerts.json", "w") as f:
                json.dump(res, f)
            print("  [NWS] Alerts successfully updated!")
    except Exception as e:
        print(f"  [NWS] Error fetching alerts: {e}")

# --- HERBIE NWP ENGINE ---
def generate_legends():
    if not os.path.exists('static'): os.makedirs('static')
    figsize = (1.6, 10)
    label_color = '#ffffff'
    def quick_save(cb, name, label):
        cb.ax.tick_params(labelsize=20, colors='white')
        cb.set_label(label, color=label_color, fontsize=26, fontweight='bold', labelpad=25)
        plt.savefig(f'static/legend_{name}.png', transparent=True, bbox_inches='tight', dpi=200)
        plt.close()
        
    fig, ax = plt.subplots(figsize=figsize)
    cb = matplotlib.colorbar.ColorbarBase(ax, cmap=plt.get_cmap('coolwarm'), norm=mcolors.Normalize(vmin=-20, vmax=110))
    quick_save(cb, 'temp', 'Temperature (°F)')
    
    fig, ax = plt.subplots(figsize=figsize)
    cb = matplotlib.colorbar.ColorbarBase(ax, cmap=plt.get_cmap('plasma'), norm=mcolors.Normalize(vmin=0, vmax=80))
    quick_save(cb, 'wind', 'Wind Speed (mph)')
    
    fig, ax = plt.subplots(figsize=figsize)
    jet_cmap = mcolors.LinearSegmentedColormap.from_list('jet', ['#00ff00', '#ffff00', '#ff0000', '#ff00ff', '#ffffff'])
    cb = matplotlib.colorbar.ColorbarBase(ax, cmap=jet_cmap, norm=mcolors.Normalize(vmin=55, vmax=160))
    quick_save(cb, 'upper_wind', 'Jet Streak (kt)')
    
    fig, ax = plt.subplots(figsize=figsize)
    cb = matplotlib.colorbar.ColorbarBase(ax, cmap=plt.get_cmap('YlGn'), norm=mcolors.Normalize(vmin=10, vmax=80))
    quick_save(cb, 'dewpoint', 'Dew Point (°F)')
    
    fig, ax = plt.subplots(figsize=figsize)
    cb = matplotlib.colorbar.ColorbarBase(ax, cmap=plt.get_cmap('YlOrRd'), norm=mcolors.Normalize(vmin=0, vmax=5000))
    quick_save(cb, 'cape', 'CAPE (J/kg)')
    
    fig, ax = plt.subplots(figsize=figsize)
    cb = matplotlib.colorbar.ColorbarBase(ax, cmap=plt.get_cmap('Blues_r'), norm=mcolors.Normalize(vmin=-500, vmax=0))
    quick_save(cb, 'cin', 'CIN (J/kg)')
    
    fig, ax = plt.subplots(figsize=figsize)
    cb = matplotlib.colorbar.ColorbarBase(ax, cmap=plt.get_cmap('PuRd'), norm=mcolors.Normalize(vmin=0, vmax=800))
    quick_save(cb, 'srh', 'SRH (m2/s2)')
    
    fig, ax = plt.subplots(figsize=figsize)
    radar_cmap = mcolors.ListedColormap(RADAR_COLORS)
    radar_bounds = np.arange(5, 85, 5)
    cb = matplotlib.colorbar.ColorbarBase(ax, cmap=radar_cmap, norm=mcolors.BoundaryNorm(radar_bounds, radar_cmap.N), boundaries=radar_bounds, ticks=radar_bounds)
    quick_save(cb, 'refc', 'Simulated Reflectivity (dBZ)')
    
    fig, ax = plt.subplots(figsize=figsize)
    qpf_colors = ['#76E576', '#00CC00', '#008E00', '#104E8B', '#1E90FF', '#00B2EE', '#00EEEE', '#8968CD', '#912CEE', '#B23AEE', '#FF0000', '#EE0000', '#CD0000', '#FF8C00']
    qpf_cmap = mcolors.ListedColormap(qpf_colors)
    qpf_bounds = [0.01, 0.1, 0.25, 0.5, 0.75, 1.0, 1.5, 2.0, 2.5, 3.0, 4.0, 5.0, 7.0, 10.0, 15.0]
    cb = matplotlib.colorbar.ColorbarBase(ax, cmap=qpf_cmap, norm=mcolors.BoundaryNorm(qpf_bounds, qpf_cmap.N), boundaries=qpf_bounds, ticks=qpf_bounds)
    quick_save(cb, 'qpf', 'QPF (in)')
    
    fig, ax = plt.subplots(figsize=figsize)
    bounds = [0.1, 1, 2, 4, 6, 8, 12, 18, 24, 30]
    cb = matplotlib.colorbar.ColorbarBase(ax, cmap=plt.get_cmap('PuBu'), norm=mcolors.BoundaryNorm(bounds, 256), boundaries=bounds, ticks=bounds)
    quick_save(cb, 'snow', 'Snow (in)')
    
    fig, ax = plt.subplots(figsize=figsize)
    bounds = [0, 0.25, 0.5, 1, 2, 3, 5, 10]
    cb = matplotlib.colorbar.ColorbarBase(ax, cmap=plt.get_cmap('Greys_r'), norm=mcolors.BoundaryNorm(bounds, 256), boundaries=bounds, ticks=bounds)
    quick_save(cb, 'visibility', 'Visibility (mi)')

def cleanup_old_cache(keep_hours=24):
    now = time.time()
    cutoff = now - (keep_hours * 3600)
    files = glob.glob('static/*_conus_*.png') + glob.glob('static/*_conus_*.json')
    for f in files:
        if 'legend' in f: continue
        try:
            if os.path.getmtime(f) < cutoff: os.remove(f)
        except: pass

def get_recent_runs(model_type='hrrr'):
    """Bypasses Herbie Inventory check entirely to prevent missing model runs"""
    now = datetime.datetime.utcnow()
    shifted = now - datetime.timedelta(hours=1) 
    starting_run_hour = (shifted.hour // 6) * 6
    base_time = shifted.replace(hour=starting_run_hour, minute=0, second=0, microsecond=0)
    runs = []
    # Always pull the 2 most recent mathematical 6-hour cycles
    for i in range(2):
        run_time = base_time - datetime.timedelta(hours=6 * i)
        runs.append({'date': run_time.strftime('%Y-%m-%d'), 'hour': run_time.strftime('%H')})
    return runs

def get_ds_safe(target, search_str):
    try:
        ds = target.xarray(search_str)
        if isinstance(ds, list): 
            if len(ds) > 0: ds = ds[0]
            else: return None
        ds.load()
        return ds
    except Exception:
        try:
            var_name = search_str.split(':')[0]
            ds = target.xarray(var_name)
            if isinstance(ds, list): 
                if len(ds) > 0: ds = ds[0]
                else: return None
            ds.load()
            return ds
        except:
            return None

def extract_2d(ds, lon_sort_indices=None):
    raw = list(ds.data_vars.values())[0].squeeze().values
    if raw.ndim == 3: raw = raw[0]
    elif raw.ndim == 4: raw = raw[0, 0]
    if lon_sort_indices is not None:
        raw = raw[:, lon_sort_indices]
    return raw

def build_model_cache(model_type='hrrr', max_fxx=48):
    print(f"\n--- [SYNC] {model_type.upper()} Start ---")
    generate_legends()

    radar_cmap = mcolors.ListedColormap(RADAR_COLORS)
    radar_cmap.set_under((0, 0, 0, 0))
    qpf_colors = ['#76E576', '#00CC00', '#008E00', '#104E8B', '#1E90FF', '#00B2EE', '#00EEEE', '#8968CD', '#912CEE', '#B23AEE', '#FF0000', '#EE0000', '#CD0000', '#FF8C00']
    qpf_cmap = mcolors.ListedColormap(qpf_colors)
    qpf_cmap.set_under((0, 0, 0, 0))
    qpf_bounds = [0.01, 0.1, 0.25, 0.5, 0.75, 1.0, 1.5, 2.0, 2.5, 3.0, 4.0, 5.0, 7.0, 10.0, 15.0]
    
    recent_runs = get_recent_runs(model_type)
    apcp_buffer = {}
    
    for run in recent_runs:
        date_str = run['date']
        run_hour = run['hour']
        city_indices = {}
        ts_data = {c['name']: {'lat': c['lat'], 'lon': c['lon'], 'fxx': [], 'temp': [], 'qpf': [], 'snow': [], 'wind': []} for c in CITIES}
        
        qpf_history = {}
        running_qpf = None
        running_snow = None
        _lon_sort = None 
        
        def update_ts(var_name, raw_data, transform=None):
            if raw_data is None: return
            for c in CITIES:
                idx = city_indices.get(c['name'])
                if idx:
                    try:
                        val = float(raw_data[idx])
                        if transform: val = transform(val)
                        ts_data[c['name']][var_name][-1] = round(val, 2)
                    except: pass

        def get_apcp_data(f):
            if f <= 0: return None
            if f in apcp_buffer: return apcp_buffer[f]
            try:
                if model_type == 'hrrr': h_obj = Herbie(f"{date_str} {run_hour}:00", model='hrrr', product='sfc', fxx=f)
                elif model_type == 'gfs': h_obj = Herbie(f"{date_str} {run_hour}:00", model='gfs', product='pgrb2.0p25', fxx=f)
                elif model_type == 'nam': h_obj = Herbie(f"{date_str} {run_hour}:00", model='nam', product='conusnest.hiresf', fxx=f)
                elif model_type == 'rrfs': h_obj = Herbie(f"{date_str} {run_hour}:00", model='rrfs', product='prs', fxx=f)
                else: return None
                
                ds = get_ds_safe(h_obj, "APCP:surface") or get_ds_safe(h_obj, "APCP")
                if ds is not None:
                    data = extract_2d(ds, _lon_sort) / 25.4
                    data = np.nan_to_num(data, nan=0.0) 
                    apcp_buffer[f] = data
                    ds.close(); del ds
                    return data
            except: pass
            return None
            
        total_fxx = max_fxx + 1
        start_time = time.time()
        
        for fxx in range(total_fxx):
            check_resources(model_type) 
            
            for c in CITIES:
                ts_data[c['name']]['fxx'].append(fxx)
                ts_data[c['name']]['temp'].append(None)
                ts_data[c['name']]['qpf'].append(None)
                ts_data[c['name']]['snow'].append(None)
                ts_data[c['name']]['wind'].append(None)

            check_file_temp = f"static/{model_type}_temp_conus_{date_str}_{run_hour}_{fxx}.png"
            check_file_upper = f"static/{model_type}_upper_500_conus_{date_str}_{run_hour}_{fxx}.json"
            check_file_snow = f"static/{model_type}_snow_accum_conus_{date_str}_{run_hour}_{fxx}.json"
            
            has_temp = os.path.exists(check_file_temp)
            has_upper = os.path.exists(check_file_upper)
            has_snow = os.path.exists(check_file_snow)

            is_valid_cache = (has_temp and has_upper and has_snow) if model_type != 'rrfs' else (has_temp and has_upper)

            if is_valid_cache:
                print(f"  [SKIP] {model_type.upper()} F{fxx:02} completely cached.")
                ts_var_map = {
                    'temp':  f"static/{model_type}_temp_conus_{date_str}_{run_hour}_{fxx}.json",
                    'wind':  f"static/{model_type}_wind_conus_{date_str}_{run_hour}_{fxx}.json",
                    'snow':  f"static/{model_type}_snow_accum_conus_{date_str}_{run_hour}_{fxx}.json",
                    'qpf':   f"static/{model_type}_qpf_6h_conus_{date_str}_{run_hour}_{fxx}.json",
                }
                for var, path in ts_var_map.items():
                    try:
                        if os.path.exists(path):
                            with open(path) as jf: city_vals = json.load(jf)
                            lookup = {entry['name']: entry['value'] for entry in city_vals if 'name' in entry and 'value' in entry}
                            for c in CITIES:
                                val = lookup.get(c['name'])
                                if val is not None: ts_data[c['name']][var][-1] = val
                    except Exception: pass
                
                try:
                    with open(f"static/{model_type}_timeseries_{date_str}_{run_hour}.json", "w") as f:
                        json.dump(ts_data, f)
                except Exception: pass

                try:
                    with open(f"static/{model_type}_status.json", "w") as sf: 
                        json.dump({"is_building": True, "eta_seconds": 0, "fxx": fxx}, sf)
                except: pass
                
                try:
                    if fxx in apcp_buffer:
                        running_qpf = apcp_buffer[fxx]
                        qpf_history[fxx] = running_qpf
                except: pass
                continue
                
            try:
                if model_type == 'hrrr':
                    H = Herbie(f"{date_str} {run_hour}:00", model='hrrr', product='sfc', fxx=fxx)
                    H_prs = Herbie(f"{date_str} {run_hour}:00", model='hrrr', product='prs', fxx=fxx)
                elif model_type == 'gfs':
                    H = Herbie(f"{date_str} {run_hour}:00", model='gfs', product='pgrb2.0p25', fxx=fxx)
                    H_prs = H
                elif model_type == 'nam':
                    H = Herbie(f"{date_str} {run_hour}:00", model='nam', product='conusnest.hiresf', fxx=fxx)
                    H_prs = H
                elif model_type == 'rrfs':
                    H = Herbie(f"{date_str} {run_hour}:00", model='rrfs', product='prs', fxx=fxx)
                    H_prs = H
                else: break
                
                ds_coords = get_ds_safe(H, "TMP:2 m") or get_ds_safe(H, "TMP:2 m above ground") or get_ds_safe(H_prs, "HGT:500 mb")
                if ds_coords is None:
                    raise Exception(f"NOAA has not finished uploading {model_type.upper()} F{fxx:02} yet.")
                    
                lats_raw = ds_coords.latitude.values
                lons_raw = ds_coords.longitude.values
                lons, lats = np.meshgrid(lons_raw, lats_raw) if lats_raw.ndim == 1 else (lons_raw, lats_raw)
                lons = np.where(lons > 180, lons - 360, lons)
                ds_coords.close(); del ds_coords

                if _lon_sort is None and lats_raw.ndim == 1 and np.any(np.diff(lons[0]) < 0):
                    _lon_sort = np.argsort(lons[0])
                    lons = lons[:, _lon_sort]
                    lats = lats[:, _lon_sort]

                if not city_indices:
                    for c in CITIES:
                        dist = (lats - c['lat'])**2 + (lons - c['lon'])**2
                        city_indices[c['name']] = np.unravel_index(np.argmin(dist, axis=None), dist.shape)

                def save_plot(data, fname, cmap, levs, ext, smooth=True, raw_pixels=False):
                    if data is None or not np.any(data): return 
                    fig, ax = plt.subplots(figsize=(10, 8))
                    ax.axis('off')
                    ax.set_xlim(CONUS['minLon'], CONUS['maxLon'])
                    ax.set_ylim(CONUS['minLat'], CONUS['maxLat'])
                    try:
                        mask = (lons >= -135) & (lons <= -60) & (lats >= 20) & (lats <= 55)
                        clean = np.where(mask, data, np.nan)
                        valid_max = np.nanmax(clean) if not np.all(np.isnan(clean)) else 0.0
                        if raw_pixels or valid_max >= levs[0]:
                            if raw_pixels:
                                norm = mcolors.BoundaryNorm(levs, cmap.N)
                                ax.pcolormesh(lons, lats, clean, cmap=cmap, norm=norm, shading='nearest', antialiased=False)
                            else:
                                if smooth: clean = gaussian_filter(np.nan_to_num(clean, nan=0.0), sigma=1.0)
                                if np.nanmax(clean) >= levs[0]:
                                    ax.contourf(lons, lats, clean, levels=levs, cmap=cmap, extend=ext, antialiased=True)
                    except Exception: pass
                    out_dpi = 300 if raw_pixels else 150
                    plt.savefig(fname, transparent=True, bbox_inches='tight', pad_inches=0, dpi=out_dpi)
                    plt.close(fig)

                def save_upper_plot(wind_data, hgt_data, filename, hgt_levels):
                    if wind_data is None or hgt_data is None: return
                    fig, ax = plt.subplots(figsize=(10, 8), dpi=150)
                    ax.axis('off')
                    ax.set_xlim(CONUS['minLon'], CONUS['maxLon'])
                    ax.set_ylim(CONUS['minLat'], CONUS['maxLat'])
                    mask = (lons >= -135) & (lons <= -60) & (lats >= 20) & (lats <= 55)
                    try:
                        clean_wind = np.where(mask, gaussian_filter(np.nan_to_num(wind_data, nan=0.0), sigma=1.0), np.nan)
                        clean_hgt = np.where(mask, gaussian_filter(np.nan_to_num(hgt_data, nan=0.0), sigma=1.0), np.nan)
                        if np.nanmax(clean_wind) >= 55:
                            jet_cmap = mcolors.LinearSegmentedColormap.from_list('jet_streak', ['#00ff00', '#ffff00', '#ff0000', '#ff00ff', '#ffffff'])
                            CS = ax.contourf(lons, lats, clean_wind, levels=np.arange(55, 165, 5), cmap=jet_cmap, extend='max', antialiased=True)
                            for c in CS.collections: c.set_edgecolor("face"); c.set_linewidth(0.0)
                        CS2 = ax.contour(lons, lats, clean_hgt, levels=hgt_levels, colors='black', linewidths=1.0, alpha=0.8)
                        ax.clabel(CS2, inline=True, fontsize=8, fmt='%1.0f')
                    except Exception: pass
                    plt.savefig(filename, format='png', transparent=True, bbox_inches='tight', pad_inches=0)
                    plt.close(fig)

                def save_json(data, fname):
                    if data is None or not np.any(data): return
                    out = []
                    for c in CITIES:
                        try:
                            val = float(data[city_indices[c['name']]])
                            if not np.isnan(val): out.append({"name":c['name'],"lat":c['lat'],"lon":c['lon'],"rank":c['rank'],"value":round(val,2)})
                        except: pass
                    with open(fname, 'w') as f: json.dump(out, f)

                def process_var(herbie_strs, file_prefix, cmap, levs, ext, is_prs=False, transform=None, smooth=True, raw_pixels=False):
                    try:
                        target = H_prs if is_prs else H
                        ds = None
                        for s in herbie_strs:
                            ds = get_ds_safe(target, s)
                            if ds is not None: break
                        if ds is not None:
                            raw = extract_2d(ds, _lon_sort)
                            if transform: raw = transform(raw)
                            save_plot(raw, f"static/{model_type}_{file_prefix}_conus_{date_str}_{run_hour}_{fxx}.png", cmap, levs, ext, smooth, raw_pixels)
                            save_json(raw, f"static/{model_type}_{file_prefix}_conus_{date_str}_{run_hour}_{fxx}.json")
                            ds.close(); del ds
                    except Exception: pass

                # --- ISOLATED PROCESSING BLOCKS ---
                try:
                    ds_tmp = get_ds_safe(H, "TMP:2 m") or get_ds_safe(H, "TMP:2 m above ground")
                    if ds_tmp:
                        t_raw = extract_2d(ds_tmp, _lon_sort)
                        t_data = (t_raw - 273.15) * 9/5 + 32
                        save_plot(t_data, f"static/{model_type}_temp_conus_{date_str}_{run_hour}_{fxx}.png", 'coolwarm', np.arange(-20,110,2), 'both')
                        save_json(t_data, f"static/{model_type}_temp_conus_{date_str}_{run_hour}_{fxx}.json")
                        update_ts('temp', t_raw, transform=lambda x: (x - 273.15) * 9/5 + 32)
                        ds_tmp.close(); del ds_tmp
                except Exception: pass
                
                process_var(["DPT:2 m", "DPT:2 m above ground"], "dewpoint", plt.get_cmap('YlGn'), np.arange(10, 80, 2), "both", transform=lambda x: (x - 273.15) * 9/5 + 32)
                process_var(["REFC:entire atmosphere", "REFD:1000 m", "MAXREF", "REFC"], "refc", radar_cmap, np.arange(5,85,5), 'max', False, raw_pixels=True)
                
                try:
                    ds_u = get_ds_safe(H, "UGRD:10 m") or get_ds_safe(H, "UGRD:10 m above ground")
                    ds_v = get_ds_safe(H, "VGRD:10 m") or get_ds_safe(H, "VGRD:10 m above ground")
                    if ds_u and ds_v:
                        u_val = extract_2d(ds_u, _lon_sort)
                        v_val = extract_2d(ds_v, _lon_sort)
                        w_data = np.sqrt(u_val**2 + v_val**2) * 1.94384
                        save_plot(w_data, f"static/{model_type}_wind_conus_{date_str}_{run_hour}_{fxx}.png", 'plasma', np.arange(0,80,2), 'max')
                        save_json(w_data, f"static/{model_type}_wind_conus_{date_str}_{run_hour}_{fxx}.json")
                        update_ts('wind', w_data)
                        ds_u.close(); ds_v.close()
                    else: w_data = None
                except Exception: w_data = None
                
                try:
                    curr_apcp = get_apcp_data(fxx)
                    if curr_apcp is not None:
                        if model_type == 'gfs':
                            running_qpf = curr_apcp
                        else:
                            if running_qpf is None: running_qpf = np.zeros_like(curr_apcp)
                            running_qpf += curr_apcp
                        
                        update_ts('qpf', running_qpf)
                        qpf_history[fxx] = running_qpf
                        
                        for h in [6, 12, 24]:
                            qpf_final = None
                            if fxx >= h:
                                prev_qpf = qpf_history.get(fxx - h)
                                if running_qpf is not None and prev_qpf is not None:
                                    qpf_final = np.clip(running_qpf - prev_qpf, 0, None)
                            elif fxx > 0:
                                qpf_final = running_qpf
                                
                            save_plot(qpf_final, f"static/{model_type}_qpf_{h}h_conus_{date_str}_{run_hour}_{fxx}.png", qpf_cmap, qpf_bounds, 'max', False, raw_pixels=True)
                            save_json(qpf_final, f"static/{model_type}_qpf_{h}h_conus_{date_str}_{run_hour}_{fxx}.json")
                except Exception as e:
                    print(f"    [!] QPF error: {e}")
                
                process_var(["CAPE:surface", "CAPE:0-"], "sbcape", plt.get_cmap('YlOrRd'), np.arange(100, 5000, 250), "max")
                process_var(["CIN:surface", "CIN:0-"], "sbcin", plt.get_cmap('Blues_r'), np.arange(-500, 0, 50), "min")
                process_var(["HLCY:1000-0 m above ground", "HLCY:0-1000"], "srh1", plt.get_cmap('PuRd'), np.arange(50, 800, 50), "max")
                
                process_var(["CAPE:90-0 mb above ground", "CAPE:180-0 mb above ground"], "mlcape", plt.get_cmap('YlOrRd'), np.arange(100, 5000, 250), "max")
                process_var(["CIN:90-0 mb above ground", "CIN:180-0 mb above ground"], "mlcin", plt.get_cmap('Blues_r'), np.arange(-500, 0, 50), "min")
                process_var(["HLCY:3000-0 m above ground", "HLCY:0-3000"], "srh3", plt.get_cmap('PuRd'), np.arange(50, 800, 50), "max")
                process_var(["VIS:surface", "VIS"], "visibility", plt.get_cmap('Greys_r'), [0, 0.25, 0.5, 1, 2, 3, 5, 10], "max", transform=lambda x: x / 1609.34)

                try:
                    raw_500_wind = None
                    for level in [850, 500, 200]:
                        ds_hgt = get_ds_safe(H_prs, f"HGT:{level} mb")
                        ds_uwind = get_ds_safe(H_prs, f"UGRD:{level} mb")
                        ds_vwind = get_ds_safe(H_prs, f"VGRD:{level} mb")
                        if ds_hgt and ds_uwind and ds_vwind:
                            raw_hgt = extract_2d(ds_hgt, _lon_sort); u_wind = extract_2d(ds_uwind, _lon_sort); v_wind = extract_2d(ds_vwind, _lon_sort)
                            raw_upper_wind = np.sqrt(u_wind**2 + v_wind**2) * 1.94384
                            if level == 500: raw_500_wind = raw_upper_wind
                            hgt_levels = np.arange(1000, 2000, 30) if level == 850 else (np.arange(4800, 6200, 60) if level == 500 else np.arange(10000, 13000, 120))
                            save_upper_plot(raw_upper_wind, raw_hgt, f"static/{model_type}_upper_{level}_conus_{date_str}_{run_hour}_{fxx}.png", hgt_levels)
                            save_json(raw_upper_wind, f"static/{model_type}_upper_{level}_conus_{date_str}_{run_hour}_{fxx}.json")
                            ds_hgt.close(); ds_uwind.close(); ds_vwind.close()
                except Exception: pass
                
                try:
                    if raw_500_wind is not None and w_data is not None:
                        shear_data = raw_500_wind - w_data
                        save_plot(shear_data, f"static/{model_type}_shear_conus_{date_str}_{run_hour}_{fxx}.png", 'plasma', np.arange(20, 100, 5), 'max')
                        save_json(shear_data, f"static/{model_type}_shear_conus_{date_str}_{run_hour}_{fxx}.json")
                except Exception: pass
                
                try:
                    ds_snod = get_ds_safe(H, "SNOD:surface") or get_ds_safe(H, "SNOD")
                    if ds_snod:
                        raw = extract_2d(ds_snod, _lon_sort)
                        save_plot(raw * 39.3701, f"static/{model_type}_snow_depth_conus_{date_str}_{run_hour}_{fxx}.png", plt.get_cmap('PuBu'), [0.1, 1, 2, 4, 6, 8, 12, 18, 24, 30], "max", True, False)
                        save_json(raw * 39.3701, f"static/{model_type}_snow_depth_conus_{date_str}_{run_hour}_{fxx}.json")
                        ds_snod.close(); del ds_snod
                        
                    ds_asnow = get_ds_safe(H, "ASNOW:surface") or get_ds_safe(H, "ASNOW")
                    if ds_asnow:
                        raw = extract_2d(ds_asnow, _lon_sort)
                        running_snow = raw * 39.3701
                        ds_asnow.close(); del ds_asnow
                    else:
                        ds_weasd = get_ds_safe(H, "WEASD:surface") or get_ds_safe(H, "WEASD")
                        if ds_weasd:
                            raw = extract_2d(ds_weasd, _lon_sort)
                            running_snow = (raw / 25.4) * 10
                            ds_weasd.close(); del ds_weasd
                            
                    if running_snow is not None:
                        save_plot(running_snow, f"static/{model_type}_snow_accum_conus_{date_str}_{run_hour}_{fxx}.png", plt.get_cmap('PuBu'), [0.1, 1, 2, 4, 6, 8, 12, 18, 24, 30], "max", True, False)
                        save_json(running_snow, f"static/{model_type}_snow_accum_conus_{date_str}_{run_hour}_{fxx}.json")
                        update_ts('snow', running_snow)
                except Exception as e:
                    print(f"    [!] Snow error: {e}")

                try:
                    with open(f"static/{model_type}_timeseries_{date_str}_{run_hour}.json", "w") as f:
                        json.dump(ts_data, f)
                except Exception: pass

                processed = fxx + 1
                elapsed = time.time() - start_time
                avg_time = elapsed / processed
                remaining = total_fxx - processed
                eta_seconds = int(avg_time * remaining)
                mins, secs = divmod(eta_seconds, 60)
                print(f"  [✓] {model_type.upper()} F{fxx:02} complete. | ETA: {mins}m {secs}s")
                try:
                    with open(f"static/{model_type}_status.json", "w") as sf: 
                        json.dump({"is_building": remaining > 0, "eta_seconds": eta_seconds, "fxx": fxx}, sf)
                except Exception: pass
                time.sleep(1.0)
                gc.collect()
                
            except Exception as e:
                print(f"  [!] Failed F{fxx} in {model_type}: {e}")
                time.sleep(5.0)  
                continue
                
        try:
            with open(f"static/{model_type}_status.json", "w") as sf: 
                json.dump({"is_building": False, "eta_seconds": 0, "fxx": max_fxx}, sf)
        except Exception: pass
        
    apcp_buffer.clear()
    cleanup_old_cache()

def slow_models_loop():
    """Combined parallel loop for heavier, slower models so they don't max out RAM"""
    print("\n--- [BOOT] NAM/RRFS Dedicated Worker Initialized ---")
    models_to_run = ['nam', 'rrfs']
    while True:
        for model_name in models_to_run:
            try:
                build_model_cache(model_type=model_name, max_fxx=60)
            except Exception as e:
                print(f"\n--- [ERROR] {model_name.upper()} Pipeline Failed: {e}. ---")
            time.sleep(30)
        print("--- [STANDBY] Slow models cycled. Resting for 2 minutes ---")
        time.sleep(120)

def run_model(model, fxx):
    while True:
        build_model_cache(model, fxx)
        print(f"\n[STANDBY] {model.upper()} Cycle complete. Waiting for next NOAA update...")
        time.sleep(120)

if __name__ == '__main__':
    print("\n[SYSTEM] Booting Continuous TJFWeather Pipeline (Multi-Core Mode)...")
    
    threading.Thread(target=backend_services_loop, daemon=True).start()
    
    p_hrrr = multiprocessing.Process(target=run_model, args=('hrrr', 48))
    p_gfs = multiprocessing.Process(target=run_model, args=('gfs', 120))
    p_slow = multiprocessing.Process(target=slow_models_loop)
    
    p_hrrr.start()
    time.sleep(5)
    p_gfs.start()
    time.sleep(5)
    p_slow.start()
    
    p_hrrr.join()
    p_gfs.join()
    p_slow.join()