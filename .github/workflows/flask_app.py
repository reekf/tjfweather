from flask import Flask, render_template, request, jsonify
import multiprocessing
import time
import threading
from build_cache import build_model_cache, backend_services_loop

app = Flask(__name__)

# Declare globals for the queue and cooldown tracker so the routes can access them, 
# but DO NOT initialize them here. Initializing multiprocessing tools at the top 
# level causes instant crashes when worker processes spawn.
fetch_cooldowns = None
task_queue = None

@app.route('/')
def home():
    return render_template('index.html')

def on_demand_worker(q):
    """
    Runs continuously in a SINGLE dedicated parallel process.
    It pulls fetch requests from the queue one at a time so the server's RAM
    never gets overwhelmed by simultaneous Herbie/xarray builds.
    """
    print("\n--- [BOOT] On-Demand Queue Worker Initialized ---")
    import build_cache
    
    while True:
        # This will block/wait here until a task is added to the queue
        task = q.get() 
        model, date, cycle, fxx = task
        
        print(f"\n--- [ON-DEMAND] Processing fetch from queue: {model.upper()} {cycle}Z F{fxx:02} ---")
        
        # Save the original function just in case
        original_finder = build_cache.get_recent_runs
        
        # Create a fake run finder that points explicitly to the requested cycle
        def forced_run_finder(model_type):
            return [{'date': date, 'hour': cycle}]
            
        # Inject the fake run finder (Monkeypatching)
        build_cache.get_recent_runs = forced_run_finder
        
        try:
            # It will skip all the existing files and build the missing FXX
            build_cache.build_model_cache(model_type=model, max_fxx=fxx)
        except Exception as e:
            print(f"  [ON-DEMAND ERROR] Failed to force-fetch {model.upper()} F{fxx:02}: {e}")
        finally:
            # Restore the original logic before grabbing the next task
            build_cache.get_recent_runs = original_finder

@app.route('/api/force_fetch', methods=['POST'])
def force_fetch():
    """
    Listens for the frontend crawler and adds the requested frame to the queue.
    """
    model = request.args.get('model')
    date = request.args.get('date')
    cycle = request.args.get('cycle')
    
    try:
        fxx = int(request.args.get('fxx'))
    except (TypeError, ValueError):
        return jsonify({"status": "error", "message": "Invalid FXX"}), 400

    task_id = f"{model}_{date}_{cycle}_{fxx}"
    current_time = time.time()
    
    # Check if we recently tried to force fetch this exact frame (60-second cooldown)
    if fetch_cooldowns is not None and task_id in fetch_cooldowns:
        time_since_last = current_time - fetch_cooldowns[task_id]
        if time_since_last < 60:
            return jsonify({"status": "Cooldown active", "message": "Checked recently. Waiting for NOAA."})
            
    # Mark the current time for this task to trigger the cooldown
    if fetch_cooldowns is not None:
        fetch_cooldowns[task_id] = current_time

    print(f"  [QUEUE] Adding {model.upper()} {cycle}Z F{fxx:02} to the processing queue.")
    
    # Drop the task into the single-file line instead of spawning a new process
    if task_queue is not None:
        task_queue.put((model, date, cycle, fxx))
    
    return jsonify({"status": "Queued", "model": model, "fxx": fxx})

# --- PARALLEL WORKER DEFINITIONS ---

def run_model_loop(model_name, max_fxx):
    """Dedicated parallel loop for a single model"""
    print(f"\n--- [BOOT] {model_name.upper()} Dedicated Worker Initialized ---")
    while True:
        try:
            build_model_cache(model_type=model_name, max_fxx=max_fxx)
        except Exception as e:
            print(f"\n--- [ERROR] {model_name.upper()} Pipeline Failed: {e}. ---")
        
        print(f"--- [STANDBY] {model_name.upper()} cycle complete. Resting for 2 minutes ---")
        time.sleep(120)

if __name__ == '__main__':
    # Initialize the heavy multiprocessing items safely inside the main thread block!
    manager = multiprocessing.Manager()
    fetch_cooldowns = manager.dict()
    task_queue = multiprocessing.Queue()

    # 1. Start Support Services (NWS Point Forecasts, NWS Alerts, ASOS Conditions)
    services_thread = threading.Thread(target=backend_services_loop, daemon=True)
    services_thread.start()

    # 2. Boot up the parallel backend sync workers
    p_hrrr = multiprocessing.Process(target=run_model_loop, args=('hrrr', 48))
    p_hrrr.daemon = True
    p_hrrr.start()
    time.sleep(2)

    p_gfs = multiprocessing.Process(target=run_model_loop, args=('gfs', 120))
    p_gfs.daemon = True
    p_gfs.start()
    time.sleep(2)

    # 3. Boot up the queue worker for on-demand frontend requests
    od_worker = multiprocessing.Process(target=on_demand_worker, args=(task_queue,))
    od_worker.daemon = True
    od_worker.start()

    # Run the Flask app
    app.run(host='0.0.0.0', debug=True, port=5000, use_reloader=False)