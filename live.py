import pandas as pd
import random
import time
from datetime import datetime
from pathlib import Path

# --- Configuration ---
SENSORS = [f"SENSOR_{i:02d}" for i in range(1, 11)]
SPECIFIC_CITY = "Nagpur"
INTERVAL_SECONDS = 5  # How often to generate new "live" data

def generate_live_reading():
    """Generates a single snapshot of data for all sensors at the current moment."""
    readings = []
    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    for sensor_id in SENSORS:
        rate = round(random.uniform(15.0, 45.0), 2)
        readings.append({
            'timestamp': current_time,
            'sensor_id': sensor_id,
            'city': SPECIFIC_CITY,
            'flow_rate': rate,
            'pressure': round(rate * random.uniform(0.10, 0.50), 2),
            'status': random.choices(['Active', 'Maintenance', 'Error'], weights=[95, 3, 2])[0]
        })
    return readings

def main():
    print(f"📡 Starting Live Sensor Stream for {SPECIFIC_CITY}...")
    print(f"Logging data every {INTERVAL_SECONDS} seconds. Press Ctrl+C to stop.\n")
    
    output_dir = Path("live_data")
    output_dir.mkdir(exist_ok=True)
    output_file = output_dir / "nagpur_live_stream.csv"

    while True:
        try:
            # 1. Generate new data
            new_data = generate_live_reading()
            df_new = pd.DataFrame(new_data)

            # 2. Append to CSV (header=False if file already exists)
            file_exists = output_file.exists()
            df_new.to_csv(output_file, mode='a', index=False, header=not file_exists)

            # 3. Print to console for "Live" effect
            print(f"[{datetime.now().strftime('%H:%M:%S')}] Sent {len(SENSORS)} readings to {output_file.name}")
            
            # 4. Wait for the next interval
            time.sleep(INTERVAL_SECONDS)
            
        except KeyboardInterrupt:
            print("\n🛑 Live stream stopped by user.")
            break

if __name__ == "__main__":
    # main()