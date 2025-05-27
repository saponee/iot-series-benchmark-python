import random
from datetime import datetime, timezone

class Device:
    def __init__(self):
        # Se device_id non è specificato, genera un ID casuale da 'sensor_1' a 'sensor_10'
        self.device_id = f"sensor_{random.randint(1, 10)}"

    def generate_data(self, timestamp=None):
        # Se il timestamp non è specificato, usa il timestamp corrente UTC
        if not timestamp:
            timestamp = datetime.now(timezone.utc)
        return {
            "device": self.device_id,
            "temperature": round(random.uniform(20, 30), 2),
            "humidity": round(random.uniform(30, 60), 2),
            "timestamp": timestamp
        }