import threading
import requests
import os
import time
from dotenv import load_dotenv
load_dotenv()

def _send_log_worker(payload, url):
    try:
        requests.post(url, json=payload, timeout=30)
    except Exception as e:
        print(str(e))
        pass

def send_log_async(log_type, function_name, parameters, message, url=f"{os.getenv("LOGGER_URL")}/logs"):
    payload = {
        "type": log_type,
        "function_name": function_name,
        "parameters": parameters,
        "message": message
    }
    threading.Thread(target=_send_log_worker, args=(payload, url), daemon=True).start()

def service_wake_up():
    count = 1
    while(True):
        print(f"calling {count}")
        if call_health():
            break 
        time.sleep(5)
        count+=1
    return

def call_health():
    url = f"{os.getenv('LOGGER_URL')}/health"
    try:
        response = requests.get(url, timeout=5)
        return response.status_code == 200
    except Exception:
        return False    