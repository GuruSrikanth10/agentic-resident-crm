import json
import sys
import os
import requests
from dotenv import load_dotenv

# Without this, AGENTIC_RESIDENT_CRM_API_KEY falls back to "dev-secret-key" while the
# API server -- which does load .env -- validates against the real key, so
# every local run got a 403 that looked like a server problem.
load_dotenv()

def run_local(file_path: str):
    if not os.path.exists(file_path):
        print(f"Error: File {file_path} not found.")
        return

    with open(file_path, "r", encoding="utf-8") as f:
        data = json.load(f)
        
    print(f"Read packet {data.get('eventId')}. Sending it to the internal FastAPI endpoint.")
    
    try:
        # We need to pass the API key for the new security middleware
        api_key = os.environ.get("AGENTIC_RESIDENT_CRM_API_KEY", "dev-secret-key")
        headers = {"X-API-Key": api_key}
        
        response = requests.post("http://localhost:8000/process-rejection", json=data, headers=headers)
        response.raise_for_status()
        print("Processed successfully.")
        print(response.json())
    except Exception as e:
        print(f"Error communicating with local server: {e}")
        if "response" in locals() and hasattr(response, "text"):
            print(f"Server response: {response.text}")
        print("Make sure the server is running: python src/main_api.py")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python local_run.py path/to/packet.json")
        sys.exit(1)
        
    run_local(sys.argv[1])
