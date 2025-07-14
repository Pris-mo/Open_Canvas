import os
import json
import requests

class StorageManager:
    def __init__(self, base_dir, logger):
        self.base_dir = base_dir
        self.logger   = logger

    def write_json(self, record):
        # Use type + id if present, else just type
        suffix = f"_{record['id']}" if "id" in record else ""
        filename = f"{record['type']}{suffix}.json"
        
        path = os.path.join(self.base_dir, "json_output", filename)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(record, f, indent=2)
        self.logger.debug(f"Wrote JSON → {path}")

    def download_file(self, url, file_path):
        full_path = os.path.join(self.base_dir, "raw_files", file_path.lstrip("/"))
        os.makedirs(os.path.dirname(full_path), exist_ok=True)
        r = requests.get(url, stream=True)
        r.raise_for_status()
        with open(full_path, "wb") as f:
            for chunk in r.iter_content(1024):
                f.write(chunk)
        self.logger.debug(f"Downloaded raw file → {full_path}")
