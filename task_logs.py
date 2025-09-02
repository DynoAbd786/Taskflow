import json
import os
from datetime import datetime
from pathlib import Path

class TaskLogger:
    def __init__(self, log_dir=None):
        if log_dir is None:
            # Use absolute path relative to the script location
            current_dir = Path(__file__).parent.absolute()
            log_dir = current_dir / "task_logs"
        else:
            log_dir = Path(log_dir)
        self.log_dir = log_dir
        self.log_dir.mkdir(exist_ok=True)
        
    def log_success(self, task_id, task_name, result, duration):
        log_entry = {
            "timestamp": datetime.now().isoformat(),
            "task_id": task_id,
            "task_name": task_name,
            "status": "success",
            "result": result,
            "duration": duration
        }
        
        success_file = self.log_dir / "successes.jsonl"
        try:
            with open(success_file, "a") as f:
                f.write(json.dumps(log_entry) + "\n")
        except Exception:
            pass
    
    def log_failure(self, task_id, task_name, error, duration):
        log_entry = {
            "timestamp": datetime.now().isoformat(),
            "task_id": task_id,
            "task_name": task_name,
            "status": "failure", 
            "error": str(error),
            "duration": duration
        }
        
        failure_file = self.log_dir / "failures.jsonl"
        try:
            with open(failure_file, "a") as f:
                f.write(json.dumps(log_entry) + "\n")
        except Exception:
            pass

# Global logger instance
task_logger = TaskLogger()