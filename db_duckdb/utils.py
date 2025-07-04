import os
import psutil
import logging

# Configure logging to file and console
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s',
    handlers=[
        logging.FileHandler('duckdb_memory.log'),
        logging.StreamHandler()
    ]
)

def log_memory_usage(stage=""):
    """
    Log current memory usage (RSS) in MB, with an optional stage description.
    Logs to both console and file.
    """
    process = psutil.Process(os.getpid())
    mem_mb = process.memory_info().rss / 1024 / 1024
    msg = f"[MEMORY] {stage}: {mem_mb:.2f} MB used"
    print(msg)
    logging.info(msg) 