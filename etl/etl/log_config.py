import os
import logging
from datetime import datetime

# Ensure the /log directory exists
os.makedirs('log', exist_ok=True)

# Configure logging to write to a file in the /log folder with datetime in the filename
log_filename = f"log/etl_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_filename),
        logging.StreamHandler()
    ]
)