import logging
import os

def setup_logging(job_id, job_dir):
    """Standardized logging setup for both CLI and API"""
    # Create log file path
    log_file = os.path.join(job_dir, f"{job_id}.log")
    os.makedirs(os.path.dirname(log_file), exist_ok=True)
    
    # Clear any existing handlers
    logging.getLogger().handlers = []
    
    # Configure logging without timestamps
    logging.basicConfig(
        level=logging.INFO,
        format='%(message)s',  # Removed timestamp
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler()
        ]
    )
    
    logger = logging.getLogger(job_id)
    
    # Add source identification
    logger.info(f"Job ID: {job_id}")
    return logger 