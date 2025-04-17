import logging
import os
import sys
from rich.logging import RichHandler # Use RichHandler instead of colorlog
# import threading # Remove threading import again

# (Removed LockingStreamHandler as spinner is on stderr and logs go to stdout)

def setup_logging(job_id, job_dir, args_namespace=None, console=None): # Accept parsed args
    """Standardized logging setup for both CLI and API, optionally using a Rich Console."""
    # Create log file path
    log_file = os.path.join(job_dir, f"{job_id}.log")
    os.makedirs(os.path.dirname(log_file), exist_ok=True)
    
    # Clear any existing handlers
    root_logger = logging.getLogger()
    console_handler = None # Keep track of the console handler

    if not root_logger.hasHandlers():
        root_logger.handlers = []
        root_logger.setLevel(logging.INFO)

    # Define formatters
        log_format = '%(asctime)s - %(levelname)s - %(message)s'
        file_formatter = logging.Formatter(log_format, datefmt='%Y-%m-%d %H:%M:%S')
        # Removed colorlog formatters and SymbolFormatter class
    
    # Create file handler
        file_handler = logging.FileHandler(log_file)
        file_handler.setFormatter(file_formatter)
        root_logger.addHandler(file_handler)

        # Use RichHandler for console output
        # It handles colors, formatting, and tracebacks automatically
        console_handler = RichHandler(
            console=console,    # Use the explicitly passed console (stdout)
            rich_tracebacks=True,
            markup=True,
            show_time=False,
            show_level=True,
            show_path=True,       # Enable showing file path and line number
            log_time_format="[%H:%M:%S]" # Simpler time format
        )
        # No need to set a formatter for RichHandler
        root_logger.addHandler(console_handler)
    else:
        # Find existing console handler if logger was already configured
        for handler in root_logger.handlers:
            # Check for the RichHandler instance
            if isinstance(handler, RichHandler):
                console_handler = handler
                break
    # Get the specific logger for the job_id, but configuration is done on root
    logger = logging.getLogger(job_id)
    logger.setLevel(logging.INFO)

    # Log initial info
    logger.info(f"Job ID: {job_id}")
    logger.info(f"Log File: {log_file}")

    # Log the parsed arguments if provided
    if args_namespace:
        logger.info("--- Runtime Parameters ---")
        # Log relevant parameters (adjust which ones are important)
        params_to_log = ['fasta', 'input_dir', 'categories', 'gene_bed', 'reference_id', 'output_dir', 'flanks',
                         'mono', 'di', 'tri', 'tetra', 'penta', 'hexa', 'min_len', 'max_len', 'unfair', 'threads',
                         'min_repeat_count', 'min_genome_count', 'plots']
        for param in params_to_log:
            if hasattr(args_namespace, param):
                 value = getattr(args_namespace, param)
                 if value is not None: # Only log if set or has a default
                     logger.info(f"  {param}: {value}")
        logger.info("--------------------------")

    # Removed explicit flush - RichHandler manages its output
    return logger