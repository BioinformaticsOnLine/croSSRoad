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
    
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)

    # --- Handler Management --- 
    # Remove existing File and Console Handlers to avoid duplication/conflicts
    for handler in root_logger.handlers[:]:
        if isinstance(handler, (logging.FileHandler, RichHandler, logging.StreamHandler)):
            # Keep handlers not belonging to these types (e.g., from other libs)
            root_logger.removeHandler(handler)
            try:
                 handler.close()
            except Exception:
                 pass # Ignore errors on close, e.g., if already closed

    # --- File Handler --- 
    log_format = '%(asctime)s - %(levelname)s [%(name)s:%(lineno)d] - %(message)s'
    file_formatter = logging.Formatter(log_format, datefmt='%Y-%m-%d %H:%M:%S')
    file_handler = logging.FileHandler(log_file)
    file_handler.setFormatter(file_formatter)
    root_logger.addHandler(file_handler)

    # --- Console Handler (Always add RichHandler) ---
    # Use the passed 'console' object (important for CLI), 
    # RichHandler defaults if console is None (likely API context)
    console_handler = RichHandler(
        console=console, 
        rich_tracebacks=True,
        markup=True,
        show_time=False, 
        show_level=True,
        show_path=False, # Keep path off for console to reduce noise
        log_time_format="[%H:%M:%S]"
    )
    root_logger.addHandler(console_handler)
    # --------------------------

    logger = logging.getLogger(job_id)

    # Log initial info (will go to both file and console)
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