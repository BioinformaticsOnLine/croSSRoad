import argparse
import json
import logging
import os
import sys
import traceback
from pathlib import Path
from datetime import datetime, timezone # Added timezone

# Ensure the project root is in the Python path to allow imports like crossroad.api.main
# The --root-dir argument will provide this.
# This script is intended to be called with PYTHONPATH including the project root,
# but this is an additional safeguard.

def setup_slurm_job_logging(job_id: str, job_dir: Path):
    """Sets up logging for the Slurm runner script."""
    log_dir = job_dir # Log directly into the job's main directory or a sub-log dir
    os.makedirs(log_dir, exist_ok=True)
    log_file_path = log_dir / f"{job_id}_slurm_runner.log"

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s [%(name)s:%(lineno)d] - %(message)s',
        handlers=[
            logging.FileHandler(log_file_path),
            logging.StreamHandler(sys.stdout) # Also print to Slurm job's stdout
        ]
    )
    return logging.getLogger(f"SlurmRunner.{job_id}")

def update_status_on_disk(job_dir: Path, job_id: str, message: str, progress: float, status: str = "running", error_details: str = None):
    """
    Reads, updates, and writes the status.json file for a job.
    This provides a thread-safe way for a Slurm job to report progress.
    """
    status_file_path = job_dir / "status.json"
    payload = {}
    try:
        # Read existing data if file exists
        if status_file_path.exists():
            with open(status_file_path, 'r') as f:
                payload = json.load(f)

        # Update fields
        payload['status'] = status
        payload['message'] = message
        payload['progress'] = progress
        if error_details:
            payload['error_details'] = error_details
        
        payload['updated_at'] = datetime.now(timezone.utc).isoformat()

        # Write back to file
        with open(status_file_path, 'w') as f:
            json.dump(payload, f, indent=4)

    except (IOError, json.JSONDecodeError) as e:
        # If status update fails, log it. The job will continue.
        logging.getLogger(f"SlurmRunner.{job_id}").error(f"Failed to update status file {status_file_path}: {e}")


def main():
    parser = argparse.ArgumentParser(description="CrossRoad Slurm Job Runner")
    parser.add_argument("--job-id", required=True, help="The unique ID for this job.")
    parser.add_argument("--params-file", required=True, type=Path, help="Path to the JSON file containing task parameters.")
    parser.add_argument("--root-dir", required=True, type=Path, help="Path to the CrossRoad project root directory.")
    
    args = parser.parse_args()

    # --- Path and Module Setup ---
    if str(args.root_dir) not in sys.path:
        sys.path.insert(0, str(args.root_dir))

    try:
        from crossroad.api.main import run_analysis_pipeline, PerfParams, JobStatus
        from crossroad.core.logger import setup_logging as setup_pipeline_logging
    except ImportError as e:
        # This is a critical failure. Log and attempt to write a failure status.
        logging.basicConfig(level=logging.ERROR)
        logging.error(f"Failed to import modules: {e}", exc_info=True)
        job_dir_for_status = args.root_dir / "jobOut" / args.job_id
        job_dir_for_status.mkdir(exist_ok=True)
        update_status_on_disk(
            job_dir=job_dir_for_status, job_id=args.job_id,
            status=JobStatus.FAILED.value,
            message="Slurm runner failed: Critical import error.",
            progress=0.0,
            error_details=f"ImportError: {e}\nPYTHONPATH: {os.getenv('PYTHONPATH')}\nsys.path: {sys.path}"
        )
        sys.exit(1)

    # --- Parameter and Logging Setup ---
    if not args.params_file.exists():
        logging.basicConfig(level=logging.ERROR)
        logging.error(f"Parameters file {args.params_file} not found.")
        sys.exit(1)

    with open(args.params_file, 'r') as f:
        task_params = json.load(f)

    job_dir = Path(task_params['job_dir'])
    logger = setup_slurm_job_logging(args.job_id, job_dir)
    logger.info(f"Slurm runner started for job {args.job_id}. Slurm Job ID: {os.getenv('SLURM_JOB_ID')}")
    logger.info(f"Loaded task parameters from {args.params_file}")

    # --- Prepare Pipeline Arguments ---
    pipeline_logger = setup_pipeline_logging(args.job_id, str(job_dir))
    task_params['logger'] = pipeline_logger
    
    # Re-hydrate Pydantic model
    if 'perf_params' in task_params and isinstance(task_params['perf_params'], dict):
        task_params['perf_params'] = PerfParams(**task_params['perf_params'])

    # Create the status update callback
    def status_callback(message: str, progress: float):
        update_status_on_disk(job_dir, args.job_id, message, progress, status=JobStatus.RUNNING.value)

    task_params['status_update_callback'] = status_callback

    # --- Execute Pipeline ---
    final_status = JobStatus.COMPLETED
    error_details_str = None
    final_message = "Analysis completed successfully via Slurm."
    final_progress = 0.0

    try:
        logger.info(f"Calling run_analysis_pipeline for job {args.job_id}...")
        # Update status to 'running' just before execution starts
        update_status_on_disk(job_dir, args.job_id, "Starting analysis pipeline...", 0.0, status=JobStatus.RUNNING.value)
        
        run_analysis_pipeline(**task_params)
        
        logger.info(f"run_analysis_pipeline completed for job {args.job_id}.")
        final_progress = 1.0

    except Exception as e:
        logger.error(f"Exception during run_analysis_pipeline for job {args.job_id}: {e}", exc_info=True)
        final_status = JobStatus.FAILED
        final_message = f"Analysis failed in Slurm: {str(e)}"
        error_details_str = traceback.format_exc()
        # Try to get the last known progress from the status file
        try:
            with open(job_dir / "status.json", 'r') as f:
                final_progress = json.load(f).get('progress', 0.0)
        except Exception:
            final_progress = 0.0 # Default if reading fails
    finally:
        logger.info(f"Finalizing job {args.job_id}. Status: {final_status.value}")
        update_status_on_disk(
            job_dir=job_dir,
            job_id=args.job_id,
            status=final_status.value,
            message=final_message,
            progress=final_progress,
            error_details=error_details_str
        )

    logger.info(f"Slurm runner finished for job {args.job_id}.")
    if final_status == JobStatus.FAILED:
        sys.exit(1)

if __name__ == "__main__":
    main()
