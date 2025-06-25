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

def main():
    parser = argparse.ArgumentParser(description="CrossRoad Slurm Job Runner")
    parser.add_argument("--job-id", required=True, help="The unique ID for this job.")
    parser.add_argument("--params-file", required=True, type=Path, help="Path to the JSON file containing task parameters.")
    parser.add_argument("--root-dir", required=True, type=Path, help="Path to the CrossRoad project root directory.")
    
    args = parser.parse_args()

    # Add root_dir to sys.path to allow for correct imports
    if str(args.root_dir) not in sys.path:
        sys.path.insert(0, str(args.root_dir))

    # Now that sys.path is configured, we can import from the project
    try:
        from crossroad.api.main import run_analysis_pipeline, PerfParams, JobStatus # Assuming enums/models are accessible
        from crossroad.core.logger import setup_logging as setup_pipeline_logging # For the pipeline's own logger
    except ImportError as e:
        # Fallback basic logging if main logger setup fails before imports
        logging.basicConfig(level=logging.ERROR)
        logging.error(f"Failed to import necessary modules. Ensure --root-dir is correct and PYTHONPATH is set. Error: {e}", exc_info=True)
        # Try to write a status file indicating this critical failure
        job_dir_for_status = args.root_dir / "jobOut" / args.job_id
        status_file_path = job_dir_for_status / "status.json"
        try:
            os.makedirs(job_dir_for_status, exist_ok=True)
            with open(status_file_path, 'w') as sf:
                json.dump({
                    "status": "failed", # Using string directly as JobStatus enum might not be imported
                    "message": "Slurm runner failed: Critical import error.",
                    "error_details": f"ImportError: {e}\nPYTHONPATH: {os.getenv('PYTHONPATH')}\nsys.path: {sys.path}",
                    "progress": 0.0,
                    "timestamp": datetime.now(timezone.utc).isoformat() + "Z"
                }, sf, indent=4)
        except Exception as e_stat:
            logging.error(f"Additionally failed to write critical failure status to {status_file_path}: {e_stat}")
        sys.exit(1)


    # Load parameters
    if not args.params_file.exists():
        # This case should ideally be caught before sbatch submission, but good to check.
        # Basic logging as full logger might not be set up.
        logging.basicConfig(level=logging.ERROR)
        logging.error(f"Parameters file {args.params_file} not found for job {args.job_id}.")
        sys.exit(1) # Exit, Slurm will mark job as failed.

    with open(args.params_file, 'r') as f:
        task_params = json.load(f)

    job_dir = Path(task_params['job_dir']) # This path is absolute, from the API's perspective
    
    # Setup logging for this runner script itself
    logger = setup_slurm_job_logging(args.job_id, job_dir)
    logger.info(f"Slurm runner started for job {args.job_id}.")
    logger.info(f"PYTHONPATH: {os.getenv('PYTHONPATH')}")
    logger.info(f"sys.path: {sys.path}")
    logger.info(f"Loaded task parameters from {args.params_file}: {task_params}")


    # Re-hydrate Pydantic models and other necessary objects
    # The 'logger' in task_params was for the API side; pipeline needs its own.
    # The 'loop' is not applicable here.
    pipeline_logger = setup_pipeline_logging(args.job_id, job_dir) # Setup the job-specific logger for the pipeline
    task_params['logger'] = pipeline_logger
    task_params['loop'] = None # Explicitly set to None, as there's no API event loop here

    if 'perf_params' in task_params and isinstance(task_params['perf_params'], dict):
        try:
            task_params['perf_params'] = PerfParams(**task_params['perf_params'])
        except Exception as e:
            logger.error(f"Failed to re-instantiate PerfParams: {e}", exc_info=True)
            # Update status.json to FAILED
            status_file_path = job_dir / "status.json"
            with open(status_file_path, 'w') as sf:
                json.dump({
                    "status": JobStatus.FAILED.value,
                    "message": "Slurm runner failed: PerfParams instantiation error.",
                    "error_details": traceback.format_exc(),
                    "progress": 0.0,
                    "reference_id": task_params.get("reference_id"),
                    "timestamp": datetime.now(timezone.utc).isoformat() + "Z"
                }, sf, indent=4)
            sys.exit(1)
    
    final_status = JobStatus.COMPLETED
    error_details_str = None
    final_message = "Analysis completed successfully via Slurm."

    try:
        logger.info(f"Calling run_analysis_pipeline for job {args.job_id}...")
        # Ensure all paths in task_params are absolute or resolvable from current context
        # The paths should have been stored as absolute strings by the API.
        run_analysis_pipeline(**task_params)
        logger.info(f"run_analysis_pipeline completed for job {args.job_id}.")
        final_progress = 1.0

    except Exception as e:
        logger.error(f"Exception during run_analysis_pipeline for job {args.job_id}: {e}", exc_info=True)
        final_status = JobStatus.FAILED
        final_message = f"Analysis failed in Slurm: {str(e)}"
        error_details_str = traceback.format_exc()
        final_progress = task_params.get("progress", 0.0) # Keep last known progress or 0
    finally:
        logger.info(f"Finalizing job {args.job_id}. Status: {final_status.value}")
        status_payload = {
            "status": final_status.value,
            "message": final_message,
            "progress": final_progress if final_status == JobStatus.COMPLETED else task_params.get("progress",0.0) , # task_params might not have progress if it failed early
            "error_details": error_details_str,
            "reference_id": task_params.get("reference_id"), # Get from original params
            "slurm_job_id": os.getenv("SLURM_JOB_ID", "N/A"), # Add Slurm job ID from environment
            "completed_at": datetime.now(timezone.utc).isoformat() + "Z"
        }
        status_file_path = job_dir / "status.json"
        try:
            with open(status_file_path, 'w') as sf:
                json.dump(status_payload, sf, indent=4)
            logger.info(f"Final status for job {args.job_id} written to {status_file_path}")
        except Exception as e_stat:
            logger.error(f"CRITICAL: Failed to write final status to {status_file_path} for job {args.job_id}: {e_stat}", exc_info=True)

    logger.info(f"Slurm runner finished for job {args.job_id}.")
    if final_status == JobStatus.FAILED:
        sys.exit(1) # Ensure Slurm marks the job as failed if the pipeline failed

if __name__ == "__main__":
    main()