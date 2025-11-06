from fastapi import (
    FastAPI, UploadFile, File, Form, HTTPException, Request, BackgroundTasks,
    Depends # Added for dependency injection if needed later
)
from fastapi.middleware.cors import CORSMiddleware # Import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse, Response # Added Response
import shutil
import os
import time
import json
from typing import Optional, Dict, Any, Tuple, List # Added List, Tuple
import uvicorn
from pydantic import BaseModel
import argparse
import logging
from datetime import datetime
import asyncio
from enum import Enum
import pyarrow as pa # For Arrow conversion
import pyarrow.ipc as ipc # For Arrow IPC format
import pandas as pd # To read data files
from contextlib import asynccontextmanager # For lifespan management
import traceback # For error details
from pathlib import Path
from typing import Callable # Add Callable for the callback

# --- Centralized Config and Slurm Manager ---
from crossroad import config
from crossroad.core.slurm_manager import SlurmManager
# --- End Centralized Config ---

from crossroad.core.logger import setup_logging
# Update imports to use absolute imports from core
from crossroad.core import m2
from crossroad.core import gc2
from crossroad.core import process_ssr_results
# from crossroad.core.plotting import generate_all_plots # Commented out - Plots handled by frontend

# --- Configuration (Now handled by config.py) ---
# MAX_CONCURRENT_JOBS = 2 # Now in config.py
# ROOT_DIR = Path(os.getenv("CROSSROAD_ROOT", Path(__file__).resolve().parents[2])) # Now in config.py
# --- Job Status Enum ---
class JobStatus(str, Enum):
    QUEUED = "queued"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"

# --- In-Memory Job Tracking and Queuing ---
# This is used for 'local' execution mode.
job_statuses: Dict[str, Dict[str, Any]] = {} # {job_id: {"status": JobStatus, "message": str, "progress": float, "error_details": str|None}}
job_queue: asyncio.Queue[Tuple[str, Dict[str, Any]]] = asyncio.Queue() # Stores (job_id, task_params)
active_job_count = 0
queue_lock = asyncio.Lock() # To protect access to active_job_count and job_statuses

# --- Background Status Poller (for Slurm mode) ---
async def slurm_status_poller():
    """
    Periodically checks the status.json files for jobs submitted to Slurm
    and updates the in-memory job_statuses dictionary.
    """
    logger = logging.getLogger("SlurmPoller")
    logger.info("Slurm status poller started.")
    while True:
        try:
            async with queue_lock:
                # Create a copy of job IDs to check to avoid issues with dict size changing
                job_ids_to_check = list(job_statuses.keys())

            for job_id in job_ids_to_check:
                async with queue_lock:
                    # Re-check if job still exists and is in a pollable state
                    if job_id not in job_statuses:
                        continue
                    
                    current_status_in_mem = job_statuses[job_id].get("status")
                    # Only poll for jobs that are not in a final state
                    if current_status_in_mem not in [JobStatus.QUEUED, JobStatus.RUNNING]:
                        continue

                # Read status from disk without holding the lock
                status_file = config.JOB_OUTPUT_DIR / job_id / "status.json"
                if status_file.exists():
                    try:
                        with open(status_file, 'r') as f:
                            data_on_disk = json.load(f)
                        
                        status_on_disk = JobStatus(data_on_disk.get("status", "failed"))
                        
                        # If status has changed, update it in memory and log it
                        if status_on_disk != current_status_in_mem:
                            async with queue_lock:
                                # Final check before update
                                if job_id in job_statuses:
                                    job_statuses[job_id]["status"] = status_on_disk
                                    job_statuses[job_id]["message"] = data_on_disk.get("message", "")
                                    job_statuses[job_id]["progress"] = data_on_disk.get("progress", 0.0)
                                    job_statuses[job_id]["error_details"] = data_on_disk.get("error_details")
                                    logger.info(f"Job {job_id} status updated to: {status_on_disk.value}")

                    except (json.JSONDecodeError, KeyError, ValueError) as e:
                        logger.warning(f"Could not parse status file for job {job_id} during polling: {e}")

            await asyncio.sleep(10) # Poll every 10 seconds

        except asyncio.CancelledError:
            logger.info("Slurm status poller cancelling...")
            break
        except Exception as e:
            logger.error(f"Error in Slurm status poller: {e}", exc_info=True)
            await asyncio.sleep(30) # Wait longer after an unexpected error


# --- Lifespan Management (for starting queue consumer) ---
@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup: Initialize directories and start queue consumer if in local mode
    print("Initializing Crossroad API...")
    config.initialize_directories() # Create jobOut dir if it doesn't exist
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s [%(name)s:%(lineno)d] - %(message)s')
    
    consumer_task = None
    poller_task = None

    await load_persistent_statuses()

    if config.EXECUTION_MODE == "local":
        print(f"Execution mode: 'local'. Starting queue consumer with concurrency {config.MAX_CONCURRENT_JOBS}.")
        consumer_task = asyncio.create_task(queue_consumer())
    else:
        print(f"Execution mode: 'slurm'. API will delegate jobs to Slurm. Starting status poller.")
        poller_task = asyncio.create_task(slurm_status_poller())

    yield
    
    # Shutdown: Cancel the consumer task gracefully if it was started
    if consumer_task:
        print("Shutting down queue consumer...")
        consumer_task.cancel()
        try:
            await consumer_task
        except asyncio.CancelledError:
            print("Queue consumer task cancelled.")
    
    if poller_task:
        print("Shutting down Slurm status poller...")
        poller_task.cancel()
        try:
            await poller_task
        except asyncio.CancelledError:
            print("Slurm status poller cancelled.")

    print("Crossroad API shutdown complete.")

# Create app instance with lifespan manager
app = FastAPI(
    title="CrossRoad Analysis Pipeline",
    description="API for analyzing SSRs in genomic data with job queuing and Slurm support",
    version="0.3.5", # Version bump
    lifespan=lifespan # Add lifespan manager
)

# --- Add CORS Middleware ---
origins = ["*"] # Allow all origins

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"], # Allows all methods
    allow_headers=["*"], # Allows all headers
)

async def load_persistent_statuses():
    """Loads job statuses from status.json files in the job output directory."""
    logger = logging.getLogger("startup")
    if not config.JOB_OUTPUT_DIR.exists():
        logger.info(f"Job output directory {config.JOB_OUTPUT_DIR} not found, skipping status load.")
        return
    
    logger.info(f"Scanning for existing jobs in {config.JOB_OUTPUT_DIR}...")
    for job_dir in config.JOB_OUTPUT_DIR.iterdir():
        if not job_dir.is_dir():
            continue
        
        status_file = job_dir / "status.json"
        if status_file.exists():
            try:
                with open(status_file) as f:
                    data = json.load(f)
                # Basic validation
                if "status" in data and "message" in data:
                    job_statuses[job_dir.name] = {
                        "status": JobStatus(data["status"]),
                        "message": data.get("message", ""),
                        "progress": data.get("progress", 0.0),
                        "error_details": data.get("error_details"),
                        "reference_id": data.get("reference_id"),
                        "slurm_job_id": data.get("slurm_job_id")
                    }
                    logger.info(f"Loaded status for job {job_dir.name}: {data['status']}")
                else:
                    logger.warning(f"Skipping invalid status.json for {job_dir.name} (missing fields).")
            except (json.JSONDecodeError, KeyError, ValueError) as e:
                logger.warning(f"Failed to parse status.json for {job_dir.name}: {e}")
        else:
            # If no status file, but output exists, assume it's a legacy completed job
            merged_file = job_dir / "output" / "main" / "mergedOut.tsv"
            if merged_file.exists():
                logger.info(f"Found legacy completed job {job_dir.name} (no status.json).")
                job_statuses[job_dir.name] = {
                    "status": JobStatus.COMPLETED,
                    "message": "Completed (loaded from existing output files)",
                    "progress": 1.0,
                    "error_details": None,
                    "reference_id": None,
                    "slurm_job_id": None
                }
                # Create a status.json for it for future consistency
                try:
                    with open(status_file, "w") as sf:
                        json.dump({k: (v.value if isinstance(v, JobStatus) else v) for k, v in job_statuses[job_dir.name].items()}, sf, indent=4)
                except Exception as persist_err:
                    logger.warning(f"Could not write new status.json for legacy job {job_dir.name}: {persist_err}")


# --- Performance Parameters Model ---
class PerfParams(BaseModel):
    mono: int = 10
    di: int = 6
    tri: int = 4
    tetra: int = 3
    penta: int = 2
    hexa: int = 2
    minLen: int = 1000
    maxLen: int = 10000000
    unfair: int = 0
    thread: int = 50
    min_repeat_count: int = 1
    min_genome_count: int = 2

# --- Queue Consumer Task ---
async def queue_consumer():
    """Continuously checks the queue and starts jobs if concurrency allows."""
    global active_job_count
    logger = logging.getLogger("QueueConsumer")
    logger.info("Local queue consumer started.")
    while True:
        try:
            async with queue_lock:
                can_start_immediately = active_job_count < config.MAX_CONCURRENT_JOBS

            if can_start_immediately:
                try:
                    job_id, task_params = await asyncio.wait_for(job_queue.get(), timeout=1.0)
                except asyncio.TimeoutError:
                    await asyncio.sleep(1)
                    continue

                logger.info(f"Dequeued job {job_id}. Attempting to start.")
                async with queue_lock:
                    if active_job_count < config.MAX_CONCURRENT_JOBS:
                        active_job_count += 1
                        job_statuses[job_id]["status"] = JobStatus.RUNNING
                        job_statuses[job_id]["message"] = "Starting analysis..."
                        logger.info(f"Starting job {job_id}. Active jobs now: {active_job_count}")
                        asyncio.create_task(run_analysis_pipeline_wrapper(job_id, task_params))
                    else:
                        logger.warning(f"Concurrency limit reached just before starting {job_id}. Re-queuing.")
                        await job_queue.put((job_id, task_params))
                job_queue.task_done()
            else:
                await asyncio.sleep(1)

        except asyncio.CancelledError:
            logger.info("Queue consumer cancelling...")
            break
        except Exception as e:
            logger.error(f"Error in queue consumer: {e}", exc_info=True)
            await asyncio.sleep(5)


# --- Wrapper for Background Task to handle completion/failure ---
async def run_analysis_pipeline_wrapper(job_id: str, task_params: Dict[str, Any]):
    """Wraps the main analysis function to update status and manage concurrency count."""
    global active_job_count
    logger = task_params['logger'] # Get logger from params
    loop = asyncio.get_running_loop() # Get the loop *before* starting the thread
    task_params['loop'] = loop # Add the loop to the parameters passed to the thread

    # --- Status Update Helper (for local execution) ---
    def update_status_sync(message: str, progress: float):
        async def _update():
             async with queue_lock:
                 if job_id in job_statuses and job_statuses[job_id]["status"] == JobStatus.RUNNING:
                     job_statuses[job_id]["message"] = message
                     job_statuses[job_id]["progress"] = progress
        future = asyncio.run_coroutine_threadsafe(_update(), loop)
        try:
            future.result(timeout=5)
        except Exception as e:
             logger.error(f"Error submitting status update for job {job_id}: {e}")

    try:
        # Add the callback to the task parameters for the pipeline
        task_params['status_update_callback'] = update_status_sync
        
        # Run the synchronous analysis function in a thread pool
        await asyncio.to_thread(run_analysis_pipeline, **task_params)
        
        # Update status upon successful completion
        async with queue_lock:
            if job_id in job_statuses:
                job_statuses[job_id]["status"] = JobStatus.COMPLETED
                job_statuses[job_id]["message"] = "Analysis finished successfully."
                job_statuses[job_id]["progress"] = 1.0
                logger.info(f"Job {job_id} completed successfully.")
                # Persist final status
                status_path = config.JOB_OUTPUT_DIR / job_id / "status.json"
                with open(status_path, "w") as f:
                    json.dump({k: (v.value if isinstance(v, JobStatus) else v) for k, v in job_statuses[job_id].items()}, f, indent=4)

    except Exception as e:
        async with queue_lock:
             if job_id in job_statuses:
                job_statuses[job_id]["status"] = JobStatus.FAILED
                error_message = f"Analysis failed: {str(e)}"
                job_statuses[job_id]["message"] = error_message
                job_statuses[job_id]["error_details"] = traceback.format_exc()
                logger.error(f"Job {job_id} failed: {error_message}", exc_info=True)
                # Persist final status
                status_path = config.JOB_OUTPUT_DIR / job_id / "status.json"
                with open(status_path, "w") as f:
                    json.dump({k: (v.value if isinstance(v, JobStatus) else v) for k, v in job_statuses[job_id].items()}, f, indent=4)
    finally:
        async with queue_lock:
            active_job_count -= 1
            logger.info(f"Job {job_id} finished (Success/Fail). Active jobs now: {active_job_count}")


# --- Analysis Pipeline Function (Synchronous Logic) ---
def run_analysis_pipeline(
    job_id: str,
    job_dir: str,
    main_dir: str,
    intrim_dir: str,
    fasta_path: str,
    cat_path: Optional[str],
    gene_bed_path: Optional[str],
    reference_id: Optional[str],
    perf_params: PerfParams,
    flanks: bool,
    logger: logging.Logger,
    dynamic_column: str,
    status_update_callback: Optional[Callable] = None,
    **kwargs # To absorb unused params like 'loop' or 'input_dir'
):
    """
    The actual analysis pipeline running synchronously.
    This function is now independent of the execution context (local vs. slurm).
    """
    logger.info(f"Core analysis pipeline started for job {job_id}")

    def update_status(message: str, progress: float):
        """Wrapper to safely call the provided status update callback."""
        if status_update_callback:
            try:
                status_update_callback(message=message, progress=progress)
            except Exception as e:
                logger.warning(f"Status update callback failed for job {job_id}: {e}")

    try:
        update_status("Running M2 pipeline...", 0.1)
        # --- Module 1: M2 pipeline ---
        m2_args = argparse.Namespace(
            fasta=fasta_path, cat=cat_path, out=main_dir, tmp=intrim_dir, flanks=flanks, logger=logger,
            mono=perf_params.mono, di=perf_params.di, tri=perf_params.tri, tetra=perf_params.tetra,
            penta=perf_params.penta, hexa=perf_params.hexa, minLen=perf_params.minLen, maxLen=perf_params.maxLen,
            unfair=perf_params.unfair, thread=perf_params.thread
        )
        m2_result = m2.main(m2_args)
        if isinstance(m2_result, tuple) and len(m2_result) == 3:
            merged_out, locicons_file, pattern_summary = m2_result
        else:
            raise RuntimeError("m2.main did not return expected tuple.")

        logger.info(f"M2 pipeline completed for job {job_id}. Merged output: {merged_out}")
        update_status("M2 complete. Checking for GC2...", 0.4)

        # pattern_summary is now generated directly in output/main/flanks by m2.py
        # No need to copy it anymore.
        # if pattern_summary and os.path.exists(pattern_summary):
        #     try:
        #         # Define the target directory
        #         flanks_dir = os.path.join(main_dir, "flanks")
        #         os.makedirs(flanks_dir, exist_ok=True)
        #         target_path = os.path.join(flanks_dir, "pattern_summary.csv")
        #         shutil.copy2(pattern_summary, target_path)
        #         logger.info(f"Pattern summary generated and copied to {target_path}")
        #     except Exception as copy_err:
        #          logger.warning(f"Failed to copy pattern summary: {copy_err}")

        # --- Module 2: GC2 pipeline ---
        ssr_combo = None
        if gene_bed_path and os.path.exists(gene_bed_path):
            update_status("Running GC2 pipeline...", 0.5)
            gc2_args = argparse.Namespace(merged=merged_out, gene=gene_bed_path, jobOut=main_dir, tmp=intrim_dir, logger=logger)
            ssr_combo = gc2.main(gc2_args)
            logger.info(f"GC2 pipeline completed for job {job_id}. SSR Combo: {ssr_combo}")

            # --- Module 3: Process SSR Results ---
            if ssr_combo and os.path.exists(ssr_combo):
                update_status("GC2 complete. Processing SSR results...", 0.7)
                ssr_args = argparse.Namespace(
                    ssrcombo=ssr_combo, jobOut=main_dir, tmp=intrim_dir, logger=logger, reference=reference_id,
                    min_repeat_count=perf_params.min_repeat_count, min_genome_count=perf_params.min_genome_count,
                    dynamic_column=dynamic_column
                )
                process_ssr_results.main(ssr_args)
                logger.info(f"SSR result processing completed for job {job_id}")
                update_status("SSR processing complete.", 0.9)
            else:
                logger.warning(f"SSR combo file {ssr_combo} not found after GC2 run for job {job_id}. Skipping SSR processing.")
                update_status("GC2 complete. SSR combo file missing.", 0.9)
        else:
             logger.info(f"Skipping GC2 and SSR Processing for job {job_id} as no valid gene BED path provided.")
             update_status("Skipped GC2/SSR Processing.", 0.9)

        update_status("Finalizing results...", 1.0)
        logger.info(f"Core analysis logic finished for job {job_id}.")

    except Exception as pipeline_error:
        logger.error(f"Pipeline execution failed for job {job_id}: {pipeline_error}", exc_info=True)
        # Update status to FAILED - the wrapper will catch this exception
        raise pipeline_error # Re-raise to be caught by the wrapper


# --- Helper to convert DataFrame to Arrow Bytes ---
def dataframe_to_arrow_bytes(df: pd.DataFrame) -> bytes:
    """Converts a Pandas DataFrame to Arrow IPC Stream format bytes."""
    try:
        table = pa.Table.from_pandas(df, preserve_index=False)
        sink = pa.BufferOutputStream()
        with ipc.new_stream(sink, table.schema) as writer:
            writer.write_table(table)
        buf = sink.getvalue()
        return buf.to_pybytes()
    except Exception as e:
        logger = logging.getLogger()
        logger.error(f"Error converting DataFrame to Arrow: {e}", exc_info=True)
        raise


@app.get("/health", tags=["Health Check"])
async def health_check():
    """Simple health check endpoint to verify the API is running."""
    return JSONResponse(content={"status": "ok", "message": "CrossRoad API is operational."})
# --- API Endpoints ---

@app.post("/analyze_ssr/", status_code=202)
async def analyze_ssr_endpoint(
    request: Request,
    fasta_file: UploadFile = File(...),
    categories_file: Optional[UploadFile] = File(None),
    gene_bed: Optional[UploadFile] = File(None),
    reference_id: Optional[str] = Form(None),
    perf_params: Optional[str] = Form(None),
    flanks: Optional[bool] = Form(False)
):
    """
    Accepts analysis parameters and files, queues the job locally or submits to Slurm,
    and returns the job ID and status URLs.
    """
    job_id = f"job_{int(time.time() * 1000)}_{os.urandom(4).hex()}"
    job_dir = config.JOB_OUTPUT_DIR / job_id

    # Create directories
    input_dir = job_dir / "input"
    output_dir = job_dir / "output"
    main_dir = output_dir / "main"
    intrim_dir = output_dir / "intrim"
    try:
        input_dir.mkdir(parents=True, exist_ok=True)
        main_dir.mkdir(parents=True, exist_ok=True)
        intrim_dir.mkdir(parents=True, exist_ok=True)
    except OSError as e:
        logging.getLogger().error(f"Failed to create directories for job {job_id}: {e}")
        raise HTTPException(status_code=500, detail="Failed to create job directories.")

    logger = setup_logging(job_id, str(job_dir))
    logger.info(f"Received analysis request for job {job_id} from {request.client.host}")

    # Save input files
    fasta_path = input_dir / "all_genome.fa"
    cat_path, gene_bed_path = None, None
    try:
        with open(fasta_path, "wb") as f:
            shutil.copyfileobj(fasta_file.file, f)
        if categories_file and categories_file.filename:
            cat_path = input_dir / "genome_categories.tsv"
            with open(cat_path, "wb") as f:
                shutil.copyfileobj(categories_file.file, f)
        if gene_bed and gene_bed.filename:
            gene_bed_path = input_dir / "gene.bed"
            with open(gene_bed_path, "wb") as f:
                shutil.copyfileobj(gene_bed.file, f)
    except Exception as e:
         logger.error(f"Error saving input files for job {job_id}: {e}", exc_info=True)
         raise HTTPException(status_code=500, detail="Error saving uploaded files.")
    finally:
        await fasta_file.close()
        if categories_file: await categories_file.close()
        if gene_bed: await gene_bed.close()

    # Parse PERF parameters
    try:
        perf_params_obj = PerfParams(**json.loads(perf_params)) if perf_params else PerfParams()
    except Exception as e:
         raise HTTPException(status_code=400, detail=f"Invalid performance parameters: {e}")

    # --- Determine Dynamic Column Name from Uploaded File ---
    dynamic_column_name = 'optional_category' # Default
    if cat_path:
        try:
            with open(cat_path, 'r') as f:
                header = f.readline().strip().split('\t')
                if len(header) >= 3:
                    dynamic_column_name = header[2]
                    logger.info(f"Detected dynamic column from API upload: '{dynamic_column_name}'")
        except Exception as e:
            logger.warning(f"Could not read header from uploaded categories file. Defaulting to '{dynamic_column_name}'. Error: {e}")

    # Prepare task parameters dictionary
    task_params = {
        "job_id": job_id, "job_dir": str(job_dir), "main_dir": str(main_dir),
        "intrim_dir": str(intrim_dir), "fasta_path": str(fasta_path),
        "cat_path": str(cat_path) if cat_path else None,
        "gene_bed_path": str(gene_bed_path) if gene_bed_path else None,
        "reference_id": reference_id, "perf_params": perf_params_obj,
        "flanks": flanks, "logger": logger,
        "dynamic_column": dynamic_column_name
    }

    # --- Execute based on mode ---
    if config.EXECUTION_MODE == "slurm":
        try:
            logger.info(f"Handing off job {job_id} to SlurmManager.")
            slurm_manager = SlurmManager(job_id, task_params)
            slurm_job_id = slurm_manager.submit()
            
            # Set initial status for Slurm job
            async with queue_lock:
                job_statuses[job_id] = {
                    "status": JobStatus.QUEUED,
                    "message": f"Job submitted to Slurm with ID: {slurm_job_id}",
                    "progress": 0.0, "error_details": None,
                    "reference_id": reference_id, "slurm_job_id": slurm_job_id
                }
            # Persist initial status so it can be tracked immediately
            status_file = job_dir / "status.json"
            with open(status_file, 'w') as sf:
                json.dump({k: (v.value if isinstance(v, JobStatus) else v) for k, v in job_statuses[job_id].items()}, sf, indent=4)

        except Exception as e:
            logger.error(f"Slurm submission failed for job {job_id}: {e}", exc_info=True)
            raise HTTPException(status_code=500, detail=f"Failed to submit job to Slurm: {e}")
    else: # 'local' mode
        logger.info(f"Adding job {job_id} to local queue.")
        async with queue_lock:
            job_statuses[job_id] = {
                "status": JobStatus.QUEUED,
                "message": "Job received and queued for local execution.",
                "progress": 0.0, "error_details": None,
                "reference_id": reference_id, "slurm_job_id": None
            }
            await job_queue.put((job_id, task_params))
            logger.info(f"Job {job_id} added to queue. Queue size: {job_queue.qsize()}")

    # --- Return response to client ---
    status_url = f"/api/job/{job_id}/status"
    results_base_url = f"/api/job/{job_id}/plot_data/"
    download_all_url = f"/api/job/{job_id}/download_zip"

    # Get the just-created status to return in the initial response
    current_status = job_statuses[job_id]["status"]

    return JSONResponse(
        status_code=202,
        content={
            "job_id": job_id,
            "status": current_status.value,
            "status_url": status_url,
            "results_base_url": results_base_url,
            "download_all_url": download_all_url
        }
    )


@app.get("/api/job/{job_id}/status")
async def get_job_status(job_id: str):
    """
    Endpoint to get the current status of a job.
    It checks in-memory cache first, then falls back to reading the status.json file.
    """
    # Check in-memory cache first
    async with queue_lock:
        status_info = job_statuses.get(job_id)

    if status_info:
        return JSONResponse(content={
            "job_id": job_id,
            "status": status_info["status"].value,
            "message": status_info["message"],
            "progress": status_info.get("progress", 0.0),
            "error_details": status_info.get("error_details"),
            "reference_id": status_info.get("reference_id"),
            "slurm_job_id": status_info.get("slurm_job_id")
        })

    # If not in memory, try reading from disk (useful for Slurm jobs or after API restart)
    status_file = config.JOB_OUTPUT_DIR / job_id / "status.json"
    if not status_file.exists():
        raise HTTPException(status_code=404, detail=f"Job ID {job_id} not found.")

    try:
        with open(status_file, 'r') as f:
            data = json.load(f)
        # Cache it for subsequent requests
        async with queue_lock:
            job_statuses[job_id] = {
                "status": JobStatus(data["status"]),
                "message": data.get("message", ""),
                "progress": data.get("progress", 0.0),
                "error_details": data.get("error_details"),
                "reference_id": data.get("reference_id"),
                "slurm_job_id": data.get("slurm_job_id")
            }
        return JSONResponse(content=data)
    except (json.JSONDecodeError, KeyError, ValueError) as e:
        raise HTTPException(status_code=500, detail=f"Could not parse status file for job {job_id}: {e}")


@app.get("/api/job/{job_id}/plot_data/{plot_key}")
async def get_plot_data(job_id: str, plot_key: str):
    """Endpoint to get specific plot data in Apache Arrow format."""
    logger = logging.getLogger()
    logger.info(f"Request for plot data: job={job_id}, plot_key={plot_key}")

    async with queue_lock:
        status_info = job_statuses.get(job_id)

    if not status_info:
        # If not in memory, check the disk as a fallback
        status_file = config.JOB_OUTPUT_DIR / job_id / "status.json"
        if status_file.exists():
            with open(status_file, 'r') as f:
                status_info = json.load(f)
        else:
            raise HTTPException(status_code=404, detail=f"Job ID {job_id} not found.")

    # Use .get() to avoid KeyErrors if status is a dict from json
    current_status = status_info.get("status")
    if current_status != JobStatus.COMPLETED.value:
        logger.warning(f"Attempted to get plot data for job {job_id} but status is {current_status}")
        raise HTTPException(status_code=409, detail=f"Job {job_id} is not complete. Current status: {current_status}")

    # --- Determine file path based on plot_key ---
    job_dir = config.JOB_OUTPUT_DIR / job_id
    main_dir = job_dir / "output" / "main"
    intrim_dir = job_dir / "output" / "intrim"
    flanks_dir = main_dir / "flanks" # Define flanks directory path
    file_path = None
    read_func = pd.read_csv
    read_kwargs = {'low_memory': False}

    plot_file_map: Dict[str, List[str]] = {
        "plot_source": [os.path.join(main_dir, 'mergedOut.tsv'), os.path.join(intrim_dir, 'reformatted.tsv')],
        "hssr_data": [os.path.join(main_dir, 'hssr_data.csv')],
        "hotspot": [os.path.join(main_dir, 'mutational_hotspot.csv')],
        "ssr_gene_intersect": [os.path.join(main_dir, 'ssr_genecombo.tsv')],
        "category_sankey": [os.path.join(main_dir, 'mergedOut.tsv')],
        "ssr_conservation": [os.path.join(main_dir, 'mergedOut.tsv')],
        "motif_conservation": [os.path.join(main_dir, 'mergedOut.tsv')],
        "relative_abundance": [os.path.join(main_dir, 'mergedOut.tsv')],
        "repeat_distribution": [os.path.join(main_dir, 'mergedOut.tsv')],
        "ssr_gc": [os.path.join(main_dir, 'mergedOut.tsv')],
        "upset": [os.path.join(main_dir, 'mergedOut.tsv')],
        "motif_distribution": [os.path.join(main_dir, 'mergedOut.tsv')],
        "gene_country_sankey": [os.path.join(main_dir, 'hssr_data.csv')],
        "temporal_faceted_scatter": [os.path.join(main_dir, 'hssr_data.csv')],
        "gene_motif_dot_plot": [os.path.join(main_dir, 'hssr_data.csv')],
        "reference_ssr_distribution": [os.path.join(main_dir, 'ssr_genecombo.tsv')],
        # Add new keys for flanking data
        "flanked_data": [os.path.join(flanks_dir, 'flanked.tsv')],
        "pattern_summary": [os.path.join(flanks_dir, 'pattern_summary.csv')],
    }
    # Add flank files to the list requiring tab separation
    tsv_files = ['mergedOut.tsv', 'reformatted.tsv', 'ssr_genecombo.tsv', 'flanked.tsv']
    # CSV files (default separator)
    csv_files = ['pattern_summary.csv'] # Add pattern summary here

    possible_paths = plot_file_map.get(plot_key)
    if not possible_paths:
         raise HTTPException(status_code=404, detail=f"Unknown plot key: '{plot_key}'")

    for p_str in possible_paths: # Iterate over string paths
        p = Path(p_str) # Convert to Path object for easier handling
        if p.exists():
            file_path = p
            if p.name in tsv_files:
                read_kwargs['sep'] = '\t'
            # No need for specific check for csv_files, as default is comma
            break

    if not file_path:
        logger.error(f"Required data file(s) not found for plot_key '{plot_key}' in job {job_id}. Checked: {possible_paths}")
        raise HTTPException(status_code=404, detail=f"Data for plot '{plot_key}' not found.")

    # --- Read file and convert to Arrow ---
    try:
        df = await asyncio.to_thread(read_func, file_path, **read_kwargs)
        if df.empty:
             logger.warning(f"Data file {file_path} for plot '{plot_key}' is empty.")
             return Response(status_code=204)

        arrow_bytes = await asyncio.to_thread(dataframe_to_arrow_bytes, df)
        logger.info(f"Successfully converted data for plot '{plot_key}' to Arrow format ({len(arrow_bytes)} bytes).")

        return Response(
            content=arrow_bytes,
            media_type="application/vnd.apache.arrow.stream"
        )
    except FileNotFoundError:
         logger.error(f"File disappeared before read for plot '{plot_key}': {file_path}")
         raise HTTPException(status_code=404, detail=f"Data file for plot '{plot_key}' missing.")
    except Exception as e:
        logger.error(f"Error reading or converting data for plot '{plot_key}' (job {job_id}): {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Error processing data for plot '{plot_key}'.")


@app.get("/api/job/{job_id}/download_zip")
async def download_results_zip(job_id: str):
    """Endpoint to download the full results zip for a given job ID."""
    logger = logging.getLogger()
    logger.info(f"Request received to download full zip for job {job_id}")

    async with queue_lock:
        status_info = job_statuses.get(job_id)

    if not status_info:
        status_file = config.JOB_OUTPUT_DIR / job_id / "status.json"
        if status_file.exists():
            with open(status_file, 'r') as f:
                status_info = json.load(f)
        else:
            raise HTTPException(status_code=404, detail=f"Job ID {job_id} not found.")

    current_status = status_info.get("status")
    if current_status != JobStatus.COMPLETED.value:
         logger.warning(f"Attempted to download zip for job {job_id} but status is {current_status}")
         raise HTTPException(status_code=409, detail=f"Job {job_id} is not complete. Current status: {current_status}")

    job_dir = config.JOB_OUTPUT_DIR / job_id
    output_dir = job_dir / "output"
    # Create zip in the job_dir, not the output dir, to avoid zipping the zip itself
    output_zip = job_dir / f"ssr_analysis_{job_id}_full.zip"

    if not output_dir.is_dir():
        logger.error(f"Output directory not found for completed job {job_id}: {output_dir}")
        raise HTTPException(status_code=404, detail=f"Results directory not found for job ID {job_id}")

    # Use output_zip.with_suffix('') to pass the base name to make_archive
    if not output_zip.exists():
        try:
            logger.info(f"Creating full results zip for job {job_id} at {output_zip}")
            await asyncio.to_thread(shutil.make_archive, str(output_zip.with_suffix('')), 'zip', str(output_dir))
            logger.info(f"Full results zip created for job {job_id}")
        except Exception as e:
            logger.error(f"Error creating zip file for job {job_id}: {e}", exc_info=True)
            if output_zip.exists(): output_zip.unlink() # Cleanup partial zip
            raise HTTPException(status_code=500, detail="Error creating results zip file.")
    else:
         logger.info(f"Using existing full results zip for job {job_id}: {output_zip}")

    return FileResponse(
        path=output_zip,
        media_type="application/zip",
        filename=output_zip.name
    )


@app.get("/api/job/{job_id}/logs")
async def get_job_logs(job_id: str):
    """Serve the per-job log file as plain text."""
    logger = logging.getLogger()
    log_path = config.JOB_OUTPUT_DIR / job_id / f"{job_id}.log"
    logger.info(f"Attempting to serve log file for job {job_id} from path: {log_path}")

    if not log_path.exists():
        # Also check for the slurm log as a fallback
        slurm_log_path = config.JOB_OUTPUT_DIR / job_id / f"slurm_{job_id}.log"
        if slurm_log_path.exists():
            logger.info(f"Main log not found, serving Slurm log instead: {slurm_log_path}")
            return FileResponse(path=slurm_log_path, media_type="text/plain", filename=f"slurm_{job_id}.log")
        
        logger.error(f"Log file for job {job_id} not found at {log_path} or as a Slurm log.")
        raise HTTPException(status_code=404, detail=f"Log file for job {job_id} not found.")
        
    return FileResponse(path=log_path, media_type="text/plain", filename=f"{job_id}.log")


# --- Main execution block ---
# This block is useful for direct execution, but the primary entry point will be the CLI.
if __name__ == "__main__":
    print("Starting Crossroad API directly...")
    # The lifespan manager will handle the initial logging setup.
    uvicorn.run(
        "main:app", 
        host=os.getenv("CROSSROAD_HOST", "0.0.0.0"), 
        port=int(os.getenv("CROSSROAD_PORT", "8000")),
        reload=False # Reload is not recommended for production
    )
