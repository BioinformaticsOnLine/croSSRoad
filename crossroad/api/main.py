from fastapi import (
    FastAPI, UploadFile, File, Form, HTTPException, Request, BackgroundTasks,
    Depends # Added for dependency injection if needed later
)
from fastapi.responses import FileResponse, JSONResponse, Response # Added Response
from fastapi.middleware.cors import CORSMiddleware
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

from crossroad.core.logger import setup_logging
# Update imports to use absolute imports from core
from crossroad.core import m2
from crossroad.core import gc2
from crossroad.core import process_ssr_results
# from crossroad.core.plotting import generate_all_plots # Commented out - Plots handled by frontend

# --- Configuration ---
MAX_CONCURRENT_JOBS = 2 # Example limit - Make this configurable later if needed
ROOT_DIR = Path(__file__).resolve().parents[2] # Project root for absolute jobOut paths

# --- Job Status Enum ---
class JobStatus(str, Enum):
    QUEUED = "queued"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"

# --- In-Memory Job Tracking and Queuing (Simple Example) ---
# Warning: This in-memory approach is not suitable for multi-worker deployments.
# Consider Redis/Celery/RQ for production.
job_statuses: Dict[str, Dict[str, Any]] = {} # {job_id: {"status": JobStatus, "message": str, "progress": float, "error_details": str|None}}
job_queue: asyncio.Queue[Tuple[str, Dict[str, Any]]] = asyncio.Queue() # Stores (job_id, task_params)
active_job_count = 0
queue_lock = asyncio.Lock() # To protect access to active_job_count and job_statuses

# --- Lifespan Management (for starting queue consumer) ---
@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup: Start the queue consumer task
    print("Starting queue consumer...")
    # Ensure root logger is configured before starting consumer
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s [%(name)s:%(lineno)d] - %(message)s')
    await load_persistent_statuses()
    consumer_task = asyncio.create_task(queue_consumer())
    yield
    # Shutdown: Cancel the consumer task gracefully
    print("Shutting down queue consumer...")
    consumer_task.cancel()
    try:
        await consumer_task
    except asyncio.CancelledError:
        print("Queue consumer task cancelled.")

# Create app instance with lifespan manager
app = FastAPI(
    title="CrossRoad Analysis Pipeline",
    description="API for analyzing SSRs in genomic data with job queuing",
    version="1.1.0", # Version bump
    lifespan=lifespan # Add lifespan manager
)

# Add CORS middleware
origins = [
    "http://localhost", # Base localhost
    "http://localhost:3000", # Common React dev port
    "http://localhost:5173", # Common Vite dev port
    "*" # Allow all for broad development - REMOVE/RESTRICT FOR PRODUCTION
]
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# On startup, load persisted job statuses from disk
@app.on_event("startup")
async def load_persistent_statuses():
    logger = logging.getLogger("startup")
    job_out_root = ROOT_DIR / "jobOut"
    if not job_out_root.exists():
        logger.info("No jobOut directory found, skipping status load.")
        return
    for job_dir in job_out_root.iterdir():
        if not job_dir.is_dir():
            continue
        status_file = job_dir / "status.json"
        if status_file.exists():
            try:
                with open(status_file) as f:
                    data = json.load(f)
                job_statuses[job_dir.name] = {
                    "status": JobStatus(data["status"]),
                    "message": data.get("message", ""),
                    "progress": data.get("progress", 0.0),
                    "error_details": data.get("error_details", None),
                    "reference_id": data.get("reference_id")
                }
                logger.info(f"Loaded status for job {job_dir.name}: {data['status']}")
            except Exception as e:
                logger.warning(f"Failed to load status.json for {job_dir.name}: {e}")
        else:
            merged_file = job_dir / "output" / "main" / "mergedOut.tsv"
            if merged_file.exists():
                job_statuses[job_dir.name] = {
                    "status": JobStatus.COMPLETED,
                    "message": "Loaded from disk",
                    "progress": 1.0,
                    "error_details": None,
                    "reference_id": None
                }
                logger.info(f"Assumed completed job {job_dir.name} (found mergedOut.tsv)")
                # Persist status.json for future restarts
                try:
                    with open(status_file, "w") as sf:
                        json.dump({
                            "status": job_statuses[job_id]["status"].value,
                            "message": job_statuses[job_id]["message"],
                            "progress": job_statuses[job_id]["progress"],
                            "error_details": None,
                            "reference_id": None
                        }, sf)
                except Exception as persist_err:
                    logger.warning(f"Could not write status.json for {job_dir.name}: {persist_err}")


# --- Performance Parameters Model ---
class PerfParams(BaseModel):
    mono: int = 12
    di: int = 4
    tri: int = 3
    tetra: int = 3
    penta: int = 3
    hexa: int = 2
    minLen: int = 156000
    maxLen: int = 10000000
    unfair: int = 50
    thread: int = 50
    min_repeat_count: int = 1
    min_genome_count: int = 5

# --- Queue Consumer Task ---
async def queue_consumer():
    """Continuously checks the queue and starts jobs if concurrency allows."""
    global active_job_count
    logger = logging.getLogger("QueueConsumer") # Get a logger
    logger.info("Queue consumer started.")
    while True:
        try:
            # Check if we can start a job *before* waiting on the queue
            async with queue_lock:
                can_start_immediately = active_job_count < MAX_CONCURRENT_JOBS

            if can_start_immediately:
                try:
                    # Wait for a short time for an item, non-blocking if queue is empty
                    job_id, task_params = await asyncio.wait_for(job_queue.get(), timeout=0.5)
                except asyncio.TimeoutError:
                    # Queue is empty, wait longer before next check
                    await asyncio.sleep(1)
                    continue # Go back to the start of the loop

                logger.info(f"Dequeued job {job_id}. Attempting to start.")
                async with queue_lock:
                    # Double-check concurrency limit just before starting
                    if active_job_count < MAX_CONCURRENT_JOBS:
                        active_job_count += 1
                        job_statuses[job_id]["status"] = JobStatus.RUNNING
                        job_statuses[job_id]["message"] = "Starting analysis..."
                        logger.info(f"Starting job {job_id}. Active jobs now: {active_job_count}")
                        # Use asyncio.create_task for better control than BackgroundTasks here
                        asyncio.create_task(run_analysis_pipeline_wrapper(job_id, task_params))
                    else:
                        logger.warning(f"Concurrency limit reached before starting {job_id}. Re-queuing.")
                        await job_queue.put((job_id, task_params)) # Put it back at the end
                job_queue.task_done() # Mark task as processed (even if re-queued)
            else:
                # If concurrency limit reached, wait before checking again
                await asyncio.sleep(1) # Check again in 1 second

        except asyncio.CancelledError:
            logger.info("Queue consumer cancelling...")
            break
        except Exception as e:
            logger.error(f"Error in queue consumer: {e}", exc_info=True)
            await asyncio.sleep(5) # Wait longer after an error


# --- Wrapper for Background Task to handle completion/failure ---
async def run_analysis_pipeline_wrapper(job_id: str, task_params: Dict[str, Any]):
    """Wraps the main analysis function to update status and manage concurrency count."""
    global active_job_count
    logger = task_params['logger'] # Get logger from params
    loop = asyncio.get_running_loop() # Get the loop *before* starting the thread
    task_params['loop'] = loop # Add the loop to the parameters passed to the thread

    try:
        # Run the synchronous analysis function in a thread pool
        await asyncio.to_thread(run_analysis_pipeline, **task_params)
        # Update status upon successful completion
        async with queue_lock:
            if job_id in job_statuses:
                job_statuses[job_id]["status"] = JobStatus.COMPLETED
                job_statuses[job_id]["message"] = "Analysis finished successfully."
                job_statuses[job_id]["progress"] = 1.0
                logger.info(f"Job {job_id} completed successfully.")
                # Persist completed status
                status_path = os.path.join(ROOT_DIR, "jobOut", job_id, "status.json")
                with open(status_path, "w") as f:
                    json.dump({
                        "status": job_statuses[job_id]["status"].value,
                        "message": job_statuses[job_id]["message"],
                        "progress": job_statuses[job_id]["progress"],
                        "error_details": job_statuses[job_id].get("error_details"),
                        "reference_id": job_statuses[job_id].get("reference_id")
                    }, f)
    except Exception as e:
        async with queue_lock:
             if job_id in job_statuses:
                job_statuses[job_id]["status"] = JobStatus.FAILED
                error_message = f"Analysis failed: {str(e)}"
                job_statuses[job_id]["message"] = error_message
                job_statuses[job_id]["error_details"] = traceback.format_exc()
                logger.error(f"Job {job_id} failed: {error_message}", exc_info=True)
                # Persist failed status
                status_path = os.path.join(ROOT_DIR, "jobOut", job_id, "status.json")
                with open(status_path, "w") as f:
                    json.dump({
                        "status": job_statuses[job_id]["status"].value,
                        "message": job_statuses[job_id]["message"],
                        "progress": job_statuses[job_id].get("progress", 0.0),
                        "error_details": job_statuses[job_id].get("error_details"),
                        "reference_id": job_statuses[job_id].get("reference_id")
                    }, f)
    finally:
        async with queue_lock:
            active_job_count -= 1
            logger.info(f"Job {job_id} finished (Success/Fail). Active jobs now: {active_job_count}")


# --- Analysis Pipeline Function (Synchronous Logic) ---
def run_analysis_pipeline(
    job_id: str,
    job_dir: str,
    input_dir: str,
    output_dir: str,
    main_dir: str,
    intrim_dir: str,
    fasta_path: str,
    cat_path: Optional[str],
    gene_bed_path: Optional[str],
    reference_id: Optional[str],
    perf_params: PerfParams,
    flanks: bool,
    logger: logging.Logger,
    loop: asyncio.AbstractEventLoop
):
    """The actual analysis pipeline running synchronously."""
    logger.info(f"Background task started execution for job {job_id}")

    # --- Update Status Helper ---
    def update_status_sync(message: str, progress: float):
        async def _update():
             async with queue_lock:
                 if job_id in job_statuses and job_statuses[job_id]["status"] == JobStatus.RUNNING:
                     job_statuses[job_id]["message"] = message
                     job_statuses[job_id]["progress"] = progress
                 else:
                      logger.warning(f"Skipping status update for job {job_id} as its status is no longer RUNNING.")
        future = asyncio.run_coroutine_threadsafe(_update(), loop)
        try:
            future.result(timeout=5)
        except TimeoutError:
             logger.warning(f"Status update for job {job_id} timed out.")
        except Exception as e:
             logger.error(f"Error submitting status update for job {job_id}: {e}")

    try:
        update_status_sync("Running M2 pipeline...", 0.1)
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
        update_status_sync("M2 complete. Checking for GC2...", 0.4)

        if pattern_summary and os.path.exists(pattern_summary):
            try:
                shutil.copy2(pattern_summary, os.path.join(main_dir, "pattern_summary.csv"))
                logger.info("Pattern summary generated and copied")
            except Exception as copy_err:
                 logger.warning(f"Failed to copy pattern summary: {copy_err}")

        # --- Module 2: GC2 pipeline ---
        ssr_combo = None
        if gene_bed_path and os.path.exists(gene_bed_path):
            update_status_sync("Running GC2 pipeline...", 0.5)
            gc2_args = argparse.Namespace(merged=merged_out, gene=gene_bed_path, jobOut=main_dir, tmp=intrim_dir, logger=logger)
            ssr_combo = gc2.main(gc2_args)
            logger.info(f"GC2 pipeline completed for job {job_id}. SSR Combo: {ssr_combo}")

            # --- Module 3: Process SSR Results ---
            if ssr_combo and os.path.exists(ssr_combo):
                update_status_sync("GC2 complete. Processing SSR results...", 0.7)
                ssr_args = argparse.Namespace(
                    ssrcombo=ssr_combo, jobOut=main_dir, tmp=intrim_dir, logger=logger, reference=reference_id,
                    min_repeat_count=perf_params.min_repeat_count, min_genome_count=perf_params.min_genome_count
                )
                process_ssr_results.main(ssr_args)
                logger.info(f"SSR result processing completed for job {job_id}")
                update_status_sync("SSR processing complete.", 0.9)
            else:
                logger.warning(f"SSR combo file {ssr_combo} not found after GC2 run for job {job_id}. Skipping SSR processing.")
                update_status_sync("GC2 complete. SSR combo file missing.", 0.9)
        else:
             logger.info(f"Skipping GC2 and SSR Processing for job {job_id} as no valid gene BED path provided.")
             update_status_sync("Skipped GC2/SSR Processing.", 0.9)

        update_status_sync("Finalizing results...", 1.0)
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
    Accepts analysis parameters and files, queues the job,
    and returns the job ID and status URLs.
    """
    job_id = f"job_{int(time.time() * 1000)}_{os.urandom(4).hex()}"
    job_dir = os.path.abspath(os.path.join(ROOT_DIR, "jobOut", job_id))

    # Create directories first
    input_dir = os.path.join(job_dir, "input")
    output_dir = os.path.join(job_dir, "output")
    main_dir = os.path.join(output_dir, "main")
    intrim_dir = os.path.join(output_dir, "intrim")
    try:
        os.makedirs(input_dir, exist_ok=True)
        os.makedirs(main_dir, exist_ok=True)
        os.makedirs(intrim_dir, exist_ok=True)
    except OSError as e:
        logging.getLogger().error(f"Failed to create directories for job {job_id}: {e}")
        raise HTTPException(status_code=500, detail="Failed to create job directories.")

    # Setup logger *after* directories exist
    logger = setup_logging(job_id, job_dir)
    logger.info(f"Received analysis request for job {job_id} from {request.client.host}")

    # --- Save input files ---
    fasta_path = os.path.join(input_dir, "all_genome.fa")
    cat_path = None
    gene_bed_path = None
    try:
        with open(fasta_path, "wb") as f_fasta:
            shutil.copyfileobj(fasta_file.file, f_fasta)
        logger.info(f"FASTA file saved to {fasta_path}")

        if categories_file and categories_file.filename:
            cat_path = os.path.join(input_dir, "genome_categories.tsv")
            with open(cat_path, "wb") as f_cat:
                shutil.copyfileobj(categories_file.file, f_cat)
            logger.info(f"Categories file saved to {cat_path}")
        else:
            logger.info("Categories file not provided.")

        if gene_bed and gene_bed.filename:
            gene_bed_path = os.path.join(input_dir, "gene.bed")
            with open(gene_bed_path, "wb") as f_bed:
                shutil.copyfileobj(gene_bed.file, f_bed)
            logger.info(f"Gene BED file saved to {gene_bed_path}")
        else:
            logger.info("Gene BED file not provided.")

    except Exception as e:
         logger.error(f"Error saving input files for job {job_id}: {e}", exc_info=True)
         raise HTTPException(status_code=500, detail="Error saving uploaded files.")
    finally:
        await fasta_file.close()
        if categories_file: await categories_file.close()
        if gene_bed: await gene_bed.close()

    # --- Parse PERF parameters ---
    try:
        perf_params_obj = PerfParams()
        if perf_params:
            perf_dict = json.loads(perf_params)
            perf_params_obj = PerfParams(**perf_dict)
        logger.info(f"PERF parameters: {perf_params_obj}")
    except Exception as e:
         logger.error(f"Error parsing performance parameters: {e}", exc_info=True)
         raise HTTPException(status_code=400, detail=f"Invalid performance parameters: {e}")

    # --- Prepare task parameters ---
    task_params = dict(
        job_id=job_id, job_dir=job_dir, input_dir=input_dir, output_dir=output_dir,
        main_dir=main_dir, intrim_dir=intrim_dir, fasta_path=fasta_path, cat_path=cat_path,
        gene_bed_path=gene_bed_path, reference_id=reference_id, perf_params=perf_params_obj,
        flanks=flanks, logger=logger
        # loop is added by the wrapper
    )

    # --- Add job to queue and set initial status ---
    async with queue_lock:
        job_statuses[job_id] = {
            "status": JobStatus.QUEUED,
            "message": "Job received and queued.",
            "progress": 0.0,
            "error_details": None,
            "reference_id": reference_id
        }
        await job_queue.put((job_id, task_params))
        logger.info(f"Job {job_id} added to queue. Queue size: {job_queue.qsize()}")
        current_status = job_statuses[job_id]["status"]

    # --- Return response to client ---
    status_url = f"/api/job/{job_id}/status"
    results_base_url = f"/api/job/{job_id}/plot_data/"
    download_all_url = f"/api/job/{job_id}/download_zip"

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
    """Endpoint to get the current status of a job."""
    async with queue_lock:
        status_info = job_statuses.get(job_id)

    if not status_info:
        # Fallback: if on-disk output exists, assume completed
        job_dir = ROOT_DIR / "jobOut" / job_id
        merged_file = job_dir / "output" / "main" / "mergedOut.tsv"
        if merged_file.exists():
            return JSONResponse(content={
                "job_id": job_id,
                "status": JobStatus.COMPLETED.value,
                "message": "Loaded from disk",
                "progress": 1.0,
                "error_details": None,
                "reference_id": None
            })
        raise HTTPException(status_code=404, detail=f"Job ID {job_id} not found.")

    return JSONResponse(content={
        "job_id": job_id,
        "status": status_info["status"].value,
        "message": status_info["message"],
        "progress": status_info.get("progress", 0.0),
        "error_details": "An error occurred during processing." if status_info["status"] == JobStatus.FAILED else None,
        "reference_id": status_info.get("reference_id")
    })


@app.get("/api/job/{job_id}/plot_data/{plot_key}")
async def get_plot_data(job_id: str, plot_key: str):
    """Endpoint to get specific plot data in Apache Arrow format."""
    logger = logging.getLogger()
    logger.info(f"Request for plot data: job={job_id}, plot_key={plot_key}")

    async with queue_lock:
        status_info = job_statuses.get(job_id)

    if not status_info:
        raise HTTPException(status_code=404, detail=f"Job ID {job_id} not found.")

    if status_info["status"] != JobStatus.COMPLETED:
        logger.warning(f"Attempted to get plot data for job {job_id} but status is {status_info['status']}")
        raise HTTPException(status_code=409, detail=f"Job {job_id} is not complete. Current status: {status_info['status']}")

    # --- Determine file path based on plot_key ---
    job_dir = ROOT_DIR / "jobOut" / job_id
    main_dir = job_dir / "output" / "main"
    intrim_dir = job_dir / "output" / "intrim"
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
    }
    tsv_files = ['mergedOut.tsv', 'reformatted.tsv', 'ssr_genecombo.tsv']

    possible_paths = plot_file_map.get(plot_key)
    if not possible_paths:
         raise HTTPException(status_code=404, detail=f"Unknown plot key: '{plot_key}'")

    for p in possible_paths:
        if os.path.exists(p):
            file_path = p
            if os.path.basename(file_path) in tsv_files:
                read_kwargs['sep'] = '\t'
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
        raise HTTPException(status_code=404, detail=f"Job ID {job_id} not found.")

    if status_info["status"] != JobStatus.COMPLETED:
         logger.warning(f"Attempted to download zip for job {job_id} but status is {status_info['status']}")
         raise HTTPException(status_code=409, detail=f"Job {job_id} is not complete. Current status: {status_info['status']}")

    job_dir = ROOT_DIR / "jobOut" / job_id
    output_dir = job_dir / "output"
    output_zip = job_dir / f"ssr_analysis_{job_id}_full.zip"

    if not os.path.isdir(output_dir):
        logger.error(f"Output directory not found for completed job {job_id}: {output_dir}")
        raise HTTPException(status_code=404, detail=f"Results directory not found for job ID {job_id}")

    if not os.path.exists(output_zip):
        try:
            logger.info(f"Creating full results zip for job {job_id} at {output_zip}")
            await asyncio.to_thread(shutil.make_archive, output_zip.stem, 'zip', output_dir)
            logger.info(f"Full results zip created for job {job_id}")
        except Exception as e:
            logger.error(f"Error creating zip file for job {job_id}: {e}", exc_info=True)
            if os.path.exists(output_zip): os.remove(output_zip) # Cleanup partial zip
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
    logger = logging.getLogger() # Get root logger to see this message
    log_path = ROOT_DIR / "jobOut" / job_id / f"{job_id}.log"
    logger.info(f"Current working directory: {os.getcwd()}") # Log CWD
    logger.info(f"Attempting to serve log file for job {job_id} from path: {log_path}")

    # Log directory contents
    parent_dir = log_path.parent
    try:
        if os.path.exists(parent_dir):
            logger.info(f"Contents of {parent_dir}: {os.listdir(parent_dir)}")
        else:
            logger.warning(f"Parent directory {parent_dir} does not exist.")
    except Exception as e:
        logger.error(f"Error listing directory {parent_dir}: {e}")

    file_exists = os.path.exists(log_path)
    logger.info(f"os.path.exists check returned: {file_exists}")
    if not file_exists:
        raise HTTPException(status_code=404, detail=f"Log file for job {job_id} not found at {log_path}.")
    return FileResponse(path=log_path, media_type="text/plain", filename=f"{job_id}.log")


# --- Main execution block ---
if __name__ == "__main__":
    # Basic logging setup for running directly (before lifespan manager takes over)
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s [%(name)s:%(lineno)d] - %(message)s')
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=False) # Reference app by string, disable reload for stability