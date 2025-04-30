# API Plan: Dedicated Endpoints, Apache Arrow, Queuing & Status

This document outlines a refined API structure for the CrossRoad analysis pipeline, prioritizing performance for large tabular data using dedicated endpoints and Apache Arrow, while incorporating job queuing and status reporting.

## Goals

*   Improve data transfer speed for plotting tables compared to zipping text files.
*   Provide specific endpoints for frontend components to fetch only necessary plot data.
*   Use an efficient binary format (Apache Arrow) for tabular data transfer.
*   Implement job queuing to limit concurrent analyses.
*   Allow clients to query job status (queued, running, completed, failed).
*   Retain an option to download the complete analysis results as a zip file.

## Core Technologies

*   **Backend Framework:** FastAPI
*   **Data Serialization (Tabular):** Apache Arrow (IPC Stream Format) via `pyarrow`
*   **Data Serialization (Control/Status):** JSON
*   **Asynchronous Tasks/Queuing:** FastAPI `BackgroundTasks` (simple) or a dedicated library like Celery/RQ (more robust).
*   **Concurrency/Queue Management:** Requires a mechanism (e.g., global counter/lock, Redis, task queue features) to track active jobs and manage the queue.

## HTTP Endpoints

### 1. Initiate Analysis

*   **Endpoint:** `POST /analyze_ssr/`
*   **Request:** `multipart/form-data` containing input files (FASTA, optional Category TSV, optional Gene BED) and parameters (reference_id, perf_params, flanks).
*   **Processing:**
    1.  Generate unique `job_id`.
    2.  Save input files to `jobOut/{job_id}/input/`.
    3.  Check current number of active jobs against `MAX_CONCURRENT_JOBS` limit (e.g., 2).
    4.  **If below limit:**
        *   Mark job status as `running`.
        *   Start the analysis pipeline (M2, GC2, etc.) asynchronously.
        *   Return `202 Accepted` response with status `running`.
    5.  **If at limit:**
        *   Add job details (input paths, params, `job_id`) to a waiting queue.
        *   Mark job status as `queued`.
        *   Return `202 Accepted` response with status `queued`.
*   **Response (`202 Accepted`):** `application/json`
    ```json
    {
      "job_id": "job_abc789",
      "status": "queued", // or "running"
      "status_url": "/api/job/job_abc789/status",
      "results_base_url": "/api/job/job_abc789/plot_data/", // Base URL for plot data endpoints
      "download_all_url": "/api/job/job_abc789/download_zip" // URL for full download
    }
    ```
*   **Response (Error):** `400 Bad Request` (invalid params), `422 Unprocessable Entity` (validation error), `500 Internal Server Error`.

### 2. Get Job Status

*   **Endpoint:** `GET /api/job/{job_id}/status`
*   **Path Parameter:** `job_id` (string).
*   **Processing:** Retrieve the current status and any associated messages/progress for the specified `job_id` from the job tracking mechanism.
*   **Response (`200 OK`):** `application/json`
    ```json
    {
      "job_id": "job_abc789",
      "status": "running", // "queued", "completed", "failed"
      "message": "Processing GC2 module...", // Optional: Current step/message
      "progress": 0.65, // Optional: Overall progress (0.0-1.0)
      "error_details": null // Populated if status is "failed"
    }
    ```
*   **Response (Error):** `404 Not Found` (invalid `job_id`).

### 3. Get Plot Data

*   **Endpoint:** `GET /api/job/{job_id}/plot_data/{plot_key}`
*   **Path Parameters:**
    *   `job_id` (string).
    *   `plot_key` (string): Identifier for the specific plot dataset needed (e.g., `hotspot`, `ssr_gc`, `category_sankey`, `hssr_data`, `ssr_gene_intersect`, `plot_source`). The `plot_source` key should handle checking for `mergedOut.tsv` first, then `reformatted.tsv`.
*   **Processing:**
    1.  Verify `job_id` exists.
    2.  Check job status. If not `completed`, return `409 Conflict` (or `404` if preferred).
    3.  Determine the required source file path(s) based on `plot_key` (e.g., `plot_key='hotspot'` -> `jobOut/{job_id}/output/main/mutational_hotspot.csv`).
    4.  If file(s) exist, read into a Pandas DataFrame. Handle potential errors (file not found, read errors).
    5.  Convert the DataFrame to Apache Arrow IPC Stream format using `pyarrow`.
*   **Response (Success - `200 OK`):** Raw binary Arrow data.
    *   `Content-Type: application/vnd.apache.arrow.stream` (Preferred) or `application/octet-stream`.
*   **Response (Error):** `404 Not Found` (job or file missing), `409 Conflict` (job not completed), `500 Internal Server Error` (processing error).

### 4. Download Full Results Zip

*   **Endpoint:** `GET /api/job/{job_id}/download_zip`
*   **Path Parameter:** `job_id` (string).
*   **Processing:**
    1.  Verify `job_id` exists.
    2.  Check job status. If not `completed`, return `409 Conflict` (or `404`).
    3.  Locate the `jobOut/{job_id}/output/` directory.
    4.  Create a zip archive of this directory (if it doesn't already exist).
*   **Response (Success - `200 OK`):** The zip file.
    *   `Content-Type: application/zip`
    *   `Content-Disposition: attachment; filename="ssr_analysis_{job_id}_full.zip"`
*   **Response (Error):** `404 Not Found`, `409 Conflict`, `500 Internal Server Error`.

## Background Task / Queue Worker Logic

*   Picks up a job (`job_id`, inputs, params) when started or when dequeued.
*   Updates job status (`running`, `failed`, `completed`) in the shared tracking mechanism.
*   Optionally updates progress/messages during execution.
*   Runs the core analysis steps (M2, GC2, SSR processing).
*   Saves output files to the correct `jobOut/{job_id}/output/main/` or `intrim/` directories.
*   Upon completion (success or failure), triggers the queue manager to check if a waiting job can be started.

## Frontend Interaction Flow

1.  User submits analysis via UI -> `POST /analyze_ssr/`.
2.  Frontend receives `job_id`, `status`, and URLs. Stores `job_id`.
3.  If status is `queued` or `running`, frontend periodically polls `GET /api/job/{job_id}/status`.
4.  UI updates based on status (e.g., "Queued", "Running X%", "Failed: Error message").
5.  When status becomes `completed`:
    *   Frontend enables UI elements for viewing plots/results.
    *   When a specific plot is needed, frontend calls `GET /api/job/{job_id}/plot_data/{plot_key}`.
    *   Receives binary Arrow data, parses using Arrow JS, renders plot/table.
    *   Frontend provides a button linked to `GET /api/job/{job_id}/download_zip` for full download.

This plan provides a robust and performant architecture addressing your requirements.