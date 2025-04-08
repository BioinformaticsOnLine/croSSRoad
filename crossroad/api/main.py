from fastapi import FastAPI, UploadFile, File, Form, HTTPException, Request
from fastapi.responses import FileResponse
import shutil
import os
import time
import json
from typing import Optional
import uvicorn
from pydantic import BaseModel
import argparse
import logging
from datetime import datetime
from crossroad.core.logger import setup_logging

# Update imports to use absolute imports from core
from crossroad.core import m2
from crossroad.core import gc2
from crossroad.core import process_ssr_results
from crossroad.core.plotting import generate_all_plots
app = FastAPI(
    title="CrossRoad Analysis Pipeline",
    description="API for analyzing SSRs in genomic data",
    version="1.0.0"
)

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

@app.post("/analyze_ssr/")
async def analyze_ssr(
    request: Request,
    fasta_file: UploadFile = File(...),
    categories_file: UploadFile = File(...),
    gene_bed: Optional[UploadFile] = File(None),
    reference_id: Optional[str] = Form(None),
    perf_params: Optional[str] = Form(None),
    flanks: Optional[bool] = Form(False)
):
    # Create job ID and directories
    job_id = f"job_{int(time.time() * 1000)}"
    job_dir = os.path.abspath(os.path.join("jobOut", job_id))
    
    # Set up logging using the centralized logger
    logger = setup_logging(job_id, job_dir)
    
    # Add source identification
    logger.info("Running in API mode")
    logger.info(f"Client IP: {request.client.host}")
    
    try:
        # Create directory structure
        input_dir = os.path.join(job_dir, "input")
        output_dir = os.path.join(job_dir, "output")
        main_dir = os.path.join(output_dir, "main")
        tmp_dir = os.path.join(output_dir, "tmp")
        
        # Create all necessary directories
        os.makedirs(input_dir, exist_ok=True)
        os.makedirs(main_dir, exist_ok=True)
        os.makedirs(tmp_dir, exist_ok=True)
        
        # Save input files
        fasta_path = os.path.join(input_dir, "all_genome.fa")
        cat_path = os.path.join(input_dir, "genome_categories.tsv")
        
        with open(fasta_path, "wb") as f:
            shutil.copyfileobj(fasta_file.file, f)
        with open(cat_path, "wb") as f:
            shutil.copyfileobj(categories_file.file, f)
        
        logger.info("Input files saved successfully")
        
        # Parse PERF parameters
        if perf_params:
            perf_dict = json.loads(perf_params)
            perf_params = PerfParams(**perf_dict)
        else:
            perf_params = PerfParams()
        
        logger.info(f"PERF parameters: {perf_params}")

        # Module 1: M2 pipeline
        m2_args = argparse.Namespace(
            fasta=fasta_path,
            cat=cat_path,
            out=main_dir,    
            tmp=tmp_dir,
            flanks=flanks,
            logger=logger,  # Pass logger to m2
            # Add PERF parameters
            mono=perf_params.mono,
            di=perf_params.di,
            tri=perf_params.tri,
            tetra=perf_params.tetra,
            penta=perf_params.penta,
            hexa=perf_params.hexa,
            minLen=perf_params.minLen,
            maxLen=perf_params.maxLen,
            unfair=perf_params.unfair,
            thread=perf_params.thread
        )
        merged_out, locicons_file, pattern_summary = m2.main(m2_args)
        
        # If pattern summary was generated, copy it to main directory
        if pattern_summary:
            shutil.copy2(pattern_summary, os.path.join(main_dir, "pattern_summary.csv"))
            logger.info("Pattern summary generated and copied")

        # Module 2: GC2 pipeline (if gene_bed provided)
        if gene_bed:
            gene_bed_path = os.path.join(input_dir, "gene.bed")
            with open(gene_bed_path, "wb") as f:
                shutil.copyfileobj(gene_bed.file, f)
            logger.info("Gene BED file processed")

            gc2_args = argparse.Namespace(
                merged=merged_out,
                gene=gene_bed_path,
                jobOut=main_dir,    
                tmp=tmp_dir,
                logger=logger  # Pass logger to gc2
            )
            ssr_combo = gc2.main(gc2_args)

            # Module 3: Process SSR Results
            ref_id_to_pass = reference_id if reference_id else None

            ssr_args = argparse.Namespace(
                ssrcombo=ssr_combo,
                jobOut=main_dir,    
                tmp=tmp_dir,
                logger=logger,
                reference=ref_id_to_pass,
                min_repeat_count=perf_params.min_repeat_count,
                min_genome_count=perf_params.min_genome_count
            )

            process_ssr_results.main(ssr_args)

        # --- Generate Plots ---
        try:
            logger.info("Starting post-processing: Generating plots...")
            plots_output_dir = os.path.join(output_dir, "plots")
            # main_dir is already defined (line 65)
            generate_all_plots(main_dir, plots_output_dir, reference_id) # Pass reference_id
            logger.info("Finished generating plots.")
        except Exception as plot_err:
            logger.error(f"An error occurred during plot generation: {plot_err}", exc_info=True)
            # Logged the error, but continue to zip and return main results

        # Create zip file of results (including plots if generated)
        output_zip = os.path.join(job_dir, "results.zip")
        shutil.make_archive(output_zip[:-4], 'zip', output_dir)
        logger.info("Results (including plots) archived successfully")

        return FileResponse(
            output_zip,
            media_type="application/zip",
            filename=f"ssr_analysis_{job_id}.zip"
        )

    except Exception as e:
        logger.error(f"Error processing job: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)