#!/usr/bin/env python3
import argparse
import logging
import os
import time
from datetime import datetime

from crossroad.core import m2, gc2, process_ssr_results
from crossroad.core.logger import setup_logging
from crossroad.core.plotting import generate_all_plots
def main():
    parser = argparse.ArgumentParser(
        description="CrossRoad: A tool for analyzing SSRs in genomic data"
    )
    parser.add_argument("--fasta", required=True,
                        help="Input FASTA file")
    parser.add_argument("--categories", required=True,
                        help="Genome categories TSV file")
    parser.add_argument("--gene-bed", 
                        help="Gene BED file (optional)")
    parser.add_argument("--reference-id",
                        help="Reference genome ID (optional)")
    parser.add_argument("--output-dir", default="output",
                        help="Output directory")
    parser.add_argument("--flanks", action="store_true",
                        help="Process flanking regions")
    
    # PERF parameters
    parser.add_argument("--mono", type=int, default=12)
    parser.add_argument("--di", type=int, default=4)
    parser.add_argument("--tri", type=int, default=3)
    parser.add_argument("--tetra", type=int, default=3)
    parser.add_argument("--penta", type=int, default=3)
    parser.add_argument("--hexa", type=int, default=2)
    parser.add_argument("--min-len", type=int, default=156000)
    parser.add_argument("--max-len", type=int, default=10000000)
    parser.add_argument("--unfair", type=int, default=50)
    parser.add_argument("--threads", type=int, default=50)

    # Add new parameters for filtering
    parser.add_argument("--min-repeat-count", type=int, default=1,
                      help="Minimum repeat count for filtering (default: 1)")
    parser.add_argument("--min-genome-count", type=int, default=4,
                      help="Minimum genome count for filtering (default: 4)")

    args = parser.parse_args()

    # Create job ID and setup directories
    job_id = f"job_{int(time.time() * 1000)}"
    job_dir = os.path.abspath(os.path.join("jobOut", job_id))
    logger = setup_logging(job_id, job_dir)
    
    # Add source identification
    logger.info("Running in CLI mode")

    try:
        # Create directory structure
        output_dir = os.path.join(job_dir, "output")
        main_dir = os.path.join(output_dir, "main")
        tmp_dir = os.path.join(output_dir, "tmp")

        os.makedirs(main_dir, exist_ok=True)
        os.makedirs(tmp_dir, exist_ok=True)

        # Run M2 pipeline
        m2_args = argparse.Namespace(
            fasta=args.fasta,
            cat=args.categories,
            out=main_dir,
            tmp=tmp_dir,
            flanks=args.flanks,
            logger=logger,
            mono=args.mono,
            di=args.di,
            tri=args.tri,
            tetra=args.tetra,
            penta=args.penta,
            hexa=args.hexa,
            minLen=args.min_len,
            maxLen=args.max_len,
            unfair=args.unfair,
            thread=args.threads
        )
        merged_out, locicons_file, pattern_summary = m2.main(m2_args)

        # Run GC2 pipeline if gene bed provided
        if args.gene_bed:
            gc2_args = argparse.Namespace(
                merged=merged_out,
                gene=args.gene_bed,
                jobOut=main_dir,
                tmp=tmp_dir,
                logger=logger
            )
            ssr_combo = gc2.main(gc2_args)

            # Process SSR Results
            ssr_args = argparse.Namespace(
                ssrcombo=ssr_combo,
                jobOut=main_dir,
                tmp=tmp_dir,
                logger=logger,
                reference=args.reference_id,
                min_repeat_count=args.min_repeat_count,
                min_genome_count=args.min_genome_count
            )
            
            process_ssr_results.main(ssr_args)

        # --- Generate Plots ---
        try:
            logger.info("Starting post-processing: Generating plots...")
            plots_output_dir = os.path.join(output_dir, "plots")
            # main_dir is already defined (line 59)
            generate_all_plots(main_dir, plots_output_dir, args.reference_id) # Pass reference_id
            logger.info("Finished generating plots.")
        except Exception as plot_err:
            logger.error(f"An error occurred during plot generation: {plot_err}", exc_info=True)
            # Logged the error, but continue to report main analysis success

        logger.info("Analysis completed successfully")
        print(f"Results available in: {output_dir}")

    except Exception as e:
        logger.error(f"Error: {str(e)}")
        raise

if __name__ == "__main__":
    main()