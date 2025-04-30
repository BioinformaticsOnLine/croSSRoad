# crossroad/core/plotting.py

import pandas as pd
import numpy as np # Keep numpy as it might be used by loaded dataframes or future plots
import os
import logging
import traceback
import plotly.io as pio # Keep for theme setting if desired globally
import sys
import select
import time
import io
# Import the plotting functions from the new modules
from .plots.category_country_sankey import create_category_country_sankey # Removed color_to_plotly_rgba import
from .plots.gene_country_sankey import create_gene_country_sankey
from .plots.hotspot_plot import create_hotspot_plot
from .plots.ssr_conservation_plot import create_ssr_conservation_plot # Renamed file and function
from .plots.motif_conservation_plot import create_motif_conservation_plot
from .plots.relative_abundance_plot import create_relative_abundance_plot
from .plots.relative_density_plot import create_repeat_distribution_plot
from .plots.ssr_gc_plot import create_ssr_gc_plot
from .plots.ssr_gene_intersect_plot import create_ssr_gene_intersect_plot
from .plots.temporal_faceted_scatter import create_temporal_faceted_scatter
from .plots.reference_ssr_distribution import create_scientific_ssr_plot # Added import
from .plots.upset_plot import create_upset_plot # Added import for UpSet plot
from .plots.motif_distribution_heatmap import create_motif_distribution_heatmap # Renamed import
from .plots.gene_motif_count_dot_plot import create_ssr_gene_genome_dot_plot # Use the new function name
# Set default theme (can be set here or managed elsewhere)
# pio.templates.default = "plotly_white" # Keep commented if set elsewhere or per-plot

# Define colors locally for stage markers (as they are not passed from main)
# These should visually match the ones used in main.py
# Removed local ANSI color codes - rely on Rich markup passed to logger
logger = logging.getLogger(__name__) # Get logger instance

# --- Helper function to check for skip key ---
def check_skip_key():
    """
    Checks if 'k' (followed by Enter) has been pressed on stdin without blocking.
    Returns True if 'k' was pressed, False otherwise.
    """
    # Check if stdin is a TTY (interactive terminal)
    is_tty = sys.stdin.isatty()
    logger.debug(f"Checking for skip key. sys.stdin.isatty() = {is_tty}")
    if not is_tty:
        return False

    try:
        # Use select to check for input without blocking (slightly longer timeout)
        # Increase timeout slightly to potentially catch input better
        ready, _, _ = select.select([sys.stdin], [], [], 0.05) # Increased timeout to 0.05s
        if ready:
            # Input is available, read the line non-blockingly if possible
            # Note: readline() itself might block if select reported ready spuriously
            # or if input arrives between select and readline.
            # This is a limitation without more complex terminal handling.
            inp = sys.stdin.readline().strip() # Read line, remove leading/trailing whitespace
            logger.debug(f"Input detected: '{inp}'") # Log detected input
            # Check if the input is empty (i.e., only Enter was pressed)
            if not inp:
                # Provide immediate feedback to the user in the terminal
                # Using logger.info ensures it goes to the log file too
                logger.info("[bold yellow]Skipping plot due to user input (Enter pressed).[/]")
                # Try to clear any remaining buffer for this line - best effort
                while select.select([sys.stdin], [], [], 0.0)[0]:
                    try:
                        sys.stdin.read(1) # Read one char at a time
                    except: # Catch potential errors during buffer clear
                        break
                return True
            # If input was not empty, log it and continue
            elif inp:
                 logger.info(f"Input '{inp}' detected. Continuing.")

    except (OSError, ValueError, BlockingIOError, io.UnsupportedOperation) as e:
        # Handle potential errors if stdin is not as expected or non-blocking read fails
        logger.debug(f"Error checking stdin for skip key: {e}")
        # Assume no skip if there's an issue checking.
        pass
    except Exception as e: # Catch any other unexpected errors
        logger.error(f"Unexpected error checking stdin for skip key: {e}", exc_info=True)
        pass

    return False

# --- Main Orchestration Function ---

def generate_all_plots(job_output_main_dir, job_output_intrim_dir, job_output_plots_dir, reference_id=None):
    """
    Loads data, generates plots, and saves them.
    Prioritizes 'mergedOut.tsv' from job_output_main_dir for full plotting.
    If 'mergedOut.tsv' is missing, falls back to 'reformatted.tsv' from
    job_output_intrim_dir for a limited set of plots (FASTA-only mode).

    Args:
        job_output_main_dir (str): Path to the main output directory (e.g., .../output/main).
        job_output_intrim_dir (str): Path to the intermediate directory (e.g., .../output/intrim).
        job_output_plots_dir (str): Path to the base directory for plot outputs (e.g., .../output/plots).
        reference_id (str, optional): The ID of the reference genome. Defaults to None.
    """
    # Use the new stage marker style
    # Note: Colors defined above are used here directly in the f-string,
    # as the logger itself might not have these specific color codes mapped
    # in the same way as the main script's direct logging.
    # The logger.info() call will still use its configured format (e.g., adding levelname).
    # We add a newline before the start marker for spacing.
    # Use Rich markup for stage start - Note: Rule is not directly loggable, send styled text
    # The main CLI already logs the Rule before calling this function.
    # We can log a confirmation message here instead.
    logger.info("[bold cyan]Starting Stage 4: Multi-Modal Data Visualization[/]")
    logger.info(f"Main Data Source Dir: {job_output_main_dir}")
    logger.info(f"Intermediate Data Source Dir: {job_output_intrim_dir}")
    logger.info(f"Plot Output Target: {job_output_plots_dir}")
    logger.info(f"Reference ID: {reference_id if reference_id else 'Not Provided'}")

    # Ensure the base output directory exists
    try:
        os.makedirs(job_output_plots_dir, exist_ok=True)
        logger.info(f"Ensured base plots output directory exists: {job_output_plots_dir}")
    except OSError as e:
        logger.error(f"Could not create base plots output directory {job_output_plots_dir}: {e}")
        return # Cannot proceed without output directory

    # --- Load Dataframes (Load once if used by multiple plots) ---
    # --- Determine Data Source and Load ---
    df_plot_source = None
    data_source_path = None
    is_fasta_only_mode = False

    merged_out_path = os.path.join(job_output_main_dir, 'mergedOut.tsv')
    reformatted_path = os.path.join(job_output_intrim_dir, 'reformatted.tsv')

    if os.path.exists(merged_out_path):
        data_source_path = merged_out_path
        logger.info(f"Found full data file: {data_source_path}. Proceeding with all plots.")
    elif os.path.exists(reformatted_path):
        data_source_path = reformatted_path
        is_fasta_only_mode = True
        logger.info(f"Found reformatted data file: {data_source_path}. Proceeding in FASTA-only plot mode.")
    else:
        logger.error(f"Could not find required data file ('mergedOut.tsv' in {job_output_main_dir} or 'reformatted.tsv' in {job_output_intrim_dir}). Skipping all plots.")
        return # Cannot proceed without data

    # Load the determined data source
    try:
        logger.info(f"Loading data from {data_source_path}...")
        df_plot_source = pd.read_csv(data_source_path, sep='\t', low_memory=False)
        if df_plot_source.empty:
            logger.warning(f"Loaded dataframe from {data_source_path} is empty. Skipping plots.")
            df_plot_source = None
    except Exception as e:
        logger.error(f"Failed to load data from {data_source_path}: {e}\n{traceback.format_exc()}")
        df_plot_source = None

    # --- Load Other Required Dataframes (if they exist, regardless of mode) ---
    # These are only used if not in FASTA-only mode, but load them if present

    df_hssr = None
    hssr_data_path = os.path.join(job_output_main_dir, 'hssr_data.csv')
    if os.path.exists(hssr_data_path):
        try:
            logger.info(f"Loading data from {hssr_data_path}...")
            df_hssr = pd.read_csv(hssr_data_path)
            if df_hssr.empty:
                logger.warning(f"Loaded dataframe from {hssr_data_path} is empty.")
                df_hssr = None
        except Exception as e:
            logger.error(f"Failed to load data from {hssr_data_path}: {e}\n{traceback.format_exc()}")
            df_hssr = None
    else:
        logger.warning(f"Input file not found: {hssr_data_path}")

    df_hotspot = None
    hotspot_path = os.path.join(job_output_main_dir, 'mutational_hotspot.csv')
    if os.path.exists(hotspot_path):
        try:
            logger.info(f"Loading data from {hotspot_path}...")
            df_hotspot = pd.read_csv(hotspot_path)
            if df_hotspot.empty:
                logger.warning(f"Loaded dataframe from {hotspot_path} is empty.")
                df_hotspot = None
        except Exception as e:
            logger.error(f"Failed to load data from {hotspot_path}: {e}\n{traceback.format_exc()}")
            df_hotspot = None
    else:
        logger.warning(f"Input file not found: {hotspot_path}")

    df_ssr_gene = None
    ssr_gene_path = os.path.join(job_output_main_dir, 'ssr_genecombo.tsv') # Path for the new plot's data
    if os.path.exists(ssr_gene_path):
        try:
            logger.info(f"Loading data from {ssr_gene_path}...")
            df_ssr_gene = pd.read_csv(ssr_gene_path, sep='\t')
            if df_ssr_gene.empty:
                logger.warning(f"Loaded dataframe from {ssr_gene_path} is empty.")
                df_ssr_gene = None
        except Exception as e:
            logger.error(f"Failed to load data from {ssr_gene_path}: {e}\n{traceback.format_exc()}")
            df_ssr_gene = None
    else:
        logger.warning(f"Input file not found: {ssr_gene_path}")


    # --- Generate Plots (Call functions with loaded dataframes) ---
    logger.info("[bold yellow]Plotting started. You will have 3 seconds to press Enter to skip each plot after its prompt.[/]") # Updated prompt
    plot_status = {} # Dictionary to track success/failure/skip status

    if df_plot_source is None:
        logger.warning("Plot source dataframe is None. Skipping all plot generation.")
        return

    # --- Plot Generation Loop ---
    # Define plots to generate along with their requirements
    plot_definitions = [
        # Plot Name, Function, Data Source(s) list, Required Columns list, FASTA-only skip reason (str or None)
        ("Category->Country Sankey", create_category_country_sankey, [df_plot_source], ['category', 'country'], "Requires category/country data"),
        ("Gene->Country Sankey", create_gene_country_sankey, [df_hssr], ['gene', 'country'], "Requires hssr_data.csv"),
        ("Motif Repeat Count (Hotspot)", create_hotspot_plot, [df_hotspot], ['motif', 'repeat_count'], "Requires mutational_hotspot.csv"),
        ("SSR Conservation", create_ssr_conservation_plot, [df_plot_source], ['genomeID', 'loci'], None), # Works in both modes
        ("Motif Conservation", create_motif_conservation_plot, [df_plot_source], ['genomeID', 'motif'], None), # Works in both modes
        ("Relative Abundance", create_relative_abundance_plot, [df_plot_source], ['category', 'genomeID', 'length_of_motif', 'length_genome'], "Requires category/length_genome data"),
        ("Relative Density", create_repeat_distribution_plot, [df_plot_source], ['category', 'genomeID', 'length_of_motif', 'length_genome', 'length_of_ssr'], "Requires category/length_genome data"),
        ("SSR GC Distribution", create_ssr_gc_plot, [df_plot_source], ['genomeID', 'GC_per'], None), # Works in both modes
        ("SSR Gene Intersection", create_ssr_gene_intersect_plot, [df_ssr_gene], ['gene', 'ssr_position'], "Requires ssr_genecombo.tsv"),
        ("Temporal Faceted Scatter", create_temporal_faceted_scatter, [df_hssr], ['motif', 'year', 'length_of_ssr', 'gene', 'genomeID'], "Requires hssr_data.csv"),
        ("UpSet", create_upset_plot, [df_plot_source], ['motif', 'category', 'genomeID', 'GC_per', 'country'], "Requires category/country data"),
        ("Motif Distribution Heatmap", create_motif_distribution_heatmap, [df_plot_source], ['genomeID', 'loci'], None), # Works in both modes
        # Special plots handled separately below: Reference SSR, Gene Motif Dot Plot
    ]

    # Data source paths for better error messages
    data_source_paths = {
        id(df_plot_source): data_source_path,
        id(df_hssr): hssr_data_path,
        id(df_hotspot): hotspot_path,
        id(df_ssr_gene): ssr_gene_path,
        id(None): "N/A" # Handle None case gracefully
    }

    # Iterate through the defined plots
    for plot_name, plot_func, data_sources, required_cols, fasta_skip_reason in plot_definitions:
        logger.info(f"-> Preparing [bold]{plot_name}[/]. Press Enter within 3s to skip...") # Updated prompt

        # Check for skip key BEFORE attempting to generate the plot
        time.sleep(3.0) # Give user 3 seconds to press Enter
        if check_skip_key():
            # Message is logged inside check_skip_key if 'k' is pressed
            plot_status[plot_name] = "Skipped (User)"
            continue # Move to the next plot in the list

        # --- Data and Mode Checks ---
        skip = False
        skip_reason = ""

        # Check FASTA-only mode
        if is_fasta_only_mode and fasta_skip_reason:
            skip = True
            skip_reason = f"Skipped ({fasta_skip_reason} - FASTA-only mode)"

        # Check if data sources are available
        if not skip:
            for source_df in data_sources:
                if source_df is None:
                    source_path = data_source_paths.get(id(source_df), "Unknown Data Source")
                    base_name = os.path.basename(source_path) if source_path and source_path != "N/A" else "required dataframe"
                    skip = True
                    skip_reason = f"Skipped (No Data: {base_name})"
                    break # No need to check other sources for this plot

        # Check required columns in the primary data source (usually the first one)
        if not skip and data_sources and data_sources[0] is not None:
             primary_df = data_sources[0]
             missing_cols = [col for col in required_cols if col not in primary_df.columns]
             if missing_cols:
                 skip = True
                 skip_reason = f"Skipped (Missing Columns: {missing_cols})"

        # --- Generate or Skip ---
        if skip:
            logger.warning(f"   {skip_reason} for {plot_name} plot.")
            plot_status[plot_name] = skip_reason
        else:
            try:
                logger.info(f"   Generating {plot_name} plot...")
                # Prepare arguments for the plot function - pass copies
                plot_args = [df.copy() for df in data_sources if df is not None]
                plot_args.append(job_output_plots_dir) # Add output dir

                # Call the plot function
                plot_func(*plot_args) # Unpack arguments
                logger.info(f"   {plot_name} plot generation complete.")
                plot_status[plot_name] = "Success"
            except Exception as e:
                logger.error(f"   Failed to plot {plot_name}: {e}")
                logger.debug(traceback.format_exc())
                plot_status[plot_name] = "Failed"

    # --- Handle Special Plots (Reference SSR, Gene Motif Dot Plot) ---

    # 11. Reference SSR Distribution (Requires ssr_genecombo.tsv and reference_id)
    plot_name_base = "Reference SSR Distribution"
    plot_name = f"{plot_name_base} ({reference_id})" if reference_id else plot_name_base
    logger.info(f"-> Preparing [bold]{plot_name}[/]. Press Enter within 3s to skip...") # Updated prompt
    time.sleep(3.0) # Give user 3 seconds to press Enter
    if check_skip_key():
        # Message logged inside check_skip_key
        plot_status[plot_name] = "Skipped (User)"
    elif not reference_id:
        logger.info(f"-> Skipping {plot_name_base} plot: No reference_id provided.")
        plot_status[plot_name_base] = "Skipped (No Ref ID)"
    elif is_fasta_only_mode:
        logger.info(f"-> Skipping {plot_name} plot: Requires ssr_genecombo.tsv (FASTA-only mode).")
        plot_status[plot_name] = "Skipped (FASTA-only)"
    elif df_ssr_gene is None:
        logger.warning(f"-> Skipping {plot_name} plot: Data not available from {ssr_gene_path}.")
        plot_status[plot_name] = "Skipped (No Data)"
    else:
        required_ref_cols = ['genomeID', 'ssr_position']
        missing_ref = [col for col in required_ref_cols if col not in df_ssr_gene.columns]
        if missing_ref:
            logger.warning(f"-> Skipping {plot_name} plot: Missing required columns {missing_ref} in {ssr_gene_path}.")
            plot_status[plot_name] = "Skipped (Missing Column)"
        else:
            try:
                logger.info(f"   Generating {plot_name} plot...")
                create_scientific_ssr_plot(df_ssr_gene.copy(), reference_id, job_output_plots_dir)
                logger.info(f"   {plot_name} plot generation complete.")
                plot_status[plot_name] = "Success"
            except Exception as e:
                logger.error(f"   Failed to plot {plot_name}: {e}")
                logger.debug(traceback.format_exc())
                plot_status[plot_name] = "Failed"


    # 14. Gene Motif Count Dot Plot (Requires hssr_data.csv, uses merged for category sorting)
    plot_name = "SSR Gene Genome Dot Plot" # Update plot name for logging/status
    logger.info(f"-> Preparing [bold]{plot_name}[/]. Press Enter within 3s to skip...") # Updated prompt
    time.sleep(3.0) # Give user 3 seconds to press Enter
    if check_skip_key():
        # Message logged inside check_skip_key
        plot_status[plot_name] = "Skipped (User)"
    elif df_hssr is None:
         logger.warning(f"-> Skipping {plot_name} plot: Data not available (hssr_data.csv).")
         plot_status[plot_name] = "Skipped (No Data)"
    else:
        # Basic check for required columns in df_hssr (plot function should validate more deeply)
        required_dot_cols = ['gene', 'motif', 'genomeID'] # Example minimal check
        missing_dot = [col for col in required_dot_cols if col not in df_hssr.columns]
        if missing_dot:
            logger.warning(f"-> Skipping {plot_name} plot: Missing required columns {missing_dot} in {hssr_data_path}.")
            plot_status[plot_name] = "Skipped (Missing Column)"
        else:
            try:
                logger.info(f"   Generating {plot_name} plot...")
                # Pass df_plot_source copy if not in FASTA-only mode, otherwise pass None for metadata
                # Pass reference_id to the plot function (metadata_df removed)
                create_ssr_gene_genome_dot_plot(df_hssr.copy(), job_output_plots_dir, reference_id)
                logger.info(f"   {plot_name} plot generation complete.")
                plot_status[plot_name] = "Success"
            except Exception as e:
                logger.error(f"   Failed to plot {plot_name}: {e}")
                logger.debug(traceback.format_exc())
                plot_status[plot_name] = "Failed"


    # (This entire block from line 158 to 451 is replaced by the loop structure above)

    # --- Add calls for other plots here, following the same pattern ---
    # --- Plotting Summary ---
    # Use box style for summary and end markers
    # Use Rich markup for summary
    logger.info("[bold magenta]--- Plot Summary ---[/]")
    for name, status in plot_status.items():
        if status == "Success":
            status_markup = f"[bold green]{status}[/]"
        elif "Skipped" in status:
            status_markup = f"[yellow]{status}[/]"
        else: # Failed
            status_markup = f"[bold red]{status}[/]"
        logger.info(f"  - {name}: {status_markup}")
    logger.info("[bold magenta]--------------------[/]")
    # Stage End marker is now handled by main.py after this function returns


# --- Example Usage (for testing purposes) ---
# This section can remain for standalone testing of this orchestration script,
# but it won't directly test the individual plot scripts unless they are run independently.
if __name__ == '__main__':
    # Configure basic logging for testing
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - [%(name)s] - %(message)s')

    # Create dummy data and directories for testing
    test_job_id = "job_test_refactor_123"
    base_dir = "." # Or specify a test directory
    test_main_dir = os.path.join(base_dir, "jobOut", test_job_id, "output", "main")
    test_plots_dir = os.path.join(base_dir, "jobOut", test_job_id, "output", "plots")
    os.makedirs(test_main_dir, exist_ok=True)
    # generate_all_plots will create the plots dir and subdirs

    # [ Removed misplaced heatmap code block that was here ]

    # Create dummy input files
    # mergedOut.tsv (needs columns for all plots using it)
    dummy_merged_data = {
        'category': ['A', 'A', 'B', 'B', 'A', 'C', 'A', 'B'],
        'country': ['USA', 'CAN', 'USA', 'MEX', 'CAN', 'USA', 'USA', 'USA'],
        'genomeID': [f'g{i}' for i in range(8)],
        'loci': [f'L{i//2}' for i in range(8)], # Example loci
        'motif': ['A', 'T', 'AG', 'TC', 'A', 'G', 'A', 'AG'], # Example motifs
        'repeat': [10, 12, 5, 6, 11, 8, 10, 5], # Example repeats
        'length_of_motif': [1, 1, 2, 2, 1, 1, 1, 2], # Corresponds to motif
        'length_genome': [5000000, 5100000, 4900000, 5050000, 5150000, 4950000, 5000000, 4900000],
        'length_of_ssr': [10, 12, 10, 12, 11, 8, 10, 10], # Example SSR lengths
        'GC_per': [45.5, 50.1, 60.2, 55.0, 48.0, 52.5, 46.0, 61.0] # Example GC percentages
    }
    pd.DataFrame(dummy_merged_data).to_csv(os.path.join(test_main_dir, 'mergedOut.tsv'), sep='\t', index=False)

    # hssr_data.csv
    dummy_hssr_data = {
        'gene': ['Gene1', 'Gene1', 'Gene2', 'Gene3', 'Gene2', 'Gene1'],
        'country': ['UK', 'DE', 'UK', 'FR', 'DE', 'DE'],
        'genomeID': [f'h{i}' for i in range(6)],
        'motif': ['T', 'A', 'G', 'C', 'T', 'A'], # Added dummy motif
        'year': [2010, 2011, 2010, 2012, 2011, 2012], # Added dummy year
        'length_of_ssr': [15, 20, 18, 22, 19, 21] # Added dummy ssr length
    }
    pd.DataFrame(dummy_hssr_data).to_csv(os.path.join(test_main_dir, 'hssr_data.csv'), index=False)

    # mutational_hotspot.csv
    dummy_hotspot_data = {
        'motif': [f'm{i}' for i in range(8)],
        'gene': ['GeneX', 'GeneY', 'GeneX', 'GeneZ', 'GeneY', 'GeneX', 'GeneZ', 'GeneY'],
        'repeat_count': [10, 5, 8, 12, 3, 15, 7, 9]
    }
    pd.DataFrame(dummy_hotspot_data).to_csv(os.path.join(test_main_dir, 'mutational_hotspot.csv'), index=False)

    # ssr_genecombo.tsv
    dummy_ssr_gene_data = {
        'gene': ['Gene1', 'Gene1', 'Gene2', 'Gene3', 'Gene2', 'Gene1', 'Gene4', 'Gene4'],
        'ssr_position': ['IN', 'intersect_start', 'IN', 'intersect_stop', 'IN', 'IN', 'intersect_start', 'intersect_stop']
        # Add other columns if needed by the plot function, though only gene/ssr_position are strictly required by current logic
    }
    pd.DataFrame(dummy_ssr_gene_data).to_csv(os.path.join(test_main_dir, 'ssr_genecombo.tsv'), sep='\t', index=False)

    # Add columns needed for UpSet Plot
    dummy_merged_data['country'] = ['USA', 'CAN', 'USA', 'MEX', 'CAN', 'USA', 'USA', 'USA'] # Re-adding country if needed by upset
    dummy_merged_data['GC_per'] = [45.5, 50.1, 60.2, 55.0, 48.0, 52.5, 46.0, 61.0] # Re-adding GC% if needed by upset

    pd.DataFrame(dummy_merged_data).to_csv(os.path.join(test_main_dir, 'mergedOut.tsv'), sep='\t', index=False)

    print(f"Created dummy data in: {test_main_dir}")
    print(f"Running plot generation. Output will be in subdirectories under: {test_plots_dir}")

    # Run the main plotting function (with a dummy reference ID for testing)
    generate_all_plots(test_main_dir, test_main_dir.replace("main", "intrim"), test_plots_dir, reference_id="g0") # Added intrim dir path

    print("Dummy run complete. Check the output directory and its subdirectories.")
