# crossroad/core/plotting.py

import pandas as pd
import numpy as np # Keep numpy as it might be used by loaded dataframes or future plots
import os
import logging
import traceback
import plotly.io as pio # Keep for theme setting if desired globally

# Import the plotting functions from the new modules
from .plots.category_country_sankey import create_category_country_sankey, sns_to_plotly_rgba # Import helper if needed globally? No, keep it local.
from .plots.gene_country_sankey import create_gene_country_sankey
from .plots.hotspot_plot import create_hotspot_plot
from .plots.loci_conservation_plot import create_loci_conservation_plot
from .plots.motif_conservation_plot import create_motif_conservation_plot
from .plots.relative_abundance_plot import create_relative_abundance_plot
from .plots.repeat_distribution_plot import create_repeat_distribution_plot
from .plots.ssr_gc_plot import create_ssr_gc_plot
from .plots.ssr_gene_intersect_plot import create_ssr_gene_intersect_plot
from .plots.temporal_faceted_scatter import create_temporal_faceted_scatter
from .plots.reference_ssr_distribution import create_scientific_ssr_plot # Added import
from .plots.upset_plot import create_upset_plot # Added import for UpSet plot
# Set default theme (can be set here or managed elsewhere)
# pio.templates.default = "plotly_white" # Keep commented if set elsewhere or per-plot

logger = logging.getLogger(__name__)

# --- Main Orchestration Function ---

def generate_all_plots(job_output_main_dir, job_output_plots_dir, reference_id=None):
    """
    Loads data from job_output_main_dir, generates all defined plots by calling
    functions from the crossroad.core.plots package, and saves them to
    job_output_plots_dir (each plot type in its own subdirectory).
    Optionally generates a reference-specific plot if reference_id is provided.

    Args:
        job_output_main_dir (str): Path to the directory containing main output files.
        job_output_plots_dir (str): Path to the base directory where plot subdirectories will be created.
        reference_id (str, optional): The ID of the reference genome. Defaults to None.
    """
    logger.info(f"Starting plot generation. Data source: {job_output_main_dir}, Output target: {job_output_plots_dir}, Reference ID: {reference_id}")

    # Ensure the base output directory exists
    try:
        os.makedirs(job_output_plots_dir, exist_ok=True)
        logger.info(f"Ensured base plots output directory exists: {job_output_plots_dir}")
    except OSError as e:
        logger.error(f"Could not create base plots output directory {job_output_plots_dir}: {e}")
        return # Cannot proceed without output directory

    # --- Load Dataframes (Load once if used by multiple plots) ---
    df_merged = None
    merged_out_path = os.path.join(job_output_main_dir, 'mergedOut.tsv')
    if os.path.exists(merged_out_path):
        try:
            logger.info(f"Loading data from {merged_out_path}...")
            df_merged = pd.read_csv(merged_out_path, sep='\t', low_memory=False)
            if df_merged.empty:
                logger.warning(f"Loaded dataframe from {merged_out_path} is empty.")
                df_merged = None # Treat as not loaded if empty
        except Exception as e:
            logger.error(f"Failed to load data from {merged_out_path}: {e}\n{traceback.format_exc()}")
            df_merged = None
    else:
        logger.warning(f"Input file not found: {merged_out_path}")

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

    # 1. Category -> Country Sankey (uses df_merged)
    if df_merged is not None:
        try:
            logger.info(f"Generating Category->Country Sankey plot...")
            create_category_country_sankey(df_merged.copy(), job_output_plots_dir) # Pass copy
        except Exception as e:
            logger.error(f"Failed to plot Category->Country Sankey: {e}\n{traceback.format_exc()}")
    else:
        logger.warning(f"Skipping Category->Country Sankey plot: Data not available.")

    # 2. Gene -> Country Sankey (uses df_hssr)
    if df_hssr is not None:
        try:
            logger.info(f"Generating Gene->Country Sankey plot...")
            create_gene_country_sankey(df_hssr.copy(), job_output_plots_dir) # Pass copy
        except Exception as e:
            logger.error(f"Failed to plot Gene->Country Sankey: {e}\n{traceback.format_exc()}")
    else:
        logger.warning(f"Skipping Gene->Country Sankey plot: Data not available.")

    # 3. Motif Repeat Count (uses df_hotspot)
    if df_hotspot is not None:
        try:
            logger.info(f"Generating Motif Repeat Count plot...")
            create_hotspot_plot(df_hotspot.copy(), job_output_plots_dir) # Pass copy
        except Exception as e:
            logger.error(f"Failed to plot Motif Repeat Count: {e}\n{traceback.format_exc()}")
    else:
        logger.warning(f"Skipping Motif Repeat Count plot: Data not available.")

    # 4. Loci Conservation Pie Chart (uses df_merged)
    if df_merged is not None:
         try:
             logger.info(f"Generating Loci Conservation plot...")
             create_loci_conservation_plot(df_merged.copy(), job_output_plots_dir) # Pass copy
         except Exception as e:
             logger.error(f"Failed to plot Loci Conservation: {e}\n{traceback.format_exc()}")
    else:
        logger.warning(f"Skipping Loci Conservation plot: Data not available.")

    # 5. Motif Conservation Pie Chart (uses df_merged)
    if df_merged is not None:
         try:
             logger.info(f"Generating Motif Conservation plot...")
             if 'motif' in df_merged.columns:
                 create_motif_conservation_plot(df_merged.copy(), job_output_plots_dir) # Pass copy
             else:
                 logger.warning(f"Skipping Motif Conservation plot: 'motif' column not found in merged data.")
         except Exception as e:
             logger.error(f"Failed to plot Motif Conservation: {e}\n{traceback.format_exc()}")
    else:
        logger.warning(f"Skipping Motif Conservation plot: Data not available.")

    # 6. Relative Abundance Bar Chart (uses df_merged)
    if df_merged is not None:
        try:
            logger.info(f"Generating Relative Abundance plot...")
            # Check required columns specifically for this plot
            required_ra_cols = ['category', 'genomeID', 'length_of_motif', 'length_genome']
            if all(col in df_merged.columns for col in required_ra_cols):
                 create_relative_abundance_plot(df_merged.copy(), job_output_plots_dir) # Pass copy
            else:
                 missing_ra = [col for col in required_ra_cols if col not in df_merged.columns]
                 logger.warning(f"Skipping Relative Abundance plot: Missing required columns {missing_ra} in merged data.")
        except Exception as e:
            logger.error(f"Failed to plot Relative Abundance: {e}\n{traceback.format_exc()}")
    else:
        logger.warning(f"Skipping Relative Abundance plot: Data not available.")

    # 7. Repeat Distribution Bar Chart (uses df_merged)
    if df_merged is not None:
        try:
            logger.info(f"Generating Repeat Distribution plot...")
            # Check required columns specifically for this plot
            required_rd_cols = ['category', 'genomeID', 'length_of_motif', 'length_genome', 'length_of_ssr']
            if all(col in df_merged.columns for col in required_rd_cols):
                create_repeat_distribution_plot(df_merged.copy(), job_output_plots_dir) # Pass copy
            else:
                missing_rd = [col for col in required_rd_cols if col not in df_merged.columns]
                logger.warning(f"Skipping Repeat Distribution plot: Missing required columns {missing_rd} in merged data.")
        except Exception as e:
            logger.error(f"Failed to plot Repeat Distribution: {e}\n{traceback.format_exc()}")
    else:
        logger.warning(f"Skipping Repeat Distribution plot: Data not available.")

    # 8. SSR GC Distribution Scatter Plot (uses df_merged)
    if df_merged is not None:
        try:
            logger.info(f"Generating SSR GC Distribution plot...")
            # Check required columns specifically for this plot
            required_gc_cols = ['genomeID', 'GC_per']
            if all(col in df_merged.columns for col in required_gc_cols):
                create_ssr_gc_plot(df_merged.copy(), job_output_plots_dir) # Pass copy
            else:
                missing_gc = [col for col in required_gc_cols if col not in df_merged.columns]
                logger.warning(f"Skipping SSR GC Distribution plot: Missing required columns {missing_gc} in merged data.")
        except Exception as e:
            logger.error(f"Failed to plot SSR GC Distribution: {e}\n{traceback.format_exc()}")
    else:
        logger.warning(f"Skipping SSR GC Distribution plot: Data not available.")

    # 9. SSR Gene Intersection Plot (uses df_ssr_gene)
    if df_ssr_gene is not None:
        try:
            logger.info(f"Generating SSR Gene Intersection plot...")
            # Check required columns specifically for this plot
            required_intersect_cols = ['gene', 'ssr_position']
            if all(col in df_ssr_gene.columns for col in required_intersect_cols):
                create_ssr_gene_intersect_plot(df_ssr_gene.copy(), job_output_plots_dir) # Pass copy
            else:
                missing_intersect = [col for col in required_intersect_cols if col not in df_ssr_gene.columns]
                logger.warning(f"Skipping SSR Gene Intersection plot: Missing required columns {missing_intersect} in {ssr_gene_path}.")
        except Exception as e:
            logger.error(f"Failed to plot SSR Gene Intersection: {e}\n{traceback.format_exc()}")
    else:
        logger.warning(f"Skipping SSR Gene Intersection plot: Data not available from {ssr_gene_path}.")

    # 10. Temporal Faceted Scatter Plot (uses df_hssr)
    if df_hssr is not None:
        try:
            logger.info(f"Generating Temporal Faceted Scatter plot...")
            # Check required columns specifically for this plot
            required_temporal_cols = ['motif', 'year', 'length_of_ssr', 'gene', 'genomeID']
            if all(col in df_hssr.columns for col in required_temporal_cols):
                create_temporal_faceted_scatter(df_hssr.copy(), job_output_plots_dir) # Pass copy
            else:
                missing_temporal = [col for col in required_temporal_cols if col not in df_hssr.columns]
                logger.warning(f"Skipping Temporal Faceted Scatter plot: Missing required columns {missing_temporal} in {hssr_data_path}.")
        except Exception as e:
            logger.error(f"Failed to plot Temporal Faceted Scatter: {e}\n{traceback.format_exc()}")
    else:
        logger.warning(f"Skipping Temporal Faceted Scatter plot: Data not available from {hssr_data_path}.")

    # 11. Reference SSR Distribution (uses df_ssr_gene, conditional on reference_id)
    if reference_id and df_ssr_gene is not None:
        try:
            logger.info(f"Generating Reference SSR Distribution plot for {reference_id}...")
            # Check required columns specifically for this plot
            required_ref_cols = ['genomeID', 'ssr_position']
            if all(col in df_ssr_gene.columns for col in required_ref_cols):
                create_scientific_ssr_plot(df_ssr_gene.copy(), reference_id, job_output_plots_dir) # Pass copy and ref_id
            else:
                missing_ref = [col for col in required_ref_cols if col not in df_ssr_gene.columns]
                logger.warning(f"Skipping Reference SSR Distribution plot: Missing required columns {missing_ref} in {ssr_gene_path}.")
        except Exception as e:
            logger.error(f"Failed to plot Reference SSR Distribution: {e}\n{traceback.format_exc()}")
    elif reference_id:
         logger.warning(f"Skipping Reference SSR Distribution plot: Data not available from {ssr_gene_path}.")
    else:
        logger.info("Skipping Reference SSR Distribution plot: No reference_id provided.")

    # 12. UpSet Plot (uses df_merged)
    if df_merged is not None:
        try:
            logger.info(f"Generating UpSet plot...")
            # Check required columns specifically for this plot (already done inside _prepare_upset_data)
            create_upset_plot(df_merged.copy(), job_output_plots_dir) # Pass copy
        except Exception as e:
            logger.error(f"Failed to plot UpSet Plot: {e}\n{traceback.format_exc()}")
    else:
        logger.warning(f"Skipping UpSet plot: Data not available.")


    # --- Add calls for other plots here, following the same pattern ---

    logger.info(f"Finished plot generation process for data in {job_output_main_dir}.")


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
    generate_all_plots(test_main_dir, test_plots_dir, reference_id="g0") # Example reference ID from dummy data

    print("Dummy run complete. Check the output directory and its subdirectories.")
