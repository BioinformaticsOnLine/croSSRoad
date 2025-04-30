# crossroad/core/plots/upset_plot.py

import pandas as pd
import matplotlib.pyplot as plt
from upsetplot import UpSet, from_indicators
import warnings
import os
import logging
import traceback

# Ignore pandas warnings for cleaner output during processing
warnings.simplefilter(action='ignore', category=FutureWarning)
warnings.simplefilter(action='ignore', category=pd.errors.PerformanceWarning)

logger = logging.getLogger(__name__)

def _prepare_upset_data(df):
    """
    Processes the DataFrame for UpSet plotting.

    Args:
        df (pandas.DataFrame): The input DataFrame.

    Returns:
        pandas.DataFrame: Processed DataFrame ready for plotting,
                        or None if required columns are missing.
    """
    required_cols = ['motif', 'category']
    if not all(col in df.columns for col in required_cols):
        missing = [col for col in required_cols if col not in df.columns]
        logger.error(f"Input DataFrame missing required columns for UpSet plot: {missing}")
        return None

    try:
        # Pivot the dataframe: index=motif, columns=category, values=count
        pivot_df = df.pivot_table(index='motif', columns='category', aggfunc='size', fill_value=0)
        logger.debug(f"Pivoted DataFrame shape: {pivot_df.shape}")

        # Apply thresholding: Convert counts > 0 to 1 (presence)
        pivot_df = pivot_df.map(lambda x: 1 if x > 0 else 0)

        # Reset index to make "motif" a regular column
        pivot_df.reset_index(inplace=True)
        return pivot_df

    except Exception as e:
        logger.error(f"An error occurred during UpSet data preparation: {e}\n{traceback.format_exc()}")
        return None


def create_upset_plot(df_merged, output_base_dir):
    """
    Generates an UpSet plot showing motif conservation across categories.

    Args:
        df_merged (pandas.DataFrame): DataFrame loaded from mergedOut.tsv.
        output_base_dir (str): The base directory to save plots.
    """
    logger.info("Attempting to generate UpSet plot...")
    plot_name = "motif_conservation_upset"
    output_dir = os.path.join(output_base_dir, "upset_plot")
    output_png_path = os.path.join(output_dir, f"{plot_name}.png")
    output_csv_path = os.path.join(output_dir, f"{plot_name}_summary.csv")  # Path for the summary table

    try:
        os.makedirs(output_dir, exist_ok=True)
        processed_df = _prepare_upset_data(df_merged)

        if processed_df is None or processed_df.empty:
            logger.warning("Skipping UpSet plot generation: Data preparation failed or resulted in empty data.")
            return

        # Get category columns (excluding 'motif')
        category_cols = [col for col in processed_df.columns if col != 'motif']

        if not category_cols:
            logger.warning("Skipping UpSet plot generation: No category columns found after preparation.")
            return

        # Convert indicator columns to boolean
        indicator_df = processed_df[category_cols].astype(bool)
        upset_data = from_indicators(category_cols, indicator_df)

        # Create and save the plot
        plt.figure(figsize=(14, 10))
        upset = UpSet(
            upset_data, 
            subset_size='count', 
            show_counts=True,
            sort_by='cardinality',
            min_subset_size=1
        )
        upset.plot()
        plt.suptitle("Motif Conservation Across Categories (UpSet Plot)", fontsize=16)
        plt.savefig(output_png_path, dpi=300, bbox_inches='tight')
        plt.close()
        
        logger.info(f"Successfully generated and saved UpSet plot to {output_png_path}")

        # Save the summary table
        try:
            processed_df.to_csv(output_csv_path, index=False)
            logger.info(f"Successfully saved UpSet plot summary table to {output_csv_path}")
        except Exception as e_csv:
            logger.error(f"Failed to save UpSet plot summary table: {e_csv}\n{traceback.format_exc()}")

    except ImportError:
        logger.error("Failed to generate UpSet plot: 'upsetplot' library not found. Please install it.", exc_info=False)
    except Exception as e:
        logger.error(f"An error occurred during UpSet plot generation: {e}\n{traceback.format_exc()}") 