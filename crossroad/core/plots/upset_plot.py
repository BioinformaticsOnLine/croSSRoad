# crossroad/core/plots/upset_plot.py

import pandas as pd
import plotly.graph_objects as go
from plotly_upset.plotting import plot_upset
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
    Processes the merged DataFrame for UpSet plotting.

    Args:
        df (pandas.DataFrame): The input DataFrame (e.g., from mergedOut.tsv).

    Returns:
        pandas.DataFrame: Processed DataFrame ready for plotting,
                          or None if required columns are missing.
    """
    required_cols = ['motif', 'category', 'genomeID', 'GC_per', 'country']
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

        # Calculate aggregate data before resetting the index
        # Ensure grouping keys exist after potential filtering
        if 'motif' in df.columns:
            genome_counts = df.groupby('motif')['genomeID'].nunique()
            gc_percentages = df.groupby('motif')['GC_per'].mean()
            country_counts = df.groupby('motif')['country'].nunique()
        else:
             logger.warning("Motif column not found for aggregation. Aggregates will be zero.")
             # Create empty series to avoid errors if motif was somehow dropped
             idx = pivot_df.index
             genome_counts = pd.Series(0, index=idx, name='genomeID')
             gc_percentages = pd.Series(0.0, index=idx, name='GC_per')
             country_counts = pd.Series(0, index=idx, name='country')


        # Add aggregate columns to the pivot table
        pivot_df['genome_count'] = genome_counts.reindex(pivot_df.index, fill_value=0)
        pivot_df['GC%'] = gc_percentages.reindex(pivot_df.index, fill_value=0)
        pivot_df['country_count'] = country_counts.reindex(pivot_df.index, fill_value=0)

        # Reset index to make "motif" a regular column
        pivot_df.reset_index(inplace=True)
        logger.debug(f"Processed DataFrame shape after adding aggregates: {pivot_df.shape}")

        return pivot_df

    except Exception as e:
        logger.error(f"An error occurred during UpSet data preparation: {e}\n{traceback.format_exc()}")
        return None


def create_upset_plot(df_merged, output_base_dir):
    """
    Generates an UpSet plot showing motif conservation across categories
    and saves it to a 'upset_plot' subdirectory.

    Args:
        df_merged (pandas.DataFrame): DataFrame loaded from mergedOut.tsv.
        output_base_dir (str): The base directory to save plots.
    """
    logger.info("Attempting to generate UpSet plot...")
    plot_name = "motif_conservation_upset"
    output_dir = os.path.join(output_base_dir, "upset_plot")
    output_path = os.path.join(output_dir, f"{plot_name}.html")

    try:
        os.makedirs(output_dir, exist_ok=True)

        processed_df = _prepare_upset_data(df_merged)

        if processed_df is None or processed_df.empty:
            logger.warning("Skipping UpSet plot generation: Data preparation failed or resulted in empty data.")
            return

        # --- Plotting Logic ---
        metadata_cols = ['motif', 'genome_count', 'GC%', 'country_count']
        category_cols = [col for col in processed_df.columns if col not in metadata_cols]

        if not category_cols:
            logger.warning("Skipping UpSet plot generation: No category columns found after preparation.")
            return

        upset_matrix_df = processed_df[category_cols]
        marginal_data = [
            processed_df['genome_count'],
            processed_df['GC%'],
            processed_df['country_count']
        ]
        marginal_titles = ['genome_count', 'GC%', 'country_count']

        logger.debug(f"Plotting UpSet with {len(category_cols)} categories.")

        fig = plot_upset(
            dataframes=[upset_matrix_df],
            legendgroups=["Motif conservation"],
            exclude_zeros=True,
            sorted_x="d",
            sorted_y="a",
            row_heights=[0.4, 0.6],
            vertical_spacing=0.,
            horizontal_spacing=0.18,
            marginal_data=marginal_data,
            marginal_title=marginal_titles
        )

        # Customize colors (using a predefined list, cycling if necessary)
        colors = ['#A62961', 'red', 'cornflowerblue', '#FFBC30', '#F7E37D', 'black',
                  'pink', 'magenta', 'lime', 'brown', 'gray', 'turquoise', 'purple',
                  'orange', 'cyan', 'olive']

        num_traces = len(fig.data)
        for i, trace in enumerate(fig.data):
            if isinstance(trace, go.Bar):
                trace.marker.color = colors[i % len(colors)]

        # Update layout
        fig.update_layout(
            height=1200,
            width=1000,
            font_family="Arial, sans-serif", # Use a common sans-serif font
            font=dict(size=12),
            title=f'Motif Conservation Across Categories (UpSet Plot)',
            title_x=0.5 # Center title
        )

        # Save the plot
        fig.write_html(output_path, full_html=False, include_plotlyjs='cdn')
        logger.info(f"Successfully generated and saved UpSet plot to {output_path}")

    except ImportError:
         logger.error("Failed to generate UpSet plot: 'plotly-upset' library not found. Please install it.", exc_info=False) # Don't need full traceback if it's just missing lib
    except Exception as e:
        logger.error(f"An error occurred during UpSet plot generation: {e}\n{traceback.format_exc()}") 