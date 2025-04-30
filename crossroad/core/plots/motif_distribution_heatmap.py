# crossroad/core/plots/motif_distribution_heatmap.py

import pandas as pd
import plotly.graph_objects as go
import plotly.io as pio
import os
import logging
import traceback
import numpy as np

logger = logging.getLogger(__name__)

def create_motif_distribution_heatmap(df, output_dir): # Renamed function
    """
    Creates a heatmap visualizing the count of specific motifs within each genome,
    optionally excluding motifs present in all genomes.

    Args:
        df (pd.DataFrame): DataFrame which might be from reformatted.tsv (needs 'genomeID', 'motif')
                           or mergedOut.tsv (potentially includes 'category', 'country', 'year').
        output_dir (str): Base directory where plot-specific subdirectories will be created.

    Returns:
        None: Saves files directly.
    """
    plot_name = "motif_distribution_heatmap" # Renamed plot as it now shows motifs
    logger.info(f"Processing data for {plot_name} plot...")

    # --- Basic Validation ---
    # Need 'genomeID' and 'motif' for the heatmap structure
    required_cols = ['genomeID', 'motif']
    if not all(col in df.columns for col in required_cols):
        missing = [col for col in required_cols if col not in df.columns]
        logger.error(f"{plot_name}: Missing required columns for heatmap structure: {missing}")
        raise ValueError(f"Missing required columns for heatmap structure: {missing}")

    # Keep all occurrences to count them, using 'motif' now
    df_proc = df[required_cols].dropna().copy()
    df_proc['genomeID'] = df_proc['genomeID'].astype(str)
    df_proc['motif'] = df_proc['motif'].astype(str)

    if df_proc.empty:
        logger.warning(f"{plot_name}: DataFrame is empty after cleaning. Cannot generate plot.")
        return

    # --- Prepare Metadata (if available) ---
    metadata_cols = ['category', 'country', 'year']
    has_metadata = all(col in df.columns for col in metadata_cols)
    genome_metadata = {}
    if has_metadata:
        logger.info(f"{plot_name}: Found metadata columns ({', '.join(metadata_cols)}). Preparing annotations.")
        # Get unique metadata per genomeID
        meta_df = df[['genomeID'] + metadata_cols].drop_duplicates(subset=['genomeID']).set_index('genomeID')
        # Store category separately for sorting
        genome_categories = meta_df['category'].to_dict()
        # Create a formatted string for each genome
        genome_metadata = meta_df.apply(lambda row: f"{row['category']} | {row['country']} | {row['year']}", axis=1).to_dict()
    else:
        logger.info(f"{plot_name}: Metadata columns not found. Proceeding without genome annotations.")

    # --- Create Pivot Table (Counts Matrix) ---
    logger.info(f"{plot_name}: Creating counts matrix...")
    try:
        # Group by genome and motif, then count occurrences
        counts_df = df_proc.groupby(['genomeID', 'motif']).size().reset_index(name='count')
        # Pivot to get genomes as rows, motifs as columns, and counts as values
        pivot_df = counts_df.pivot_table(index='genomeID', columns='motif', values='count', fill_value=0)
    except Exception as e:
        logger.error(f"{plot_name}: Failed to create pivot table: {e}\n{traceback.format_exc()}")
        return

    if pivot_df.empty:
        logger.warning(f"{plot_name}: Pivot table is empty. Cannot generate plot.")
        return

    initial_motif_count = pivot_df.shape[1]
    logger.info(f"{plot_name}: Initial unique motifs in pivot table: {initial_motif_count}")

    # --- Filter out completely conserved motifs ---
    logger.info(f"{plot_name}: Checking for conserved motifs to exclude...")
    # A motif is conserved if it has a count > 0 in every single genome (row)
    conserved_motifs = pivot_df.columns[(pivot_df > 0).all(axis=0)]
    if not conserved_motifs.empty:
        num_conserved = len(conserved_motifs)
        logger.info(f"{plot_name}: Found {num_conserved} conserved motifs to exclude: {', '.join(conserved_motifs)}")
        pivot_df = pivot_df.drop(columns=conserved_motifs)
    else:
        num_conserved = 0
        logger.info(f"{plot_name}: No completely conserved motifs found to exclude.")

    if pivot_df.empty or pivot_df.shape[1] == 0:
        logger.warning(f"{plot_name}: Pivot table is empty after excluding conserved motifs. Cannot generate plot.")
        return

    num_remaining = pivot_df.shape[1]
    logger.info(f"{plot_name}: Motifs remaining after exclusion: {num_remaining} (Expected: {initial_motif_count - num_conserved})")

    # Removed max_columns filtering to show all non-conserved motifs
    # max_columns = 100 # Example limit - adjust as needed
    # if pivot_df.shape[1] > max_columns:
    #      logger.warning(f"{plot_name}: Number of non-conserved motifs ({pivot_df.shape[1]}) exceeds limit ({max_columns}). Selecting top {max_columns} by total count.")
    #      # Calculate total count for each remaining motif across all genomes
    #      motif_total_counts = pivot_df.sum(axis=0).sort_values(ascending=False)
    #      top_motifs = motif_total_counts.head(max_columns).index.tolist()
    #      pivot_df = pivot_df[top_motifs] # Keep only columns for top motifs by count

    logger.info(f"{plot_name}: Final matrix shape (Genomes x Motifs): {pivot_df.shape}")

    # --- Sort by Category if metadata exists ---
    if has_metadata:
        logger.info(f"{plot_name}: Attempting to sort genomes by category...")
        # Create a sorting key based on category then genomeID
        try:
            # Create a temporary DataFrame for robust sorting
            temp_sort_df = pd.DataFrame({
                'genomeID': pivot_df.index,
                'category': pivot_df.index.map(genome_categories).fillna('')
            })
            # Sort by category, then by genomeID
            temp_sort_df = temp_sort_df.sort_values(by=['category', 'genomeID'])
            # Get the sorted list of genomeIDs
            sorted_genome_index = temp_sort_df['genomeID'].tolist()
            # Reindex the pivot table using the sorted list
            pivot_df = pivot_df.loc[sorted_genome_index]
            logger.info(f"{plot_name}: Sorting complete. First 5 index values after sort: {pivot_df.index[:5].tolist()}")
        except Exception as sort_err:
             logger.error(f"{plot_name}: Error during sorting: {sort_err}. Proceeding without sorting.")
             # Keep original pivot_df order if sorting fails

    # [ Duplicate sorting block removed ]

    # [ Duplicate sorting block removed ]

    # --- Create Heatmap ---
    logger.info(f"{plot_name}: Creating heatmap figure...")
    fig = go.Figure(data=go.Heatmap(
        z=pivot_df.values,
        x=pivot_df.columns,
        y=[f"{genome_metadata.get(idx, '')} {idx}" if has_metadata else idx for idx in pivot_df.index], # Prepend metadata if available
        colorscale='Viridis', # Change colorscale to Viridis
        xgap=1, # Add horizontal gap between cells
        ygap=1, # Add vertical gap between cells
        showscale=True, # Show the colorscale bar
        colorbar=dict(title='Count'), # Add title to colorbar
        hovertemplate="<b>Genome:</b> %{y}<br><b>Motif:</b> %{x}<br><b>Count:</b> %{z}<extra></extra>" # Update hover text
    ))

    # --- Customize Layout ---
    title_font = dict(size=16, family="Arial Black, Gadget, sans-serif", color='#333333')
    axis_label_font = dict(size=12, family="Arial, sans-serif", color='#444444')
    tick_font = dict(size=9, family="Arial, sans-serif", color='#555555') # Slightly smaller tick font
    signature_font = dict(size=8, family="Arial, sans-serif", color='#888888', style='italic') # Define signature font

    # Adjust height based on number of genomes, width based on SSRs (within limits)
    plot_height = max(600, len(pivot_df.index) * 15) # Min height 600px, add 15px per genome
    # Increase width allocation per motif column to give labels more space
    # Increase width allocation per motif column significantly more
    plot_width = max(1000, len(pivot_df.columns) * 25) # Min width 1000px, add 25px per motif
    plot_height = min(plot_height, 4000) # Max height limit
    plot_width = min(plot_width, 12000) # Increase max width limit further


    fig.update_layout(
        title=dict(
            text=('<b>Motif Counts Across Genomes (Excluding Conserved)</b>' if len(conserved_motifs) > 0 \
                  else '<b>Motif Counts Across Genomes</b>'), # Main title only
            font=title_font, x=0.5, xanchor='center', y=0.99, yanchor='top' # Slightly higher y position
        ),
        # Add annotation for "Powered by Crossroad"
        annotations=[
            dict(
                text="<i>Powered by Crossroad</i>",
                x=0.5,
                y=1.0, # Position below title
                xref="paper",
                yref="paper",
                showarrow=False,
                font=dict(size=4), # Small font size
                xanchor="center",
                yanchor="top"
            )
        ],
        height=plot_height,
        width=plot_width,
        xaxis_title=dict(text='Motif', font=axis_label_font), # Update x-axis title
        yaxis_title=dict(text='Genome ID' + (' (Category | Country | Year)' if has_metadata else ''), font=axis_label_font), # Update y-axis title
        xaxis=dict(tickangle=-90, tickfont=tick_font, automargin=True), # Steeper angle
        yaxis=dict(tickfont=tick_font, automargin=True),
        paper_bgcolor='white',
        plot_bgcolor='white',
        margin=dict(l=50, r=50, t=120, b=150) # Keep increased top margin
    )

    # Signature removed, integrated into title

    # --- Create Subdirectory and Save Outputs ---
    plot_specific_dir = os.path.join(output_dir, plot_name)
    try:
        os.makedirs(plot_specific_dir, exist_ok=True)
        logger.info(f"Ensured output directory exists: {plot_specific_dir}")
    except OSError as e:
        logger.error(f"Could not create plot directory {plot_specific_dir}: {e}")
        return

    logger.info(f"{plot_name}: Saving plot outputs to {plot_specific_dir}...")
    try:
        html_path = os.path.join(plot_specific_dir, f"{plot_name}.html")
        fig.write_html(html_path, include_plotlyjs='cdn')
        logger.info(f"Saved HTML plot to {html_path}")
    except Exception as html_err:
        logger.error(f"Failed to save HTML plot {plot_name}: {html_err}\n{traceback.format_exc()}")

    # Save static images (optional, requires kaleido)
    for fmt in ["png"]: # Keep static formats minimal for potentially large plots
        try:
            img_path = os.path.join(plot_specific_dir, f"{plot_name}.{fmt}")
            fig.write_image(img_path,scale=1) # Use scale=1 for large plots
            logger.info(f"Saved {fmt.upper()} plot to {img_path}")
        except ValueError as img_err:
             logger.error(f"Error saving {fmt.upper()} {plot_name}: {img_err}. Ensure 'kaleido' is installed.")
        except Exception as img_save_err:
             logger.error(f"An unexpected error during {fmt.upper()} saving for {plot_name}: {img_save_err}\n{traceback.format_exc()}")

    # Save the pivot table data
    try:
        output_csv_path = os.path.join(plot_specific_dir, f'{plot_name}_matrix.csv')
        pivot_df.to_csv(output_csv_path, float_format='%d') # Save counts as integers
        logger.info(f"Motif distribution matrix saved to: {output_csv_path}")
    except Exception as csv_err:
        logger.error(f"Failed to save CSV data for {plot_name}: {csv_err}\n{traceback.format_exc()}")

    logger.info(f"{plot_name}: Plot generation and saving complete.")