# crossroad/core/plots/reference_ssr_distribution.py

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
from matplotlib.ticker import FuncFormatter
import os
import logging
import traceback

logger = logging.getLogger(__name__)

# --- Plotting Function ---

def create_scientific_ssr_plot(df, reference_id, output_dir):
    """
    Creates a publication-quality scientific plot for Crossroad SSR analysis
    using matplotlib with a professional aesthetic, specifically for the reference genome.
    Saves outputs to a specific subdirectory.

    Args:
        df (pd.DataFrame): DataFrame containing 'genomeID' and 'ssr_position' columns.
                           Typically loaded from 'ssr_genecombo.tsv'.
        reference_id (str): The ID of the reference genome to filter by.
        output_dir (str): Base directory where plot-specific subdirectories will be created.

    Returns:
        None: Saves files directly.
    """
    plot_name = "reference_ssr_distribution"
    logger.info(f"Processing data for {plot_name} plot (Reference: {reference_id})...")

    # --- Basic Validation and Type Conversion ---
    required_cols = ['genomeID', 'ssr_position']
    if not all(col in df.columns for col in required_cols):
        missing = [col for col in required_cols if col not in df.columns]
        logger.error(f"{plot_name}: Missing required columns: {missing}")
        raise ValueError(f"Missing required columns: {missing}")

    # Filter for reference ID and ensure correct types
    df_proc = df[df['genomeID'] == reference_id].copy()
    if df_proc.empty:
        logger.warning(f"{plot_name}: No data found for reference genome {reference_id}. Skipping plot.")
        return

    df_proc['ssr_position'] = df_proc['ssr_position'].astype(str)

    # Count occurrences
    position_counts = df_proc['ssr_position'].value_counts().reset_index()
    position_counts.columns = ['Position', 'Count']
    position_counts = position_counts.sort_values(by='Position') # Sort for consistent bar order

    if position_counts.empty:
        logger.warning(f"{plot_name}: No SSR position data found for reference genome {reference_id} after filtering. Skipping plot.")
        return

    # Calculate statistics
    total_ssrs = position_counts['Count'].sum()
    max_count = position_counts['Count'].max()
    min_count = position_counts['Count'].min()

    stats = {
        'reference_genome': reference_id,
        'total_ssrs_in_ref': total_ssrs,
        'max_count_per_position': max_count,
        'min_count_per_position': min_count,
    }
    logger.info(f"{plot_name}: Summary statistics calculated.")

    # --- Create Plot ---
    logger.info(f"{plot_name}: Creating plot figure...")
    plt.style.use('seaborn-v0_8-whitegrid')
    sns.set_style("ticks")
    fig, ax = plt.subplots(figsize=(8, 5), dpi=300)

    colors = sns.color_palette("Set2", n_colors=len(position_counts))

    bars = ax.barh(position_counts['Position'], position_counts['Count'], color=colors)

    # Add value labels on bars
    for i, bar in enumerate(bars):
        count = position_counts['Count'].iloc[i]
        percentage = (count / total_ssrs) * 100 if total_ssrs > 0 else 0
        text_color = 'white' if bar.get_width() > max_count / 10 else 'black'
        text_position = bar.get_width() * 0.95 if text_color == 'white' else bar.get_width() * 1.02
        ax.text(text_position, bar.get_y() + bar.get_height() / 2,
                f'{count:,} ({percentage:.1f}%)',
                va='center', ha='right' if text_color == 'white' else 'left',
                color=text_color, fontsize=10, fontweight='bold')

    # Customize axis
    ax.set_xlabel('Count of SSRs', fontsize=12, fontweight='bold')
    ax.set_ylabel('SSR Position', fontsize=12, fontweight='bold')
    ax.xaxis.set_major_formatter(FuncFormatter(lambda x, p: format(int(x), ',')))
    ax.tick_params(axis='both', which='major', labelsize=10)

    # Customize grid and spines
    ax.grid(True, axis='x', linestyle='--', alpha=0.7)
    ax.set_axisbelow(True)
    sns.despine()

    # Titles
    plt.suptitle('Distribution of SSRs by Position in Reference Genome',
                 fontsize=16, fontweight='bold', y=0.98) # Adjusted y slightly
    plt.title(f'Reference: {reference_id}', fontsize=12, fontweight='bold', pad=10, color='#404040')

    # Add statistics text box
    stats_text = (f'Total SSRs: {total_ssrs:,}\n'
                  f'Max Count: {max_count:,}\n'
                  f'Min Count: {min_count:,}')
    ax.text(0.95, 0.97, stats_text, transform=ax.transAxes,
            bbox=dict(facecolor='white', edgecolor='gray', alpha=0.8, boxstyle='round,pad=0.5'),
            verticalalignment='top', horizontalalignment='right', fontsize=9)

    # Add Crossroad signature
    plt.figtext(0.99, 0.01, 'Powered by Crossroad', ha='right', va='bottom',
                fontsize=8, style='italic', alpha=0.7)

    plt.tight_layout(rect=[0, 0.03, 1, 0.95]) # Adjust layout to prevent title overlap

    # --- Prepare Export Data ---
    export_df = position_counts.copy()

    # --- Create Subdirectory and Save Outputs ---
    plot_specific_dir = os.path.join(output_dir, plot_name)
    try:
        os.makedirs(plot_specific_dir, exist_ok=True)
        logger.info(f"Ensured output directory exists: {plot_specific_dir}")
    except OSError as e:
        logger.error(f"Could not create plot directory {plot_specific_dir}: {e}")
        plt.close(fig) # Close the figure to free memory
        return

    logger.info(f"{plot_name}: Saving plot outputs to {plot_specific_dir}...")
    base_filename = os.path.join(plot_specific_dir, f"{plot_name}_{reference_id.replace('.', '_')}") # Make filename safe

    for fmt in ["png", "pdf", "svg", "tiff"]:
        try:
            save_path = f"{base_filename}.{fmt}"
            fig.savefig(save_path, dpi=600 if fmt in ['png', 'tiff'] else None,
                        bbox_inches='tight', pad_inches=0.1)
            logger.info(f"Saved {fmt.upper()} plot to {save_path}")
        except Exception as img_save_err:
             logger.error(f"An unexpected error occurred during {fmt.upper()} saving for {plot_name}: {img_save_err}\n{traceback.format_exc()}")

    plt.close(fig) # Close the figure after saving

    # --- Save the export data to CSV ---
    if not export_df.empty:
        try:
            output_csv_path = f"{base_filename}_data.csv"
            export_df.to_csv(output_csv_path, index=False)
            logger.info(f"Data for {plot_name} saved to: {output_csv_path}")
        except Exception as csv_err:
            logger.error(f"Failed to save CSV data for {plot_name}: {csv_err}\n{traceback.format_exc()}")
    else:
        logger.warning(f"{plot_name}: No export data generated.")

    # --- Optionally save summary stats ---
    if stats:
         try:
             stats_path = f"{base_filename}_summary_statistics.txt"
             with open(stats_path, 'w') as f:
                 f.write(f"Summary Statistics for {plot_name} (Reference: {reference_id}):\n")
                 f.write("------------------------------------------------------------\n")
                 for key, value in stats.items():
                     key_title = key.replace('_', ' ').title()
                     if isinstance(value, (int, np.integer)):
                         f.write(f"{key_title}: {value:,}\n")
                     else:
                         f.write(f"{key_title}: {value}\n")
             logger.info(f"Summary statistics for {plot_name} saved to: {stats_path}")
         except Exception as stats_err:
             logger.error(f"Failed to save summary statistics for {plot_name}: {stats_err}\n{traceback.format_exc()}")

    logger.info(f"{plot_name}: Plot generation and saving complete for reference {reference_id}.")