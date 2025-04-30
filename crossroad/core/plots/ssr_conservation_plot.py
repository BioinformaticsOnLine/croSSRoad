# crossroad/core/plots/ssr_conservation_plot.py

import pandas as pd
import plotly.graph_objects as go
import plotly.io as pio
import numpy as np
import os
import logging
import traceback

logger = logging.getLogger(__name__)

# --- Plotting Function ---

def create_ssr_conservation_plot(df, output_dir):
    """
    Creates a publication-quality pie chart visualizing SSR conservation
    based on genome presence, with summary statistics and export options.
    Saves outputs to a specific subdirectory.

    Args:
        df (pd.DataFrame): DataFrame containing 'genomeID', 'SSR',
                        'motif', and 'repeat' columns.
        output_dir (str): Base directory where plot-specific subdirectories will be created.

    Returns:
        None: Saves files directly.
    """
    plot_name = "ssr_conservation"
    logger.info(f"Processing data for {plot_name} plot...")

    # --- Basic Validation and Type Conversion ---
    required_cols = ['genomeID', 'loci', 'motif', 'repeat'] # Expect 'loci' column from input
    if not all(col in df.columns for col in required_cols):
        missing = [col for col in required_cols if col not in df.columns]
        logger.error(f"{plot_name}: Missing required columns: {missing}")
        raise ValueError(f"Missing required columns: {missing}")

    df_proc = df[required_cols].dropna().copy()

    # Ensure correct types
    df_proc['genomeID'] = df_proc['genomeID'].astype(str)
    df_proc['loci'] = df_proc['loci'].astype(str) # Process the 'loci' column
    df_proc['motif'] = df_proc['motif'].astype(str)
    df_proc['repeat'] = pd.to_numeric(df_proc['repeat'], errors='coerce').fillna(0).astype(int)
    df_proc = df_proc[df_proc['repeat'] > 0] # Filter out invalid repeats if necessary

    if df_proc.empty:
        logger.warning(f"{plot_name}: DataFrame is empty after cleaning/filtering. Cannot generate plot.")
        return

    # --- Calculate Total Unique Genomes ---
    total_unique_genomes = df_proc['genomeID'].nunique()
    logger.info(f"{plot_name}: Total unique genomes found: {total_unique_genomes}")
    if total_unique_genomes == 0:
        logger.warning(f"{plot_name}: No unique genomes found. Cannot calculate conservation.")
        return

    # --- SSR Conservation Calculation ---
    logger.info(f"{plot_name}: Analyzing SSR conservation...")
    ssr_genome_counts = df_proc[['loci', 'genomeID']].drop_duplicates().groupby('loci')['genomeID'].count() # Group by 'loci' column

    conserved_ssr_count = 0
    shared_ssr_count = 0
    unique_ssr_count = 0

    for locus, count in ssr_genome_counts.items(): # Iterate using 'locus' from the grouped 'loci' column
        if count == total_unique_genomes:
            conserved_ssr_count += 1 # Changed 'loci' to 'ssr'
        elif count > 1:
            shared_ssr_count += 1 # Changed 'loci' to 'ssr'
        else: # count == 1
            unique_ssr_count += 1 # Changed 'loci' to 'ssr'

    total_ssr = len(ssr_genome_counts) # Changed 'loci' to 'ssr'
    logger.info(f"{plot_name}: Total unique SSRs found: {total_ssr}") # Changed 'loci' to 'SSRs'
    if total_ssr == 0:
        logger.warning(f"{plot_name}: No unique SSRs found. Cannot generate plot.") # Changed 'loci' to 'SSRs'
        return

    conserved_percent = (conserved_ssr_count / total_ssr) * 100 if total_ssr > 0 else 0 # Changed 'loci' to 'ssr'
    shared_percent = (shared_ssr_count / total_ssr) * 100 if total_ssr > 0 else 0 # Changed 'loci' to 'ssr'
    unique_percent = (unique_ssr_count / total_ssr) * 100 if total_ssr > 0 else 0 # Changed 'loci' to 'ssr'

    pie_labels = ['Conserved', 'Shared', 'Unique']
    pie_values = [conserved_percent, shared_percent, unique_percent]
    pie_counts = [conserved_ssr_count, shared_ssr_count, unique_ssr_count] # Changed 'loci' to 'ssr'
    pie_colors = ['#87CEEB', '#90EE90', '#F08080'] # SkyBlue, LightGreen, LightCoral

    logger.info(f"{plot_name}: SSR Categories - Conserved: {conserved_ssr_count} ({conserved_percent:.2f}%), " # Changed 'Loci' to 'SSR', 'loci' to 'ssr'
                f"Shared: {shared_ssr_count} ({shared_percent:.2f}%), " # Changed 'loci' to 'ssr'
                f"Unique: {unique_ssr_count} ({unique_percent:.2f}%)") # Changed 'loci' to 'ssr'

    # --- Repeated Motif Conservation Calculation (for Export) ---
    logger.info(f"{plot_name}: Analyzing repeated motif conservation for export...")
    try:
        df_proc['repeated_motif'] = df_proc.apply(lambda row: str(row['motif']) * int(row['repeat']), axis=1)
    except Exception as e:
        logger.warning(f"{plot_name}: Error constructing repeated motifs - {e}. Skipping motif analysis for export.")
        df_proc['repeated_motif'] = None

    motifs_by_category = {'Conserved': [], 'Shared': [], 'Unique': []}
    if 'repeated_motif' in df_proc.columns and df_proc['repeated_motif'].notna().any():
        motif_genome_counts_repeated = df_proc[['repeated_motif', 'genomeID']].drop_duplicates().groupby('repeated_motif')['genomeID'].count()
        for motif_str, count in motif_genome_counts_repeated.items():
            if count == total_unique_genomes:
                motifs_by_category['Conserved'].append(motif_str)
            elif count > 1:
                motifs_by_category['Shared'].append(motif_str)
            else: # count == 1
                motifs_by_category['Unique'].append(motif_str)
        logger.info(f"{plot_name}: Finished analyzing repeated motif conservation for export.")
    else:
        logger.info(f"{plot_name}: Skipped repeated motif analysis for export due to previous errors or no data.")

    # --- Calculate Summary Statistics ---
    stats = {
        'total_unique_genomes': total_unique_genomes,
        'total_unique_ssr': total_ssr, # Changed 'loci' to 'ssr'
        'conserved_ssr_count': conserved_ssr_count, # Changed 'loci' to 'ssr'
        'shared_ssr_count': shared_ssr_count, # Changed 'loci' to 'ssr'
        'unique_ssr_count': unique_ssr_count, # Changed 'loci' to 'ssr'
        'conserved_ssr_percent': conserved_percent, # Changed 'loci' to 'ssr'
        'shared_ssr_percent': shared_percent, # Changed 'loci' to 'ssr'
        'unique_ssr_percent': unique_percent, # Changed 'loci' to 'ssr'
    }
    logger.info(f"{plot_name}: Summary statistics calculated.")

    # --- Create Pie Chart ---
    logger.info(f"{plot_name}: Creating pie chart figure...")
    fig = go.Figure(data=[go.Pie(
        labels=pie_labels,
        values=pie_values,
        customdata=pie_counts,
        marker_colors=pie_colors,
        hoverinfo='label+percent',
        textinfo='percent+label',
        textfont_size=12,
        hovertemplate=(
            "<b>%{label} SSRs</b><br>" + # Changed 'Loci' to 'SSRs'
            "Count: %{customdata:,}<br>" +
            "Percentage: %{percent:.1%}" +
            "<extra></extra>"
        ),
        insidetextorientation='radial',
        hole=0.3
    )])

    # --- Customize Layout ---
    title_font = dict(size=16, family="Arial Black, Gadget, sans-serif", color='#333333')
    legend_font = dict(size=11, family="Arial, sans-serif", color='#444444')
    annotation_font = dict(size=9, family="Arial, sans-serif", color='#666666')
    signature_font = dict(size=8, family="Arial, sans-serif", color='#888888', style='italic')

    fixed_top_margin = 80
    fixed_bottom_margin = 100
    fixed_left_margin = 70
    fixed_right_margin = 70

    fig.update_layout(
        title=dict(
            text="SSR Conservation Across Representative Taxa",
            font=dict(size=16),
            x=0.5,
            y=0.95
        ),
        xaxis=dict(
            title='Representative Taxa',
            tickfont=dict(size=11)
        ),
        yaxis=dict(
            title='Motif',
            tickfont=dict(size=11)
        ),
        margin=dict(l=60, r=60, t=100, b=60),
        paper_bgcolor='white',
        plot_bgcolor='white',
        height=600,
        width=800,
        showlegend=False
    )

    # --- Add Annotations ---
    stats_lines = ["<b>Summary Stats:</b>",
                   f"Total Genomes: {stats['total_unique_genomes']:,}",
                   f"Total SSRs: {stats['total_unique_ssr']:,}", "---", # Changed 'Loci' to 'SSRs', 'loci' to 'ssr'
                   f"Conserved: {stats['conserved_ssr_count']:,} ({stats['conserved_ssr_percent']:.1f}%)", # Changed 'loci' to 'ssr'
                   f"Shared: {stats['shared_ssr_count']:,} ({stats['shared_ssr_percent']:.1f}%)", # Changed 'loci' to 'ssr'
                   f"Unique: {stats['unique_ssr_count']:,} ({stats['unique_ssr_percent']:.1f}%)"] # Changed 'loci' to 'ssr'
    stats_text = "<br>".join(stats_lines)

    fig.add_annotation(
        xref="paper", yref="paper", x=0.02, y=0.98, text=stats_text,
        showarrow=False, font=annotation_font, align='left',
        bordercolor="#cccccc", borderwidth=1, borderpad=4,
        bgcolor="rgba(255, 255, 255, 0.8)", xanchor='left', yanchor='top'
    )

    # Signature removed, integrated into title

    # --- Prepare Data for CSV Export ---
    logger.info(f"{plot_name}: Preparing data for CSV export...")
    export_data = []
    for i, category in enumerate(pie_labels):
        motifs_str = "; ".join(sorted(motifs_by_category[category])) if motifs_by_category[category] else "-"
        export_data.append({
            'Category': category,
            'Count': pie_counts[i],
            'Percentage': pie_values[i],
            'Motifs': motifs_str # Contains repeated motifs
        })
    export_df = pd.DataFrame(export_data)
    export_df = export_df[['Category', 'Count', 'Percentage', 'Motifs']]

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

    for fmt in ["png", "pdf", "svg"]:
        try:
            img_path = os.path.join(plot_specific_dir, f"{plot_name}.{fmt}")
            fig.write_image(img_path, scale=3 if fmt == "png" else None)
            logger.info(f"Saved {fmt.upper()} plot to {img_path}")
        except ValueError as img_err:
             logger.error(f"Error saving {fmt.upper()} {plot_name}: {img_err}. Ensure 'kaleido' is installed.")
        except Exception as img_save_err:
             logger.error(f"An unexpected error during {fmt.upper()} saving for {plot_name}: {img_save_err}\n{traceback.format_exc()}")

    if not export_df.empty:
        try:
            output_csv_path = os.path.join(plot_specific_dir, f'{plot_name}_summary.csv')
            export_df.to_csv(output_csv_path, index=False, float_format='%.2f')
            logger.info(f"Summary data for {plot_name} saved to: {output_csv_path}")
        except Exception as csv_err:
            logger.error(f"Failed to save CSV data for {plot_name}: {csv_err}\n{traceback.format_exc()}")
    else:
        logger.warning(f"{plot_name}: No export data generated.")

    if stats:
         try:
             stats_path = os.path.join(plot_specific_dir, f'{plot_name}_summary_statistics.txt')
             with open(stats_path, 'w') as f:
                 f.write(f"Summary Statistics for {plot_name}:\n")
                 f.write("------------------------------------\n")
                 for key, value in stats.items():
                     key_title = key.replace('_', ' ').title()
                     if 'percent' in key:
                         f.write(f"{key_title}: {value:.1f}%\n")
                     else:
                         f.write(f"{key_title}: {value:,}\n")
             logger.info(f"Summary statistics for {plot_name} saved to: {stats_path}")
         except Exception as stats_err:
             logger.error(f"Failed to save summary statistics for {plot_name}: {stats_err}\n{traceback.format_exc()}")

    logger.info(f"{plot_name}: Plot generation and saving complete.")