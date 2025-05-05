# crossroad/core/plots/ssr_gc_plot.py

import pandas as pd
import plotly.graph_objects as go
import plotly.io as pio
import numpy as np
import os
import logging
import traceback

logger = logging.getLogger(__name__)

# --- Plotting Function ---

def create_ssr_gc_plot(df, output_dir):
    """
    Creates a publication-quality scatter plot showing the number of motifs vs. genome ID,
    with point size and color determined by the mean GC% of motifs in that genome.
    Saves outputs to a specific subdirectory.

    Args:
        df (pd.DataFrame): DataFrame containing 'genomeID' and 'GC_per' columns.
        output_dir (str): Base directory where plot-specific subdirectories will be created.

    Returns:
        None: Saves files directly.
    """
    plot_name = "ssr_gc_distribution" # Changed name slightly for clarity
    logger.info(f"Processing data for {plot_name} plot...")

    # --- Basic Validation and Type Conversion ---
    required_cols = ['genomeID', 'GC_per']
    if not all(col in df.columns for col in required_cols):
        missing = [col for col in required_cols if col not in df.columns]
        logger.error(f"{plot_name}: Missing required columns: {missing}")
        raise ValueError(f"Missing required columns: {missing}")

    df_proc = df[required_cols].dropna().copy()

    # Ensure correct types
    df_proc['genomeID'] = df_proc['genomeID'].astype(str)
    df_proc['GC_per'] = pd.to_numeric(df_proc['GC_per'], errors='coerce')

    # Drop rows where GC_per conversion failed
    df_proc = df_proc.dropna(subset=['GC_per'])

    if df_proc.empty:
        logger.warning(f"{plot_name}: DataFrame is empty after cleaning/filtering. Cannot generate plot.")
        return

    # --- Group by GenomeID and Aggregate ---
    logger.info(f"{plot_name}: Aggregating data per genome...")
    genome_agg = df_proc.groupby('genomeID').agg(
        motifCount=('GC_per', 'size'),
        meanGC=('GC_per', 'mean')
    ).reset_index()

    # Sort by genomeID alphabetically
    genome_agg = genome_agg.sort_values(by='genomeID').reset_index(drop=True)

    if genome_agg.empty:
        logger.warning(f"{plot_name}: No data after aggregation. Cannot generate plot.")
        return

    logger.info(f"{plot_name}: Aggregated data for {len(genome_agg)} genomes.")

    # --- Calculate Symbol Size (Log scale based on Motif Count) ---
    min_size = 5
    max_size = 20
    min_count = genome_agg['motifCount'].min()
    max_count = genome_agg['motifCount'].max()

    if max_count > min_count:
        # Use log scale for counts
        log_min = np.log10(min_count + 1) # Add 1 to handle count=0 or count=1
        log_max = np.log10(max_count + 1)
        if log_max > log_min:
            genome_agg['symbolSize'] = genome_agg['motifCount'].apply(
                lambda count: min_size + (max_size - min_size) * (np.log10(count + 1) - log_min) / (log_max - log_min)
            )
        else: # Handle case where all counts are the same after log+1
            genome_agg['symbolSize'] = (min_size + max_size) / 2
    else: # Handle case where all counts are identical
        genome_agg['symbolSize'] = (min_size + max_size) / 2

    genome_agg['symbolSize'] = genome_agg['symbolSize'].fillna(min_size)

    # --- Calculate Summary Statistics ---
    stats = {
        'total_genomes': len(genome_agg),
        'total_motifs': genome_agg['motifCount'].sum(),
        'overall_mean_gc': df_proc['GC_per'].mean(), # Use original data for overall mean
        'min_gc': genome_agg['meanGC'].min(),
        'max_gc': genome_agg['meanGC'].max(),
    }
    logger.info(f"{plot_name}: Summary statistics calculated.")

    # --- Create Plot ---
    logger.info(f"{plot_name}: Creating plot figure...")
    fig = go.Figure()

    fig.add_trace(go.Scatter(
        x=genome_agg['genomeID'],
        y=genome_agg['motifCount'],
        mode='markers',
        marker=dict(
            size=genome_agg['symbolSize'],
            color=genome_agg['meanGC'],
            colorscale='Viridis',
            colorbar=dict(
                title=dict(
                    text="Mean GC%",
                    side="right",
                    font=dict(size=11, family="Arial, sans-serif")
                ),
                thickness=15,
                tickfont=dict(size=10, family="Arial, sans-serif"),
                x=1.02,
                xanchor='left'
            ),
            showscale=True,
            opacity=0.8,
            line=dict(width=0.5, color='DarkSlateGrey')
        ),
        customdata=genome_agg[['meanGC', 'motifCount']],
        hovertemplate=(
            "<b>Genome ID:</b> %{x}<br>" +
            "<b>Number of Motifs (Size):</b> %{customdata[1]:,}<br>" +
            "<b>Mean GC% (Color):</b> %{customdata[0]:.2f}%" +
            "<extra></extra>"
        ),
        name=''
    ))
    # --- Customize Layout ---
    logger.info(f"{plot_name}: Customizing layout...")

    title_font = dict(size=16, family="Arial Black, Gadget, sans-serif", color='#333333')
    axis_label_font = dict(size=12, family="Arial, sans-serif", color='#444444')
    tick_font = dict(size=8, family="Arial, sans-serif", color='#555555')
    annotation_font = dict(size=9, family="Arial, sans-serif", color='#666666')
    # signature_font removed as noted in original code

    # Removed unused fixed_bottom_margin variable

    # Set a moderate width for slightly more spacing
    plot_width = 1000 # Adjust this value (e.g., 900, 1100, 1200) as needed

    logger.info(f"Setting plot width to {plot_width}px.")


    fig.update_layout(
        title=dict(
            text='<b>SSR Distribution and GC Content Across Genomes</b>',
            font=title_font, x=0.5, xanchor='center', y=0.95, yanchor='top'
        ),
        # --- Key Change: Added moderate width ---
        width=plot_width,  # Set plot width to 1000px
        height=700,        # Keep existing height
        # --- End Key Change ---
        paper_bgcolor='white',
        plot_bgcolor='white',
        xaxis=dict(
            title=None,
            type='category',
            tickangle=-45,
            automargin=True, # Let Plotly adjust margins for labels
            tickfont=tick_font,
            showline=True, linewidth=1, linecolor='black', mirror=True,
            gridcolor='#eef0f2',
            zeroline=False,
        ),
        yaxis=dict(
            title=dict(text='Number of SSR Motifs', font=axis_label_font),
            tickfont=tick_font,
            showline=True, linewidth=1, linecolor='black', mirror=True,
            gridcolor='#eef0f2', gridwidth=1,
            tickformat=',d',
            zeroline=False,
        ),
        hovermode='closest',
        xaxis_rangeslider_visible=False,
        legend=dict(traceorder='normal', font=dict(family='sans-serif', size=10), bgcolor='rgba(255,255,255,0.7)'),
        # Use slightly increased bottom margin for rotated labels
        margin=dict(l=60, r=250, t=100, b=120) # Keep bottom margin slightly larger
    )

    # --- Add Annotations ---
    stats_text = (f"Total Genomes: {stats['total_genomes']:,}<br>"
                  f"Total Motifs: {stats['total_motifs']:,}<br>"
                  f"Overall Mean GC: {stats['overall_mean_gc']:.2f}%<br>"
                  f"GC Range: {stats['min_gc']:.2f}% - {stats['max_gc']:.2f}%")

    fig.add_annotation(
        xref="paper", yref="paper", x=1.15, y=0.98, text=stats_text, # <<< Increased x value
        showarrow=False, font=annotation_font, align='left',
        bordercolor="#cccccc", borderwidth=1, borderpad=4,
        bgcolor="rgba(255, 255, 255, 0.8)", xanchor='left', yanchor='top'
    )
    # Signature removed, integrated into title

    # --- Prepare Data for CSV Export ---
    logger.info(f"{plot_name}: Preparing data for CSV export...")
    export_df = genome_agg[['genomeID', 'motifCount', 'meanGC']].copy()
    export_df.rename(columns={
        'genomeID': 'Genome ID',
        'motifCount': 'Number of Motifs',
        'meanGC': 'Mean GC%'
    }, inplace=True)

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
        # HTML (Interactive)
        html_path = os.path.join(plot_specific_dir, f"{plot_name}.html")
        fig.write_html(html_path, include_plotlyjs='cdn')
        logger.info(f"Saved HTML plot to {html_path}")
    except Exception as html_err:
        logger.error(f"Failed to save HTML plot {plot_name}: {html_err}\n{traceback.format_exc()}")

    for fmt in ["png", "svg"]:
        try:
            img_path = os.path.join(plot_specific_dir, f"{plot_name}.{fmt}")
            fig.write_image(img_path, scale=3 if fmt == "png" else None)
            logger.info(f"Saved {fmt.upper()} plot to {img_path}")
        except ValueError as img_err:
             logger.error(f"Error saving {fmt.upper()} {plot_name}: {img_err}. Ensure 'kaleido' is installed.")
        except Exception as img_save_err:
             logger.error(f"An unexpected error during {fmt.upper()} saving for {plot_name}: {img_save_err}\n{traceback.format_exc()}")

    # --- Save the export data to CSV ---
    if not export_df.empty:
        try:
            output_csv_path = os.path.join(plot_specific_dir, f'{plot_name}_summary.csv')
            export_df.to_csv(output_csv_path, index=False, float_format='%.2f')
            logger.info(f"Summary data for {plot_name} saved to: {output_csv_path}")
        except Exception as csv_err:
            logger.error(f"Failed to save CSV data for {plot_name}: {csv_err}\n{traceback.format_exc()}")
    else:
        logger.warning(f"{plot_name}: No export data generated.")

    # --- Optionally save summary stats ---
    if stats:
         try:
             stats_path = os.path.join(plot_specific_dir, f'{plot_name}_summary_statistics.txt')
             with open(stats_path, 'w') as f:
                 f.write(f"Summary Statistics for {plot_name}:\n")
                 f.write("------------------------------------\n")
                 for key, value in stats.items():
                     key_title = key.replace('_', ' ').title()
                     if isinstance(value, float):
                         f.write(f"{key_title}: {value:.2f}\n") # Keep % for GC if needed, check stats dict keys
                     else:
                         f.write(f"{key_title}: {value:,}\n")
             logger.info(f"Summary statistics for {plot_name} saved to: {stats_path}")
         except Exception as stats_err:
             logger.error(f"Failed to save summary statistics for {plot_name}: {stats_err}\n{traceback.format_exc()}")

    logger.info(f"{plot_name}: Plot generation and saving complete.")