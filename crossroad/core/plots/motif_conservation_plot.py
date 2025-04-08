# crossroad/core/plots/motif_conservation_plot.py

import pandas as pd
import plotly.graph_objects as go
import plotly.io as pio
import numpy as np
import os
import logging
import traceback

logger = logging.getLogger(__name__)

# --- Plotting Function ---

def create_motif_conservation_plot(df, output_dir):
    """
    Creates a publication-quality pie chart visualizing motif conservation
    based on genome presence, with summary statistics and export options.
    Saves outputs to a specific subdirectory.

    Args:
        df (pd.DataFrame): DataFrame containing 'genomeID' and 'motif' columns.
        output_dir (str): Base directory where plot-specific subdirectories will be created.

    Returns:
        None: Saves files directly.
    """
    plot_name = "motif_conservation"
    logger.info(f"Processing data for {plot_name} plot...")

    # --- Basic Validation and Type Conversion ---
    required_cols = ['genomeID', 'motif']
    if not all(col in df.columns for col in required_cols):
        missing = [col for col in required_cols if col not in df.columns]
        logger.error(f"{plot_name}: Missing required columns: {missing}")
        raise ValueError(f"Missing required columns: {missing}")

    df_proc = df[required_cols].dropna().copy()

    # Ensure correct types
    df_proc['genomeID'] = df_proc['genomeID'].astype(str)
    df_proc['motif'] = df_proc['motif'].astype(str)

    if df_proc.empty:
        logger.warning(f"{plot_name}: DataFrame is empty after cleaning. Cannot generate plot.")
        return

    # --- Calculate Total Unique Genomes ---
    total_unique_genomes = df_proc['genomeID'].nunique()
    logger.info(f"{plot_name}: Total unique genomes found: {total_unique_genomes}")
    if total_unique_genomes == 0:
        logger.warning(f"{plot_name}: No unique genomes found. Cannot calculate conservation.")
        return

    # --- Motif Conservation Calculation ---
    logger.info(f"{plot_name}: Analyzing motif conservation...")
    motif_genome_counts = df_proc[['motif', 'genomeID']].drop_duplicates().groupby('motif')['genomeID'].count()

    conserved_motif_count = 0
    shared_motif_count = 0
    unique_motif_count = 0
    motifs_by_category = {'Conserved': [], 'Shared': [], 'Unique': []}

    for motif, count in motif_genome_counts.items():
        if count == total_unique_genomes:
            conserved_motif_count += 1
            motifs_by_category['Conserved'].append(motif)
        elif count > 1:
            shared_motif_count += 1
            motifs_by_category['Shared'].append(motif)
        else: # count == 1
            unique_motif_count += 1
            motifs_by_category['Unique'].append(motif)

    total_motifs = len(motif_genome_counts)
    logger.info(f"{plot_name}: Total unique motifs found: {total_motifs}")
    if total_motifs == 0:
        logger.warning(f"{plot_name}: No unique motifs found. Cannot generate plot.")
        return

    conserved_percent = (conserved_motif_count / total_motifs) * 100 if total_motifs > 0 else 0
    shared_percent = (shared_motif_count / total_motifs) * 100 if total_motifs > 0 else 0
    unique_percent = (unique_motif_count / total_motifs) * 100 if total_motifs > 0 else 0

    pie_labels = ['Conserved', 'Shared', 'Unique']
    pie_values = [conserved_percent, shared_percent, unique_percent]
    pie_counts = [conserved_motif_count, shared_motif_count, unique_motif_count]
    pie_colors = ['#87CEEB', '#90EE90', '#F08080'] # SkyBlue, LightGreen, LightCoral

    logger.info(f"{plot_name}: Motif Categories - Conserved: {conserved_motif_count} ({conserved_percent:.2f}%), "
                f"Shared: {shared_motif_count} ({shared_percent:.2f}%), "
                f"Unique: {unique_motif_count} ({unique_percent:.2f}%)")

    # --- Calculate Summary Statistics ---
    stats = {
        'total_unique_genomes': total_unique_genomes,
        'total_unique_motifs': total_motifs,
        'conserved_motifs_count': conserved_motif_count,
        'conserved_motifs_percent': conserved_percent,
        'shared_motifs_count': shared_motif_count,
        'shared_motifs_percent': shared_percent,
        'unique_motifs_count': unique_motif_count,
        'unique_motifs_percent': unique_percent,
    }
    logger.info(f"{plot_name}: Summary statistics calculated.")

    # --- Create Pie Chart ---
    logger.info(f"{plot_name}: Creating pie chart figure...")
    fig = go.Figure(data=[go.Pie(
        labels=pie_labels,
        values=pie_values,
        marker_colors=pie_colors,
        customdata=pie_counts,
        textinfo='percent+label',
        textfont_size=12,
        hovertemplate=(
            "<b>%{label}</b><br>" +
            "Percentage: %{value:.2f}%<br>" +
            "Motif Count: %{customdata:,}" +
            "<extra></extra>"
        ),
        insidetextorientation='horizontal',
        pull=[0.05 if cat == 'Conserved' else 0 for cat in pie_labels]
    )])

    # --- Customize Layout ---
    title_font = dict(size=16, family="Arial Black, Gadget, sans-serif", color='#333333')
    legend_font = dict(size=11, family="Arial, sans-serif", color='#444444')
    annotation_font = dict(size=9, family="Arial, sans-serif", color='#666666')
    signature_font = dict(size=8, family="Arial, sans-serif", color='#888888', style='italic')

    fixed_top_margin = 80
    fixed_bottom_margin = 100
    fixed_left_margin = 50
    fixed_right_margin = 180

    fig.update_layout(
        title=dict(
            text='<b>Distribution of Motif Categories by Conservation</b>',
            font=title_font, x=0.5, xanchor='center',
            y=1 - (fixed_top_margin / (600 + fixed_top_margin + fixed_bottom_margin)) * 0.5, yanchor='top'
        ),
        height=600 + fixed_top_margin + fixed_bottom_margin,
        paper_bgcolor='white',
        plot_bgcolor='white',
        showlegend=True,
        legend=dict(
            title=dict(text='Motif Category', font=legend_font), font=legend_font,
            orientation="v", yanchor="top", y=0.95, xanchor="left", x=1.02,
            bgcolor='rgba(255,255,255,0.8)', bordercolor='#cccccc', borderwidth=1
        ),
        margin=dict(l=fixed_left_margin, r=fixed_right_margin, t=fixed_top_margin, b=fixed_bottom_margin),
    )

    # --- Add Annotations ---
    stats_lines = ["<b>Summary Stats:</b>",
                   f"Total Genomes: {stats['total_unique_genomes']:,}",
                   f"Total Motifs: {stats['total_unique_motifs']:,}", "---",
                   f"Conserved: {stats['conserved_motifs_count']:,} ({stats['conserved_motifs_percent']:.1f}%)",
                   f"Shared: {stats['shared_motifs_count']:,} ({stats['shared_motifs_percent']:.1f}%)",
                   f"Unique: {stats['unique_motifs_count']:,} ({stats['unique_motifs_percent']:.1f}%)"]
    stats_text = "<br>".join(stats_lines)

    fig.add_annotation(
        xref="paper", yref="paper", x=1.01, y=0.65, text=stats_text,
        showarrow=False, font=annotation_font, align='left',
        bordercolor="#cccccc", borderwidth=1, borderpad=4,
        bgcolor="rgba(255, 255, 255, 0.8)", xanchor='left', yanchor='top'
    )

    signature_y_position = - (fixed_bottom_margin / fig.layout.height) * 0.3 if fig.layout.height else -0.1
    signature_y_position = max(signature_y_position, -0.12)

    fig.add_annotation(
        xref="paper", yref="paper", x=0.98, y=signature_y_position,
        text="<i>Powered by Crossroad</i>", showarrow=False,
        font=signature_font, align='right', yanchor='top'
    )

    # --- Prepare Data for CSV Export ---
    logger.info(f"{plot_name}: Preparing data for CSV export...")
    export_data = []
    for i, category in enumerate(pie_labels):
        motifs_str = "; ".join(sorted(motifs_by_category[category])) if motifs_by_category[category] else "-"
        export_data.append({
            'Category': category,
            'Count': pie_counts[i],
            'Percentage': pie_values[i],
            'Motifs': motifs_str # Base motifs
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
                     if isinstance(value, float):
                         f.write(f"{key_title}: {value:.2f}%\n")
                     else:
                         f.write(f"{key_title}: {value:,}\n")
             logger.info(f"Summary statistics for {plot_name} saved to: {stats_path}")
         except Exception as stats_err:
             logger.error(f"Failed to save summary statistics for {plot_name}: {stats_err}\n{traceback.format_exc()}")

    logger.info(f"{plot_name}: Plot generation and saving complete.")