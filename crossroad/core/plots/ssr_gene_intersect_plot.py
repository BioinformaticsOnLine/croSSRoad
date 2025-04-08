# crossroad/core/plots/ssr_gene_intersect_plot.py

import pandas as pd
import plotly.graph_objects as go
import plotly.io as pio
import numpy as np
import os
import logging
import traceback

logger = logging.getLogger(__name__)

# --- Plotting Function ---

def create_ssr_gene_intersect_plot(df, output_dir):
    """
    Creates a publication-quality stacked bar chart showing the count of motifs per gene,
    stacked by their position relative to the gene (ssr_position).
    Saves outputs to a specific subdirectory.

    Args:
        df (pd.DataFrame): DataFrame containing 'gene' and 'ssr_position' columns.
        output_dir (str): Base directory where plot-specific subdirectories will be created.

    Returns:
        None: Saves files directly.
    """
    plot_name = "ssr_gene_intersection" # Changed name slightly for clarity
    logger.info(f"Processing data for {plot_name} plot...")

    # --- Basic Validation and Type Conversion ---
    required_cols = ['gene', 'ssr_position']
    if not all(col in df.columns for col in required_cols):
        missing = [col for col in required_cols if col not in df.columns]
        logger.error(f"{plot_name}: Missing required columns: {missing}")
        raise ValueError(f"Missing required columns: {missing}")

    df_proc = df[required_cols].dropna().copy()

    # Ensure correct types
    df_proc['gene'] = df_proc['gene'].astype(str)
    df_proc['ssr_position'] = df_proc['ssr_position'].astype(str)

    if df_proc.empty:
        logger.warning(f"{plot_name}: DataFrame is empty after cleaning/filtering. Cannot generate plot.")
        return

    # --- Aggregate Data: Count occurrences per gene and ssr_position ---
    logger.info(f"{plot_name}: Aggregating counts per gene and position...")
    position_counts = df_proc.groupby(['gene', 'ssr_position']).size().unstack(fill_value=0)

    # Define a consistent order for positions
    preferred_position_order = ['IN', 'intersect_start', 'intersect_stop'] # Add others if they exist
    available_positions = [p for p in preferred_position_order if p in position_counts.columns]
    other_positions = sorted([p for p in position_counts.columns if p not in preferred_position_order])
    final_position_order = available_positions + other_positions
    position_counts = position_counts[final_position_order] # Reorder columns

    if position_counts.empty:
        logger.warning(f"{plot_name}: No data after aggregation. Cannot generate plot.")
        return

    logger.info(f"{plot_name}: Aggregated data for {len(position_counts)} genes.")

    # --- Calculate Summary Statistics ---
    total_motifs = int(position_counts.sum().sum()) # Ensure integer
    total_genes = len(position_counts)
    position_totals = position_counts.sum().astype(int).to_dict() # Totals per position as int

    stats = {
        'total_genes': total_genes,
        'total_motifs': total_motifs,
        **{f'motifs_{pos}': count for pos, count in position_totals.items()} # Add counts per position
    }
    logger.info(f"{plot_name}: Summary statistics calculated.")

    # --- Prepare Data for Plotting ---
    genes = position_counts.index.tolist()
    positions = position_counts.columns.tolist()

    plotly_series = []
    logger.info(f"{plot_name}: Creating traces for stacked bar chart...")
    for position in positions:
        plotly_series.append(go.Bar(
            name=position,
            x=genes,
            y=position_counts[position],
            hovertemplate=(
                f"<b>Gene:</b> %{{x}}<br>" +
                f"<b>Position:</b> {position}<br>" +
                f"<b>Count:</b> %{{y:,}}" + # Add comma separator
                "<extra></extra>"
            )
        ))

    # --- Create Plot ---
    logger.info(f"{plot_name}: Creating plot figure...")
    fig = go.Figure(data=plotly_series)

    # --- Customize Layout ---
    logger.info(f"{plot_name}: Customizing layout...")

    title_font = dict(size=16, family="Arial Black, Gadget, sans-serif", color='#333333')
    axis_label_font = dict(size=12, family="Arial, sans-serif", color='#444444')
    tick_font = dict(size=10, family="Arial, sans-serif", color='#555555')
    legend_font = dict(size=11, family="Arial, sans-serif", color='#444444')
    annotation_font = dict(size=9, family="Arial, sans-serif", color='#666666')
    signature_font = dict(size=8, family="Arial, sans-serif", color='#888888', style='italic')

    fixed_bottom_margin = 180
    fixed_right_margin = 160
    fixed_top_margin = 100

    fig.update_layout(
        title=dict(
            text='<b>Distribution of SSR Motifs by Gene and Position</b>',
            font=title_font, x=0.5, xanchor='center', y=0.97, yanchor='top'
        ),
        height=700,
        barmode='stack',
        paper_bgcolor='white',
        plot_bgcolor='white',
        xaxis=dict(
            title=dict(text='Gene', font=axis_label_font),
            type='category', tickangle=-45, automargin=True,
            tickfont=tick_font, showline=True, linewidth=1, linecolor='black', mirror=True,
            gridcolor='#eef0f2', gridwidth=1, zeroline=False
        ),
        yaxis=dict(
            title=dict(text='Count of SSR Motifs', font=axis_label_font),
            tickfont=tick_font, showline=True, linewidth=1, linecolor='black', mirror=True,
            gridcolor='#eef0f2', gridwidth=1, tickformat=',d', zeroline=False
        ),
        legend_title_text='SSR Position',
        legend=dict(
            orientation="h", yanchor="bottom", y=1.02, xanchor="center", x=0.5,
            font=legend_font, bgcolor='rgba(255,255,255,0.8)',
            bordercolor='#cccccc', borderwidth=1
        ),
        margin=dict(l=70, r=fixed_right_margin, t=fixed_top_margin, b=fixed_bottom_margin),
        xaxis_rangeslider_visible=False
    )

    # --- Add Annotations ---
    stats_lines = [f"Total Genes: {stats['total_genes']:,}",
                   f"Total Motifs: {stats['total_motifs']:,}"]
    stats_lines.append("---")
    for pos in final_position_order:
        count = stats.get(f'motifs_{pos}', 0)
        if total_motifs > 0:
            percentage = (count / total_motifs) * 100
            stats_lines.append(f"{pos}: {count:,} ({percentage:.1f}%)")
        else:
            stats_lines.append(f"{pos}: {count:,}")

    stats_text = "<br>".join(stats_lines)

    fig.add_annotation(
        xref="paper", yref="paper", x=1.01, y=0.98, text=stats_text,
        showarrow=False, font=annotation_font, align='left',
        bordercolor="#cccccc", borderwidth=1, borderpad=4,
        bgcolor="rgba(255, 255, 255, 0.8)", xanchor='left', yanchor='top'
    )

    signature_y_position = -0.28

    fig.add_annotation(
        xref="paper", yref="paper", x=0.98, y=signature_y_position,
        text="<i>Powered by Crossroad</i>", showarrow=False,
        font=signature_font, align='right', yanchor='top'
    )

    # --- Prepare Data for CSV Export ---
    logger.info(f"{plot_name}: Preparing data for CSV export...")
    export_df = position_counts.copy()
    export_df['Total'] = export_df.sum(axis=1).astype(int)
    export_df = export_df.reset_index()
    export_df.rename(columns={'gene': 'Gene'}, inplace=True)

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

    for fmt in ["png", "pdf", "svg"]:
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
            export_df.to_csv(output_csv_path, index=False, float_format='%.0f') # Counts are integers
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
                     f.write(f"{key.replace('_', ' ').title()}: {value:,}\n") # Add comma formatting
             logger.info(f"Summary statistics for {plot_name} saved to: {stats_path}")
         except Exception as stats_err:
             logger.error(f"Failed to save summary statistics for {plot_name}: {stats_err}\n{traceback.format_exc()}")

    logger.info(f"{plot_name}: Plot generation and saving complete.")