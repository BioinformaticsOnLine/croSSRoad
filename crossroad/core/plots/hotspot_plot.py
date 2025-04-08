# crossroad/core/plots/hotspot_plot.py

import pandas as pd
import numpy as np
import plotly.graph_objects as go
import plotly.express as px
import plotly.io as pio
import os
import logging
import traceback

logger = logging.getLogger(__name__)

# --- Plotting Function ---

def create_hotspot_plot(df, output_dir):
    """
    Creates a publication-quality stacked horizontal bar chart showing repeat
    counts per motif occurrence, stacked and colored by gene, with summary
    statistics and export options. Saves outputs to the specified directory.

    Args:
        df (pd.DataFrame): DataFrame containing 'motif', 'gene', and 'repeat_count' columns.
        output_dir (str): Base directory where plot-specific subdirectories will be created.

    Returns:
        None: Saves files directly.
    """
    plot_name = "motif_repeat_count_by_gene" # Keep original name for consistency if needed elsewhere
    logger.info(f"Processing data for {plot_name}...")

    # --- Basic Validation and Type Conversion ---
    required_cols = ['motif', 'gene', 'repeat_count']
    if not all(col in df.columns for col in required_cols):
        missing = [col for col in required_cols if col not in df.columns]
        logger.error(f"{plot_name}: Missing required columns: {missing}")
        raise ValueError(f"Missing required columns: {missing}")

    df_proc = df[required_cols].dropna().copy()

    # Ensure correct types
    df_proc['motif'] = df_proc['motif'].astype(str)
    df_proc['gene'] = df_proc['gene'].astype(str)
    df_proc['repeat_count'] = pd.to_numeric(df_proc['repeat_count'], errors='coerce').fillna(0)
    df_proc = df_proc[df_proc['repeat_count'] > 0]

    if df_proc.empty:
        logger.warning(f"{plot_name}: DataFrame is empty after cleaning/filtering. Cannot generate plot.")
        return

    # --- Data Preparation ---
    df_plot = df_proc.reset_index(drop=True).reset_index() # Create 'index' column

    unique_genes = sorted(df_plot['gene'].unique())
    unique_motifs = df_plot['motif'].unique()
    total_occurrences = len(df_plot)
    total_repeat_count = df_plot['repeat_count'].sum()
    logger.info(f"{plot_name}: Found {len(unique_genes)} unique genes and {len(unique_motifs)} unique motifs across {total_occurrences} occurrences.")

    # --- Calculate Summary Statistics ---
    gene_repeat_sums = df_plot.groupby('gene')['repeat_count'].sum().sort_values(ascending=False)
    stats = {
        'total_occurrences': total_occurrences,
        'total_unique_motifs': len(unique_motifs),
        'total_unique_genes': len(unique_genes),
        'total_repeat_count': total_repeat_count,
        **{f'repeats_in_{gene}': count for gene, count in gene_repeat_sums.head(5).items()}
    }
    logger.info(f"{plot_name}: Summary statistics calculated.")

    # --- Create the Plot using Plotly Express ---
    logger.info(f"{plot_name}: Creating plot figure...")
    fig = px.bar(
        df_plot,
        x='repeat_count',
        y='index',
        color='gene',
        orientation='h',
        hover_name='motif',
        hover_data={
            'index': False, 'gene': True, 'repeat_count': True, 'motif': True
        },
        color_discrete_sequence=px.colors.qualitative.Plotly,
        category_orders={"gene": unique_genes}
    )

    # --- Customize Layout ---
    title_font = dict(size=16, family="Arial Black, Gadget, sans-serif", color='#333333')
    axis_label_font = dict(size=12, family="Arial, sans-serif", color='#444444')
    tick_font = dict(size=10, family="Arial, sans-serif", color='#555555')
    legend_font = dict(size=11, family="Arial, sans-serif", color='#444444')
    annotation_font = dict(size=9, family="Arial, sans-serif", color='#666666')
    signature_font = dict(size=8, family="Arial, sans-serif", color='#888888', style='italic')

    fixed_left_margin = 150
    fixed_right_margin = 180
    fixed_top_margin = 100
    fixed_bottom_margin = 100

    fig.update_layout(
        title=dict(
            text='<b>Motif Repeat Count by Gene (Hotspots)</b>',
            font=title_font, x=0.5, xanchor='center',
            y=1 - (fixed_top_margin / (max(600, total_occurrences * 15 + fixed_top_margin + fixed_bottom_margin))) * 0.4,
            yanchor='top'
        ),
        height=max(600, total_occurrences * 15 + fixed_top_margin + fixed_bottom_margin),
        font=dict(family="Arial, sans-serif"),
        paper_bgcolor='white',
        plot_bgcolor='white',
        xaxis=dict(
            title=dict(text='Total Repeat Count', font=axis_label_font),
            tickfont=tick_font, showline=True, linewidth=1, linecolor='black', mirror=True,
            gridcolor='#eef0f2', gridwidth=1, zeroline=False,
        ),
        yaxis=dict(
            tickvals=df_plot['index'], ticktext=df_plot['motif'],
            title=dict(text='Motif Occurrence', font=axis_label_font),
            tickfont=tick_font, showline=True, linewidth=1, linecolor='black', mirror=True,
            gridcolor='#eef0f2', gridwidth=1, zeroline=False,
            autorange="reversed"
        ),
        barmode='stack',
        legend=dict(
            title=dict(text='Gene', font=legend_font), font=legend_font,
            orientation="h", yanchor="top", y=-0.15, xanchor="center", x=0.5,
            bgcolor='rgba(255,255,255,0.8)', bordercolor='#cccccc', borderwidth=1
        ),
        margin=dict(l=fixed_left_margin, r=fixed_right_margin, t=fixed_top_margin, b=fixed_bottom_margin),
        hovermode='closest',
        hoverlabel=dict(bgcolor="white", font_size=10, font_family="Arial, sans-serif")
    )
    fig.update_traces(
        hovertemplate="<b>Motif: %{hovertext}</b><br>Gene: %{fullData.name}<br>Repeat Count: %{x}<extra></extra>"
    )

    # --- Add Annotations ---
    stats_lines = ["<b>Summary Stats:</b>",
                   f"Occurrences: {stats['total_occurrences']:,}",
                   f"Unique Motifs: {stats['total_unique_motifs']:,}",
                   f"Unique Genes: {stats['total_unique_genes']:,}",
                   f"Total Repeats: {stats['total_repeat_count']:,}", "---",
                   "<b>Top 5 Genes (by Repeats):</b>"]
    top_genes = {k: v for k, v in stats.items() if k.startswith('repeats_in_')}
    if top_genes:
        for gene_key, count in top_genes.items():
            gene_name = gene_key.replace('repeats_in_', '')
            percentage = (count / stats['total_repeat_count']) * 100 if stats['total_repeat_count'] > 0 else 0
            stats_lines.append(f"{gene_name}: {count:,} ({percentage:.1f}%)")
    else:
        stats_lines.append("N/A")
    stats_text = "<br>".join(stats_lines)

    fig.add_annotation(
        xref="paper", yref="paper", x=1.01, y=0.95, text=stats_text,
        showarrow=False, font=annotation_font, align='left',
        bordercolor="#cccccc", borderwidth=1, borderpad=4,
        bgcolor="rgba(255, 255, 255, 0.8)", xanchor='left', yanchor='top'
    )

    fixed_signature_y = -0.25
    fig.add_annotation(
        xref="paper", yref="paper", x=0.98, y=fixed_signature_y,
        text="<i>Powered by Crossroad</i>", showarrow=False,
        font=signature_font, align='right', yanchor='top'
    )

    # --- Prepare Data for CSV Export (Pivot Table) ---
    logger.info(f"{plot_name}: Preparing data for CSV export...")
    try:
        export_df = df_proc.pivot_table(
            index='motif', columns='gene', values='repeat_count',
            aggfunc='sum', fill_value=0
        )
        export_df['Total'] = export_df.sum(axis=1)
        export_df = export_df.reset_index()
    except Exception as pivot_err:
        logger.warning(f"{plot_name}: Could not create pivot table for export - {pivot_err}. Exporting raw data instead.")
        export_df = df_proc[['motif', 'gene', 'repeat_count']].copy() # Fallback to processed data

    # --- Create Subdirectory and Save Outputs ---
    plot_specific_dir = os.path.join(output_dir, plot_name)
    try:
        os.makedirs(plot_specific_dir, exist_ok=True)
        logger.info(f"Ensured output directory exists: {plot_specific_dir}")
    except OSError as e:
        logger.error(f"Could not create plot directory {plot_specific_dir}: {e}")
        return # Cannot save if directory creation fails

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
             logger.error(f"Error saving {fmt.upper()} {plot_name}: {img_err}. Ensure 'kaleido' is installed: pip install -U kaleido")
             # Don't break saving other formats if one fails
        except Exception as img_save_err:
             logger.error(f"An unexpected error occurred during {fmt.upper()} saving for {plot_name}: {img_save_err}\n{traceback.format_exc()}")

    if not export_df.empty:
        try:
            csv_filename = f'{plot_name}_summary.csv' if 'Total' in export_df.columns else f'{plot_name}_data.csv'
            output_csv_path = os.path.join(plot_specific_dir, csv_filename)
            export_df.to_csv(output_csv_path, index=False, float_format='%.0f')
            logger.info(f"Export data for {plot_name} saved to: {output_csv_path}")
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
                     key_title = key.replace('_', ' ').replace('repeats in', 'Repeats in').title()
                     if isinstance(value, (int, np.integer)):
                         f.write(f"{key_title}: {value:,}\n")
                     elif isinstance(value, (float, np.floating)):
                          f.write(f"{key_title}: {value:,.2f}\n")
                     else:
                          f.write(f"{key_title}: {value}\n")
             logger.info(f"Summary statistics for {plot_name} saved to: {stats_path}")
         except Exception as stats_err:
             logger.error(f"Failed to save summary statistics for {plot_name}: {stats_err}\n{traceback.format_exc()}")

    logger.info(f"{plot_name}: Plot generation and saving complete.")