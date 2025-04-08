# crossroad/core/plots/temporal_faceted_scatter.py

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import plotly.io as pio
import numpy as np
import math
import os
import logging
import traceback

logger = logging.getLogger(__name__)

# --- Plotting Function ---

def create_temporal_faceted_scatter(df, output_dir):
    """
    Creates a publication-quality faceted scatter plot showing motif length vs. year,
    colored by motif, with separate plots (facet rows) for each gene,
    and includes genomeID in hover data. Saves outputs to a specific subdirectory.

    Args:
        df (pd.DataFrame): DataFrame containing 'motif', 'year', 'length_of_ssr', 'gene', 'genomeID'.
        output_dir (str): Base directory where plot-specific subdirectories will be created.

    Returns:
        None: Saves files directly.
    """
    plot_name = "temporal_ssr_distribution" # Changed name slightly for clarity
    logger.info(f"Processing data for {plot_name} plot...")

    # --- Data Processing ---
    required_cols = ['motif', 'year', 'length_of_ssr', 'gene', 'genomeID']
    if not all(col in df.columns for col in required_cols):
        missing = [col for col in required_cols if col not in df.columns]
        logger.error(f"{plot_name}: Missing required columns: {missing}")
        raise ValueError(f"Missing required columns: {missing}")

    df_proc = df[required_cols].dropna(subset=['motif', 'year', 'length_of_ssr']).copy()

    # Type conversions
    df_proc['motif'] = df_proc['motif'].astype(str)
    df_proc['year'] = pd.to_numeric(df_proc['year'], errors='coerce')
    df_proc['length_of_ssr'] = pd.to_numeric(df_proc['length_of_ssr'], errors='coerce')
    df_proc['gene'] = df_proc['gene'].fillna('N/A').astype(str)
    df_proc['genomeID'] = df_proc['genomeID'].fillna('N/A').astype(str)

    df_proc = df_proc.dropna(subset=['year', 'length_of_ssr'])
    df_proc['year'] = df_proc['year'].astype(int)

    if df_proc.empty:
        logger.warning(f"{plot_name}: DataFrame is empty after cleaning. Cannot generate plot.")
        return

    # --- Calculate Statistics ---
    stats = {
        'total_data_points': len(df_proc),
        'unique_motifs': df_proc['motif'].nunique(),
        'unique_genes': df_proc['gene'].nunique(),
        'unique_genomes': df_proc['genomeID'].nunique(),
        'year_range_min': df_proc['year'].min(),
        'year_range_max': df_proc['year'].max(),
        'ssr_length_min': df_proc['length_of_ssr'].min(),
        'ssr_length_max': df_proc['length_of_ssr'].max(),
        'average_ssr_length': df_proc['length_of_ssr'].mean()
    }
    logger.info(f"{plot_name}: Summary statistics calculated.")

    # --- Calculate Axis Ranges ---
    year_padding = max(1, (stats['year_range_max'] - stats['year_range_min']) * 0.05) if stats['year_range_max'] > stats['year_range_min'] else 1
    length_padding = max(1, (stats['ssr_length_max'] - stats['ssr_length_min']) * 0.05) if stats['ssr_length_max'] > stats['ssr_length_min'] else 1

    x_range = [
        math.floor(stats['year_range_min'] - year_padding),
        math.ceil(stats['year_range_max'] + year_padding)
    ]
    y_range = [
        max(0, math.floor(stats['ssr_length_min'] - length_padding)),
        math.ceil(stats['ssr_length_max'] + length_padding)
    ]

    # --- Create Plot ---
    logger.info(f"{plot_name}: Creating plot figure...")
    fig = px.scatter(
        df_proc.sort_values(['gene', 'year']),
        x="year",
        y="length_of_ssr",
        color="motif",
        facet_row="gene",
        hover_name="motif",
        hover_data={
            'year': True,
            'length_of_ssr': True,
            'motif': False,
            'gene': False,
            'genomeID': True
        },
        color_discrete_sequence=px.colors.qualitative.Set2,
        height=max(600, 100 + stats['unique_genes'] * 180),
        labels={
            "year": "Year",
            "length_of_ssr": "SSR Length",
            "motif": "Motif",
            "gene": "Gene",
            "genomeID": "Genome ID"
        }
    )

    # --- Style Configuration ---
    fonts = {
        'title': dict(size=20, family="Arial, sans-serif", color='#2f4f4f'),
        'axis': dict(size=14, family="Arial, sans-serif", color='#2f4f4f'),
        'tick': dict(size=12, family="Arial, sans-serif", color='#2f4f4f'),
        'legend': dict(size=12, family="Arial, sans-serif", color='#2f4f4f'),
        'annotation': dict(size=11, family="Arial, sans-serif", color='#2f4f4f')
    }

    fig.update_traces(
        marker=dict(
            size=8,
            opacity=0.75,
            line=dict(width=1, color='DarkSlateGrey')
        ),
        selector=dict(mode='markers')
    )

    fig.update_layout(
        title=dict(
            text='<b>Temporal Analysis of SSR Length Distribution by Gene</b>',
            font=fonts['title'], x=0.5, xanchor='center', y=0.98
        ),
        paper_bgcolor='white',
        plot_bgcolor='white',
        showlegend=True,
        legend=dict(
            title=dict(text='<b>Motif</b>', font=fonts['legend']),
            bgcolor='rgba(255,255,255,0.9)', bordercolor='#2f4f4f', borderwidth=1,
            orientation="v", yanchor="top", y=0.98, xanchor="left", x=1.02
        ),
        margin=dict(l=80, r=200, t=100, b=80)
    )

    fig.update_xaxes(
        mirror=True, range=x_range, showgrid=True, gridwidth=1, gridcolor='rgba(128,128,128,0.2)',
        zeroline=False, showline=True, linewidth=1, linecolor='#2f4f4f',
        tickfont=fonts['tick'], title_font=fonts['axis'], tickformat='d'
    )

    fig.update_yaxes(
        mirror=True, range=y_range, showgrid=True, gridwidth=1, gridcolor='rgba(128,128,128,0.2)',
        zeroline=False, showline=True, linewidth=1, linecolor='#2f4f4f',
        tickfont=fonts['tick'], title_font=fonts['axis']
    )

    fig.for_each_annotation(
        lambda a: a.update(
            text=f"<b>{a.text.split('=')[-1]}</b>",
            font=dict(size=14, color='#2f4f4f')
        ) if '=' in a.text else a
    )

    # Add statistics annotation
    stats_text = "<br>".join([
        "<b>Summary Statistics:</b>",
        f"Total Data Points: {stats['total_data_points']:,}",
        f"Unique Motifs: {stats['unique_motifs']}",
        f"Unique Genes: {stats['unique_genes']}",
        f"Unique Genomes: {stats['unique_genomes']}",
        f"Year Range: {stats['year_range_min']}-{stats['year_range_max']}",
        f"Length Range: {stats['ssr_length_min']:.0f}-{stats['ssr_length_max']:.0f}",
        f"Average Length: {stats['average_ssr_length']:.1f}"
    ])

    fig.add_annotation(
        xref="paper", yref="paper", x=1.01, y=0.7, text=stats_text,
        showarrow=False, font=fonts['annotation'], align='left',
        bgcolor='rgba(255,255,255,0.9)', bordercolor='#2f4f4f', borderwidth=1, borderpad=4,
        xanchor='left', yanchor='top'
    )

    # Add signature
    fig.add_annotation(
        xref="paper", yref="paper", x=0.99, y=0.01, text="<i>Powered by Crossroad</i>",
        showarrow=False, font=dict(size=10, color='#666666'), align='right',
        xanchor='right', yanchor='bottom'
    )

    # --- Prepare Export Data ---
    export_df = df_proc[['year', 'length_of_ssr', 'motif', 'gene', 'genomeID']].copy()
    export_df.rename(columns={
        'year': 'Year', 'length_of_ssr': 'SSR_Length', 'motif': 'Motif',
        'gene': 'Gene', 'genomeID': 'Genome_ID'
    }, inplace=True)
    export_df = export_df.sort_values(['Gene', 'Year', 'Motif', 'Genome_ID'])

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
            fig.write_image(img_path, scale=2 if fmt == "png" else None) # Use scale=2 for PNG
            logger.info(f"Saved {fmt.upper()} plot to {img_path}")
        except ValueError as img_err:
             logger.error(f"Error saving {fmt.upper()} {plot_name}: {img_err}. Ensure 'kaleido' is installed.")
        except Exception as img_save_err:
             logger.error(f"An unexpected error during {fmt.upper()} saving for {plot_name}: {img_save_err}\n{traceback.format_exc()}")

    # --- Save the export data to CSV ---
    if not export_df.empty:
        try:
            output_csv_path = os.path.join(plot_specific_dir, f'{plot_name}_data.csv')
            export_df.to_csv(output_csv_path, index=False)
            logger.info(f"Data for {plot_name} saved to: {output_csv_path}")
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
                     if isinstance(value, (int, np.integer)):
                         f.write(f"{key_title}: {value:,}\n")
                     elif isinstance(value, float):
                         f.write(f"{key_title}: {value:.2f}\n")
                     else:
                         f.write(f"{key_title}: {value}\n")
             logger.info(f"Summary statistics for {plot_name} saved to: {stats_path}")
         except Exception as stats_err:
             logger.error(f"Failed to save summary statistics for {plot_name}: {stats_err}\n{traceback.format_exc()}")

    logger.info(f"{plot_name}: Plot generation and saving complete.")