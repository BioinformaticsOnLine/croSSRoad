# crossroad/core/plots/relative_abundance_plot.py

import pandas as pd
import plotly.graph_objects as go
import plotly.io as pio
import numpy as np
import seaborn as sns # Keep seaborn for the palette
import os
import logging
import traceback

logger = logging.getLogger(__name__)

# --- Plotting Function ---

def create_relative_abundance_plot(df, output_dir):
    """
    Creates a publication-quality stacked horizontal bar chart showing relative
    abundance of SSR motifs per category, normalized by genome count and average length.
    Saves outputs to a specific subdirectory.

    Args:
        df (pd.DataFrame): DataFrame containing 'category', 'genomeID',
                           'length_of_motif', and 'length_genome' columns.
        output_dir (str): Base directory where plot-specific subdirectories will be created.

    Returns:
        None: Saves files directly.
    """
    plot_name = "relative_abundance"
    logger.info(f"Processing data for {plot_name} plot...")

    # --- Basic Validation and Type Conversion ---
    required_cols = ['category', 'genomeID', 'length_of_motif', 'length_genome']
    if not all(col in df.columns for col in required_cols):
        missing = [col for col in required_cols if col not in df.columns]
        logger.error(f"{plot_name}: Missing required columns: {missing}")
        raise ValueError(f"Missing required columns: {missing}")

    df_proc = df[required_cols].dropna().copy()

    # Ensure correct types
    df_proc['category'] = df_proc['category'].astype(str)
    df_proc['genomeID'] = df_proc['genomeID'].astype(str)
    df_proc['length_of_motif'] = pd.to_numeric(df_proc['length_of_motif'], errors='coerce').fillna(0).astype(int)
    df_proc['length_genome'] = pd.to_numeric(df_proc['length_genome'], errors='coerce')

    # Filter out rows where conversion failed or length is invalid
    df_proc = df_proc[df_proc['length_of_motif'] > 0]
    df_proc = df_proc.dropna(subset=['length_genome'])

    if df_proc.empty:
        logger.warning(f"{plot_name}: DataFrame is empty after cleaning/filtering. Cannot generate plot.")
        return

    # --- Calculate Key Metrics ---
    avg_genome_length_mb = df_proc['length_genome'].mean() / 1e6
    logger.info(f"{plot_name}: Overall average genome length: {avg_genome_length_mb:.4f} Mb")
    if avg_genome_length_mb <= 0:
       logger.warning(f"{plot_name}: Average genome length is zero or negative. Using 1 Mb for normalization.")
       avg_genome_length_mb = 1

    genomes_per_category = df_proc.groupby('category')['genomeID'].nunique()
    total_unique_genomes = df_proc['genomeID'].nunique() # Overall unique genomes
    logger.info(f"{plot_name}: Unique genomes per category calculated.")

    motif_counts = df_proc.groupby(['category', 'length_of_motif']).size().unstack(fill_value=0)
    logger.info(f"{plot_name}: Raw motif counts per category and length calculated.")

    # --- Prepare Data for Plotting ---
    sorted_categories = sorted(motif_counts.index.tolist())
    sorted_motif_lengths = sorted(motif_counts.columns.tolist())

    motif_type_names = {
        1: 'Monomer', 2: 'Dimer', 3: 'Trimer',
        4: 'Tetramer', 5: 'Pentamer', 6: 'Hexamer'
    }

    # Define the colorway using Seaborn "Accent"
    palette = sns.color_palette("Accent", n_colors=len(sorted_motif_lengths))
    hex_palette = palette.as_hex()
    # Map colors to the actual motif lengths present in the data
    color_map = {length: hex_palette[i % len(hex_palette)] for i, length in enumerate(sorted_motif_lengths)}


    plotly_series = []
    normalized_data_dict = {} # Store normalized data for stats and export

    logger.info(f"{plot_name}: Calculating normalized values (motifs per Mb per genome)...")
    for length in sorted_motif_lengths:
        series_name = motif_type_names.get(length, f'Length {length}')
        normalized_values = []
        category_values = {}

        for category in sorted_categories:
            count = motif_counts.loc[category, length] if length in motif_counts.columns and category in motif_counts.index else 0
            num_genomes = genomes_per_category.get(category, 0)

            if num_genomes > 0 and avg_genome_length_mb > 0:
                norm_value = count / avg_genome_length_mb / num_genomes
            else:
                norm_value = 0

            normalized_values.append(norm_value)
            category_values[category] = norm_value

        normalized_data_dict[series_name] = pd.Series(category_values)

        # Add series for the plot
        plotly_series.append(go.Bar(
            name=series_name,
            y=sorted_categories,
            x=normalized_values,
            orientation='h',
            marker_color=color_map.get(length), # Assign specific color
            hovertemplate=(
                f"<b>Category:</b> %{{y}}<br>" +
                f"<b>Motif:</b> {series_name}<br>" +
                f"<b>Norm. Count:</b> %{{x:.2f}} per Mb/Genome" + # Updated hover
                "<extra></extra>"
            )
        ))

    # --- Create Intermediate DataFrame for Stats and Export ---
    export_df_intermediate = pd.DataFrame(normalized_data_dict)
    export_df_intermediate.index.name = 'Category'

    # --- Calculate Summary Statistics ---
    total_normalized_motifs = export_df_intermediate.sum().sum()
    stats = {
        'total_categories': len(sorted_categories),
        'total_unique_genomes': total_unique_genomes,
        'avg_genome_length_mb': avg_genome_length_mb,
        'total_normalized_motifs': total_normalized_motifs,
        **{f'norm_motifs_{name.lower()}': series.sum() for name, series in normalized_data_dict.items()}
    }
    logger.info(f"{plot_name}: Summary statistics calculated.")

    # --- Create Plot ---
    logger.info(f"{plot_name}: Creating plot figure...")
    fig = go.Figure(data=plotly_series)

    # --- Customize Layout for Professional Look ---
    logger.info(f"{plot_name}: Customizing layout...")

    # Define fonts
    title_font = dict(size=16, family="Arial Black, Gadget, sans-serif", color='#333333')
    axis_label_font = dict(size=12, family="Arial, sans-serif", color='#444444')
    tick_font = dict(size=10, family="Arial, sans-serif", color='#555555')
    legend_font = dict(size=11, family="Arial, sans-serif", color='#444444')
    annotation_font = dict(size=9, family="Arial, sans-serif", color='#666666')
    signature_font = dict(size=8, family="Arial, sans-serif", color='#888888', style='italic')

    # Define margins (adjust as needed, especially left for long category names)
    fixed_left_margin = 150
    fixed_right_margin = 180 # For legend and stats box
    fixed_top_margin = 100
    fixed_bottom_margin = 80

    fig.update_layout(
        title=dict(
            text='<b>Relative Abundance of SSR Motifs (per Mb per Genome)</b>',
            font=title_font,
            x=0.5,
            xanchor='center',
            # Adjust title y based on margin and figure height
            y=1 - (fixed_top_margin / (max(600, len(sorted_categories) * 25 + fixed_top_margin + fixed_bottom_margin))) * 0.5,
            yanchor='top'
        ),
        height=max(600, len(sorted_categories) * 25 + fixed_top_margin + fixed_bottom_margin), # Adjust height based on categories + margins
        barmode='stack',
        paper_bgcolor='white',
        plot_bgcolor='white',
        xaxis=dict(
            title=dict(text='Normalized Motif Count (per Mb per Genome)', font=axis_label_font),
            tickfont=tick_font,
            showline=True, linewidth=1, linecolor='black', mirror=True, # Axis line
            showgrid=True, gridwidth=1, gridcolor='#eef0f2', # Lighter grid
            zeroline=False,
            tickformat=',.2f' # Format ticks
        ),
        yaxis=dict(
            title=dict(text='Category', font=axis_label_font),
            tickfont=tick_font,
            showline=True, linewidth=1, linecolor='black', mirror=True, # Axis line
            showgrid=False, # Typically no grid on category axis
            autorange="reversed" # Keep category order intuitive
        ),
        legend=dict(
            title=dict(text='Motif Length', font=legend_font),
            font=legend_font,
            traceorder='normal',
            orientation="v", # Vertical legend
            x=1.02, # Position to the right of plot area
            y=1,    # Align top
            xanchor='left',
            yanchor='top',
            bgcolor='rgba(255,255,255,0.8)', # Box styling
            bordercolor='#cccccc',
            borderwidth=1
        ),
        margin=dict(l=fixed_left_margin, r=fixed_right_margin, t=fixed_top_margin, b=fixed_bottom_margin),
        # colorway removed - applied per trace
    )

    # --- Add Annotations ---
    # Build stats text
    stats_lines = ["<b>Summary Stats:</b>",
                   f"Categories: {stats['total_categories']:,}",
                   f"Genomes: {stats['total_unique_genomes']:,}",
                   f"Avg Genome Size: {stats['avg_genome_length_mb']:.2f} Mb",
                   f"Total Norm. Motifs: {stats['total_normalized_motifs']:.2f}"]
    stats_lines.append("---")
    for length in sorted_motif_lengths:
         series_name = motif_type_names.get(length, f'Length {length}')
         norm_val = stats.get(f'norm_motifs_{series_name.lower()}', 0)
         if total_normalized_motifs > 0:
             percentage = (norm_val / total_normalized_motifs) * 100
             stats_lines.append(f"{series_name}: {norm_val:.2f} ({percentage:.1f}%)")
         else:
             stats_lines.append(f"{series_name}: {norm_val:.2f}")

    stats_text = "<br>".join(stats_lines)

    # Position stats box below legend in the right margin
    fig.add_annotation(
        xref="paper", yref="paper",
        x=1.01, y=0.6, # Adjust y position (below legend)
        text=stats_text,
        showarrow=False,
        font=annotation_font,
        align='left',
        bordercolor="#cccccc",
        borderwidth=1,
        borderpad=4,
        bgcolor="rgba(255, 255, 255, 0.8)",
        xanchor='left',
        yanchor='top' # Anchor to top-left of annotation box
    )

    # Signature annotation - Use a fixed negative y-coordinate
    signature_y_position = -0.18 # Adjust this value as needed for desired overlap/position

    fig.add_annotation(
        xref="paper", yref="paper",
        x=0.98,
        y=signature_y_position, # Use the fixed negative value
        text="<i>Powered by Crossroad</i>",
        showarrow=False,
        font=signature_font,
        align='right',
        yanchor='top'
    )


    # --- Prepare Data for CSV Export ---
    logger.info(f"{plot_name}: Preparing data for CSV export...")
    export_df = export_df_intermediate.copy() # Use the df created for stats
    export_df['Total'] = export_df.sum(axis=1)
    export_df = export_df.reset_index() # Category becomes a column

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
            export_df.to_csv(output_csv_path, index=False, float_format='%.4f') # Use more precision for normalized data
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
                     key_title = key.replace('_', ' ').replace('norm motifs', 'Norm. Motifs').title()
                     if isinstance(value, float):
                         f.write(f"{key_title}: {value:.2f}\n")
                     else:
                         f.write(f"{key_title}: {value:,}\n")
             logger.info(f"Summary statistics for {plot_name} saved to: {stats_path}")
         except Exception as stats_err:
             logger.error(f"Failed to save summary statistics for {plot_name}: {stats_err}\n{traceback.format_exc()}")

    logger.info(f"{plot_name}: Plot generation and saving complete.")