# crossroad/core/plots/repeat_distribution_plot.py

import pandas as pd
import plotly.graph_objects as go
import plotly.io as pio
import numpy as np
import os
import logging
import traceback

logger = logging.getLogger(__name__)

# --- Plotting Function ---

def create_repeat_distribution_plot(df, output_dir):
    """
    Creates a publication-quality stacked horizontal bar chart showing the
    sum of SSR lengths per category, normalized by genome count and average length.
    Saves outputs to a specific subdirectory.

    Args:
        df (pd.DataFrame): DataFrame containing 'category', 'genomeID',
                           'length_of_motif', 'length_genome', and 'length_of_ssr'.
        output_dir (str): Base directory where plot-specific subdirectories will be created.

    Returns:
        None: Saves files directly.
    """
    plot_name = "repeat_distribution"
    logger.info(f"Processing data for {plot_name} plot...")

    # --- Basic Validation and Type Conversion ---
    required_cols = ['category', 'genomeID', 'length_of_motif', 'length_genome', 'length_of_ssr']
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
    df_proc['length_of_ssr'] = pd.to_numeric(df_proc['length_of_ssr'], errors='coerce') # Value to be summed

    # Filter out rows where conversion failed or length is invalid
    df_proc = df_proc[df_proc['length_of_motif'] > 0]
    df_proc = df_proc.dropna(subset=['length_genome', 'length_of_ssr'])

    if df_proc.empty:
        logger.warning(f"{plot_name}: DataFrame is empty after cleaning/filtering. Cannot generate plot.")
        return

    # --- Calculate Key Metrics ---
    avg_genome_length_mb = df_proc['length_genome'].mean() / 1e6
    logger.info(f"{plot_name}: Overall average genome length: {avg_genome_length_mb:.4f} Mb")
    if avg_genome_length_mb <= 0:
       logger.warning(f"{plot_name}: Average genome length is zero or negative. Using 1 Mb for normalization.")
       avg_genome_length_mb = 1 # Avoid division by zero

    genomes_per_category = df_proc.groupby('category')['genomeID'].nunique()
    total_unique_genomes = df_proc['genomeID'].nunique() # Added for stats
    logger.info(f"{plot_name}: Unique genomes per category calculated.")

    ssr_length_sums = df_proc.pivot_table(
        index='category',
        columns='length_of_motif',
        values='length_of_ssr',
        aggfunc='sum',
        fill_value=0
    )
    logger.info(f"{plot_name}: Sum of SSR lengths per category and motif length calculated.")

    # --- Prepare Data for Plotting ---
    sorted_categories = sorted(ssr_length_sums.index.tolist())
    sorted_motif_lengths = sorted(ssr_length_sums.columns.tolist())

    motif_type_names = {
        1: 'Monomer', 2: 'Dimer', 3: 'Trimer',
        4: 'Tetramer', 5: 'Pentamer', 6: 'Hexamer'
    }
    # Define the colorway here to ensure it matches the order of sorted_motif_lengths
    base_colors = [
        '#FFB6C1', '#87CEEB', '#98FB98',
        '#FFD700', '#FFA07A', '#AFEEEE'
    ]
    # Map colors to the actual motif lengths present in the data
    color_map = {length: base_colors[i % len(base_colors)] for i, length in enumerate(sorted_motif_lengths)}

    plotly_series = []
    normalized_data_dict = {} # Store normalized data for stats and export

    logger.info(f"{plot_name}: Calculating normalized values (sum of SSR length per Mb per genome)...")
    for length in sorted_motif_lengths:
        series_name = motif_type_names.get(length, f'Length {length}')
        normalized_values = []
        category_values = {}

        for category in sorted_categories:
            sum_length = ssr_length_sums.loc[category, length] if length in ssr_length_sums.columns and category in ssr_length_sums.index else 0
            num_genomes = genomes_per_category.get(category, 0)

            if num_genomes > 0 and avg_genome_length_mb > 0:
                norm_value = sum_length / avg_genome_length_mb / num_genomes
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
                f"<b>Norm. Sum Length:</b> %{{x:.2f}} per Mb/Genome" + # Updated hover
                "<extra></extra>"
            )
        ))

    # --- Calculate Summary Statistics from Normalized Data ---
    export_df_intermediate = pd.DataFrame(normalized_data_dict)
    total_norm_length = export_df_intermediate.sum().sum()
    norm_length_per_motif = export_df_intermediate.sum().to_dict() # Sum per motif type

    stats = {
        'total_categories': len(sorted_categories),
        'total_unique_genomes': total_unique_genomes, # Added stat
        'avg_genome_length_mb': avg_genome_length_mb,
        'total_normalized_ssr_length': total_norm_length,
        **{f'norm_length_{name.lower()}': val for name, val in norm_length_per_motif.items()}
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

    # Define margins - Adjust left/bottom/right as needed
    fixed_bottom_margin = 80
    fixed_left_margin = 150 # May need adjustment based on category name length
    fixed_right_margin = 180 # Increased for stats box and legend
    fixed_top_margin = 100

    fig.update_layout(
        title=dict(
            text='<b>Relative Distribution of SSR Lengths by Category</b>', # Updated title
            font=title_font,
            x=0.5,
            xanchor='center',
            y=1 - (fixed_top_margin / (max(600, len(sorted_categories) * 25 + fixed_top_margin + fixed_bottom_margin))) * 0.5, # Adjust title y based on margin
            yanchor='top'
        ),
        height=max(600, len(sorted_categories) * 25 + fixed_bottom_margin + fixed_top_margin), # Adjust height dynamically + margins
        barmode='stack',
        xaxis=dict(
            title=dict(text='Normalized Sum of SSR Length (per Mb per Genome)', font=axis_label_font), # Updated X axis title
            tickfont=tick_font,
            showline=True, linewidth=1, linecolor='black', mirror=True,
            gridcolor='#eef0f2', gridwidth=1, zeroline=False,
            tickformat=',.2f' # Format ticks to 2 decimal places
        ),
        yaxis=dict(
            title=dict(text='Category', font=axis_label_font),
            tickfont=tick_font,
            showline=True, linewidth=1, linecolor='black', mirror=True,
            gridcolor='#eef0f2', gridwidth=1, zeroline=False,
            autorange="reversed" # Keep categories top-to-bottom as in input
        ),
        legend=dict(
            title=dict(text='Motif Type', font=legend_font), # Added legend title
            font=legend_font,
            traceorder='normal', # Keep order same as data input
            bgcolor='rgba(255,255,255,0.8)', # Boxed legend
            bordercolor='#cccccc',
            borderwidth=1,
            orientation="v", yanchor="top", y=1, xanchor="left", x=1.02 # Vertical legend next to plot
        ),
        paper_bgcolor='white',
        plot_bgcolor='white',
        margin=dict(l=fixed_left_margin, r=fixed_right_margin, t=fixed_top_margin, b=fixed_bottom_margin),
        # colorway is handled by marker_color in traces now
    )

    # --- Add Annotations ---
    stats_lines = ["<b>Summary Stats:</b>",
                   f"Categories: {stats['total_categories']:,}",
                   f"Genomes: {stats['total_unique_genomes']:,}", # Added stat
                   f"Avg Genome Size: {stats['avg_genome_length_mb']:.2f} Mb",
                   f"Total Norm. Length: {stats['total_normalized_ssr_length']:.2f}"]
    stats_lines.append("---")
    for length in sorted_motif_lengths:
         series_name = motif_type_names.get(length, f'Length {length}')
         norm_val = stats.get(f'norm_length_{series_name.lower()}', 0)
         if total_norm_length > 0:
             percentage = (norm_val / total_norm_length) * 100
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
    export_df = export_df.reset_index().rename(columns={'index': 'Category'})

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
                     key_title = key.replace('_', ' ').replace('norm length', 'Norm. Length').title() # Improved title formatting
                     if isinstance(value, float):
                         f.write(f"{key_title}: {value:.2f}\n")
                     else:
                         f.write(f"{key_title}: {value:,}\n")
             logger.info(f"Summary statistics for {plot_name} saved to: {stats_path}")
         except Exception as stats_err:
             logger.error(f"Failed to save summary statistics for {plot_name}: {stats_err}\n{traceback.format_exc()}")

    logger.info(f"{plot_name}: Plot generation and saving complete.")