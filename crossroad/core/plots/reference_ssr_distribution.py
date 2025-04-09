# crossroad/core/plots/reference_ssr_distribution.py

import pandas as pd
import plotly.graph_objects as go
import plotly.express as px # For color palettes
import plotly.io as pio
import numpy as np
import os
import logging
import traceback

logger = logging.getLogger(__name__)

# --- Plotting Function ---

def create_scientific_ssr_plot(df, reference_id, output_dir):
    """
    Creates a publication-quality Plotly plot for Crossroad SSR analysis
    specifically for the reference genome, showing SSR counts by position.
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
    # Sort alphabetically by position for consistent bar order
    position_counts = position_counts.sort_values(by='Position').reset_index(drop=True)

    if position_counts.empty:
        logger.warning(f"{plot_name}: No SSR position data found for reference genome {reference_id} after filtering. Skipping plot.")
        return

    # Calculate statistics
    total_ssrs = int(position_counts['Count'].sum()) # Ensure integer
    max_count = int(position_counts['Count'].max())
    min_count = int(position_counts['Count'].min())

    stats = {
        'reference_genome': reference_id,
        'total_ssrs_in_ref': total_ssrs,
        'max_count_per_position': max_count,
        'min_count_per_position': min_count,
    }
    logger.info(f"{plot_name}: Summary statistics calculated.")

    # --- Create Plot ---
    logger.info(f"{plot_name}: Creating plot figure using Plotly...")

    # Generate colors using a Plotly qualitative palette
    colors = px.colors.qualitative.Set2
    num_colors = len(colors)

    # Create text labels for bars (Count + Percentage)
    bar_texts = []
    for i, count in enumerate(position_counts['Count']):
        percentage = (count / total_ssrs) * 100 if total_ssrs > 0 else 0
        bar_texts.append(f'{count:,} ({percentage:.1f}%)')

    fig = go.Figure()

    fig.add_trace(go.Bar(
        y=position_counts['Position'], # Categories on Y-axis for horizontal bars
        x=position_counts['Count'],     # Counts on X-axis
        orientation='h',
        marker_color=[colors[i % num_colors] for i in range(len(position_counts))],
        text=bar_texts,
        textposition='auto', # Let Plotly decide best position (inside/outside)
        textfont=dict(
            family="Arial, sans-serif",
            size=10,
            color='black' # Ensure text is readable on different bar colors
        ),
        insidetextanchor='end', # Align text to the end of the bar if inside
        outsidetextfont=dict(color='black'), # Explicitly set outside text color if needed
        hovertemplate=(
            "<b>Position:</b> %{y}<br>" +
            "<b>Count:</b> %{x:,}<br>" +
            "<extra></extra>"
        )
    ))

    # --- Customize Layout ---
    logger.info(f"{plot_name}: Customizing layout...")

    title_font = dict(size=16, family="Arial Black, Gadget, sans-serif", color='#333333')
    axis_label_font = dict(size=12, family="Arial, sans-serif", color='#444444')
    tick_font = dict(size=10, family="Arial, sans-serif", color='#555555')
    annotation_font = dict(size=9, family="Arial, sans-serif", color='#666666')
    signature_font = dict(size=8, family="Arial, sans-serif", color='#888888', style='italic')

    fixed_bottom_margin = 80
    fixed_left_margin = 120 # Adjust if position names are long
    fixed_right_margin = 180 # Space for stats box
    fixed_top_margin = 100

    fig.update_layout(
        title=dict(
            text=f'<b>Distribution of SSRs by Position in Reference Genome ({reference_id})</b>',
            font=title_font, x=0.5, xanchor='center', y=0.97, yanchor='top'
        ),
        height=max(500, len(position_counts) * 25 + fixed_top_margin + fixed_bottom_margin), # Dynamic height
        paper_bgcolor='white',
        plot_bgcolor='white',
        xaxis=dict(
            title=dict(text='Count of SSRs', font=axis_label_font),
            tickfont=tick_font, showline=True, linewidth=1, linecolor='black', mirror=True,
            gridcolor='#eef0f2', gridwidth=1, zeroline=False,
            tickformat=',d' # Format ticks as integers with commas
        ),
        yaxis=dict(
            title=dict(text='SSR Position', font=axis_label_font),
            tickfont=tick_font, showline=True, linewidth=1, linecolor='black', mirror=True,
            gridcolor='#eef0f2', gridwidth=1, zeroline=False,
            autorange="reversed" # Keep order same as input data
        ),
        bargap=0.2, # Add some gap between bars
        margin=dict(l=fixed_left_margin, r=fixed_right_margin, t=fixed_top_margin, b=fixed_bottom_margin),
        hovermode='closest'
    )

    # --- Add Annotations ---
    stats_lines = ["<b>Summary Stats:</b>",
                   f"Total SSRs: {stats['total_ssrs_in_ref']:,}",
                   f"Max Count: {stats['max_count_per_position']:,}",
                   f"Min Count: {stats['min_count_per_position']:,}"]
    stats_text = "<br>".join(stats_lines)

    fig.add_annotation(
        xref="paper", yref="paper",
        x=1.01, y=0.98, # Position in the top right margin
        text=stats_text,
        showarrow=False,
        font=annotation_font,
        align='left',
        bordercolor="#cccccc",
        borderwidth=1,
        borderpad=4,
        bgcolor="rgba(255, 255, 255, 0.8)",
        xanchor='left',
        yanchor='top'
    )

    # Signature annotation
    signature_y_position = - (fixed_bottom_margin / fig.layout.height) * 0.3 if fig.layout.height else -0.1
    signature_y_position = max(signature_y_position, -0.15) # Prevent it going too far down

    fig.add_annotation(
        xref="paper", yref="paper",
        x=0.98, y=signature_y_position,
        text="<i>Powered by Crossroad</i>",
        showarrow=False,
        font=signature_font,
        align='right',
        yanchor='top'
    )


    # --- Prepare Export Data ---
    export_df = position_counts.copy()

    # --- Create Subdirectory and Save Outputs ---
    plot_specific_dir = os.path.join(output_dir, plot_name)
    try:
        os.makedirs(plot_specific_dir, exist_ok=True)
        logger.info(f"Ensured output directory exists: {plot_specific_dir}")
    except OSError as e:
        logger.error(f"Could not create plot directory {plot_specific_dir}: {e}")
        return

    logger.info(f"{plot_name}: Saving plot outputs to {plot_specific_dir}...")
    base_filename = os.path.join(plot_specific_dir, f"{plot_name}_{reference_id.replace('/', '_').replace('.', '_')}") # Make filename safe

    # Save HTML
    try:
        html_path = f"{base_filename}.html"
        fig.write_html(html_path, include_plotlyjs='cdn')
        logger.info(f"Saved HTML plot to {html_path}")
    except Exception as html_err:
        logger.error(f"Failed to save HTML plot {plot_name}: {html_err}\\n{traceback.format_exc()}")

    # Save static images (requires kaleido)
    for fmt in ["png", "pdf", "svg"]: # Removed tiff as kaleido support can be inconsistent
        try:
            save_path = f"{base_filename}.{fmt}"
            fig.write_image(save_path, scale=3 if fmt == 'png' else None) # Higher scale for PNG
            logger.info(f"Saved {fmt.upper()} plot to {save_path}")
        except ValueError as img_err:
             # Specific error if kaleido is missing
             if 'kaleido' in str(img_err).lower():
                 logger.error(f"Error saving {fmt.upper()} {plot_name}: {img_err}. Kaleido engine is required. Please install: pip install -U kaleido")
             else:
                 logger.error(f"Error saving {fmt.upper()} {plot_name}: {img_err}")
        except Exception as img_save_err:
             logger.error(f"An unexpected error occurred during {fmt.upper()} saving for {plot_name}: {img_save_err}\\n{traceback.format_exc()}")

    # --- Save the export data to CSV ---
    if not export_df.empty:
        try:
            output_csv_path = f"{base_filename}_data.csv"
            export_df.to_csv(output_csv_path, index=False)
            logger.info(f"Data for {plot_name} saved to: {output_csv_path}")
        except Exception as csv_err:
            logger.error(f"Failed to save CSV data for {plot_name}: {csv_err}\\n{traceback.format_exc()}")
    else:
        logger.warning(f"{plot_name}: No export data generated.")

    # --- Save summary stats ---
    if stats:
         try:
             stats_path = f"{base_filename}_summary_statistics.txt"
             with open(stats_path, 'w') as f:
                 f.write(f"Summary Statistics for {plot_name} (Reference: {reference_id}):\n")
                 f.write("------------------------------------------------------------\n")
                 for key, value in stats.items():
                     key_title = key.replace('_', ' ').title()
                     if isinstance(value, (int, np.integer)):
                         f.write(f"{key_title}: {value:,}\\n")
                     else:
                         f.write(f"{key_title}: {value}\\n")
             logger.info(f"Summary statistics for {plot_name} saved to: {stats_path}")
         except Exception as stats_err:
             logger.error(f"Failed to save summary statistics for {plot_name}: {stats_err}\\n{traceback.format_exc()}")

    logger.info(f"{plot_name}: Plot generation and saving complete for reference {reference_id}.")