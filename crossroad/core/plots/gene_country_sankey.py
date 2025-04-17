# crossroad/core/plots/gene_country_sankey.py

import pandas as pd
import numpy as np
import plotly.graph_objects as go
import plotly.express as px
import plotly.io as pio
import plotly.colors
import os
import logging
import traceback

logger = logging.getLogger(__name__)

# Helper function (copied from the main plotting.py - potentially reuse from plotting module instead)
def color_to_plotly_rgba(color_input, alpha=1.0):
    """Converts various color inputs (hex, rgb tuple 0-1, rgb tuple 0-255) to a Plotly rgba string."""
    try:
        if isinstance(color_input, str) and color_input.startswith('#'):
            rgb_tuple = plotly.colors.hex_to_rgb(color_input)
            return f"rgba({rgb_tuple[0]}, {rgb_tuple[1]}, {rgb_tuple[2]}, {alpha})"
        elif isinstance(color_input, tuple) and len(color_input) == 3:
            # Assuming RGB 0-255 if numbers are large, else 0-1
            if any(c > 1 for c in color_input):
                 r, g, b = int(color_input[0]), int(color_input[1]), int(color_input[2])
            else: # Assume 0-1 scale
                 r, g, b = [int(c * 255) for c in color_input]
            return f"rgba({r}, {g}, {b}, {alpha})"
        else:
             logger.warning(f"Unknown color format encountered: {color_input}. Using default grey.")
             return f"rgba(204, 204, 204, {alpha})"
    except Exception as e:
        logger.error(f"Error converting color {color_input}: {e}. Using default grey.")
        return f"rgba(204, 204, 204, {alpha})"

# --- Plotting Function ---

def create_gene_country_sankey(df, output_dir):
    """
    Creates a publication-quality Sankey diagram visualizing the flow
    of genomes from genes to countries, with summary statistics and export options.
    Saves outputs to the specified directory.

    Args:
        df (pd.DataFrame): DataFrame containing 'gene', 'country', and 'genomeID' columns.
        output_dir (str): Base directory where plot-specific subdirectories will be created.

    Returns:
        None: Saves files directly.
    """
    plot_name = "gene_country_sankey"
    logger.info(f"Processing data for {plot_name}...")

    # --- Basic Validation and Type Conversion ---
    required_cols = ['gene', 'country', 'genomeID']
    if not all(col in df.columns for col in required_cols):
        missing = [col for col in required_cols if col not in df.columns]
        logger.error(f"{plot_name}: Missing required columns: {missing}")
        raise ValueError(f"Missing required columns: {missing}")

    df_proc = df[required_cols].dropna().copy()

    # Ensure correct types
    df_proc['gene'] = df_proc['gene'].astype(str)
    df_proc['country'] = df_proc['country'].astype(str)
    df_proc['genomeID'] = df_proc['genomeID'].astype(str)

    if df_proc.empty:
        logger.warning(f"{plot_name}: DataFrame is empty after cleaning/filtering. Cannot generate plot.")
        return

    # --- Data Aggregation ---
    logger.info(f"{plot_name}: Aggregating genome counts...")
    link_data = df_proc.groupby(['gene', 'country'])['genomeID'].nunique().reset_index()
    link_data = link_data[link_data['genomeID'] > 0]

    if link_data.empty:
        logger.warning(f"{plot_name}: No valid links found after aggregation. Cannot generate plot.")
        return

    # --- Prepare Nodes and Links for Sankey ---
    logger.info(f"{plot_name}: Preparing nodes and links...")
    unique_genes = sorted(link_data['gene'].unique())
    unique_countries = sorted(link_data['country'].unique())

    nodes = unique_genes + unique_countries
    node_map = {name: i for i, name in enumerate(nodes)}

    # Assign colors: Use Plotly palettes for both
    num_genes = len(unique_genes)
    num_countries = len(unique_countries)

    # Gene colors (using Plotly qualitative palette - hex strings)
    gene_colors_hex = px.colors.qualitative.Plotly[:num_genes]
    if len(gene_colors_hex) < num_genes: # Handle palette running out
        gene_colors_hex.extend(px.colors.qualitative.Pastel[:num_genes - len(gene_colors_hex)])
        if len(gene_colors_hex) < num_genes:
            gene_colors_hex.extend(['#CCCCCC'] * (num_genes - len(gene_colors_hex))) # Fallback grey

    # Country colors (using another Plotly palette - e.g., Pastel)
    country_colors_hex = px.colors.qualitative.Pastel[:num_countries]
    if len(country_colors_hex) < num_countries: # Handle palette running out
        country_colors_hex.extend(px.colors.qualitative.Plotly[:num_countries - len(country_colors_hex)])
        if len(country_colors_hex) < num_countries:
             country_colors_hex.extend(['#AAAAAA'] * (num_countries - len(country_colors_hex))) # Darker Fallback grey

    # Combine node colors (Plotly Sankey handles hex strings directly)
    node_colors = gene_colors_hex + country_colors_hex

    # Create source, target, value lists
    sources = [node_map[gene] for gene in link_data['gene']]
    targets = [node_map[country] for country in link_data['country']]
    values = link_data['genomeID'].tolist()

    # Link colors - color by source (gene) node with transparency
    # Use the helper function to add alpha to the hex colors
    link_colors_rgba = []
    for gene in link_data['gene']:
        gene_index = unique_genes.index(gene)
        hex_color = gene_colors_hex[gene_index % len(gene_colors_hex)]
        link_colors_rgba.append(color_to_plotly_rgba(hex_color, alpha=0.6))

    # --- Calculate Summary Statistics ---
    total_unique_genomes = df_proc['genomeID'].nunique()
    total_links = len(link_data)
    total_flow = sum(values)

    stats = {
        'total_genes': num_genes,
        'total_countries': num_countries,
        'total_unique_genomes': total_unique_genomes,
        'total_links_shown': total_links,
        'total_genome_flow': total_flow,
    }
    logger.info(f"{plot_name}: Summary statistics calculated.")

    # --- Create Sankey Figure ---
    logger.info(f"{plot_name}: Creating plot figure...")
    fig = go.Figure(data=[go.Sankey(
        arrangement='snap',
        node=dict(
            pad=15,
            thickness=20,
            line=dict(color="black", width=0.5),
            label=nodes,
            color=node_colors,
            hovertemplate='%{label}<extra></extra>'
        ),
        link=dict(
            source=sources,
            target=targets,
            value=values,
            color=link_colors_rgba,
            hovertemplate='%{source.label} → %{target.label}: %{value} genomes<extra></extra>'
        )
    )])

    # --- Customize Layout ---
    title_font = dict(size=16, family="Arial Black, Gadget, sans-serif", color='#333333')
    node_font = dict(size=10, family="Arial, sans-serif", color='#444444')
    annotation_font = dict(size=9, family="Arial, sans-serif", color='#666666')
    signature_font = dict(size=8, family="Arial, sans-serif", color='#666666')

    fixed_left_margin = 50
    fixed_right_margin = 150
    fixed_top_margin = 80
    fixed_bottom_margin = 80

    fig.update_layout(
        title=dict(
            text="Genome Distribution: Hotspot Gene → Country", # Main title only
            font=title_font,
            x=0.5,
            xanchor='center'
        ),
        # Add annotation for "Powered by Crossroad"
        annotations=[
            dict(
                text="<i>Powered by Crossroad</i>",
                x=0.5,
                y=1.0, # Position below title
                xref="paper",
                yref="paper",
                showarrow=False,
                font=dict(size=4), # Small font size
                xanchor="center",
                yanchor="top"
            )
        ],
        font=node_font,
        height=max(700, num_genes * 25, num_countries * 25),
        paper_bgcolor='white',
        plot_bgcolor='white',
        margin=dict(l=fixed_left_margin, r=fixed_right_margin, t=fixed_top_margin, b=fixed_bottom_margin),
    )

    # --- Add Annotations ---
    stats_lines = ["<b>Summary Stats:</b>",
                   f"Hotspot Genes: {stats['total_genes']:,}",
                   f"Countries: {stats['total_countries']:,}",
                   f"Unique Genomes: {stats['total_unique_genomes']:,}",
                   f"Links Shown: {stats['total_links_shown']:,}",
                   f"Total Flow: {stats['total_genome_flow']:,}"]
    stats_text = "<br>".join(stats_lines)

    fig.add_annotation(
        xref="paper", yref="paper",
        x=1.01, y=0.95,
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

    # Adjust signature position to be lower
    # Add signature - positioned relative to the bottom right corner of the paper
    # Signature removed, integrated into title

    # --- Prepare Data for CSV Export ---
    logger.info(f"{plot_name}: Preparing data for CSV export...")
    export_df = link_data.rename(columns={
        'gene': 'Source_Gene',
        'country': 'Target_Country',
        'genomeID': 'Genome_Count'
    })

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

    try:
        png_path = os.path.join(plot_specific_dir, f"{plot_name}.png")
        fig.write_image(png_path, scale=3)
        logger.info(f"Saved PNG plot to {png_path}")
    except ValueError as img_err:
         logger.error(f"Error saving PNG {plot_name}: {img_err}. Ensure 'kaleido' is installed: pip install -U kaleido")
    except Exception as img_save_err:
         logger.error(f"An unexpected error occurred during PNG saving for {plot_name}: {img_save_err}\n{traceback.format_exc()}")

    if not export_df.empty:
        try:
            output_csv_path = os.path.join(plot_specific_dir, f'{plot_name}_links.csv')
            export_df.to_csv(output_csv_path, index=False, float_format='%.0f')
            logger.info(f"Link data for {plot_name} saved to: {output_csv_path}")
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
                     if isinstance(value, (int, np.integer)):
                         f.write(f"{key_title}: {value:,}\n")
                     else:
                          f.write(f"{key_title}: {value}\n")
             logger.info(f"Summary statistics for {plot_name} saved to: {stats_path}")
         except Exception as stats_err:
             logger.error(f"Failed to save summary statistics for {plot_name}: {stats_err}\n{traceback.format_exc()}")

    logger.info(f"{plot_name}: Plot generation and saving complete.")