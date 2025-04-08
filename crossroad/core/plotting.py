# crossroad/core/plotting.py

import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
import random
import colorsys
import os
import logging
import traceback
import io
from PIL import Image # Requires Pillow: pip install Pillow

# Assume logger is configured elsewhere (e.g., in crossroad.core.logger)
# If not, basic configuration can be added here.
# logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- Helper Functions ---

def generate_distinct_colors(count):
    """Generates a list of distinct HSL colors."""
    colors = []
    golden_ratio_conjugate = 0.618033988749895
    hue = random.random() # Random start point
    saturation = 0.6
    lightness = 0.7

    for _ in range(count):
        hue += golden_ratio_conjugate
        hue %= 1
        # Convert HSL to RGB for Plotly (scales 0-255)
        rgb = tuple(int(i * 255) for i in colorsys.hls_to_rgb(hue, lightness, saturation))
        colors.append(f'rgb({rgb[0]}, {rgb[1]}, {rgb[2]})')
    return colors

def _save_plot(fig, base_filename, output_dir, df_data=None):
    """Helper to save plot outputs (HTML, PNG, CSV)."""
    try:
        html_path = os.path.join(output_dir, f"{base_filename}.html")
        fig.write_html(html_path)
        logger.info(f"Saved HTML plot to {html_path}")
    except Exception as e:
        logger.error(f"Failed to save HTML plot {base_filename}: {e}\n{traceback.format_exc()}")

    try:
        # Ensure kaleido and Pillow are installed for PNG export
        png_path = os.path.join(output_dir, f"{base_filename}.png")
        # Generate high-resolution PNG bytes using Plotly/Kaleido
        img_bytes = fig.to_image(format="png", scale=4) # scale=4 for pixel density

        # Use Pillow to save the bytes with correct DPI metadata
        img = Image.open(io.BytesIO(img_bytes))
        img.save(png_path, dpi=(300, 300)) # Set DPI metadata to 300x300
        logger.info(f"Saved PNG plot with 300 DPI metadata to {png_path}")
    except ValueError as ve:
         if "kaleido" in str(ve):
             logger.error(f"Failed to save PNG plot {base_filename}: kaleido engine not found. Please install kaleido (`pip install kaleido`).")
         elif "PIL" in str(ve) or "Pillow" in str(ve):
              logger.error(f"Failed to save PNG plot {base_filename}: Pillow library not found or import error. Please install Pillow (`pip install Pillow`).")
         else:
             logger.error(f"Failed to save PNG plot {base_filename}: {ve}\n{traceback.format_exc()}")
    except ImportError:
         logger.error(f"Failed to save PNG plot {base_filename}: Pillow library not found. Please install Pillow (`pip install Pillow`).")
    except Exception as e:
        logger.error(f"Failed to save PNG plot {base_filename}: {e}\n{traceback.format_exc()}")
    if df_data is not None:
        try:
            csv_path = os.path.join(output_dir, f"{base_filename}_data.csv")
            df_data.to_csv(csv_path, index=False)
            logger.info(f"Saved plot data to {csv_path}")
        except Exception as e:
            logger.error(f"Failed to save plot data CSV {base_filename}: {e}\n{traceback.format_exc()}")

# --- Plotting Functions ---

def plot_category_country_sankey(input_file_path, output_dir):
    """
    Generates Sankey diagram (Category -> Country) from mergedOut.tsv.
    Saves HTML, PNG, and CSV outputs.
    """
    plot_name = "category_country_sankey"
    logger.info(f"Attempting to generate plot: {plot_name} from {input_file_path}")
    required_cols = ['category', 'country', 'genomeID']

    try:
        if not os.path.exists(input_file_path):
            logger.error(f"{plot_name}: Input file not found: {input_file_path}")
            return

        df = pd.read_csv(input_file_path, sep='\t')

        if df.empty:
            logger.warning(f"{plot_name}: Input file is empty: {input_file_path}")
            return

        missing_cols = [col for col in required_cols if col not in df.columns]
        if missing_cols:
            logger.error(f"{plot_name}: Missing required columns in {input_file_path}: {missing_cols}")
            return

        logger.info(f"Successfully loaded data for {plot_name}. Processing...")

        # --- Data processing ---
        category_country_map = {}
        # Ensure correct types and handle potential NaN/None
        df_proc = df[required_cols].dropna().copy()
        df_proc['category'] = df_proc['category'].astype(str)
        df_proc['country'] = df_proc['country'].astype(str)
        df_proc['genomeID'] = df_proc['genomeID'].astype(str)

        grouped = df_proc.groupby(['category', 'country'])['genomeID'].apply(set)

        for (category, country), genome_set in grouped.items():
            key = f"{category}-{country}" # Use original types in key
            category_country_map[key] = genome_set

        unique_categories = sorted(df_proc['category'].unique())
        unique_countries = sorted(df_proc['country'].unique())

        if not unique_categories or not unique_countries:
             logger.warning(f"{plot_name}: No valid category/country pairs found after processing.")
             return

        category_colors = dict(zip(unique_categories, generate_distinct_colors(len(unique_categories))))
        country_colors = dict(zip(unique_countries, generate_distinct_colors(len(unique_countries))))

        nodes = unique_categories + unique_countries
        node_map = {name: i for i, name in enumerate(nodes)} # Faster lookup
        node_colors = [category_colors[cat] for cat in unique_categories] + [country_colors[country] for country in unique_countries]

        sources = []
        targets = []
        values = []
        link_colors = []
        link_data_rows = [] # For CSV export

        for key, genomes in category_country_map.items():
            category, country = key.split('-', 1)
            if category in node_map and country in node_map:
                 source_idx = node_map[category]
                 target_idx = node_map[country]
                 value = len(genomes)
                 sources.append(source_idx)
                 targets.append(target_idx)
                 values.append(value)
                 link_colors.append(category_colors.get(category, 'rgb(128,128,128)'))
                 link_data_rows.append({'Source_Category': category, 'Target_Country': country, 'Genome_Count': value})
            else:
                logger.warning(f"{plot_name}: Skipping link '{category}' -> '{country}' as one/both nodes not found in unique lists after processing.")

        if not sources:
             logger.warning(f"{plot_name}: No links generated for Sankey diagram.")
             return

        # --- Sankey figure creation ---
        fig = go.Figure(data=[go.Sankey(
            node = dict(
                pad = 15,
                thickness = 20,
                line = dict(color = "black", width = 0.5),
                label = nodes,
                color = node_colors,
                hovertemplate='%{label}<extra></extra>'
            ),
            link = dict(
                source = sources,
                target = targets,
                value = values,
                color = [f"rgba{color[3:-1]}, 0.6)" for color in link_colors], # Apply transparency
                hovertemplate='%{source.label} → %{target.label}: %{value} genomes<extra></extra>'
            )
        )])

        fig.update_layout(
            title_text="Genome Metadata Visualization (Category → Country)",
            font_size=10,
            height=max(700, len(nodes) * 15), # Adjust height based on nodes
            paper_bgcolor='white',
            plot_bgcolor='white'
        )

        # Prepare data for CSV export
        df_link_data = pd.DataFrame(link_data_rows)

        # Save outputs
        _save_plot(fig, plot_name, output_dir, df_link_data)
        logger.info(f"Successfully generated and saved plot: {plot_name}")

    except Exception as e:
        logger.error(f"Failed to generate plot {plot_name} from {input_file_path}: {e}\n{traceback.format_exc()}")


def plot_gene_country_sankey(input_file_path, output_dir):
    """
    Generates Sankey diagram (Hotspot Gene -> Country) from hssr_data.csv.
    Saves HTML, PNG, and CSV outputs.
    """
    plot_name = "gene_country_sankey"
    logger.info(f"Attempting to generate plot: {plot_name} from {input_file_path}")
    required_cols = ['gene', 'country', 'genomeID'] # Assuming 'gene' is the column name

    try:
        if not os.path.exists(input_file_path):
            logger.error(f"{plot_name}: Input file not found: {input_file_path}")
            return

        # Adjust reading based on actual file format if needed (e.g., separator)
        df = pd.read_csv(input_file_path)

        if df.empty:
            logger.warning(f"{plot_name}: Input file is empty: {input_file_path}")
            return

        missing_cols = [col for col in required_cols if col not in df.columns]
        if missing_cols:
            logger.error(f"{plot_name}: Missing required columns in {input_file_path}: {missing_cols}")
            return

        logger.info(f"Successfully loaded data for {plot_name}. Processing...")

        # --- Data processing ---
        gene_country_map = {}
        # Ensure correct types and handle potential NaN/None
        df_proc = df[required_cols].dropna().copy()
        df_proc['gene'] = df_proc['gene'].astype(str)
        df_proc['country'] = df_proc['country'].astype(str)
        df_proc['genomeID'] = df_proc['genomeID'].astype(str)

        grouped = df_proc.groupby(['gene', 'country'])['genomeID'].apply(set)

        for (gene, country), genome_set in grouped.items():
            key = f"{gene}-{country}"
            gene_country_map[key] = genome_set

        unique_genes = sorted(df_proc['gene'].unique())
        unique_countries = sorted(df_proc['country'].unique())

        if not unique_genes or not unique_countries:
             logger.warning(f"{plot_name}: No valid gene/country pairs found after processing.")
             return

        gene_colors = dict(zip(unique_genes, generate_distinct_colors(len(unique_genes))))
        country_colors = dict(zip(unique_countries, generate_distinct_colors(len(unique_countries))))

        nodes = unique_genes + unique_countries
        node_map = {name: i for i, name in enumerate(nodes)}
        node_colors = [gene_colors[gene] for gene in unique_genes] + [country_colors[country] for country in unique_countries]

        sources = []
        targets = []
        values = []
        link_colors = []
        link_data_rows = []

        for key, genomes in gene_country_map.items():
            gene, country = key.split('-', 1)
            if gene in node_map and country in node_map:
                 source_idx = node_map[gene]
                 target_idx = node_map[country]
                 value = len(genomes)
                 sources.append(source_idx)
                 targets.append(target_idx)
                 values.append(value)
                 link_colors.append(gene_colors.get(gene, 'rgb(128,128,128)'))
                 link_data_rows.append({'Source_Gene': gene, 'Target_Country': country, 'Genome_Count': value})
            else:
                logger.warning(f"{plot_name}: Skipping link '{gene}' -> '{country}' as one/both nodes not found in unique lists after processing.")

        if not sources:
             logger.warning(f"{plot_name}: No links generated for Sankey diagram.")
             return

        # --- Sankey figure creation ---
        fig = go.Figure(data=[go.Sankey(
            node = dict(
                pad = 15,
                thickness = 20,
                line = dict(color = "black", width = 0.5),
                label = nodes,
                color = node_colors,
                hovertemplate='%{label}<extra></extra>'
            ),
            link = dict(
                source = sources,
                target = targets,
                value = values,
                color = [f"rgba{color[3:-1]}, 0.6)" for color in link_colors],
                hovertemplate='%{source.label} → %{target.label}: %{value} genomes<extra></extra>'
            )
        )])

        fig.update_layout(
            title_text="Genome Metadata Visualization (Hotspot Gene → Country)",
            font_size=10,
            height=max(700, len(nodes) * 15),
            paper_bgcolor='white',
            plot_bgcolor='white'
        )

        # Prepare data for CSV export
        df_link_data = pd.DataFrame(link_data_rows)

        # Save outputs
        _save_plot(fig, plot_name, output_dir, df_link_data)
        logger.info(f"Successfully generated and saved plot: {plot_name}")

    except Exception as e:
        logger.error(f"Failed to generate plot {plot_name} from {input_file_path}: {e}\n{traceback.format_exc()}")


def plot_motif_repeat_count(input_file_path, output_dir):
    """
    Creates a stacked horizontal bar chart (Motif Repeat Count by Gene)
    from mutational_hotspot.csv. Saves HTML, PNG, and CSV outputs.
    """
    plot_name = "motif_repeat_count_by_gene"
    logger.info(f"Attempting to generate plot: {plot_name} from {input_file_path}")
    required_cols = ['motif', 'gene', 'repeat_count']

    try:
        if not os.path.exists(input_file_path):
            logger.error(f"{plot_name}: Input file not found: {input_file_path}")
            return

        df_hotspot = pd.read_csv(input_file_path)

        if df_hotspot.empty:
            logger.warning(f"{plot_name}: Input file is empty: {input_file_path}")
            return

        missing_cols = [col for col in required_cols if col not in df_hotspot.columns]
        if missing_cols:
            logger.error(f"{plot_name}: Missing required columns in {input_file_path}: {missing_cols}")
            return

        logger.info(f"Successfully loaded data for {plot_name}. Processing...")

        # Ensure correct data types
        df_hotspot['motif'] = df_hotspot['motif'].astype(str)
        df_hotspot['gene'] = df_hotspot['gene'].astype(str)
        # Handle potential non-numeric values before converting
        df_hotspot['repeat_count'] = pd.to_numeric(df_hotspot['repeat_count'], errors='coerce')
        df_hotspot.dropna(subset=['repeat_count'], inplace=True) # Drop rows where conversion failed
        df_hotspot['repeat_count'] = df_hotspot['repeat_count'].astype(int) # Or float if needed

        if df_hotspot.empty:
            logger.warning(f"{plot_name}: No valid numeric 'repeat_count' data found after cleaning.")
            return

        # --- Data Preparation ---
        # Use index for unique Y identifier if plotting each occurrence
        # Or aggregate first if needed (e.g., sum repeat_count per motif/gene)
        # The example code seems to plot each row individually using index.
        df_plot = df_hotspot.reset_index()

        unique_genes = sorted(df_plot['gene'].unique())
        if not unique_genes:
             logger.warning(f"{plot_name}: No unique genes found.")
             return

        # --- Create Plot using Plotly Express ---
        fig = px.bar(
            df_plot,
            x='repeat_count',
            y='index', # Use index for unique y-axis position per row
            color='gene',
            orientation='h',
            hover_name='motif',
            hover_data={'index': False, 'gene': True, 'repeat_count': True, 'motif': True}, # Ensure motif shows
            title='Motif Repeat Count by Gene',
            labels={'index': 'Motif Occurrence', 'repeat_count': 'Repeat Count', 'gene': 'Gene'},
            color_discrete_sequence=generate_distinct_colors(len(unique_genes)) # Use custom colors
            # color_discrete_sequence=px.colors.qualitative.Plotly # Or use built-in
        )

        # --- Customize Layout ---
        fig.update_layout(
            height=max(600, len(df_plot) * 15),
            font_size=10,
            paper_bgcolor='white',
            plot_bgcolor='white',
            yaxis={
                'tickvals': df_plot['index'],
                'ticktext': df_plot['motif'], # Show motif names as ticks
                'title': 'Motif'
            },
            xaxis={'title': 'Repeat Count'},
            barmode='stack',
            legend_title_text='Gene',
            hovermode='closest' # Or 'y unified'
        )

        # --- Prepare data for CSV export (Pivoted Summary) ---
        try:
            pivot_df = df_hotspot.pivot_table(
                index='motif',
                columns='gene',
                values='repeat_count',
                aggfunc='sum',
                fill_value=0
            )
            pivot_df['Total'] = pivot_df.sum(axis=1)
            df_summary_data = pivot_df.reset_index()
        except Exception as pivot_e:
            logger.warning(f"{plot_name}: Could not generate pivoted summary CSV: {pivot_e}. Saving raw data instead.")
            df_summary_data = df_hotspot # Fallback to raw data

        # Save outputs
        _save_plot(fig, plot_name, output_dir, df_summary_data)
        logger.info(f"Successfully generated and saved plot: {plot_name}")

    except Exception as e:
        logger.error(f"Failed to generate plot {plot_name} from {input_file_path}: {e}\n{traceback.format_exc()}")


# --- Main Orchestration Function ---

def generate_all_plots(job_output_main_dir, job_output_plots_dir):
    """
    Generates all defined plots based on files in job_output_main_dir
    and saves them to job_output_plots_dir.
    """
    logger.info(f"Starting plot generation for job output: {job_output_main_dir}")

    # Ensure the output directory exists
    try:
        os.makedirs(job_output_plots_dir, exist_ok=True)
        logger.info(f"Ensured plots output directory exists: {job_output_plots_dir}")
    except OSError as e:
        logger.error(f"Could not create plots output directory {job_output_plots_dir}: {e}")
        return # Cannot proceed without output directory

    # --- Call individual plot functions ---

    # 1. Category -> Country Sankey
    merged_out_path = os.path.join(job_output_main_dir, 'mergedOut.tsv')
    plot_category_country_sankey(merged_out_path, job_output_plots_dir)

    # 2. Gene -> Country Sankey
    hssr_data_path = os.path.join(job_output_main_dir, 'hssr_data.csv')
    plot_gene_country_sankey(hssr_data_path, job_output_plots_dir)

    # 3. Motif Repeat Count
    hotspot_path = os.path.join(job_output_main_dir, 'mutational_hotspot.csv')
    plot_motif_repeat_count(hotspot_path, job_output_plots_dir)

    # --- Add calls for other 9 plots here ---
    # Example:
    # another_data_path = os.path.join(job_output_main_dir, 'another_table.csv')
    # plot_another_visualization(another_data_path, job_output_plots_dir)

    logger.info(f"Finished plot generation for job output: {job_output_main_dir}")


# --- Example Usage (for testing purposes) ---
if __name__ == '__main__':
    # Configure basic logging for testing
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - [%(funcName)s] - %(message)s')

    # Create dummy data and directories for testing
    test_job_id = "job_test_123"
    base_dir = "." # Or specify a test directory
    test_main_dir = os.path.join(base_dir, "jobOut", test_job_id, "output", "main")
    test_plots_dir = os.path.join(base_dir, "jobOut", test_job_id, "output", "plots")
    os.makedirs(test_main_dir, exist_ok=True)
    os.makedirs(test_plots_dir, exist_ok=True)

    # Create dummy input files
    # mergedOut.tsv
    dummy_merged_data = {
        'category': ['A', 'A', 'B', 'B', 'A', 'C'],
        'country': ['USA', 'CAN', 'USA', 'MEX', 'CAN', 'USA'],
        'genomeID': [f'g{i}' for i in range(6)]
    }
    pd.DataFrame(dummy_merged_data).to_csv(os.path.join(test_main_dir, 'mergedOut.tsv'), sep='\t', index=False)

    # hssr_data.csv
    dummy_hssr_data = {
        'gene': ['Gene1', 'Gene1', 'Gene2', 'Gene3', 'Gene2', 'Gene1'],
        'country': ['UK', 'DE', 'UK', 'FR', 'DE', 'DE'],
        'genomeID': [f'h{i}' for i in range(6)]
    }
    pd.DataFrame(dummy_hssr_data).to_csv(os.path.join(test_main_dir, 'hssr_data.csv'), index=False)

    # mutational_hotspot.csv
    dummy_hotspot_data = {
        'motif': [f'm{i}' for i in range(8)],
        'gene': ['GeneX', 'GeneY', 'GeneX', 'GeneZ', 'GeneY', 'GeneX', 'GeneZ', 'GeneY'],
        'repeat_count': [10, 5, 8, 12, 3, 15, 7, 9]
    }
    pd.DataFrame(dummy_hotspot_data).to_csv(os.path.join(test_main_dir, 'mutational_hotspot.csv'), index=False)

    print(f"Created dummy data in: {test_main_dir}")
    print(f"Running plot generation. Output will be in: {test_plots_dir}")

    # Run the main plotting function
    generate_all_plots(test_main_dir, test_plots_dir)

    print("Dummy run complete. Check the output directory.")