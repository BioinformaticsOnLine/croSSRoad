import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
import os
import logging
import traceback
import numpy as np
from math import log

logger = logging.getLogger(__name__)

def create_ssr_gene_genome_dot_plot(df_hssr, output_dir, reference_id=None):
    """
    Generates a dot plot showing SSR repeat counts for gene/motif combinations across genomes,
    with improved scaling and layout to handle datasets of various sizes.

    Args:
        df_hssr (pd.DataFrame): DataFrame containing the necessary columns
                                (genomeID, gene, motif, repeat).
        output_dir (str): The base directory to save the plot subdirectory.
        reference_id (str, optional): The ID of the reference genome to highlight. Defaults to None.
    """
    plot_name = "ssr_gene_genome_dot_plot"
    plot_subdir = os.path.join(output_dir, plot_name)
    html_file = os.path.join(plot_subdir, f"{plot_name}.html")
    png_file = os.path.join(plot_subdir, f"{plot_name}.png")
    csv_file = os.path.join(plot_subdir, f"{plot_name}_data.csv")

    try:
        os.makedirs(plot_subdir, exist_ok=True)
        logger.info(f"Created subdirectory for plot: {plot_subdir}")

        # --- Data Validation and Preparation ---
        required_cols = ['genomeID', 'gene', 'motif', 'repeat']
        if not all(col in df_hssr.columns for col in required_cols):
            missing = [col for col in required_cols if col not in df_hssr.columns]
            logger.error(f"Missing required columns in hssr_data for dot plot: {missing}. Skipping plot.")
            return

        # Drop rows with missing values in essential columns
        df_plot = df_hssr[required_cols].dropna().copy()

        # Convert 'repeat' to numeric, coercing errors
        df_plot['repeat'] = pd.to_numeric(df_plot['repeat'], errors='coerce')
        df_plot.dropna(subset=['repeat'], inplace=True)
        df_plot['repeat'] = df_plot['repeat'].astype(int)

        if df_plot.empty:
            logger.warning("No valid data remaining after cleaning for SSR Gene Genome Dot Plot. Skipping.")
            return

        # Get unique sorted lists for axes
        unique_genes = sorted(df_plot['gene'].unique())
        unique_genomes = sorted(df_plot['genomeID'].unique())
        unique_motifs = sorted(df_plot['motif'].unique())

        # Add reference flag
        df_plot['isReference'] = df_plot['genomeID'] == reference_id if reference_id else False

        # --- Plotting Setup ---
        # Define color scale for motifs - use a more distinct color palette if many motifs
        if len(unique_motifs) <= 10:
            color_scale = px.colors.qualitative.Plotly
        else:
            # Combine multiple palettes for more distinct colors
            color_scale = px.colors.qualitative.Plotly + px.colors.qualitative.Set1 + px.colors.qualitative.Dark2
        
        motif_color_map = {motif: color_scale[i % len(color_scale)] for i, motif in enumerate(unique_motifs)}
        reference_color = "#FF5722"  # Specific color for reference dots

        # --- Improved Size Scaling Logic ---
        min_repeat = df_plot['repeat'].min()
        max_repeat = df_plot['repeat'].max()
        
        # Set absolute limits for marker sizes to prevent too small or too large dots
        absolute_min_size = 4  # Never smaller than this
        absolute_max_size = 20  # Never larger than this
        
        # Dynamic size range based on data distribution
        size_range = max_repeat - min_repeat
        
        # Use logarithmic scaling for better visualization when range is large
        if size_range > 20:
            def scale_size(r):
                if max_repeat > min_repeat:
                    # Logarithmic scaling for better visualization of large ranges
                    log_min = log(min_repeat + 1, 10)  # Add 1 to handle possible zeros
                    log_max = log(max_repeat + 1, 10)
                    log_val = log(r + 1, 10)
                    
                    # Scale to our desired range
                    normalized = (log_val - log_min) / (log_max - log_min) if log_max > log_min else 0.5
                    return absolute_min_size + normalized * (absolute_max_size - absolute_min_size)
                else:
                    return (absolute_min_size + absolute_max_size) / 2
        else:
            # Linear scaling for smaller ranges
            def scale_size(r):
                if max_repeat > min_repeat:
                    normalized = (r - min_repeat) / size_range
                    return absolute_min_size + normalized * (absolute_max_size - absolute_min_size)
                else:
                    return (absolute_min_size + absolute_max_size) / 2

        # Apply size scaling to the dataframe
        df_plot['marker_size'] = df_plot['repeat'].apply(scale_size)

        # --- Create Plotly Figure ---
        fig = go.Figure()

        # --- Add Background Highlighting for Reference Genome ---
        if reference_id and reference_id in unique_genomes:
            fig.add_shape(
                type="rect", xref="paper", yref="y",
                x0=0, x1=1, y0=reference_id, y1=reference_id,
                fillcolor="#FFF9C4", opacity=0.5, layer="below", line_width=0,
                yanchor=reference_id, ysizemode='scaled', y0shift=-0.5, y1shift=0.5
            )
            fig.add_shape(
                type="line", xref="paper", yref="y",
                x0=0, x1=1, y0=reference_id, y1=reference_id,
                line=dict(color="#FF9800", width=2, dash="solid"), layer="below", opacity=0.7
            )

        # --- Add Main Scatter Trace for Dots ---
        fig.add_trace(go.Scatter(
            x=df_plot['gene'],
            y=df_plot['genomeID'],
            mode='markers',
            marker=dict(
                size=df_plot['marker_size'],
                color=df_plot.apply(lambda row: reference_color if row['isReference'] else motif_color_map.get(row['motif'], '#808080'), axis=1),
                opacity=df_plot['isReference'].apply(lambda is_ref: 1.0 if is_ref else 0.8),  # Increased opacity for better visibility
                line=dict(
                    color=df_plot['isReference'].apply(lambda is_ref: 'black' if is_ref else 'rgba(0,0,0,0)'),
                    width=df_plot['isReference'].apply(lambda is_ref: 2 if is_ref else 0)
                )
            ),
            customdata=df_plot[['genomeID', 'gene', 'motif', 'repeat', 'isReference']],
            hovertemplate=(
                "<b>Genome:</b> %{customdata[0]}%{customdata[4]|?<extra> (Reference)</extra>}<br>"
                "<b>Gene:</b> %{customdata[1]}<br>"
                "<b>Motif:</b> %{customdata[2]}<br>"
                "<b>Repeat Count:</b> %{customdata[3]}<extra></extra>"
            ),
            name='DataPoints',
            showlegend=False
        ))

        # --- Add Legend Items ---
        legend_traces_added = False

        # Motif Types Legend Items - Handle large number of motifs intelligently
        max_legend_motifs = min(15, len(unique_motifs))  # Cap the number of motifs in legend
        for i, motif in enumerate(unique_motifs[:max_legend_motifs]):
            fig.add_trace(go.Scatter(
                x=[None], y=[None], mode='markers',
                marker=dict(color=motif_color_map[motif], size=8, symbol='circle'),
                name=motif, legendgroup="1-Motifs", showlegend=True
            ))
            legend_traces_added = True
        
        # Add "Other Motifs" entry if we have too many
        if len(unique_motifs) > max_legend_motifs:
            fig.add_trace(go.Scatter(
                x=[None], y=[None], mode='markers',
                marker=dict(color='gray', size=8, symbol='circle'),
                name=f"+ {len(unique_motifs) - max_legend_motifs} more motifs", 
                legendgroup="1-Motifs", showlegend=True
            ))

        # Reference Genome Legend Item
        if reference_id:
            fig.add_trace(go.Scatter(
                x=[None], y=[None], mode='markers',
                marker=dict(color=reference_color, size=8, symbol='circle', line=dict(color='black', width=2)),
                name=f"Reference ({reference_id})", legendgroup="2-Reference", showlegend=True
            ))
            legend_traces_added = True

        # Size Legend Items - More intelligent selection of size examples
        # Use quantiles for better size representation
        size_examples = []
        if not df_plot.empty and max_repeat > min_repeat:
            quantiles = [0, 0.33, 0.66, 1.0]  # Use quartiles for better distribution
            size_examples = [int(np.quantile(df_plot['repeat'].unique(), q)) for q in quantiles]
            # Remove duplicates while preserving order
            size_examples = sorted(list(dict.fromkeys(size_examples)))
        
        if not size_examples or len(size_examples) <= 1:
            # Fallback if quantiles don't work
            if min_repeat != max_repeat:
                size_range = max_repeat - min_repeat
                step = max(1, size_range // 3)
                size_examples = list(range(min_repeat, max_repeat + 1, step))[:4]
            else:
                size_examples = [min_repeat]

        # Add a title trace for the size legend part
        fig.add_trace(go.Scatter(
            x=[None], y=[None], mode='markers', 
            marker_opacity=0, 
            name="Repeat Count:", 
            legendgroup="3-Size", 
            showlegend=True
        ))
        legend_traces_added = True

        for size_val in size_examples:
            scaled_size = scale_size(size_val)
            fig.add_trace(go.Scatter(
                x=[None], y=[None], mode='markers',
                marker=dict(color='grey', size=scaled_size, symbol='circle'),
                name=f"{size_val}", legendgroup="3-Size", showlegend=True
            ))

        # --- Intelligent Layout Configuration ---
        plot_title = "SSR Distribution Across Genomes and Genes"
        subtitle = f"Reference Genome: {reference_id}" if reference_id else "No Reference Genome Specified"

        # Define fonts
        title_font = dict(size=16, family="Arial, sans-serif", color='black')
        subtitle_font = dict(size=14, family="Arial, sans-serif", color='#FF5722' if reference_id else 'black')
        axis_label_font = dict(size=14, family="Arial, sans-serif", color='black')
        tick_font = dict(size=10)
        legend_font = dict(size=10)

        # Adaptive plot dimensions based on data size
        num_genomes = len(unique_genomes)
        num_genes = len(unique_genes)
        
        # Base dimensions
        base_width = 1000
        base_height = 800
        
        # Calculate adaptive height with minimum and maximum constraints
        # This formula scales better for both small and large datasets
        min_height = 500  # Minimum height in pixels
        max_height = 3000  # Maximum height in pixels
        height_per_genome = 30  # Pixels per genome (adjust as needed)
        
        # Dynamic height calculation with constraints
        plot_height = max(min_height, min(max_height, base_height + (num_genomes - 10) * height_per_genome))
        
        # Calculate adaptive width
        min_width = 800  # Minimum width in pixels
        max_width = 1800  # Maximum width in pixels
        width_per_gene = 100  # Pixels per gene (adjust as needed)
        
        # Dynamic width calculation with constraints
        plot_width = max(min_width, min(max_width, base_width + (num_genes - 8) * width_per_gene))
        
        # Adjust legend position based on dataset size
        legend_y_position = -0.15
        if num_genomes > 30:
            # For very large datasets, adjust legend positioning
            legend_y_position = -0.1
            
        # Responsive margin calculation
        margin_left = max(100, min(200, 80 + max([len(g) for g in unique_genomes]) * 6))
        margin_bottom = max(100, min(200, 80 + max([len(g) for g in unique_genes]) * 3))

        fig.update_layout(
            title=dict(
                text=f"{plot_title}<br><sup>{subtitle}</sup>", 
                x=0.5, 
                y=0.98, 
                xanchor='center', 
                yanchor='top',
                font=title_font
            ),
            xaxis_title=dict(text="Gene", font=axis_label_font),
            yaxis_title=dict(text="Genome ID", font=axis_label_font),
            xaxis=dict(
                type='category',
                categoryorder='array',
                categoryarray=unique_genes,
                tickangle=-45,
                tickfont=tick_font,
                automargin=True
            ),
            yaxis=dict(
                type='category',
                categoryorder='array',
                categoryarray=unique_genomes,
                tickfont=tick_font,
                automargin=True
            ),
            width=plot_width,
            height=plot_height,
            margin=dict(l=margin_left, r=50, t=100, b=margin_bottom),
            hovermode='closest',
            plot_bgcolor='white',
            xaxis_showgrid=False,
            yaxis_gridcolor='#e0e0e0',
            yaxis_gridwidth=1,
            legend=dict(
                traceorder='grouped',
                groupclick="toggleitem",
                orientation="h",
                yanchor="top", 
                y=legend_y_position,
                xanchor="center", 
                x=0.5,
                bgcolor='rgba(255,255,255,0.9)',
                bordercolor='grey',
                borderwidth=1,
                itemsizing='constant',
                font=legend_font,
            ) if legend_traces_added else dict(showlegend=False)
        )



        # --- Save Plot ---
        fig.write_html(html_file)
        logger.info(f"Saved HTML plot: {html_file}")

        # Try saving as PNG, handle missing Kaleido
        try:
            # Adaptive scale for PNG based on plot dimensions
            scale_factor = 2 if plot_width * plot_height < 2000000 else 1.5
            fig.write_image(png_file, scale=scale_factor, height=plot_height, width=plot_width)
            logger.info(f"Saved PNG plot: {png_file}")
        except ValueError as e:
            if "requires the kaleido" in str(e):
                logger.warning("Kaleido package not found. Skipping PNG export. Please install it (`pip install -U kaleido`)")
            else:
                logger.error(f"Failed to save PNG plot: {e}")
                logger.debug(traceback.format_exc())
        except Exception as e:
            logger.error(f"An unexpected error occurred while saving PNG plot: {e}")
            logger.debug(traceback.format_exc())

        # Save underlying data
        try:
            df_plot_output = df_plot[['genomeID', 'gene', 'motif', 'repeat']].sort_values(by=['genomeID', 'gene', 'motif'])
            df_plot_output.to_csv(csv_file, index=False)
            logger.info(f"SSR data used for plot saved to: {csv_file}")
        except Exception as csv_err:
            logger.error(f"Failed to save CSV data for {plot_name}: {csv_err}\n{traceback.format_exc()}")

    except Exception as e:
        logger.error(f"Failed to generate SSR Gene Genome Dot Plot: {e}")
        logger.debug(traceback.format_exc())

# Example usage (for testing purposes)
if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - [%(name)s] - %(message)s')

    # Create dummy data similar to the user's hssr_data.csv structure
    dummy_hssr_data = {
        'genomeID': ["EPI_ISL_13053218", "EPI_ISL_13053218", "EPI_ISL_13053218", "EPI_ISL_13053218", "EPI_ISL_13053218", "EPI_ISL_13053218", "EPI_ISL_13053218", "EPI_ISL_13053218", "EPI_ISL_13053218",
                     "EPI_ISL_13056233", "EPI_ISL_13056233", "EPI_ISL_13056233", "EPI_ISL_13056233", "EPI_ISL_13056233", "EPI_ISL_13056233", "EPI_ISL_13056233", "EPI_ISL_13056233", "EPI_ISL_13056233",
                     "EPI_ISL_13056234", "EPI_ISL_13056234", "EPI_ISL_13056234", "EPI_ISL_13056234", "EPI_ISL_13056234", "EPI_ISL_13056234", "EPI_ISL_13056234", "EPI_ISL_13056234", "EPI_ISL_13056234", "EPI_ISL_13056234",
                     "NC_063383.1", "NC_063383.1", "NC_063383.1", "NC_063383.1", "NC_063383.1", "NC_063383.1", "NC_063383.1"],
        'gene': ["OPG029", "OPG153", "OPG153", "OPG172", "OPG135", "OPG197", "OPG197", "OPG204", "OPG205",
                 "OPG029", "OPG153", "OPG153", "OPG172", "OPG135", "OPG197", "OPG197", "OPG204", "OPG205",
                 "OPG029", "OPG153", "OPG153", "OPG153", "OPG172", "OPG135", "OPG197", "OPG197", "OPG204", "OPG205",
                 "OPG029", "OPG204", "OPG153", "OPG172", "OPG135", "OPG197", "OPG205"],
        'motif': ["ATC", "ATC", "ATC", "ATG", "TATTAC", "GATACA", "GATACA", "GATGAA", "ATCTCA",
                  "ATC", "ATC", "ATC", "ATG", "TATTAC", "GATACA", "GATACA", "GATGAA", "ATCTCA",
                  "ATC", "ATC", "ATC", "ATC", "ATG", "TATTAC", "GATACA", "AGATAC", "GATGAA", "ATCTCA",
                  "TCA", "TCA", "TCA", "TCA", "TCA", "TCA", "TCA"],
        'repeat': [3, 18, 6, 5, 2, 23, 23, 2, 3,
                   3, 18, 6, 5, 2, 23, 23, 2, 3,
                   3, 23, 23, 5, 4, 2, 5, 2, 2, 3,
                   4, 4, 5, 3, 6, 4, 3]
    }
    df_dummy = pd.DataFrame(dummy_hssr_data)

    # Define test output directory and reference ID
    test_output_dir = "test_plots_output"
    test_ref_id = "NC_063383.1"

    print(f"Generating plot with dummy data. Output will be in: {os.path.join(test_output_dir, 'ssr_gene_genome_dot_plot')}")
    create_ssr_gene_genome_dot_plot(df_dummy, test_output_dir, reference_id=test_ref_id)
    print("Dummy plot generation complete.")