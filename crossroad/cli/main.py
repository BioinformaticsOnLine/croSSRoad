#!/usr/bin/env python3
import argparse
import logging
import os
import sys # Keep for sys.argv
# import threading # Removed for spinner
import time
# import itertools # Removed for spinner
import random # Re-added for quotes
from datetime import datetime
from rich.console import Console
from rich.panel import Panel
from rich.text import Text
# from rich.status import Status # Replaced by Progress
from rich.rule import Rule
from rich.progress import Progress, SpinnerColumn, TextColumn, TimeElapsedColumn # Added for Progress bar
from rich.logging import RichHandler # Although configured in logger.py, good practice to know it's used
from crossroad.core import m2, gc2, process_ssr_results
from crossroad.core.logger import setup_logging
# Defer importing plotting until needed to speed up startup
# from crossroad.core.plotting import generate_all_plots
# Removed terminaltexteffects imports

# Removed ANSI Color Codes - Use Rich markup instead

# Rich Logo & Welcome (Recreated using Rich Text)
# Note: Exact spacing might need slight adjustments when rendered
LOGO_TEXT = Text.assemble(
    ("    ┌─┐┬─┐┌─┐╔═╗╔═╗╦═╗┌─┐┌─┐┌┬┐\n", "bold cyan"),
    ("    │  ├┬┘│ │╚═╗╚═╗╠╦╝│ │├─┤ ││\n", "bold cyan"),
    ("    └─┘┴└─└─┘╚═╝╚═╝╩╚═└─┘┴ ┴─┴┘", "bold cyan"),
    ("v0.1.9\n\n", "white"),
    ("    Citation - croSSRoad: a tool to cross-compare SSRs across species and families\n", "dim white"),
    ("    License: Creative Commons License\n", "dim white"),
    ("    Authors: Preeti Agarwal, Pranjal Pruthi, Sahil Mahfooz, Jitendra Narayan\n", "dim white"),
    style="white" # Default style
)

WELCOME_PANEL = Panel(
    Text("WELCOME!", style="bold white", justify="center"),
    title="", # No title needed for the box itself
    border_style="bold green",
    padding=(0, 10) # Adjust padding for centering 'WELCOME!'
)



# Removed play_effect helper function

# --- Removed Custom Spinner Animation ---
# Use rich.status instead

# Quotes for Status Messages
QUOTES = [
   "Analyzing sequences...",
   "Comparing genomes...",
   "Unraveling SSR patterns...",
   "Crunching genomic data...",
   "Seeking microsatellite insights...",
   "Calculating repeat variations...",
   "Mapping genetic markers...",
   "Processing loci information...",
   "Identifying mutational hotspots...",
   "Decoding repetitive elements...",
   "Almost there...",
   "Just a moment...",
]

# Initialize Rich Console
console = Console()

# Symbols for Log Levels (Moved inside main in the next step)
# LOG_SYMBOLS = { ... } # Placeholder - will be defined inside main()

def main():
    start_time = time.time() # Record start time
    console.print(LOGO_TEXT) # Print the logo using Rich
    console.print(WELCOME_PANEL) # Print the welcome panel using Rich
    # Removed DEBUG print

    # Symbols for Log Levels (Defined inside main)
    LOG_SYMBOLS = {
        logging.INFO: "[bold green]🟢[/]",
        logging.WARNING: "[bold yellow]🟡[/]",
        logging.ERROR: "[bold red]🔴[/]",
        logging.CRITICAL: "[bold bright_red]💥[/]",
    }

    # Color codes are now defined globally
    # Removed DEBUG print
    parser = argparse.ArgumentParser(
        description="CrossRoad: A tool for analyzing SSRs in genomic data",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter # Show defaults in help
    )
    # Removed DEBUG print

    # Input file group (either individual files or input directory)
    # Input files: Either --input-dir OR --fasta must be provided. --categories is optional with --fasta.
    input_group = parser.add_argument_group('Input Files (provide --input-dir OR --fasta)')
    input_group.add_argument("--input-dir", "-i", dest="input_dir",
                             help="Directory containing input files: all_genome.fa, [genome_categories.tsv], [gene.bed]")
    input_group.add_argument("--fasta", "-fa", dest="fasta",
                             help="Input FASTA file (e.g., all_genome.fa). Use this OR --input-dir.")
    # Categories file is now optional, especially if --fasta is used directly
    parser.add_argument("--categories", "-cat", dest="categories",
                        help="Genome categories TSV file (optional if using --fasta, required within --input-dir if needed for full analysis)")
    # Gene bed is not mutually exclusive with fasta/cat, but is optional overall
    parser.add_argument("--gene-bed", "-bed", dest="gene_bed",
                        help="Gene BED file (optional, required for GC2/SSR processing)")

    # Other arguments
    parser.add_argument("--reference-id", "-ref", dest="reference_id",
                        help="Reference genome ID (optional)")
    parser.add_argument("--output-dir", "-o", dest="output_dir", default="jobOut", # Changed default
                        help="Base output directory for the job")
    parser.add_argument("--flanks", "-f", dest="flanks", action="store_true",
                        help="Process flanking regions")

    # PERF parameters
    parser.add_argument("--mono", type=int, default=12)
    parser.add_argument("--di", type=int, default=4)
    parser.add_argument("--tri", type=int, default=3)
    parser.add_argument("--tetra", type=int, default=3)
    parser.add_argument("--penta", type=int, default=3)
    parser.add_argument("--hexa", type=int, default=2)
    parser.add_argument("--min-len", "-minl", dest="min_len", type=int, default=156000,
                        help="Minimum genome length for filtering (default: 156000)")
    parser.add_argument("--max-len", "-maxl", dest="max_len", type=int, default=10000000,
                        help="Maximum genome length for filtering (default: 10000000)")
    parser.add_argument("--unfair", "-u", dest="unfair", type=int, default=50,
                        help="Maximum number of N's allowed per genome for MISA (default: 50)")
    parser.add_argument("--threads", "-t", dest="threads", type=int, default=50,
                        help="Number of threads for MISA (default: 50)")

    # Add new parameters for filtering
    parser.add_argument("--min-repeat-count", "-rc", dest="min_repeat_count", type=int, default=1,
                      help="Minimum repeat count for hotspot filtering (default: 1)")
    parser.add_argument("--min-genome-count", "-g", dest="min_genome_count", type=int, default=4,
                      help="Minimum genome count for hotspot filtering (default: 4)")
    parser.add_argument("--plots", "-p", action="store_true", default=False,
                        help="Enable plot generation (default: disabled)")
    parser.add_argument("--intrim-dir", dest="intrim_dir", default="intrim", # Renamed argument and dest
                        help="Directory for intermediate files") # Updated help text

    # Removed DEBUG print

    args = parser.parse_args() # Moved inside main()
    # Removed DEBUG print

    # Initialize terminal for effects if available
    # No terminal initialization needed now

    # --- Input File Validation and Path Resolution ---
    fasta_path = None
    cat_path = None
    gene_bed_path = None

    # --- Input File Validation and Path Resolution ---
    if not args.input_dir and not args.fasta:
        parser.error("Either --input-dir or --fasta must be provided.")
    if args.input_dir and args.fasta:
        parser.error("Use either --input-dir or --fasta, not both.")
    if args.input_dir and args.categories:
         print(f"Warning: --categories ('{args.categories}') ignored because --input-dir ('{args.input_dir}') was provided. Looking for 'genome_categories.tsv' within the input directory.")
         args.categories = None # Prioritize input_dir content

    if args.input_dir:
        # Using --input-dir
        if not os.path.isdir(args.input_dir):
            parser.error(f"Input directory not found: {args.input_dir}")

        fasta_path = os.path.join(args.input_dir, "all_genome.fa")
        cat_path_optional = os.path.join(args.input_dir, "genome_categories.tsv")
        gene_bed_path_optional = os.path.join(args.input_dir, "gene.bed")

        if not os.path.isfile(fasta_path):
            parser.error(f"Required file 'all_genome.fa' not found in {args.input_dir}")

        # Categories file is now optional within input_dir
        cat_path = cat_path_optional if os.path.isfile(cat_path_optional) else None
        if cat_path is None:
             print(f"Info: Optional file 'genome_categories.tsv' not found in {args.input_dir}. Proceeding in FASTA-only mode.")

        # Gene BED file handling
        if os.path.isfile(gene_bed_path_optional):
            gene_bed_path = gene_bed_path_optional
        elif args.gene_bed: # User specified -in AND -bed pointing elsewhere? Prioritize -in's content.
             print(f"Warning: --gene-bed ('{args.gene_bed}') ignored because --input-dir ('{args.input_dir}') was provided and 'gene.bed' was not found within it. Looking for 'gene.bed' in input directory only.")
             gene_bed_path = None # gene_bed_path remains None if not found in input_dir
        else:
             gene_bed_path = None

    else: # Using --fasta
        fasta_path = args.fasta
        if not os.path.isfile(fasta_path):
             parser.error(f"Input FASTA file not found: {fasta_path}")

        # Categories file is optional when using --fasta
        if args.categories:
            cat_path = args.categories
            if not os.path.isfile(cat_path):
                 parser.error(f"Input categories file not found: {cat_path}")
        else:
            cat_path = None
            print("Info: --categories not provided. Proceeding in FASTA-only mode.")

        # Gene BED file handling (can be provided alongside --fasta)
        gene_bed_path = args.gene_bed # Can be None if not provided
        if gene_bed_path and not os.path.isfile(gene_bed_path):
             parser.error(f"Input gene BED file not found: {gene_bed_path}")

    # --- End Input File Validation ---


    # Create job ID and setup directories using the specified output base directory
    job_id = f"job_{int(time.time() * 1000)}"
    # Use args.output_dir as the base for the job-specific directory
    job_dir = os.path.abspath(os.path.join(args.output_dir, job_id))
    # Removed DEBUG print
    # Setup logging
    # Removed DEBUG print
    logger = setup_logging(job_id, job_dir, args_namespace=args, console=console) # Pass parsed args
    # Removed DEBUG print

    # Prepare parameters for display in a Panel
    param_display_lines = []
    explicit_args_set = set(sys.argv)
    default_params_used = []

    # Identify explicitly set parameters and those using defaults
    for action in parser._actions:
        if isinstance(action, (argparse._HelpAction, argparse._ArgumentGroup)) or not hasattr(action, 'dest') or action.dest == 'help':
            continue
        dest = action.dest
        default_value = parser.get_default(dest)
        current_value = getattr(args, dest, None)
        was_set_explicitly = any(opt in explicit_args_set for opt in action.option_strings)
        is_core_input = dest in ['input_dir', 'fasta', 'categories', 'gene_bed']

        # Format explicitly set or core input parameters
        if (is_core_input and current_value is not None) or (was_set_explicitly and current_value != default_value):
            if isinstance(action, argparse._StoreTrueAction):
                value_str = "[bold green]Enabled[/]" if current_value else "[dim white]Disabled[/]"
            elif isinstance(action, argparse._StoreFalseAction):
                value_str = "[dim white]Disabled[/]" if current_value else "[bold green]Enabled[/]"
            elif current_value is None:
                 value_str = "[dim white]Not Set[/]"
            else:
                 value_str = str(current_value) # Ensure string
            param_display_lines.append(f"  [white]* {dest}:[/] {value_str}")
        # Collect parameters using default values
        elif not was_set_explicitly and not is_core_input:
            default_params_used.append(f"{dest}={default_value}")

    # Add line for defaults if any were used
    if not param_display_lines:
         param_display_lines.append("  [dim white](No specific parameters provided, using defaults)[/]")
    if default_params_used:
        param_display_lines.append(f"\n  [dim white]Defaults used for: {', '.join(default_params_used)}[/]")

    # Print parameters inside a Panel
    console.print(Panel("\n".join(param_display_lines),
                        title="[bold blue]Runtime Parameters[/]", # Changed color
                        border_style="blue", # Changed color
                        padding=(1, 2)))

    # Log the detected run mode
    if cat_path and gene_bed_path:
        logger.info("Running Mode: Full Analysis (FASTA + Categories + Gene BED)")
    elif cat_path:
        logger.info("Running Mode: Categories Analysis (FASTA + Categories, No Gene BED)")
    else:
        logger.info("Running Mode: FASTA-Only Analysis")

    try:
        # Create directory structure (Moved down)
        output_dir = os.path.join(job_dir, "output")
        main_dir = os.path.join(output_dir, "main")
        intrim_dir = os.path.join(output_dir, "intrim") # Renamed to intrim_dir and "intrim"

        os.makedirs(main_dir, exist_ok=True)
        os.makedirs(intrim_dir, exist_ok=True) # Use renamed variable

        # Use Rich Rule for stage separation
        console.print(Rule("[bold blue]Stage 1: Genome Quality Assessment", style="blue")) # Changed colors
        # Run M2 pipeline
        m2_args = argparse.Namespace(
            fasta=fasta_path, # Use resolved path
            cat=cat_path,     # Use resolved path
            out=main_dir,
            tmp=intrim_dir, # Pass using the core module's expected arg name 'tmp' and the renamed variable
            flanks=args.flanks,
            logger=logger,
            mono=args.mono,
            di=args.di,
            tri=args.tri,
            tetra=args.tetra,
            penta=args.penta,
            hexa=args.hexa,
            minLen=args.min_len,
            maxLen=args.max_len,
            unfair=args.unfair,
            thread=args.threads
        )
        # Use rich.progress for progress indication with elapsed time
        with Progress(
            SpinnerColumn(spinner_name="aesthetic"), # Use the aesthetic spinner
            TextColumn("[progress.description]{task.description}"),
            TimeElapsedColumn(), # Show elapsed time
            console=console, # Use the same console
            transient=True # Remove progress bar when done
        ) as progress:
            task_description = f"[bold green]{random.choice(QUOTES)} (Stage 1/M2)"
            task_id = progress.add_task(task_description, total=None) # Indeterminate task
            # Run the actual process
            merged_out, locicons_file, pattern_summary = m2.main(m2_args)
            # Optionally update description upon completion if needed, though transient=True removes it
            # progress.update(task_id, description=f"[bold green]Finished Stage 1/M2[/]")
        logger.info(f"{LOG_SYMBOLS[logging.INFO]} [bold green]Stage 1: Genome Quality Assessment Complete[/]") # Add symbol

        # Run GC2 pipeline if gene bed path is available (resolved from -in or -bed)
        if gene_bed_path:
            # Stage 2 Start
            console.print(Rule("[bold blue]Stage 2: SSR Detection (GC2)", style="blue")) # Changed colors
            gc2_args = argparse.Namespace(
                merged=merged_out,
                gene=gene_bed_path, # Use resolved path
                jobOut=main_dir,
                tmp=intrim_dir, # Pass using the core module's expected arg name 'tmp' and the renamed variable
                logger=logger
            )
            with Progress(
                SpinnerColumn(spinner_name="aesthetic"),
                TextColumn("[progress.description]{task.description}"),
                TimeElapsedColumn(),
                console=console,
                transient=True
            ) as progress:
                task_description = f"[bold green]{random.choice(QUOTES)} (Stage 2/GC2)"
                task_id = progress.add_task(task_description, total=None)
                ssr_combo = gc2.main(gc2_args)
            logger.info(f"{LOG_SYMBOLS[logging.INFO]} [bold green]Stage 2: SSR Detection Complete[/]") # Add symbol

            # Stage 3 Start
            console.print(Rule("[bold blue]Stage 3: Cross Compare SSRs", style="blue")) # Changed colors

            # Process SSR Results
            ssr_args = argparse.Namespace(
                ssrcombo=ssr_combo,
                jobOut=main_dir,
                tmp=intrim_dir, # Pass using the core module's expected arg name 'tmp' and the renamed variable
                logger=logger,
                reference=args.reference_id,
                min_repeat_count=args.min_repeat_count,
                min_genome_count=args.min_genome_count
            )

            with Progress(
                SpinnerColumn(spinner_name="aesthetic"),
                TextColumn("[progress.description]{task.description}"),
                TimeElapsedColumn(),
                console=console,
                transient=True
            ) as progress:
                task_description = f"[bold green]{random.choice(QUOTES)} (Stage 3/Compare)"
                task_id = progress.add_task(task_description, total=None)
                process_ssr_results.main(ssr_args)
            logger.info(f"{LOG_SYMBOLS[logging.INFO]} [bold green]Stage 3: Cross Compare SSRs Complete[/]") # Add symbol
        else:
            logger.warning(f"{LOG_SYMBOLS[logging.WARNING]} [yellow]Stages 2 & 3 Skipped: Gene BED file not provided[/]") # Add symbol

        # --- Generate Plots (Conditional) ---
        if args.plots:
            # Stage 4 Start
            console.print(Rule("[bold blue]Stage 4: Multi-Modal Data Visualization", style="blue")) # Changed colors
            try:
                logger.info("Starting post-processing: Generating plots...")
                plots_output_dir = os.path.join(output_dir, "plots")
                # main_dir is already defined
                with Progress(
                    SpinnerColumn(spinner_name="aesthetic"),
                    TextColumn("[progress.description]{task.description}"),
                    TimeElapsedColumn(),
                    console=console,
                    transient=True
                ) as progress:
                    task_description = f"[bold green]{random.choice(QUOTES)} (Stage 4/Plots)"
                    task_id = progress.add_task(task_description, total=None)
                    # Import plotting function only when needed
                    from crossroad.core.plotting import generate_all_plots
                    generate_all_plots(main_dir, intrim_dir, plots_output_dir, args.reference_id) # Pass intrim_dir
                logger.info(f"{LOG_SYMBOLS[logging.INFO]} [bold green]Finished generating plots.[/]") # Add symbol
                logger.info(f"{LOG_SYMBOLS[logging.INFO]} [bold green]Stage 4: Multi-Modal Data Visualization Complete[/]") # Add symbol
            except Exception as plot_err:
                logger.error(f"An error occurred during plot generation: {plot_err}", exc_info=True)
                # Keep previous failure message style for now, or adapt if needed
                # Note: The error itself is logged above with logger.error, which is already red.
                # This info message provides context, let's make it bold red too for consistency.
                logger.error(f"{LOG_SYMBOLS[logging.ERROR]} [bold red]Stage 4: Multi-Modal Data Visualization Failed[/]") # Add symbol, remove emoji
        else:
            logger.warning(f"{LOG_SYMBOLS[logging.WARNING]} [yellow]Stage 4: Multi-Modal Data Visualization Skipped (use --plots to enable)[/]") # Add symbol

        # Stage 5 Start/End
        console.print(Rule("[bold blue]Stage 5: Results Aggregation & Dissemination", style="blue")) # Changed colors
        logger.info(f"{LOG_SYMBOLS[logging.INFO]} [bold green]Analysis completed successfully[/]") # Add symbol
        logger.info(f"{LOG_SYMBOLS[logging.INFO]} [bold green]Stage 5: Results Aggregation & Dissemination Complete[/]") # Add symbol

        end_time = time.time() # Record end time
        duration = end_time - start_time
        minutes = int(duration // 60)
        seconds = duration % 60
        logger.info(f"\n[bold green]Total analysis time: {minutes}m {seconds:.2f}s[/]") # Log duration with Rich markup

        # Use a Panel for the final output message
        console.print(Panel(f"Results available in: [bold green]{output_dir}[/]",
                            title="[bold blue]Analysis Complete[/]", # Kept blue
                            border_style="blue")) # Kept blue

    except Exception as e:
        logger.error(f"Error: {str(e)}")
        # Optionally add a final failure effect
        # Note: The error itself is logged above with logger.error, which is already red.
        # This info message provides context, let's make it bold red too.
        logger.critical(f"{LOG_SYMBOLS[logging.CRITICAL]} [bold bright_red]ANALYSIS FAILED[/]") # Add symbol, use bright_red

        end_time = time.time() # Record end time even on failure
        duration = end_time - start_time
        minutes = int(duration // 60)
        seconds = duration % 60
        logger.info(f"\n[bold red]Analysis failed after: {minutes}m {seconds:.2f}s[/]") # Log duration with Rich markup on failure

        raise

# This block is now moved inside main()

if __name__ == "__main__":
    main()
