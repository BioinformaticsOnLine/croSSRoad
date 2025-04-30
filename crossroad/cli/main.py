#!/usr/bin/env python3
# Remove argparse import, keep others
# import argparse # Removed
import logging
import os
import sys # Keep for sys.argv
import time
import random # Re-added for quotes
from datetime import datetime
from pathlib import Path # Added for Path objects
from typing import Optional, Annotated # Added for type hinting
# Ensure argparse.Namespace is available if core modules require it
from argparse import Namespace

# Typer and Rich-related imports
import typer
import rich_click as click # Import rich-click
from rich.console import Console
from rich.panel import Panel
from rich.text import Text
from rich.rule import Rule
from rich.progress import Progress, SpinnerColumn, TextColumn, TimeElapsedColumn # Added for Progress bar
from rich.logging import RichHandler # Although configured in logger.py, good practice to know it's used

# --- Configuration for rich-click (Moved BEFORE app initialization) ---
click.rich_click.USE_RICH_MARKUP = True
click.rich_click.USE_MARKDOWN = True # Enable Markdown rendering in help
click.rich_click.SHOW_ARGUMENTS = True
click.rich_click.GROUP_ARGUMENTS_OPTIONS = True
click.rich_click.SHOW_METAVARS_COLUMN = True
click.rich_click.APPEND_METAVARS_HELP = True
click.rich_click.STYLE_ERRORS_SUGGESTION = "yellow italic"
click.rich_click.ERRORS_SUGGESTION = "Try running the command with '--help' for more information."
click.rich_click.ERRORS_EPILOGUE = "Contact support if the issue persists."
click.rich_click.STYLE_OPTION_GROUP_TITLE = "bold blue" # Style for option group titles
click.rich_click.STYLE_OPTIONS_PANEL_BORDER = "blue" # Use plain color name for border
click.rich_click.STYLE_COMMANDS_PANEL_BORDER = "blue" # Use plain color name for border
click.rich_click.OPTION_GROUPS = {
    "crossroad": [ # Group name matches the script name (usually)
        {
            "name": "Input Files (provide --input-dir OR --fasta)",
            "options": ["--input-dir", "--fasta", "--categories", "--gene-bed"],
        },
        {
            "name": "Analysis Parameters",
            "options": ["--reference-id", "--output-dir", "--flanks"],
        },
        {
            "name": "PERF SSR Detection Parameters",
            "options": ["--mono", "--di", "--tri", "--tetra", "--penta", "--hexa"],
        },
        {
            "name": "Filtering Parameters",
            "options": ["--min-len", "--max-len", "--unfair", "--min-repeat-count", "--min-genome-count"],
        },
        {
            "name": "Performance & Output",
            "options": ["--threads", "--plots", "--intrim-dir"],
        },
    ]
}
# --- End rich-click configuration ---

# Existing Crossroad imports
from crossroad.core import m2, gc2, process_ssr_results
from crossroad.core.logger import setup_logging
# Defer importing plotting until needed
# from crossroad.core.plotting import generate_all_plots
# import textwrap # No longer needed for argparse formatting

# Rich Logo & Welcome (Recreated using Rich Text)
# Note: Exact spacing might need slight adjustments when rendered
LOGO_TEXT = Text.assemble(
    ("    ┌─┐┬─┐┌─┐╔═╗╔═╗╦═╗┌─┐┌─┐┌┬┐\n", "bold cyan"),
    ("    │  ├┬┘│ │╚═╗╚═╗╠╦╝│ │├─┤ ││\n", "bold cyan"),
    ("    └─┘┴└─└─┘╚═╝╚═╝╩╚═└─┘┴ ┴─┴┘", "bold cyan"),    # Add the styled name below the art

    # Highlight SSR in the citation line
    ("    Citation - cro", "dim white"),
    ("SSR", "bold yellow"), # Highlighted SSR
    ("oad: a tool to cross-compare SSRs across species and families\n", "dim white"),
    ("    License: Creative Commons License\n", "dim white"),
    ("    Authors: Preeti Agarwal, Pranjal Pruthi, Jitendra Narayan, Sahil Mahfooz \n", "dim white"),
    style="white" # Default style
)

WELCOME_PANEL = Panel(
    Text("WELCOME!", style="bold white", justify="center"),
    title="", # No title needed for the box itself
    border_style="bold green",
    padding=(0, 10) # Adjust padding for centering 'WELCOME!'
)

# --- Example Command Panels ---
EXAMPLE_INTRO = Panel(
    Text(
        "Crossroad can be run using a directory-based input approach (-i) containing all required genomic data and a reference genome identifier (-ref). "
        "The command automatically generates output in a timestamped directory under \"jobOut/\", eliminating the need for explicit output specification. "
        "Optional visualization can be enabled with the plots flag (-p) and optionally can perform flanking analysis (-f)",
        justify="left"
    ),
    title="[bold blue]Usage Examples[/]",
    border_style="blue",
    padding=(1, 2)
)

EXAMPLE_BASIC = Panel(
    Text(
        "crossroad -i input_dir -ref REF_ID -p",
        style="bold green",
        justify="left"
    ),
    title="[bold blue]Basic Command[/]",
    subtitle="Executes the full 4-stage pipeline with SSR comparison and interactive visualizations.",
    border_style="green",
    padding=(1, 2)
)

SCENARIO_FASTA_ONLY = Panel(
    Text.assemble(
        ("Basic FASTA-only analysis\n", "bold yellow"),
        ("crossroad -fa genome.fasta -o output_dir\n\n", "bold green"),
        ("FASTA-only analysis with plots enabled\n", "bold yellow"),
        ("crossroad -fa genome.fasta -o output_dir -p", "bold green")
    ),
    title="[bold blue]Scenario 1: FASTA Only[/]",
    subtitle="Core Task: SSR detection and basic stats. Stage 1 executes and outputs reformatted.tsv. Stage 2 is skipped.",
    border_style="blue",
    padding=(1, 2)
)

SCENARIO_FASTA_CAT = Panel(
    Text.assemble(
        ("Basic FASTA + Categories analysis\n", "bold yellow"),
        ("crossroad -fa genome.fasta -cat categories.tsv -o output\n\n", "bold green"),
        ("With plots enabled\n", "bold yellow"),
        ("crossroad -fa genome.fasta -cat categories.tsv -o output -p\n\n", "bold green"),
        ("With flanking region analysis enabled\n", "bold yellow"),
        ("crossroad -fa genome.fasta -cat categories.tsv -o output -f\n\n", "bold green"),
        ("With plots AND flanking region analysis enabled\n", "bold yellow"),
        ("crossroad -fa genome.fasta -cat categories.tsv -o output -p -f\n\n", "bold green"),
        ("Note: Using -i input_dir instead of -fa genome.fasta -cat categories.tsv works if the directory\ncontains both all_genome.fa and genome_categories.tsv.", "dim white")
    ),
    title="[bold blue]Scenario 2: FASTA + Categories[/]",
    subtitle="Core Task: SSR detection + merging with category metadata. Stage 1 executes and outputs mergedOut.tsv. Stage 2 is skipped.",
    border_style="blue",
    padding=(1, 2)
)

SCENARIO_ADVANCED = Panel(
    Text.assemble(
        ("FASTA + Gene BED: Stage 1 & 2 execute. Reference comparison available with -ref flag.\n", "dim white"),
        ("crossroad -fa genome.fasta -bed gene.bed -ref MPOX -o output -p\n\n", "bold green"),
        ("All Components (FASTA + Categories + BED): Complete analysis pipeline\n", "dim white"),
        ("crossroad -i mpox_data -ref MPOX -o output -p -f", "bold green")
    ),
    title="[bold blue]Advanced Usage Scenarios[/]",
    subtitle="Complete pipeline execution with full visualization capabilities",
    border_style="blue",
    padding=(1, 2)
)

EXAMPLE_REAL = Panel(
    Text(
        "crossroad -i /Users/pranjalpruthi/Documents/GitHub/cr_test/mpox -p",
        style="bold green", 
        justify="left"
    ),
    title="[bold blue]Real-World Example[/]",
    subtitle="This example runs the full analysis on MPOX genomes with plot generation enabled",
    border_style="green",
    padding=(1, 2)
)

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

# --- Typer Application Setup ---
app = typer.Typer(
    cls=click.RichGroup, # Use RichGroup provided by rich-click
    name="crossroad",
    add_completion=True, # Corrected: Enable shell completion support
    help="""\
[bold cyan]cro[/][bold yellow]SSR[/][bold cyan]oad[/]: A comprehensive tool for analyzing SSRs in genomic data.

Use `crossroad --help` for detailed options.
""",
    epilog="""\
[bold green]Citation:[/] If you use croSSRoad in your research, please cite:
  cro[bold yellow]SSR[/]oad: a tool to cross-compare SSRs across species and families

[bold blue]Documentation:[/] https://github.com/BioinformaticsOnLine/croSSRoad
"""
)

    # Symbols for Log Levels (Defined inside main)
LOG_SYMBOLS = {
        logging.INFO: "[bold green]🟢[/]",
        logging.WARNING: "[bold yellow]🟡[/]",
        logging.ERROR: "[bold red]🔴[/]",
        logging.CRITICAL: "[bold bright_red]💥[/]",
    }

# --- Helper for Path Validation ---
def check_file_exists(path: Optional[Path]) -> Optional[Path]:
    """Callback to check if a file exists."""
    if path and not path.is_file():
        raise typer.BadParameter(f"File not found: {path}")
    return path

def check_dir_exists(path: Optional[Path]) -> Optional[Path]:
    """Callback to check if a directory exists."""
    if path and not path.is_dir():
        raise typer.BadParameter(f"Directory not found: {path}")
    return path

# --- Main Typer Command Function (Ensure short flags first) ---
@app.command(
    context_settings={"help_option_names": ["-h", "--help"]},
    help="Run the main croSSRoad analysis pipeline." # Short help for the command itself
)
def main(
    # --- Input Files Group ---
    input_dir: Annotated[Optional[Path], typer.Option(
        "-i", "--input-dir", # Ensure short flag first
        help="Directory containing: `all_genome.fa`, `[genome_categories.tsv]`, `[gene.bed]`. Exclusive with `--fasta`.",
        rich_help_panel="Input Files (provide --input-dir OR --fasta)",
        callback=check_dir_exists,
        show_default=False,
    )] = None,
    fasta: Annotated[Optional[Path], typer.Option(
        "-fa", "--fasta", # Ensure short flag first
        help="Input FASTA file (e.g., `all_genome.fa`). Alternative to `--input-dir`.",
        rich_help_panel="Input Files (provide --input-dir OR --fasta)",
        callback=check_file_exists,
        show_default=False,
    )] = None,
    categories: Annotated[Optional[Path], typer.Option(
        "-c", "--categories", # Changed from -cat to -c
        help="Genome categories TSV file. Optional if using `--fasta`. Ignored if `--input-dir` is used (looks for `genome_categories.tsv` inside).",
        rich_help_panel="Input Files (provide --input-dir OR --fasta)",
        callback=check_file_exists,
        show_default=False,
    )] = None,
    gene_bed: Annotated[Optional[Path], typer.Option(
        "-b", "--gene-bed", # Changed from -bed to -b
        help="Gene BED file for SSR-gene analysis. Optional. If `--input-dir` is used, looks for `gene.bed` inside.",
        rich_help_panel="Input Files (provide --input-dir OR --fasta)",
        callback=check_file_exists,
        show_default=False,
    )] = None,

    # --- Analysis Parameters Group ---
    reference_id: Annotated[Optional[str], typer.Option(
        "-ref", "--reference-id", # Short flag first
        help="Reference genome ID for comparative analysis. Optional parameter for reference-based comparisons.",
        rich_help_panel="Analysis Parameters",
        show_default=False,
    )] = None,
    output_dir: Annotated[Path, typer.Option(
        "-o", "--output-dir", # Short flag first
        help="Base output directory for the job.",
        rich_help_panel="Analysis Parameters",
        writable=True,
        file_okay=False,
        dir_okay=True,
        resolve_path=True,
    )] = Path("jobOut"),
    flanks: Annotated[bool, typer.Option(
        "-f", "--flanks", # Short flag first
        help="Process flanking regions.",
        rich_help_panel="Analysis Parameters",
    )] = False,

    # --- PERF Parameters Group (No short flags defined here) ---
    mono: Annotated[int, typer.Option("--mono", help="Mononucleotide repeat threshold.", rich_help_panel="PERF SSR Detection Parameters")] = 10,
    di: Annotated[int, typer.Option("--di", help="Dinucleotide repeat threshold.", rich_help_panel="PERF SSR Detection Parameters")] = 6,
    tri: Annotated[int, typer.Option("--tri", help="Trinucleotide repeat threshold.", rich_help_panel="PERF SSR Detection Parameters")] = 4,
    tetra: Annotated[int, typer.Option("--tetra", help="Tetranucleotide repeat threshold.", rich_help_panel="PERF SSR Detection Parameters")] = 3,
    penta: Annotated[int, typer.Option("--penta", help="Pentanucleotide repeat threshold.", rich_help_panel="PERF SSR Detection Parameters")] = 2,
    hexa: Annotated[int, typer.Option("--hexa", help="Hexanucleotide repeat threshold.", rich_help_panel="PERF SSR Detection Parameters")] = 2,

    # --- Filtering Parameters Group ---
    min_len: Annotated[int, typer.Option(
        "-l", "--min-len", 
        help="Minimum genome length for filtering.", 
        rich_help_panel="Filtering Parameters"
    )] = 1000,
    max_len: Annotated[int, typer.Option(
        "-L", "--max-len", 
        help="Maximum genome length for filtering.", 
        rich_help_panel="Filtering Parameters"
    )] = 10000000,
    unfair: Annotated[int, typer.Option(
        "-u", "--unfair", 
        help="Maximum number of N's allowed per genome for Crossroad analysis.", 
        rich_help_panel="Filtering Parameters"
    )] = 0,
    min_repeat_count: Annotated[int, typer.Option(
        "-rc", "--min-repeat-count", # Ensure short flag first
        help="Minimum repeat count for hotspot filtering (keeps records > this value).",
        rich_help_panel="Filtering Parameters"
    )] = 1,
    min_genome_count: Annotated[int, typer.Option(
        "-g", "--min-genome-count", # Ensure short flag first
        help="Minimum genome count for hotspot filtering (keeps records > this value).",
        rich_help_panel="Filtering Parameters"
    )] = 4,

    # --- Performance & Output Group ---
    threads: Annotated[int, typer.Option(
        "-t", "--threads", 
        help="Number of threads for Crossroad analysis.", 
        rich_help_panel="Performance & Output"
    )] = 50,
    plots: Annotated[bool, typer.Option(
        "-p", "--plots", # Short flag first
        help="Enable plot generation.", rich_help_panel="Performance & Output"
        )] = False,
    intrim_dir_name: Annotated[str, typer.Option( 
        "--intrim-dir", # No short flag
        help="Name for the intermediate files directory (within the main job output dir).",
        rich_help_panel="Performance & Output",
    )] = "intrim",
):
    """
    Main entry point for the croSSRoad analysis pipeline.

    Processes genomic data to identify and compare Simple Sequence Repeats (SSRs).
    Requires either a FASTA file (`--fasta`) or an input directory (`--input-dir`).
    """
    # Restructuring logic to separate file selection from pipeline execution
    console.print(LOGO_TEXT) # Print the logo using Rich
    console.print(WELCOME_PANEL) # Print the welcome panel using Rich
    
    # Display example commands when run with no arguments
    if len(sys.argv) <= 1:
        console.print(EXAMPLE_INTRO)
        console.print(EXAMPLE_BASIC)
        console.print(SCENARIO_FASTA_ONLY)
        console.print(SCENARIO_FASTA_CAT)
        console.print(SCENARIO_ADVANCED)
        console.print(EXAMPLE_REAL)
        return

    # --- Input File Validation and Path Resolution ---
    # Typer's `callback` handles basic existence checks.
    # Now handle mutual exclusivity and logic based on input mode.
    if not input_dir and not fasta:
        console.print("[bold red]Error:[/] Either `--input-dir` or `--fasta` must be provided.", markup=True)
        console.print("Use `crossroad --help` for usage details.", markup=True)
        raise typer.Exit(code=1)
    if input_dir and fasta:
        console.print("[bold red]Error:[/] Use either `--input-dir` or `--fasta`, not both.", markup=True)
        raise typer.Exit(code=1)

    fasta_path: Optional[Path] = None
    cat_path: Optional[Path] = None
    gene_bed_path_resolved: Optional[Path] = None # Renamed to avoid clash with Typer param

    # ----- Phase 1: File Selection Phase -----
    console.print(Rule("[bold blue]Phase 1: Input File Selection", style="blue"))
    
    if input_dir:
        console.print(f"[bold]Scanning directory:[/] {input_dir}", markup=True)
        
        # Find FASTA files in the input directory (*.fa, *.fasta, *.fna)
        fasta_files = list(input_dir.glob("*.fa")) + list(input_dir.glob("*.fasta")) + list(input_dir.glob("*.fna"))
        
        # Find TSV files for categories (*.tsv)
        tsv_files = list(input_dir.glob("*.tsv"))
        
        # Find BED files for genes (*.bed)
        bed_files = list(input_dir.glob("*.bed"))
        
        # Handle FASTA file (required)
        if len(fasta_files) == 0:
            console.print(f"[bold red]Error:[/] No FASTA files (*.fa, *.fasta, *.fna) found in {input_dir}", markup=True)
            raise typer.Exit(code=1)
        elif len(fasta_files) == 1:
            fasta_path = fasta_files[0]
            console.print(f"[bold green]Found FASTA file:[/] {fasta_path.name}", markup=True)
        else:
            # Multiple FASTA files - let user choose
            console.print(f"[bold yellow]Multiple FASTA files found in {input_dir}. Please select one:[/]", markup=True)
            for i, f in enumerate(fasta_files):
                console.print(f"  {i+1}. {f.name}")
            
            choice = -1
            while choice < 1 or choice > len(fasta_files):
                try:
                    choice = int(input(f"Enter selection (1-{len(fasta_files)}): "))
                except ValueError:
                    console.print("[bold red]Please enter a valid number.[/]", markup=True)
            
            fasta_path = fasta_files[choice-1]
            console.print(f"[bold green]Selected FASTA file:[/] {fasta_path.name}", markup=True)
        
        # Handle Categories TSV file (optional)
        if categories:  # User specified --categories explicitly
            console.print(f"[yellow]Warning:[/] `--categories` ('{categories}') ignored because `--input-dir` ('{input_dir}') was provided.", markup=True)
        
        if len(tsv_files) == 0:
            console.print(f"[dim]Info: No TSV files (*.tsv) found in {input_dir}. Proceeding without category data.[/]", markup=True)
            cat_path = None
        elif len(tsv_files) == 1:
            cat_path = tsv_files[0]
            console.print(f"[bold green]Found categories file:[/] {cat_path.name}", markup=True)
        else:
            # Multiple TSV files - let user choose
            console.print(f"[bold yellow]Multiple TSV files found in {input_dir}. Please select one for categories (or 0 for none):[/]", markup=True)
            console.print("  0. None (skip categories)")
            for i, f in enumerate(tsv_files):
                console.print(f"  {i+1}. {f.name}")
            
            choice = -1
            while choice < 0 or choice > len(tsv_files):
                try:
                    choice = int(input(f"Enter selection (0-{len(tsv_files)}): "))
                except ValueError:
                    console.print("[bold red]Please enter a valid number.[/]", markup=True)
            
            if choice == 0:
                cat_path = None
                console.print("[dim]No categories file selected.[/]", markup=True)
            else:
                cat_path = tsv_files[choice-1]
                console.print(f"[bold green]Selected categories file:[/] {cat_path.name}", markup=True)
        
        # Handle Gene BED file (optional)
        if gene_bed:  # User specified --gene-bed explicitly
            console.print(f"[yellow]Warning:[/] `--gene-bed` ('{gene_bed}') ignored because `--input-dir` ('{input_dir}') was provided.", markup=True)
        
        if len(bed_files) == 0:
            console.print(f"[dim]Info: No BED files (*.bed) found in {input_dir}. Proceeding without gene data.[/]", markup=True)
            gene_bed_path_resolved = None
        elif len(bed_files) == 1:
            gene_bed_path_resolved = bed_files[0]
            console.print(f"[bold green]Found gene BED file:[/] {gene_bed_path_resolved.name}", markup=True)
        else:
            # Multiple BED files - let user choose
            console.print(f"[bold yellow]Multiple BED files found in {input_dir}. Please select one for genes (or 0 for none):[/]", markup=True)
            console.print("  0. None (skip gene analysis)")
            for i, f in enumerate(bed_files):
                console.print(f"  {i+1}. {f.name}")
            
            choice = -1
            while choice < 0 or choice > len(bed_files):
                try:
                    choice = int(input(f"Enter selection (0-{len(bed_files)}): "))
                except ValueError:
                    console.print("[bold red]Please enter a valid number.[/]", markup=True)
            
            if choice == 0:
                gene_bed_path_resolved = None
                console.print("[dim]No gene BED file selected.[/]", markup=True)
            else:
                gene_bed_path_resolved = bed_files[choice-1]
                console.print(f"[bold green]Selected gene BED file:[/] {gene_bed_path_resolved.name}", markup=True)

    else: # Using --fasta
        fasta_path = fasta # Already validated by Typer callback
        console.print(f"[bold green]Using specified FASTA file:[/] {fasta_path.name}", markup=True)

        # Categories file is optional when using --fasta
        if categories:
            cat_path = categories # Already validated
            console.print(f"[bold green]Using specified categories file:[/] {cat_path.name}", markup=True)
        else:
            cat_path = None
            console.print("[dim]Info: `--categories` not provided. Proceeding without category data.[/]", markup=True)

        # Gene BED file handling (can be provided alongside --fasta)
        if gene_bed:
            gene_bed_path_resolved = gene_bed # Already validated
            console.print(f"[bold green]Using specified gene BED file:[/] {gene_bed_path_resolved.name}", markup=True)
        else:
            gene_bed_path_resolved = None
            console.print("[dim]Info: `--gene-bed` not provided. Proceeding without gene data.[/]", markup=True)

    # ----- Phase 2: Confirmation -----
    console.print("")
    console.print(Rule("[bold blue]Phase 2: Confirmation", style="blue"))
    
    # Summarize selected files
    console.print(Panel(
        "\n".join([
            f"FASTA file: [bold]{fasta_path.name}[/]" if fasta_path else "FASTA file: [red]None[/]",
            f"Categories file: [bold]{cat_path.name}[/]" if cat_path else "Categories file: [dim]None[/]",
            f"Gene BED file: [bold]{gene_bed_path_resolved.name}[/]" if gene_bed_path_resolved else "Gene BED file: [dim]None[/]"
        ]),
        title="[bold]Selected Files[/]",
        border_style="green"
    ), markup=True)
    
    # Ask for confirmation
    if input_dir:  # Only ask for confirmation when using input_dir mode
        console.print("\nReady to proceed with analysis using the selected files.", markup=True)
        proceed = input("Press Enter to continue, or 'q' to quit: ").strip().lower()
        if proceed == 'q':
            console.print("[yellow]Analysis cancelled by user.[/]", markup=True)
            raise typer.Exit(code=0)
    
    # ----- Phase 3: Analysis Execution -----
    console.print("")
    console.print(Rule("[bold blue]Phase 3: Analysis Execution", style="blue"))
    
    # Create job ID and setup directories using the specified output base directory
    start_time = time.time() # Record start time
    job_id = f"job_{int(time.time() * 1000)}"
    # output_dir is already an absolute Path object from Typer
    job_dir = output_dir / job_id # Use Path object directly

    # Setup logging - passing the Typer parameters
    # Create a simple namespace or dict for logger compatibility if needed
    log_args = {
        'input_dir': input_dir, 'fasta': fasta, 'categories': categories,
        'gene_bed': gene_bed, 'reference_id': reference_id, 'output_dir': output_dir,
        'flanks': flanks, 'mono': mono, 'di': di, 'tri': tri, 'tetra': tetra,
        'penta': penta, 'hexa': hexa, 'min_len': min_len, 'max_len': max_len,
        'unfair': unfair, 'threads': threads, 'min_repeat_count': min_repeat_count,
        'min_genome_count': min_genome_count, 'plots': plots, 'intrim_dir': intrim_dir_name
    }
    # The setup_logging function might need adjustment if it strictly requires an argparse.Namespace
    # Let's assume it can handle a dict for now, or pass a Namespace if needed.
    logger = setup_logging(job_id, str(job_dir), args_namespace=Namespace(**log_args), console=console)

    # --- Display Parameters ---\n    param_display_lines = []
    # Iterate through Typer parameters (can use ctx.params if needed, but direct vars are fine)
    # Note: This is simpler than argparse introspection
    param_display_lines = []
    param_display_lines.append(f"  [white]* input_mode:[/]: {'--input-dir' if input_dir else '--fasta'}")
    if input_dir: param_display_lines.append(f"  [white]* input_dir:[/]: {input_dir}")
    if fasta: param_display_lines.append(f"  [white]* fasta:[/]: {fasta}")
    if cat_path: param_display_lines.append(f"  [white]* categories:[/]: {cat_path}")
    else: param_display_lines.append(f"  [white]* categories:[/]: [dim]Not provided/found[/]")
    if gene_bed_path_resolved: param_display_lines.append(f"  [white]* gene_bed:[/]: {gene_bed_path_resolved}")
    else: param_display_lines.append(f"  [white]* gene_bed:[/]: [dim]Not provided/found[/]")
    if reference_id: param_display_lines.append(f"  [white]* reference_id:[/]: {reference_id}")
    param_display_lines.append(f"  [white]* output_dir:[/]: {job_dir}") # Show the full job dir
    param_display_lines.append(f"  [white]* flanks:[/]: {'[bold green]Enabled[/]' if flanks else '[dim white]Disabled[/]'}")
    param_display_lines.append(f"  [white]* plots:[/]: {'[bold green]Enabled[/]' if plots else '[dim white]Disabled[/]'}")
    param_display_lines.append(f"  [white]* intrim_dir_name:[/]: {intrim_dir_name}")

    # Display PERF params
    param_display_lines.append("\n  [underline]PERF Parameters:[/]")
    param_display_lines.append(f"    mono={mono}, di={di}, tri={tri}, tetra={tetra}, penta={penta}, hexa={hexa}")
    # Display Filtering params
    param_display_lines.append("\n  [underline]Filtering Parameters:[/]")
    param_display_lines.append(f"    min_len={min_len}, max_len={max_len}, unfair={unfair}")
    param_display_lines.append(f"    min_repeat_count={min_repeat_count}, min_genome_count={min_genome_count}")
    # Display Performance params
    param_display_lines.append("\n  [underline]Performance:[/]")
    param_display_lines.append(f"    threads={threads}")


    # Print parameters inside a Panel
    console.print(Panel("\n".join(param_display_lines),
                        title="[bold blue]Runtime Parameters[/]",
                        border_style="blue",
                        padding=(1, 2)))

    # Log the detected run mode using a Panel for visibility
    run_mode_message = ""
    if cat_path and gene_bed_path_resolved:
        # logger.info("Running Mode: Full Analysis (FASTA + Categories + Gene BED)")
        run_mode_message = "Running Mode: [bold]Full Analysis[/] (FASTA + Categories + Gene BED)"
    elif cat_path:
        # logger.info("Running Mode: Categories Analysis (FASTA + Categories, No Gene BED)")
        run_mode_message = "Running Mode: [bold]Categories Analysis[/] (FASTA + Categories, No Gene BED)"
    elif gene_bed_path_resolved:
        # logger.info("Running Mode: Gene-Only Analysis (FASTA + Gene BED, No Categories)")
        run_mode_message = "Running Mode: [bold]Gene-Only Analysis[/] (FASTA + Gene BED, No Categories)"
    else:
        # logger.info("Running Mode: FASTA-Only Analysis")
        run_mode_message = "Running Mode: [bold]FASTA-Only Analysis[/]"
    console.print(Panel(run_mode_message, title="[dim]Run Mode[/]", border_style="dim", padding=(0,1)))

    try:
        # Create directory structure
        # job_dir is already an absolute Path object
        main_out_dir = job_dir / "output" / "main" # Use Path operators
        intrim_out_dir = job_dir / "output" / intrim_dir_name # Use the provided name

        main_out_dir.mkdir(parents=True, exist_ok=True)
        intrim_out_dir.mkdir(parents=True, exist_ok=True)

        # Use Rich Rule for stage separation
        console.print(Rule("[bold blue]Stage 1: Genome Quality Assessment & SSR Detection", style="blue"))

        # --- Run M2 pipeline ---
        # Note: m2.main expects string paths and a Namespace/dict. Convert Paths to strings.
        # Ensure core modules can handle Namespace or adapt if they expect dicts
        m2_args_ns = Namespace(
            fasta=str(fasta_path),
            cat=str(cat_path) if cat_path else None,
            out=str(main_out_dir),
            tmp=str(intrim_out_dir), # Pass the intermediate dir path
            flanks=flanks,
            logger=logger,
            mono=mono, di=di, tri=tri, tetra=tetra, penta=penta, hexa=hexa,
            minLen=min_len, maxLen=max_len, unfair=unfair, thread=threads
        )
        with Progress(
            SpinnerColumn(spinner_name="aesthetic"),
            TextColumn("[progress.description]{task.description}"),
            TimeElapsedColumn(),
            console=console,
            transient=True
        ) as progress:
            task_description = f"[bold green]{random.choice(QUOTES)} (Stage 1/M2)"
            task_id = progress.add_task(task_description, total=None)
            # Run the actual process using the Namespace
            merged_out_path_str, locicons_file_str, pattern_summary_str = m2.main(m2_args_ns)

        console.print(Panel("Stage 1: Genome Quality Assessment & SSR Detection [bold green]Complete[/]", border_style="green", padding=(0,1)))

        # --- Run GC2 pipeline if gene bed path is available ---
        if gene_bed_path_resolved:
            console.print(Rule("[bold blue]Stage 2: Gene-Level Analysis", style="blue"))

            # Run GC2 (Part 1 of Stage 2)
            logger.info("Running GC2 sub-step...")
            gc2_args_ns = Namespace(
                merged=merged_out_path_str, # Use the string path returned by m2
                gene=str(gene_bed_path_resolved), # Use resolved path
                jobOut=str(main_out_dir),
                tmp=str(intrim_out_dir),
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
                ssr_combo_path_str = gc2.main(gc2_args_ns)
            logger.info("GC2 sub-step complete.")

            # Process SSR Results (Part 2 of Stage 2)
            logger.info("Running SSR Comparison sub-step...")
            ssr_args_ns = Namespace(
                ssrcombo=ssr_combo_path_str, # Use path from GC2
                jobOut=str(main_out_dir),
                tmp=str(intrim_out_dir),
                logger=logger,
                reference=reference_id,
                min_repeat_count=min_repeat_count,
                min_genome_count=min_genome_count
            )

            with Progress(
                SpinnerColumn(spinner_name="aesthetic"),
                TextColumn("[progress.description]{task.description}"),
                TimeElapsedColumn(),
                console=console,
                transient=True
            ) as progress:
                task_description = f"[bold green]{random.choice(QUOTES)} (Stage 2/Compare)"
                task_id = progress.add_task(task_description, total=None)
                # Ensure process_ssr_results.main takes args correctly
                process_ssr_results.main(ssr_args_ns)

            console.print(Panel("Stage 2: Gene-Level Analysis [bold green]Complete[/]", border_style="green", padding=(0,1)))
        else:
            console.print(Panel("[yellow]Stage 2 Skipped:[/] Gene BED file not provided or found", border_style="yellow", padding=(0,1)))

        # --- Generate Plots (Conditional) ---
        if plots:
            console.print(Rule("[bold blue]Stage 3: Multi-Modal Data Visualization", style="blue"))
            try:
                logger.info("Starting post-processing: Generating plots...")
                plots_output_dir = job_dir / "output" / "plots" # Path object
                # Ensure directories exist for plotting function
                plots_output_dir.mkdir(parents=True, exist_ok=True)

                with Progress(
                    SpinnerColumn(spinner_name="aesthetic"),
                    TextColumn("[progress.description]{task.description}"),
                    TimeElapsedColumn(),
                    console=console,
                    transient=True
                ) as progress:
                    task_description = f"[bold green]{random.choice(QUOTES)} (Stage 3/Plots)"
                    task_id = progress.add_task(task_description, total=None)
                    # Import plotting function only when needed
                    try:
                        from crossroad.core.plotting import generate_all_plots
                        # Pass string paths as expected by the current plotting function
                        generate_all_plots(str(main_out_dir), str(intrim_out_dir), str(plots_output_dir), reference_id)
                    except ImportError:
                         logger.error("Plotting libraries not found. Please install them (e.g., plotly, plotly-upset) to generate plots.")
                         raise # Re-raise to indicate failure

                console.print(Panel("Stage 3: Multi-Modal Data Visualization [bold green]Complete[/]", border_style="green", padding=(0,1)))
            except Exception as plot_err:
                logger.error(f"An error occurred during plot generation: {plot_err}", exc_info=True)
                console.print(Panel("[bold red]Stage 3: Multi-Modal Data Visualization Failed[/]", border_style="red", padding=(0,1)))
        else:
            console.print(Panel("[yellow]Stage 3 Skipped:[/] Multi-Modal Data Visualization (use --plots to enable)", border_style="yellow", padding=(0,1)))

        # Stage 4 Start/End
        console.print(Rule("[bold blue]Stage 4: Results Aggregation & Dissemination", style="blue"))
        logger.info(f"{LOG_SYMBOLS[logging.INFO]} [bold green]Analysis completed successfully[/]")
        logger.info(f"{LOG_SYMBOLS[logging.INFO]} [bold green]Stage 4: Results Aggregation & Dissemination Complete[/]")

        end_time = time.time()
        duration = end_time - start_time
        minutes = int(duration // 60)
        seconds = duration % 60
        duration_str = f"{minutes}m {seconds:.2f}s"
        # logger.info(f"\n[bold green]Total analysis time: {duration_str}[/]") # Logged by RichHandler, keep simple info log?
        logger.info(f"Total analysis time: {duration_str}") # Keep a plain log

        # Use a Panel for the final output message, including duration
        final_message = Text.assemble(
            "Results available in: ",
            (str(job_dir / 'output'), "bold green"),
            f"\nTotal analysis time: {duration_str}"
        )
        console.print(Panel(final_message,
                            title="[bold blue]Analysis Complete[/]",
                            border_style="blue"))

    except (typer.Exit, typer.BadParameter) as e:
        # Typer/validation errors already printed messages via console.print or callbacks.
        # Log the failure condition clearly.
        logger.critical(f"{LOG_SYMBOLS[logging.CRITICAL]} [bold bright_red]ANALYSIS HALTED DUE TO INVALID PARAMETERS OR FILE ISSUES.[/]")
        # Re-raise typer.Exit to ensure proper exit code handling by Typer
        if not isinstance(e, typer.Exit): # Ensure we exit if it was just BadParameter
             raise typer.Exit(code=1)
        else:
             raise e # Re-raise the original Exit exception
    except Exception as e:
        logger.error(f"An unexpected error occurred: {str(e)}", exc_info=True) # Log full traceback
        logger.critical(f"{LOG_SYMBOLS[logging.CRITICAL]} [bold bright_red]ANALYSIS FAILED UNEXPECTEDLY[/]")

        end_time = time.time()
        duration = end_time - start_time
        minutes = int(duration // 60)
        seconds = duration % 60
        duration_str = f"{minutes}m {seconds:.2f}s"
        # logger.info(f"\n[bold red]Analysis failed after: {duration_str}[/]") # Logged by RichHandler
        logger.info(f"Analysis failed after: {duration_str}") # Keep plain log

        # Add duration to the failure panel/message
        console.print(Panel(f"[bold bright_red]ANALYSIS FAILED UNEXPECTEDLY[/]\nAfter: {duration_str}", border_style="red"))
        raise typer.Exit(code=1) # Ensure non-zero exit code on unexpected error


if __name__ == "__main__":
    app() # Run the Typer app
