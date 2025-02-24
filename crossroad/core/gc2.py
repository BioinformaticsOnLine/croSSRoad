#!/usr/bin/env python3
import argparse
import csv
import os
import subprocess
import sys
import time
import logging

def run_cmd(cmd, cwd=None, logger=None):
    """Run a command with an optional working directory; raise on error."""
    logger.info(f"Running command: {' '.join(cmd)}")
    result = subprocess.run(cmd, cwd=cwd, capture_output=True, text=True)
    if result.returncode != 0:
        logger.error(f"Error:\n{result.stderr}")
        raise RuntimeError("Command failed: " + " ".join(cmd))
    return result.stdout

def remove_header(infile, outfile):
    """Remove the header (first line) from infile and write the remaining lines to outfile."""
    with open(infile, 'r') as fin, open(outfile, 'w') as fout:
        fin.readline()  # skip header
        for line in fin:
            fout.write(line)

def run_bedtools_intersect(a_bed, gene_bed, out_file, work_dir, logger):
    """
    Run bedtools intersect with a_bed as -a and gene_bed as -b.
    The intersect output is written to out_file.
    """
    cmd = [
        "bedtools", "intersect",
        "-a", os.path.abspath(a_bed),
        "-b", os.path.abspath(gene_bed),
        "-wa", "-wb"
    ]
    logger.info(f"Running bedtools intersect in: {work_dir}")
    output = run_cmd(cmd, cwd=work_dir, logger=logger)
    with open(out_file, 'w') as f:
        f.write(output)

def run_bedtools_intersect_no_overlap(a_bed, gene_bed, out_file, work_dir, logger):
    """
    Run bedtools intersect with -v option to find SSRs that don't overlap with genes.
    """
    cmd = [
        "bedtools", "intersect",
        "-a", os.path.abspath(a_bed),
        "-b", os.path.abspath(gene_bed),
        "-v"
    ]
    logger.info(f"Running bedtools intersect (no overlap) in: {work_dir}")
    output = run_cmd(cmd, cwd=work_dir, logger=logger)
    with open(out_file, 'w') as f:
        f.write(output)

def run_bedtools_intersect_no_overlap_genes(a_bed, gene_bed, out_file, work_dir, logger):
    """
    Run bedtools intersect with -v option to find genes that don't overlap with any SSRs.
    """
    cmd = [
        "bedtools", "intersect",
        "-a", os.path.abspath(gene_bed),
        "-b", os.path.abspath(a_bed),
        "-v"
    ]
    logger.info(f"Running bedtools intersect (no overlap genes) in: {work_dir}")
    output = run_cmd(cmd, cwd=work_dir, logger=logger)
    with open(out_file, 'w') as f:
        f.write(output)

def assign_ssr_position(intersect_file, out_file, logger):
    """
    Reads the bedtools intersect output (tab-delimited) and produces the final SSR Gene Combo table.
    For example, if the intersect output has the following columns (0-based index):
      A side (mergedOut):
         0: genomeID, 1: start1, 2: end1, 3: repeat, 4: motif,
         5: GC_per, 6: AT_per, 7: length_of_motif, 8: loci, 9: length_of_ssr,
      B side (gene BED):  
         10: category, 11: country, 12: year, 13: length_genome, 14: N_count,
         15: genome_GC_per, 16: genomeID2, 17: start2, 18: end2, 19: gene
    Then we assign the ssr_position based on comparing start1/end1 with start2/end2.
    """
    with open(intersect_file, 'r') as fin, open(out_file, 'w', newline='') as fout:
        reader = csv.reader(fin, delimiter='\t')
        writer = csv.writer(fout, delimiter='\t')
        
        # Write header to the output file
        header = [
            "genomeID", "start1", "end1", "repeat", "motif", "GC_per", "AT_per",
            "length_of_motif", "loci", "length_of_ssr",
            "category", "country", "year", "length_genome", "N_count", "genome_GC_per",
            "genomeID2", "start2", "end2", "gene", "ssr_position"
        ]
        writer.writerow(header)
        line_num = 0
        for row in reader:
            line_num += 1
            # Now we expect at least 20 columns (indices 0 to 19)
            if len(row) < 20:
                logger.warning(f"Skipping line {line_num}: expected at least 20 columns, got {len(row)}")
                continue
            
            try:
                ssr_start = int(row[1])
                ssr_end = int(row[2])
                gene_start = int(row[17])
                gene_end = int(row[18])
            except ValueError as ve:
                logger.warning(f"Skipping line {line_num} due to conversion error: {ve}")
                continue

            # Decide ssr_position: if SSR starts before gene, label "intersect_start",
            # if SSR ends after gene, label "intersect_stop", else "IN".
            if ssr_start < gene_start:
                pos_label = "intersect_start"
            elif ssr_end > gene_end:
                pos_label = "intersect_stop"
            else:
                pos_label = "IN"
            
            # Append the new field to the row
            new_row = row + [pos_label]
            writer.writerow(new_row)

def write_non_gene_ssrs(no_overlap_file, out_file, header_file):
    """
    Write SSRs that don't overlap with genes to a separate file.
    """
    # First read the header from the original mergedOut.tsv
    with open(header_file, 'r') as f:
        header = f.readline().strip().split('\t')
    
    with open(no_overlap_file, 'r') as fin, open(out_file, 'w', newline='') as fout:
        reader = csv.reader(fin, delimiter='\t')
        writer = csv.writer(fout, delimiter='\t')
        
        # Write the header
        writer.writerow(header)
        
        # Write the data
        for row in reader:
            writer.writerow(row)

def write_non_ssr_genes(no_overlap_file, out_file):
    """
    Write genes that don't overlap with SSRs to a separate file.
    Returns the count of unique genes without SSRs.
    """
    unique_genes = set()  # Use a set to store unique gene names
    
    with open(no_overlap_file, 'r') as fin, open(out_file, 'w', newline='') as fout:
        reader = csv.reader(fin, delimiter='\t')
        writer = csv.writer(fout, delimiter='\t')
        
        # Write header
        header = ["genomeID", "start", "end", "gene"]
        writer.writerow(header)
        
        # Write the data and collect unique genes
        for row in reader:
            writer.writerow(row)
            if len(row) >= 4:  # Ensure we have enough columns
                gene = row[3]  # gene is in the 4th column (0-based index)
                unique_genes.add(gene)  # Add to set - duplicates will be automatically ignored
    
    return len(unique_genes)  # Return count of unique genes

def count_genes_with_ssr(intersect_file):
    """
    Count unique genes that have SSRs.
    """
    unique_genes = set()
    with open(intersect_file, 'r') as fin:
        reader = csv.reader(fin, delimiter='\t')
        for row in reader:
            if len(row) >= 20:  # Ensure we have enough columns
                gene = row[19]  # gene is in the 20th column (0-based index)
                unique_genes.add(gene)
    return len(unique_genes)

def main(args=None):
    # Get logger from args, or create a default one
    logger = getattr(args, 'logger', logging.getLogger(__name__))
    
    # If args is not provided, parse from command line
    if args is None:
        parser = argparse.ArgumentParser(
            description="Generate SSR Gene Combo table and non-gene SSR table from mergedOut.tsv and gene.bed"
        )
        parser.add_argument("--merged", required=True,
                            help="Path to mergedOut.tsv (the merged SSR file)")
        parser.add_argument("--gene", required=True,
                            help="Path to gene.bed file")
        parser.add_argument("--out", default="ssr_genecombo.tsv",
                            help="Path to final output table (ssr_genecombo.tsv)")
        parser.add_argument("--jobOut", default="output",
                            help="Output directory; a job folder will be created inside this directory")
        parser.add_argument("--tmp", default="tmp",
                            help="Temporary directory; a job folder will be created inside this directory")
        
        args = parser.parse_args()
        # Create default logger if running standalone
        logging.basicConfig(level=logging.INFO)
        logger = logging.getLogger(__name__)

    # Use the provided tmp directory
    tmp_dir = args.tmp
    
    # Final outputs will go directly to main output directory
    final_out = os.path.join(args.jobOut, "ssr_genecombo.tsv")
    non_gene_out = os.path.join(args.jobOut, "ssr_non_gene.tsv")
    non_ssr_genes_out = os.path.join(args.jobOut, "genes_non_ssr.tsv")
    
    # Process files
    merged_bed = os.path.join(tmp_dir, "mergedOut.bed")
    logger.info("Removing header from mergedOut.tsv...")
    remove_header(args.merged, merged_bed)

    # Find overlapping SSRs
    intersect_out = os.path.join(tmp_dir, "intersect_output.bed")
    run_bedtools_intersect(merged_bed, args.gene, intersect_out, tmp_dir, logger)

    # Find non-overlapping SSRs
    no_overlap_out = os.path.join(tmp_dir, "no_overlap_output.bed")
    run_bedtools_intersect_no_overlap(merged_bed, args.gene, no_overlap_out, tmp_dir, logger)

    # Find non-overlapping genes
    no_overlap_genes_out = os.path.join(tmp_dir, "no_overlap_genes_output.bed")
    run_bedtools_intersect_no_overlap_genes(merged_bed, args.gene, no_overlap_genes_out, tmp_dir, logger)

    assign_ssr_position(intersect_out, final_out, logger)
    write_non_gene_ssrs(no_overlap_out, non_gene_out, args.merged)
    genes_without_ssr = write_non_ssr_genes(no_overlap_genes_out, non_ssr_genes_out)
    genes_with_ssr = count_genes_with_ssr(intersect_out)
    
    logger.info("\nGene Statistics:")
    logger.info(f"Total genes detected with SSR: {genes_with_ssr}")
    logger.info(f"Total genes detected without SSR: {genes_without_ssr}")
    logger.info(f"Total genes: {genes_with_ssr + genes_without_ssr}")
    
    logger.info("\nOutput files:")
    logger.info(f"SSR Gene Combo table generated at: {final_out}")
    logger.info(f"SSR Non-gene table generated at: {non_gene_out}")
    logger.info(f"Genes Non-SSR table generated at: {non_ssr_genes_out}")

    # Return the path to ssr_genecombo.tsv for the API
    return final_out

if __name__ == "__main__":
    main()
