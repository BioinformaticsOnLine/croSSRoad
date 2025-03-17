#!/usr/bin/env python3
import argparse
import os
import pandas as pd
import time
import logging

def group_ssr_records(ssrcombo_file, min_repeat_count, min_genome_count, logger):
    """Group SSR records by motif and gene, combining other fields with ':' and applying filters."""
    # Read the input data into a DataFrame
    df = pd.read_csv(ssrcombo_file, sep='\t')
    logger.info(f"Total input records: {len(df)}")
    
    # Select columns for aggregation
    selected_columns = df[['motif', 'gene', 'genomeID', 'repeat', 'length_of_motif', 
                          'loci', 'length_of_ssr', 'category', 'country', 'year', 
                          'ssr_position']]
    
    # Group by MOTIF and GENE
    grouped_data = selected_columns.groupby(['motif', 'gene'], as_index=False)
    
    # Merge other columns and join unique values with ':'
    merged_data = grouped_data.agg(lambda x: ': '.join(x.astype(str).unique()))
    
    # Add count columns
    merged_data['repeat_count'] = merged_data['repeat'].str.count(':') + 1
    merged_data['genomeID_count'] = merged_data['genomeID'].str.count(':') + 1
    
    # Apply filtering based on counts
    filtered_data = merged_data[
        (merged_data['repeat_count'] >= min_repeat_count) &
        (merged_data['genomeID_count'] >= min_genome_count)
    ]
    
    logger.info(f"Records after filtering (repeat_count >= {min_repeat_count}, genomeID_count >= {min_genome_count}): {len(filtered_data)}")
    
    return filtered_data

def filter_hotspot_records(all_records_df, ssrcombo_file, min_repeat_count=1, min_genome_count=4, logger=None):
    """Filter records based on cyclical variation."""
    try:
        if logger:
            logger.info(f"Records before variation filtering: {len(all_records_df)}")
        
        # Get the total unique genomeID count from the original SSR combo file
        original_df = pd.read_csv(ssrcombo_file, sep='\t')
        total_unique_genomes = len(original_df['genomeID'].unique())
        logger.info(f"Total unique genomes in dataset: {total_unique_genomes}")
        
        def find_variations(motif):
            """Find all circular shifts of a motif."""
            if not isinstance(motif, str):
                motif = str(motif)
            motif_length = len(motif)
            variations = []
            for i in range(motif_length):
                variation = motif[i:] + motif[:i]
                variations.append(variation)
            return ', '.join(sorted(variations))
        
        # Create motif variations and concatenated key
        all_records_df['motif_variations'] = all_records_df['motif'].apply(find_variations)
        all_records_df['concat_column'] = all_records_df['gene'] + '_' + all_records_df['motif_variations']
        
        # Group by concat_column and filter for multiple records
        grouped = all_records_df.groupby('concat_column')
        filtered_groups = grouped.filter(lambda x: len(x) > 1)
        
        # Add filter for genomeID_count using the calculated total
        valid_groups = filtered_groups.groupby('concat_column').filter(
            lambda x: x['genomeID_count'].sum() <= total_unique_genomes
        )
        
        # Drop temporary columns
        if valid_groups is not None and not valid_groups.empty:
            result = valid_groups.drop(columns=['motif_variations', 'concat_column'])
        else:
            result = pd.DataFrame(columns=all_records_df.columns)
            
        logger.info(f"Final filtered records: {len(result)}")
        return result
        
    except Exception as e:
        logger.error(f"Error in filter_hotspot_records: {str(e)}")
        logger.error(f"DataFrame info: {all_records_df.info()}")
        raise

def process_hssr_data(hotspot_file, ssrcombo_file, hssr_outfile, logger):
    """Process HSSR data using the hotspot records."""
    # Read hotspot and ssrcombo files
    hotspot_df = pd.read_csv(hotspot_file)
    ssrcombo_df = pd.read_csv(ssrcombo_file, sep='\t')
    logger.info(f"Processing {len(hotspot_df)} hotspot records against {len(ssrcombo_df)} SSR records...")
    
    # Create concatenated keys for matching
    hotspot_keys = set()
    for _, row in hotspot_df.iterrows():
        motif = row['motif']
        gene = row['gene']
        genomeIDs = str(row['genomeID']).split(':')
        for gid in genomeIDs:
            gid = gid.strip()
            key = f"{motif}{gene}:{gid}"
            hotspot_keys.add(key)
    
    # Filter ssrcombo records
    def is_hotspot(row):
        key = f"{row['motif']}{row['gene']}:{row['genomeID']}"
        return key in hotspot_keys
    
    hssr_df = ssrcombo_df[ssrcombo_df.apply(is_hotspot, axis=1)]
    logger.info(f"Found {len(hssr_df)} HSSR records")
    
    # Write to CSV
    hssr_df.to_csv(hssr_outfile, index=False)

def find_different_repeats(ssrcombo_df, reference_genome_id):
    """Find entries that have different repeats or loci compared to reference genome."""
    # Create a reference dictionary for gene+loci+repeat combinations
    ref_data = {}
    ref_rows = ssrcombo_df[ssrcombo_df['genomeID'] == reference_genome_id]
    
    for _, row in ref_rows.iterrows():
        key = f"{row['gene']}_{row['loci']}"
        ref_data[key] = row['repeat']
    
    # Filter rows that are different from reference
    different_repeats = []
    other_rows = ssrcombo_df[ssrcombo_df['genomeID'] != reference_genome_id]
    
    for _, row in other_rows.iterrows():
        key = f"{row['gene']}_{row['loci']}"
        
        # If this gene+loci combination exists in reference
        if key in ref_data:
            # Skip if it has the same repeat count
            if ref_data[key] == row['repeat']:
                continue
                
        # Add to different_repeats if it's different or not in reference
        different_repeats.append({
            'motif': row['motif'],
            'gene': row['gene'],
            'genomeID': row['genomeID'],
            'repeat': row['repeat'],
            'length_of_motif': row['length_of_motif'],
            'loci': row['loci'],
            'length_of_ssr': row['length_of_ssr'],
            'category': row['category'],
            'country': row['country'],
            'year': row['year'],
            'ssr_position': row['ssr_position']
        })
    
    # Convert to DataFrame and sort by gene and loci
    result_df = pd.DataFrame(different_repeats)
    if not result_df.empty:
        result_df = result_df.sort_values(['gene', 'loci'])
    
    return result_df

def main(args=None):
    # Get logger from args, or create a default one
    logger = getattr(args, 'logger', logging.getLogger(__name__))
    
    # Get filtering parameters with defaults
    min_repeat_count = getattr(args, 'min_repeat_count', 1)
    min_genome_count = getattr(args, 'min_genome_count', 4)
    
    logger.info(f"Using filtering parameters: min_repeat_count={min_repeat_count}, min_genome_count={min_genome_count}")
    
    # If args is not provided, parse from command line
    if args is None:
        parser = argparse.ArgumentParser(
            description="Process SSR combo file and output tables")
        parser.add_argument("--ssrcombo", required=True,
                            help="Path to ssr_genecombo.tsv file")
        parser.add_argument("--jobOut", default="output",
                            help="Output directory; a job folder will be created inside this directory")
        parser.add_argument("--reference", required=False,
                            help="Reference genome ID to compare repeats")
        parser.add_argument("--tmp", required=True,
                            help="Temporary directory")
        args = parser.parse_args()
        logging.basicConfig(level=logging.INFO)
        logger = logging.getLogger(__name__)

    tmp_dir = args.tmp
    hssr_file = os.path.join(args.jobOut, "hssr_data.csv")
    different_repeats_file = None
    all_vars_file = os.path.join(tmp_dir, "all_variations.csv")
    hotspot_file = os.path.join(tmp_dir, "mutational_hotspot.csv")

    if hasattr(args, 'reference') and args.reference is not None:
        logger.info(f"\nFinding different repeats compared to reference genome {args.reference} ...")
        # Read the original ssr_genecombo.tsv
        ssrcombo_df = pd.read_csv(args.ssrcombo, sep='\t')
        different_repeats_df = find_different_repeats(ssrcombo_df, args.reference)
        
        if not different_repeats_df.empty:
            different_repeats_file = os.path.join(args.jobOut, "ref_ssr_genecombo.csv")
            different_repeats_df.to_csv(different_repeats_file, index=False)
            logger.info(f"Reference SSR gene combo table written to: {different_repeats_file}")
            logger.info(f"Found {len(different_repeats_df)} records with different repeat counts\n")
            
            # Generate all_variations from excluded_repeats
            logger.info("\nGenerating all variations from excluded repeats ...")
            all_records_df = group_ssr_records_from_excluded(different_repeats_df, logger)
            all_records_df.to_csv(all_vars_file, index=False)
            logger.info(f"All variations table written to: {all_vars_file}")
            
            # Generate hotspots from all_variations
            logger.info("\nFiltering mutational hotspot records ...")
            hotspot_df = filter_hotspot_records(
                all_records_df, 
                args.ssrcombo,
                min_repeat_count=min_repeat_count,
                min_genome_count=min_genome_count,
                logger=logger
            )
            hotspot_df.to_csv(hotspot_file, index=False)
            logger.info(f"Mutational hotspot table written to: {hotspot_file}\n")
        else:
            logger.info("No records found with different repeat counts\n")
    else:
        # Original flow when no reference is provided
        logger.info("\nGrouping SSR combo records ...")
        all_records_df = group_ssr_records(
            args.ssrcombo,
            min_repeat_count,
            min_genome_count,
            logger
        )
        all_records_df.to_csv(all_vars_file, index=False)
        logger.info(f"All variations table written to: {all_vars_file}")

        logger.info("\nFiltering mutational hotspot records ...")
        hotspot_df = filter_hotspot_records(
            all_records_df, 
            args.ssrcombo,
            min_repeat_count=min_repeat_count,
            min_genome_count=min_genome_count,
            logger=logger
        )
        hotspot_df.to_csv(hotspot_file, index=False)
        logger.info(f"Mutational hotspot table written to: {hotspot_file}\n")

    # Process HSSR data
    logger.info("Processing HSSR data table ...")
    process_hssr_data(hotspot_file, args.ssrcombo, hssr_file, logger)
    logger.info(f"HSSR Data table written to: {hssr_file}\n")

    # Output summary
    logger.info("\nOutput files:")
    logger.info(f"Main output directory: {args.jobOut}")
    logger.info(f"1. HSSR Data table: {os.path.basename(hssr_file)}")
    if different_repeats_file:
        logger.info(f"2. Reference SSR gene combo table: {os.path.basename(different_repeats_file)}")
    logger.info(f"\nTemporary files directory: {tmp_dir}")
    logger.info(f"1. All variations: {os.path.basename(all_vars_file)}")
    logger.info(f"2. Mutational hotspots: {os.path.basename(hotspot_file)}")

    return {
        'hssr_data': hssr_file,
        'excluded_repeats': different_repeats_file,
        'all_variations': all_vars_file,
        'mutational_hotspots': hotspot_file
    }

def group_ssr_records_from_excluded(excluded_df, logger):
    """Group excluded repeats records by motif and gene, combining other fields with ':'."""
    try:
        logger.info(f"Total excluded records: {len(excluded_df)}")
        
        # Select columns for aggregation
        selected_columns = excluded_df[['motif', 'gene', 'genomeID', 'repeat', 'length_of_motif', 
                                      'loci', 'length_of_ssr', 'category', 'country', 'year', 
                                      'ssr_position']]
        
        # Group by MOTIF and GENE
        grouped_data = selected_columns.groupby(['motif', 'gene'], as_index=False)
        logger.info(f"Unique motif-gene combinations: {len(grouped_data)}")
        
        # Merge other columns and join unique values with ':'
        merged_data = grouped_data.agg(lambda x: ': '.join(map(str, x.unique())))
        
        # Calculate counts using string operations
        merged_data['repeat_count'] = merged_data['repeat'].str.count(':') + 1
        merged_data['genomeID_count'] = merged_data['genomeID'].str.count(':') + 1
        
        # Ensure integer types for count columns
        merged_data['repeat_count'] = merged_data['repeat_count'].astype('int64')
        merged_data['genomeID_count'] = merged_data['genomeID_count'].astype('int64')
        
        return merged_data
        
    except Exception as e:
        logger.error(f"Error in group_ssr_records_from_excluded: {str(e)}")
        raise

if __name__ == '__main__':
    main()
