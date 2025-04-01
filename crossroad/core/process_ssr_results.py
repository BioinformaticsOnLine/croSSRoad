#!/usr/bin/env python3
import argparse
import os
import pandas as pd
import logging

def group_records(df, logger):
    """Group records by motif and gene, combining other fields with ':'."""
    columns = ['motif', 'gene', 'genomeID', 'repeat', 'length_of_motif',
               'loci', 'length_of_ssr', 'category', 'country', 'year',
               'ssr_position']
    grouped = df[columns].groupby(['motif', 'gene'], as_index=False)
    merged = grouped.agg(lambda x: ': '.join(map(str, x.unique())))
    merged['repeat_count'] = merged['repeat'].str.count(':') + 1
    merged['genomeID_count'] = merged['genomeID'].str.count(':') + 1
    return merged

def find_variations(motif):
    """Find all circular shifts of a motif."""
    if not isinstance(motif, str):
        motif = str(motif)
    variations = [motif[i:] + motif[:i] for i in range(len(motif))]
    return ', '.join(sorted(variations))

def find_different_repeats(df, reference_id):
    """Find entries with different repeats compared to reference."""
    ref_data = {f"{row['gene']}_{row['loci']}": row['repeat']
                for _, row in df[df['genomeID'] == reference_id].iterrows()}
    different = [row.to_dict() for _, row in df[df['genomeID'] != reference_id].iterrows()
                 if f"{row['gene']}_{row['loci']}" not in ref_data or ref_data[f"{row['gene']}_{row['loci']}"] != row['repeat']]
    return pd.DataFrame(different).sort_values(['gene', 'loci']) if different else pd.DataFrame()

def process_hssr(hotspot_df, ssrcombo_df, logger):
    """Process HSSR data using hotspot records."""
    hotspot_keys = {f"{row['motif']}{row['gene']}:{gid.strip()}"
                    for _, row in hotspot_df.iterrows()
                    for gid in str(row['genomeID']).split(':')}
    hssr_df = ssrcombo_df[ssrcombo_df.apply(
        lambda row: f"{row['motif']}{row['gene']}:{row['genomeID']}" in hotspot_keys, 
        axis=1)]
    logger.info(f"Found {len(hssr_df)} HSSR records")
    return hssr_df

def main(args=None):
    # Setup logging and parse args
    logger = getattr(args, 'logger', logging.getLogger(__name__))
    if args is None:
        parser = argparse.ArgumentParser(description="Process SSR combo file")
        parser.add_argument("--ssrcombo", required=True, help="SSR combo file path")
        parser.add_argument("--jobOut", default="output", help="Output directory")
        parser.add_argument("--reference", help="Reference genome ID")
        parser.add_argument("--tmp", required=True, help="Temp directory")
        parser.add_argument("--min_repeat_count", type=int, default=1, help="Minimum repeat count")
        parser.add_argument("--min_genome_count", type=int, default=4, help="Minimum genome count")
        args = parser.parse_args()
        logging.basicConfig(level=logging.INFO)
        logger = logging.getLogger(__name__)

    # Setup paths
    out_dir = args.jobOut
    tmp_dir = args.tmp
    os.makedirs(out_dir, exist_ok=True)
    os.makedirs(tmp_dir, exist_ok=True)
    files = {
        'hssr': os.path.join(out_dir, "hssr_data.csv"),
        'all_vars': os.path.join(tmp_dir, "all_variations.csv"),
        'hotspot': os.path.join(tmp_dir, "mutational_hotspot.csv"),
        'ref_ssr': None
    }

    # Read input data
    ssrcombo_df = pd.read_csv(args.ssrcombo, sep='\t')

    if args.reference:
        logger.info(f"\nComparing against reference genome {args.reference}")
        different_df = find_different_repeats(ssrcombo_df, args.reference)
        if not different_df.empty:
            files['ref_ssr'] = os.path.join(out_dir, "ref_ssr_genecombo.csv")
            different_df.to_csv(files['ref_ssr'], index=False)
            logger.info(f"Found {len(different_df)} records with different repeats")
            all_records_df = group_records(different_df, logger)
        else:
            logger.info("No different records found")
            return files
    else:
        all_records_df = group_records(ssrcombo_df, logger)

    # Save all variations
    all_records_df.to_csv(files['all_vars'], index=False)

    # Add count columns
    all_records_df['repeat_count'] = all_records_df['repeat'].str.count(':') + 1
    all_records_df['genomeID_count'] = all_records_df['genomeID'].str.count(':') + 1

    # Generate hotspots
    logger.info("\nFiltering mutational hotspot records ...")
    all_records_df['motif_variations'] = all_records_df['motif'].apply(find_variations)
    all_records_df['concat_column'] = all_records_df['gene'] + '_' + all_records_df['motif_variations']
    
    # Find group sizes
    group_sizes = all_records_df.groupby('concat_column').size()
    
    # Groups with multiple records
    multiple_record_groups = group_sizes[group_sizes > 1].index
    hotspot_df = all_records_df[all_records_df['concat_column'].isin(multiple_record_groups)]
    
    # Valid groups where at least one record has genomeID_count >= min_genome_count
    valid_groups = hotspot_df.groupby('concat_column').apply(
        lambda g: g['genomeID_count'].max() >= args.min_genome_count
    )
    valid_groups = valid_groups[valid_groups].index
    
    # Include all records from valid groups where repeat_count >= min_repeat_count
    filtered_hotspot_df = hotspot_df[
        hotspot_df['concat_column'].isin(valid_groups) &
        (hotspot_df['repeat_count'] >= args.min_repeat_count)
    ]
    
    # Include single-record groups with stricter criteria
    single_record_groups = group_sizes[group_sizes == 1].index
    single_record_df = all_records_df[all_records_df['concat_column'].isin(single_record_groups)]
    filtered_single_df = single_record_df[
        (single_record_df['genomeID_count'] > args.min_genome_count) &  # Changed from >= to >
        (single_record_df['repeat_count'] > args.min_repeat_count)
    ]
    
    # Combine both
    filtered_hotspot_df = pd.concat([filtered_hotspot_df, filtered_single_df])
    
    # Drop temporary columns
    filtered_hotspot_df = filtered_hotspot_df.drop(columns=['motif_variations', 'concat_column'])
    
    logger.info(f"Records after filtering: {len(filtered_hotspot_df)}")
    filtered_hotspot_df.to_csv(files['hotspot'], index=False)

    # Process and save HSSR data
    hssr_df = process_hssr(filtered_hotspot_df, ssrcombo_df, logger)
    hssr_df.to_csv(files['hssr'], index=False)

    # Log output files
    logger.info("\nOutput files:")
    logger.info(f"Main directory: {out_dir}")
    logger.info(f"1. HSSR Data: {os.path.basename(files['hssr'])}")
    if files['ref_ssr']:
        logger.info(f"2. Reference SSR: {os.path.basename(files['ref_ssr'])}")
    logger.info(f"\nTemp directory: {tmp_dir}")
    logger.info(f"1. All variations: {os.path.basename(files['all_vars'])}")
    logger.info(f"2. Mutational hotspots: {os.path.basename(files['hotspot'])}")

    return files

if __name__ == '__main__':
    main()