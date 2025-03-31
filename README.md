# Crossroad *fast*API and CLI 

A comprehensive tool for analyzing Simple Sequence Repeats (SSRs) in genomic data,

## Features

- SSR analysis in genomic data
- Gene-based SSR analysis
- Mutational hotspot detection
- Reference-based comparison
- REST API support

## Installation

### Using pip

## Contributors

- Dr. Preeti Agarwal (PhD & Senior Research Fellow)
- Dr. Jitendra Narayan (Principal Investigator)
- Pranjal Pruthi (Project Scientist, CSIR IGIB)


## Institution

CSIR-Institute of Genomics and Integrative Biology
Lab of Bioinformatics and Big Data analysis
Mall Road, New Delhi - 110007, India

## License

MIT License

## Citation

If you use Crossroad in your research, please cite:
[Citation information coming soon]

| Output File | With Reference ID | Without Reference ID |
|-------------|------------------|---------------------|
| hssr_data.csv | 1. find_different_repeats() finds differences from reference<br>2. group_ssr_records_from_excluded() groups these differences<br>3. filter_hotspot_records() filters for variations<br>4. process_hssr_data() creates final HSSR data | 1. group_ssr_records() groups all SSRs<br>2. filter_hotspot_records() filters for variations<br>3. process_hssr_data() creates final HSSR data |
| ref_ssr_genecombo.csv<br>(excluded_repeats) | 1. find_different_repeats() identifies differences from reference<br>2. Saves directly to file<br>3. Only created when reference ID is given | Not created |
| all_variations.csv | 1. find_different_repeats() finds differences<br>2. group_ssr_records_from_excluded() processes these<br>3. Saves to file before filtering | 1. group_ssr_records() groups all SSRs<br>2. Saves to file before filtering |
| mutational_hotspots.csv | 1. Uses grouped excluded repeats<br>2. filter_hotspot_records() finds variations<br>3. Applies min_repeat_count and min_genome_count filters | 1. Uses grouped all SSRs<br>2. filter_hotspot_records() finds variations<br>3. Applies min_repeat_count and min_genome_count filters |


#### Each file in simple terms:


##### hssr_data.csv:
- Final output containing hotspot SSRs
- With reference: Only includes variations different from reference
- Without reference: Includes all variations meeting criteria

##### ref_ssr_genecombo.csv (excluded_repeats):
- Only created when using reference ID
- Lists all SSRs that are different from reference genome
- Raw differences before processing

##### all_variations.csv:
- With reference: All variations of different SSRs from reference
- Without reference: All variations of all SSRs
- Intermediate file before filtering

##### mutational_hotspots.csv:
- Contains filtered hotspots meeting criteria
- Uses min_repeat_count and min_genome_count

- With reference: Only from differences
- Without reference: From all SSRs
