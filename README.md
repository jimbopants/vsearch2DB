## Vsearch2DB!
Written by JG
5/14/18

## Background:
Prior to ASV variant identification methods like DADA2 for 16S data, vsearch OTU clustering was pretty good. This script converts 16s/18s amplicon data split across 3 files by default into a single database file to facilitate identifying sequence data associated with specific OTUs. Mostly used for looking at microdiversity within OTUs.

### Input files  & Descriptions:
* input_fasta - Fasta file with default headers from vseach ">barcodelabel="+Header, sequence on 1 line
* OTU map (vsearch) - https://www.drive5.com/usearch/manual/opt_uc.html
* taxa file (qiime) - 1 line per OTU, uses Silva classifications by default ("k__bacteria;__p...")

### Usage:
* -make_tables: creates a local sqlite3 database with tables for the input fasta and otu map, then joins them. Can be slow for millions of sequences
 - Requires -input_fasta, -otu_map and -taxa_file options
* -add_taxonomy: Adds taxonomy file to db (maybe merge above)
* -extract_otus: Writes a fasta file, one per taxa for a list of taxa 
* -h or --help to see all options
