#!/Users/jimbo/anaconda/bin/python
# JG 5/15/18

# Imports:
import sqlite3
import argparse
import sys
import mmap
from tqdm import tqdm
from Vsearch_2DB_functions import *

def main():
    # Parse arguments and print header
    print_ascii_name()
    args = parse_arguments()

    if args.make_tables:
        print('Making tables:\n')
        make_blank_tables(args.db)
        # Fill the tables:
        fill_otumap_table(args.db, args.otu_map)
        fill_taxa_table(args.db, args.taxa)
        fill_sequence_table(args.db, args.input_fasta)
        # Join tables:
        join_OTUs_w_Seqs(args.db)
        # Drop the pure sequence data table
        drop_table(args.db, 'SEQ_DATA')
        print('Finished making tables!')

    if args.extract_seqs:
        taxa_of_interest = get_taxa_matches(args.db, args.rank, args.taxa)


def parse_arguments():
    parser = argparse.ArgumentParser(description="Convert flat seq data to SQLite or extract taxa-specific sequences",\
     epilog="\n\nMore documentation @ https://github.com/jimbopants/vsearch2DB\n\n")

    #Options:
    parser.add_argument("-m", "--make_tables", help="Makes SQLite database and populates tables", action="store_true")
    parser.add_argument("-e", "--extract_seqs", help="Writes fasta files for a specific taxa", action="store_true")

    # Options for extract sequences:
    parser.add_argument("--rank", help="Taxa column to match name (one of: K,P,C,O,F,G,S)")
    parser.add_argument("--name", help="Must completely match taxon, i.e. Nitrosomonas")

    #File paths:
    parser.add_argument("-i", "--input_fasta", help="Fasta w/ all sequences")
    parser.add_argument("-o", "--otu_map", help="Vsearch OTU map")
    parser.add_argument("-t", "--taxa", help="Output from qiime assign_taxonomy, (col1=OTU), (col2=tax string)")
    parser.add_argument("--db", help="SQLite file, created by 'make_tables', accessed by 'extract_OTUs'  database, used if --extract_OTUs passed")
    parser.add_argument("-f", "--fasta_out", help="Path to write fasta files for OTUs")

    # Print help if no options given.
    if len(sys.argv)==1:
        parser.print_help()
        sys.exit(1)

    # Parse Command Line Arguments:
    try:
        result = parser.parse_args()
        check_arguments(parser, result)
        return result

    except Exception as e:
        parser.print_help()
        print(e)
        sys.exit(0)

def check_arguments(parser, args):
    """ Exit if invalid options specified"""
    if args.make_tables:
        if not all([args.input_fasta, args.otu_map, args.taxa, args.db]):
            parser.print_help()
            print('\n\nMust specify an input_fasta, otu_map, taxa, and db file if making database\n\n')
            sys.exit(1)

    if args.extract_seqs:
        if not all([args.db, args.rank, args.name, args.fasta_out]):
            parser.print_help()
            print('\n\nMust specify a sqlite database, taxon rank, name and output file to extract sequences\n\n')
            sys.exit(1)


def print_ascii_name():
    """The most important part of any software tool"""
    lines=[
    " __      __                      _       ___    _____  ____  ",
    " \ \    / /                     | |     |__ \  |  __ \|  _ \ ",
    "  \ \  / /__  ___  __ _ _ __ ___| |__      ) | | |  | | |_) |",
    "   \ \/ / __|/ _ \/ _` | '__/ __| '_ \    / /  | |  | |  _ < ",
    "    \  /\__ \  __/ (_| | | | (__| | | |  / /_  | |__| | |_) |",
    "     \/ |___/\___|\__,_|_|  \___|_| |_| |____| |_____/|____/ "]
    for line in lines:
        print(line)
    return

if __name__ == "__main__":
    main()
