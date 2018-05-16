# Imports:
import sqlite3
import sys
import mmap
from tqdm import tqdm


def make_blank_tables(sqlite_file):
    """Initialize database"""
    conn,c = connect(sqlite_file)

    c.execute('CREATE TABLE IF NOT EXISTS OTU_MAP \
          (Record_Type TEXT DEFAULT H, \
          Cluster_Number INTEGER, \
          Seq_Length INTEGER, \
          Percent_Identity REAL, \
          Strand TEXT, \
          Alignment TEXT, \
          Header TEXT PRIMARY KEY, \
          Target TEXT)' )

    c.execute('CREATE TABLE IF NOT EXISTS SEQ_DATA \
        (Header TEXT PRIMARY KEY, Sequence TEXT)')

    c.execute('CREATE TABLE IF NOT EXISTS TAX_DATA \
        (OTU PRIMARY KEY, \
        Kingdom TEXT, \
        Phylum TEXT, \
        Class TEXT, \
        pOrder TEXT, \
        Family TEXT, \
        Genus TEXT, \
        Species TEXT)')

    c.execute('CREATE TABLE IF NOT EXISTS OTUs_W_Seqs \
          (Cluster_Number INTEGER, \
          Header TEXT PRIMARY KEY, \
          Target TEXT, \
          Sequence TEXT)' )
    close(conn)
    return


# Next 3 functions handle data manipulation and insertion into rows of different tables:
def fill_otumap_table(sqlite_file, otu_map):
    """Fills an OTU map table with data from the otu_map.uc file"""
    conn = sqlite3.connect(sqlite_file)
    with conn:
        with open(otu_map) as f:
            for line in tqdm(f, total=get_num_lines(otu_map), desc='Populating OTU map'):
                otu_row = line.split('\t')
                seq_label = otu_row[8].split('=')[1].replace(';','.')
                otu_label = otu_row[9].replace(';','__').rstrip('\n').rstrip('_')

                conn.execute("INSERT OR IGNORE INTO OTU_MAP \
                (Record_Type, Cluster_Number, Seq_Length, Percent_Identity, \
                Strand, Alignment, Header, Target) \
                VALUES ('{0}', {1}, {2}, {3}, '{4}', '{5}', '{6}', '{7}')\
                ".format(otu_row[0], otu_row[1], otu_row[2], otu_row[3], \
                otu_row[4], otu_row[7], seq_label, otu_label))
    return


def fill_sequence_table(sqlite_file, seq_data):
    f = open(seq_data, 'r')
    conn = sqlite3.connect(sqlite_file)

    with conn:
        for i in tqdm(range(int(get_num_lines(seq_data)/2)), desc='Populating Sequence Data'):
                # Read header and sequence line, exit if at EOF:
                line1 = f.readline()
                line2 = f.readline()
                if len(line1)==0 or len(line2)==0:
                    print('Finished adding sequences!')
                    break
                # Drop the barcode= section of the header:
                header = line1[1:].split()[0]
                header = header.split('=')[1].replace(';','.')
                seq = line2.rstrip()
                # Add sequence data:
                conn.execute("INSERT OR IGNORE INTO SEQ_DATA \
                (Header, Sequence) \
                VALUES ('{0}', '{1}')".format(header, seq))
    f.close()
    return


def fill_taxa_table(sqlite_file, tax_data):
    """Fills the taxonomy table"""
    conn = sqlite3.connect(sqlite_file)
    with conn:
        with open(tax_data, 'r') as f:
            for line in tqdm(f, total=get_num_lines(tax_data)):
                tax_row = line.split("\t")
                tax_levels = tax_row[1].split('__')
                tax_levels = [x.split(';')[0] for x in tax_levels[1:]]
                tax_levels = fix_greengenes_missing_data(tax_levels)
                otu_label = tax_row[0].replace(';','__').rstrip('\n').rstrip('_')

                conn.execute("INSERT OR IGNORE INTO TAX_DATA \
                (OTU, Kingdom, Phylum, Class, pOrder, Family, Genus, Species) \
                VALUES ('{0}', '{1}', '{2}', '{3}', '{4}', '{5}', '{6}', '{7}')\
                ".format(otu_label, tax_levels[0], tax_levels[1], tax_levels[2], \
                tax_levels[3], tax_levels[4], tax_levels[5], tax_levels[6]))
    f.close()
    return


def fix_greengenes_missing_data(taxa_list):
    """ Writes taxa levels as separate list items, passes levelAbbreviation__ for missing data"""
    blank_taxa_terms = ['k__', 'p__', 'c__', 'o__', 'f__', 'g__', 's__']
    missing_terms = 7 - len(taxa_list)
    if missing_terms != 0:
        taxa_list.extend(blank_taxa_terms[-missing_terms:])
    return taxa_list


def join_OTUs_w_Seqs(sqlite_file):
    """Joins sequence and OTU data into a new table"""
    conn,c = connect(sqlite_file)
    c.execute("""SELECT * FROM OTU_MAP, SEQ_DATA WHERE OTU_MAP.Header = SEQ_DATA.Header""")

    all_rows = c.fetchall()
    for row in tqdm(all_rows, total=len(all_rows), desc='Joining Seq/OTU names'):
        c.execute("INSERT OR IGNORE INTO OTUs_W_Seqs \
        VALUES ('{0}', '{1}', '{2}', '{3}')".format(row[1], row[6], row[7], row[9]))
    close(conn)
    return


def get_taxa_matches(sqlite_file, rank, taxa):
    """Returns a list of rows from the TAXA_DATA table that match a specific taxa"""
    with sqlite3.connect(sqlite_file) as conn:
        cursor = conn.cursor()
        cursor.execute('''SELECT * FROM TAX_DATA WHERE {0} = "{1}"'''.format(rank, taxa))
        all_rows = cursor.fetchall()
    return all_rows


def get_num_lines(file_path):
    fp = open(file_path, "r+")
    buf = mmap.mmap(fp.fileno(), 0)
    lines = 0
    while buf.readline():
        lines += 1
    return lines


### Functions to get database summary
def connect(sqlite_file):
    """ Make connection to an SQLite database file """
    conn = sqlite3.connect(sqlite_file)
    c = conn.cursor()
    return conn, c


def close(conn):
    """ Commit changes and close connection to the database """
    conn.commit()
    conn.close()


def total_rows(cursor, table_name, print_out=False):
    """ Returns the total number of rows in the database """
    cursor.execute('SELECT COUNT(*) FROM {}'.format(table_name))
    count = cursor.fetchall()
    if print_out:
        print('\nTotal rows: {}'.format(count[0][0]))
    return count[0][0]


def table_col_info(cursor, table_name, print_out=False):
    """ Returns a list of tuples with column informations:
    (id, name, type, notnull, default_value, primary_key)"""
    cursor.execute('PRAGMA TABLE_INFO({})'.format(table_name))
    info = cursor.fetchall()

    if print_out:
        print("\nColumn Info:\nID, Name, Type, NotNull, DefaultVal, PrimaryKey")
        for col in info:
            print(col)
    return info


def values_in_col(cursor, table_name, print_out=True):
    """ Returns a dictionary with columns as keys
    and the number of not-null entries as associated values."""
    cursor.execute('PRAGMA TABLE_INFO({})'.format(table_name))
    info = cursor.fetchall()
    col_dict = dict()
    for col in info:
        col_dict[col[1]] = 0
    for col in col_dict:
        c.execute('SELECT ({0}) FROM {1} WHERE {0} IS NOT NULL'.format(col, table_name))
        # In my case this approach resulted in a
        # better performance than using COUNT
        number_rows = len(c.fetchall())
        col_dict[col] = number_rows
    if print_out:
        print("\nNumber of entries per column:")
        for i in col_dict.items():
            print('{}: {}'.format(i[0], i[1]))
    return col_dict


def drop_table(sqlite_file, table_name):
    """ Drops a table"""
    conn = sqlite3.connect(sqlite_file)
    c = conn.cursor()
    c.execute('''DROP TABLE {0}'''.format(table_name))
    conn.commit()
    conn.close()
    return ('Dropped {0}'.format(table_name))
