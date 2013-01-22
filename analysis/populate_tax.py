from Bio import SeqIO
from Bio.SeqUtils import GC
import os, sys, getopt;
import MySQLdb
import inspect
import math
from datetime import *

debug = 0

def usage():
   sys.stderr.write(sys.argv[0] + ' accession version\n')
   sys.exit(2)

def db_connect():
    #TODO pull this out into a module -- I think Steve has one
    db = MySQLdb.connect(host="mysql", port=3306,db="steve_private", read_default_file="~/.my.cnf")
    cur = db.cursor(MySQLdb.cursors.DictCursor)
    return cur 

def db_connect_tax():
    db = MySQLdb.connect(host="mysql", port=3306,db="ncbitax", read_default_file="~/.my.cnf")
    cur = db.cursor(MySQLdb.cursors.DictCursor)
    return cur 


def read_data(cur, accession, version):
    cur.execute("""SELECT genome_id FROM replicon where accession = %s and version = %s""", (accession, version))
    genome = cur.fetchone()
    genome_id = genome['genome_id']
    
    cur.execute("""SELECT bioproject_id, modify_date, genome_name, score_nonstd, score_contig, score, tax_id FROM genome_stats where genome_id = %s""", (genome_id))
    gs = cur.fetchone()
    bioproject_id = gs['bioproject_id']
    modify_date = gs['modify_date']
    genome_name = gs['genome_name']
    score_nonstd = gs['score_nonstd']
    score_contig = gs['score_contig']
    score = gs['score']
    tax_id = gs['tax_id']

    
    cur.execute("""SELECT stat_size_bp, stat_number_nonstd_bases, stat_perc_at, stat_number_of_genes, stat_number_of_contigs, replicon_type, trna_count_accession, rrna_count_accession FROM replicon_stats where accession = %s and version = %s""", (accession, version))
    rep = cur.fetchone()
    total_bp = rep['stat_size_bp']
    nonstd_bp = rep['stat_number_nonstd_bases']
    perc_at = rep['stat_perc_at']
    gene_count = rep['stat_number_of_genes']
    contig_count = rep['stat_number_of_contigs']
    replicon_type = rep['replicon_type']
    trna_count = rep['trna_count_accession']
    rrna_count = rep['rrna_count_accession']

    replicon_count = 1
    chromosome_count = 0
    plasmid_count = 0
    if replicon_type == 'CHROMOSOME':
        chromosome_count = 1
    if replicon_type == 'PLASMID':
        plasmid_count = 1
    at_bp = math.floor(perc_at*total_bp/100)
    all_data = (modify_date, genome_id, bioproject_id, genome_name, score_nonstd, chromosome_count, plasmid_count, replicon_count, contig_count, score_contig, score, total_bp, nonstd_bp, gene_count, at_bp, rrna_count, trna_count)

    return tax_id, all_data


def write_data(cur, accession, version, tax_id, all_data, leaf):
    (modify_date, genome_id, bioproject_id, genome_name, score_nonstd, chromosome_count, plasmid_count, replicon_count, contig_count, score_contig, score, total_bp, nonstd_bp, gene_count, at_bp, rrna_count, trna_count) = all_data


    #first, insert or update data for the replicon itself iff we are at a leaf
    #second, insert or update data for the genome


    # REPLICON

    if leaf:
        cur.execute("""SELECT * FROM tax_stats where tax_id = %s and accession = %s and version = %s""", (tax_id, accession, version))
        rows = cur.fetchall()
        if rows:
            cur.execute("""UPDATE tax_stats SET 
            score_nonstd_sum = %s,
            chromosome_count = %s,
            plasmid_count = %s,
            replicon_count = %s,
            contig_count = %s,
            score_contig_sum = %s,
            score_sum = %s,
            total_bp = %s,
            nonstd_bp = %s,
            gene_count = %s,
            at_bp = %s,
            rrna_count = %s,
            trna_count = %s
            WHERE tax_id = %s and accession = %s and version = %s
            """, (score_nonstd, chromosome_count, plasmid_count, replicon_count, contig_count, score_contig, score, total_bp, nonstd_bp, gene_count, at_bp, rrna_count, trna_count, tax_id, accession, version))
        else:
            cur.execute("""INSERT INTO tax_stats (
            tax_id,
            accession,
            version,
            score_nonstd_sum,
            chromosome_count,
            plasmid_count,
            replicon_count,
            contig_count,
            score_contig_sum,
            score_sum,
            total_bp,
            nonstd_bp,
            gene_count,
            at_bp,
            rrna_count,
            trna_count
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""", (tax_id, accession, version, score_nonstd, chromosome_count, plasmid_count, replicon_count, contig_count, score_contig, score, total_bp, nonstd_bp, gene_count, at_bp, rrna_count, trna_count))
    

    # GENOME
    
    #look in the tax path to see if this entry is already in the tax_* tables
    cur.execute("""SELECT * from tax_path where tax_id = %s and accession = %s and version = %s""", (tax_id, accession, version))
    rows = cur.fetchall()
    if rows:
        if debug:
            sys.stderr.write("create_tax_entries: not updating row in tax_path for existing accession = " + accession + ", version = " + str(version) + ", tax_id = " + str(tax_id) + "\n")
    else: #now we have data to insert or update
        #first, update the path table so we know our data will be in the stats table
        cur.execute("""INSERT INTO tax_path (tax_id, accession, version) VALUES (%s, %s, %s)""", (tax_id, accession, version))
        cur.execute("""SELECT modify_date, genome_id, bioproject_id, genome_name, score_nonstd_sum, chromosome_count, plasmid_count, replicon_count, contig_count, score_contig_sum, score_sum, total_bp, nonstd_bp, gene_count, at_bp, rrna_count, trna_count, genome_count FROM tax_stats where tax_id = %s and accession IS NULL""", (tax_id))
        row = cur.fetchone()
        if row: #then info has been added for other replicons, so we need to integrate ours
            if (modify_date <= row['modify_date']):
                modify_date = row['modify_date']
            if genome_id != row['genome_id']:
                genome_id = 0
            if bioproject_id != row['bioproject_id']:
                bioproject_id = 0
            if genome_name != row['genome_name']:
                genome_name = ""
            if leaf == 0:
                genome_id = 0
                bioproject_id = 0
                genome_name = ""
            score_nonstd_sum = score_nonstd + row['score_nonstd_sum']
            chromosome_count = chromosome_count + row['chromosome_count']
            plasmid_count = plasmid_count + row['plasmid_count']
            replicon_count = replicon_count + row['replicon_count']
            contig_count = contig_count + row['contig_count']
            score_contig_sum = score_contig + row['score_contig_sum']
            score_sum = score + row['score_sum']
            total_bp = total_bp + row['total_bp']
            nonstd_bp = nonstd_bp + row['nonstd_bp']
            gene_count = gene_count + row['gene_count']
            at_bp = at_bp + row['at_bp']
            rrna_count = rrna_count + row['rrna_count']
            trna_count = trna_count + row['trna_count']
            genome_count = 1 + row['genome_count']
            

            cur.execute("""UPDATE tax_stats set 
            modify_date = %s,
            genome_id = %s,
            bioproject_id = %s,
            genome_name = %s,
            score_nonstd_sum = %s,
            chromosome_count = %s,
            plasmid_count = %s,
            replicon_count = %s,
            contig_count = %s,
            score_contig_sum = %s,
            score_sum = %s,
            total_bp = %s,
            nonstd_bp = %s,
            gene_count = %s,
            at_bp = %s,
            rrna_count = %s,
            trna_count = %s,
            genome_count = %s
            WHERE tax_id = %s""", (modify_date, genome_id, bioproject_id, genome_name, score_nonstd, chromosome_count, plasmid_count, replicon_count, contig_count, score_contig, score, total_bp, nonstd_bp, gene_count, at_bp, rrna_count, trna_count, genome_count, tax_id))
        else:
            if leaf == 0:
                genome_id = 0
                bioproject_id = 0
                genome_name = ""
            cur.execute("""INSERT INTO tax_stats (
            tax_id,
            modify_date,
            genome_id,
            bioproject_id,
            genome_name,
            score_nonstd_sum,
            chromosome_count,
            plasmid_count,
            replicon_count,
            contig_count,
            score_contig_sum,
            score_sum,
            total_bp,
            nonstd_bp,
            gene_count,
            at_bp,
            rrna_count,
            trna_count,
            genome_count
            ) VALUES (%s, %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s, %s)""", (tax_id, modify_date, genome_id, bioproject_id, genome_name, score_nonstd, chromosome_count, plasmid_count, replicon_count, contig_count, score_contig, score, total_bp, nonstd_bp, gene_count, at_bp, rrna_count, trna_count, 1))

def get_parent(cur, tax_id):

    #first, see if the tax_name is in our local table and if not, add it

    tax_cur = db_connect_tax()

    cur.execute("""SELECT tax_name FROM tax_stats where tax_id = %s and tax_name is NOT NULL and accession is NULL""", (tax_id)) 
    row = cur.fetchone()
    if row:
        n = row['tax_name']
    else:
        tax_cur.execute("""SELECT name_txt from names where name_class = "scientific name" and tax_id = %s""", (tax_id))
        tax_name = tax_cur.fetchone()['name_txt']
        cur.execute("""UPDATE tax_stats SET tax_name=%s WHERE tax_id=%s and accession is NULL""", (tax_name, tax_id))

    tax_cur.execute("""SELECT parent_tax_id FROM nodes WHERE tax_id = %s""", (tax_id))
    return tax_cur.fetchone()['parent_tax_id']
    

def main(argv):
    try:
        accession = argv[0]
        version = argv[1]
    except:
        usage()
    if accession == "" or version == "":
        usage()
    else:
        cur = db_connect()
        tax_id, all_data = read_data(cur, accession, version)
        leaf = 1
        while tax_id != 1:
            write_data(cur, accession, version, tax_id, all_data, leaf)
            tax_id = get_parent(cur, tax_id)
            leaf = 0
        write_data(cur, accession, version, 1, all_data, leaf) #might as well update the root too (even though it's never displayed)


if __name__ == "__main__":
   main(sys.argv[1:])
