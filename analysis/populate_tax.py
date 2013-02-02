from Bio import SeqIO
from Bio.SeqUtils import GC
import os, sys, getopt;
import MySQLdb
import inspect
import math
from datetime import *
from genomeanalysis.common import *

debug = 0

def usage():
   sys.stderr.write(sys.argv[0] + ' accession version\n')
   sys.exit(2)

def db_connect_tax():
    db = MySQLdb.connect(host="mysql", port=3306,db="ncbitax", read_default_file="~/.my.cnf")
    cur = db.cursor(MySQLdb.cursors.DictCursor)
    return cur 

def score_subtract_due_to_bases(nonstd_bp, total_bp):
    score_bases = 0.1 * math.ceil(100.0 * nonstd_bp / total_bp)
    if score_bases > 0.9:
        # don't subtract more than 0.9 from the final score
        score_bases = 0.9
    return score_bases

def score_subtract_due_to_contigs(contig_count, replicon_count, total_bp):
    score_contigs = 0.2 * math.ceil( 1000000.0 * (contig_count - replicon_count) / (25.0 * total_bp))
    if score_contigs > 0.9:
        # don't subtract more than 0.9 from the final score
        score_contigs = 0.9
    return score_contigs

def calculate_score_replicon(nonstd_bp, total_bp, contig_count, replicon_count):
    score = 1
    score = score - score_subtract_due_to_bases(nonstd_bp, total_bp)
    score = score - score_subtract_due_to_contigs(contig_count, replicon_count, total_bp)
    if score < 0.1:
        # min score for a genome with a sequence is 0.1
        score = 0.1
    return score

def merge_score_genome(nonstd_bp, total_bp, contig_count, replicon_count):
    #calculated the same way as for the replicon, since the score is over the whole genome
    return calculate_score_replicon(nonstd_bp, total_bp, contig_count, replicon_count)

def merge_score_tax(new_score, score_numerator, score_denominator):
    #calculated differently, since it is an average of the scores
    #the score for a tax entry is the average of the scores of the genomes with it
    #when we add a new entry, need to add to the numerator (the sum of all previous scores) and divide by the denominator (number of genomes)
    return merge_average_tax(new_score, score_numerator, score_denominator)

def merge_percent_genome(n1, n2, d1, d2):
    return 100.0*merge_count(n1, n2) / merge_count(d1, d2)

def merge_average_tax(new_value, old_numerator, old_denominator):
    #this is an average
    return (float(new_value) + float(old_numerator)) / (1.0 + float(old_denominator))

def merge_gene_density_genome(n1, n2, d1, d2):
    #per kilo base
    return 1000.0*merge_count(n1, n2) / merge_count(d1, d2)

def merge_count(x, y):
    return float(x) + float(y)

def merge_date_genome(new, existing):
    if (new >= existing):
        return new
    else:
        sys.stderr.write("new date (" + str(new) + ") is older than existing date (" + str(existing) + ").  Stopping because this makes no sense\n")
        exit(1)

def merge_date_tax(new, existing):
    if (new >= existing):
        return new
    else:
        return existing



def read_data(cur, accession, version):
    # reads data out of replicon table

    cur.execute("""SELECT genome_id, stat_number_nonstd_bases, stat_number_of_contigs, stat_size_bp, stat_perc_at, stat_number_of_genes, replicon_type FROM replicon WHERE accession = %s and version = %s""", (accession, version))
    row = cur.fetchone()
    genome_id = row['genome_id']
    total_bp = row['stat_size_bp']
    nonstd_bp = row['stat_number_nonstd_bases']
    percent_at = row['stat_perc_at']
    gene_count = row['stat_number_of_genes']
    contig_count = row['stat_number_of_contigs']
    replicon_type = row['replicon_type']
    
    cur.execute("""SELECT bioproject_id, modify_date FROM bioproject WHERE bioproject_id IN (select bioproject_id from genome where genome_id = %s)""", (genome_id))
    row = cur.fetchone()
    bioproject_id = row['bioproject_id']
    modify_date = row['modify_date']

    cur.execute("""SELECT genome_name, tax_id FROM genome WHERE genome_id = %s""", (genome_id))
    row = cur.fetchone()
    genome_name = row['genome_name']
    tax_id = row['tax_id']

    cur.execute("""SELECT count(*) as trna_count FROM trna WHERE accession = %s and version = %s""", (accession, version))
    row = cur.fetchone()
    trna_count = row['trna_count']

    cur.execute("""SELECT count(*) as rrna_count FROM rrna WHERE accession = %s and version = %s""", (accession, version))
    row = cur.fetchone()
    rrna_count = row['rrna_count']

    replicon_count = 1
    chromosome_count = 0
    plasmid_count = 0
    if replicon_type == 'CHROMOSOME':
        chromosome_count = 1
    if replicon_type == 'PLASMID':
        plasmid_count = 1

    at_bp = math.floor(percent_at*total_bp/100)

    score = calculate_score_replicon(nonstd_bp, total_bp, contig_count, replicon_count)
    gene_density = merge_gene_density_genome(gene_count, 0, total_bp, 0)

    all_data = (modify_date, genome_id, bioproject_id, genome_name, chromosome_count, plasmid_count, replicon_count, contig_count, total_bp, nonstd_bp, gene_count, at_bp, rrna_count, trna_count, replicon_type, score, gene_density, percent_at)

    return tax_id, all_data







def update_replicon(cur, accession, version, tax_id, all_data):
    (modify_date, genome_id, bioproject_id, genome_name, chromosome_count, plasmid_count, replicon_count, contig_count, total_bp, nonstd_bp, gene_count, at_bp, rrna_count, trna_count, replicon_type, score, gene_density, percent_at) = all_data
    
    cur.execute("""SELECT * FROM replicon_stats_new where accession = %s and version = %s""", (accession, version))
    row = cur.fetchone()
    if row:
        if (genome_id != row['genome_id']):
            system.stderr.write("genome_id does not match genome_id in db.  Stopping.\n");

    else:
        cur.execute("""INSERT INTO replicon_stats_new (
        genome_id,
        accession,
        version,
        replicon_type,
        score,
        gene_density,
        percent_at,
        total_bp,
        gene_count,
        trna_count,
        rrna_count,
        nonstd_bp,
        at_bp,
        contig_count
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""", (
        genome_id,
        accession,
        version,
        replicon_type,
        score,
        merge_gene_density_genome(gene_count, 0, total_bp, 0),
        merge_percent_genome(at_bp, 0, total_bp, 0),
        total_bp,
        gene_count,
        trna_count,
        rrna_count,
        nonstd_bp,
        at_bp,
        contig_count
        ))

def update_genome(cur, accession, version, tax_id, all_data):
    # first check if this accession has been included in the genome before
    # if included, then don't add it again
    # otherwise add it

    (modify_date, genome_id, bioproject_id, genome_name, chromosome_count, plasmid_count, replicon_count, contig_count, total_bp, nonstd_bp, gene_count, at_bp, rrna_count, trna_count, replicon_type, score, gene_density, percent_at) = all_data

    cur.execute("""SELECT genome_id from genome_path where accession = %s and version = %s""", (accession, version));
    row = cur.fetchone()

    # have I been doing too much c programming?
    old_score = 0.0
    genome_score = 0.0
    old_gene_density = 0.0
    genome_gene_density = 0.0
    old_percent_at = 0.0
    genome_percent_at = 0.0
    genome_count = 0

    if row:
        # nothing to do, just do a 
        # sanity check to make sure things are ok
        if (genome_id != row['genome_id']):
            sys.stderr.write("genome_id in all_data is " + str(genome_id) + " but genome_id in genome_path is " + str(row['genome_id']) + ". Stopping because this is broken.\n")
            exit(1)

    else:
        # TODO this should all be in a transaction
        cur.execute("""INSERT INTO genome_path (genome_id, accession, version) VALUES (%s, %s, %s)""", (genome_id, accession, version))


        # get the current values from genome_stats
        cur.execute("""SELECT 
        modify_date,
        tax_id,
        bioproject_id,
        genome_name,
        score,
        gene_density,
        percent_at,
        chromosome_count,
        plasmid_count,
        replicon_count,
        contig_count,
        total_bp,
        gene_count,
        rrna_count,
        trna_count,
        at_bp,
        nonstd_bp,
        percent_nonstd_bp
        FROM genome_stats_new where genome_id = %s""", (genome_id))
        row = cur.fetchone()

        if (row):
            # save some values to pass to update_tax
            old_score = row['score']
            genome_score = merge_score_genome(merge_count(nonstd_bp, row['nonstd_bp']), merge_count(total_bp, row['total_bp']), merge_count(contig_count, row['contig_count']), merge_count(replicon_count, row['replicon_count']))

            old_gene_density = row['gene_density']
            genome_gene_density = merge_gene_density_genome(gene_count, row['gene_count'], total_bp, row['total_bp'])

            old_percent_at = row['percent_at']
            genome_percent_at = merge_percent_genome(at_bp, row['at_bp'], total_bp, row['total_bp'])

            genome_count = 0

            # then combine the current and old values appropropriately for each field and update the db
            cur.execute("""UPDATE genome_stats_new SET
            modify_date = %s,
            tax_id = %s,
            bioproject_id = %s,
            genome_name = %s,
            score = %s,
            gene_density = %s,
            percent_at = %s,
            chromosome_count = %s,
            plasmid_count = %s,
            replicon_count = %s,
            contig_count = %s,
            total_bp = %s,
            gene_count = %s,
            rrna_count = %s,
            trna_count = %s,
            at_bp = %s,
            nonstd_bp = %s,
            percent_nonstd_bp = %s
            WHERE genome_id = %s""", (
            merge_date_genome(modify_date, row['modify_date']),
            tax_id,
            bioproject_id,
            genome_name,
            genome_score,
            genome_gene_density,
            genome_percent_at,
            merge_count(chromosome_count, row['chromosome_count']),
            merge_count(plasmid_count, row['plasmid_count']),
            merge_count(replicon_count, row['replicon_count']),
            merge_count(contig_count, row['contig_count']),
            merge_count(total_bp, row['total_bp']),
            merge_count(gene_count, row['gene_count']),
            merge_count(rrna_count, row['rrna_count']),
            merge_count(trna_count, row['trna_count']),
            merge_count(at_bp, row['at_bp']),
            merge_count(nonstd_bp, row['nonstd_bp']),
            merge_percent_genome(nonstd_bp, row['nonstd_bp'], total_bp, row['total_bp']),
            genome_id
            ))

        else:
            genome_score = score
            genome_gene_density = merge_gene_density_genome(gene_count, 0, total_bp, 0)
            genome_percent_at = merge_percent_genome(at_bp, 0, total_bp, 0)
            genome_count = 1


            # there is no entry in genome_stats, so add a new one
            cur.execute("""INSERT INTO genome_stats_new (
            genome_id,
            modify_date,
            tax_id,
            bioproject_id,
            genome_name,
            score,
            gene_density,
            percent_at,
            chromosome_count,
            plasmid_count,
            replicon_count,
            contig_count,
            total_bp,
            gene_count,
            rrna_count,
            trna_count,
            at_bp,
            nonstd_bp,
            percent_nonstd_bp
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""", (
            genome_id,
            modify_date,
            tax_id,
            bioproject_id,
            genome_name,
            genome_score,
            genome_gene_density,
            genome_percent_at,
            chromosome_count,
            plasmid_count,
            replicon_count,
            contig_count,
            total_bp,
            gene_count,
            rrna_count,
            trna_count,
            at_bp,
            nonstd_bp,
            merge_percent_genome(nonstd_bp, 0, total_bp, 0),
            ))
        
    # bundle up the data that needs to be passed into tax (since it stores averages over the genome, not over the replicon)
    # only add genome_count into the tax table if this is the very first time we have seen this genome
    
    
    genome_data = { 'score': genome_score, 'old_score': old_score, 'gene_density': genome_gene_density, 'old_gene_density': old_gene_density, 'percent_at': genome_percent_at, 'old_percent_at': old_percent_at, 'genomes_to_add': genome_count}
    return genome_data


def replace_tax_entry_average(new_value, old_value, cur_value_in_db_num, cur_value_in_db_den, genomes_to_add):
    updated_value = (cur_value_in_db_num - old_value + new_value) / (cur_value_in_db_den + genomes_to_add);
    return updated_value

def replace_tax_entry_sum(new_value, old_value, cur_value_in_db):
    updated_value = cur_value_in_db - old_value + new_value
    return updated_value


def update_tax(cur, accession, version, tax_id, all_data, genome_data):
    (modify_date, genome_id, bioproject_id, genome_name, chromosome_count, plasmid_count, replicon_count, contig_count, total_bp, nonstd_bp, gene_count, at_bp, rrna_count, trna_count, replicon_type, score, gene_density, percent_at) = all_data

    #NEW
    cur.execute("""SELECT tax_id from tax_path_new where tax_id = %s and accession = %s and version = %s""", (tax_id, accession, version));
    row = cur.fetchone()

    if row:
        sys.stderr.write("create_tax_entries: not updating row in tax_path_new for existing accession = " + accession + ", version = " + str(version) + ", tax_id = " + str(tax_id) + "\n")
        # nothing to do, just do a 
        # sanity check to make sure things are ok

    else:
        # TODO this should all be in a transaction
        #NEW
        cur.execute("""INSERT INTO tax_path_new (tax_id, accession, version) VALUES (%s, %s, %s)""", (tax_id, accession, version))
        # get the current values from tax_stats
        #NEW
        cur.execute("""SELECT 
        modify_date, 
        genome_count,
        score,
        score_numerator,
        gene_density,
        gene_density_numerator,
        percent_at,
        percent_at_numerator,
        chromosome_count, 
        plasmid_count, 
        replicon_count, 
        total_bp, 
        gene_count, 
        rrna_count, 
        trna_count, 
        contig_count, 
        nonstd_bp, 
        at_bp, 
        genome_count FROM tax_stats_new where tax_id = %s""", (tax_id))
        row = cur.fetchone()

        if row: #then info has been added for other replicons, so we need to integrate ours

            # for averages over replicon, set
            #score = merge_score_tax(score, row['score_numerator'], row['genome_count']),
            #score_numerator = merge_count(score, row['score_numerator']),
            #gene_density = merge_average_tax(gene_density, row['gene_density_numerator'], row['genome_count']),
            #gene_density_numerator = merge_count(gene_density, row['gene_density_numerator']),
            #percent_at = merge_average_tax(percent_at, row['percent_at_numerator'], row['genome_count']),
            #percent_at_numerator merge_count(percent_at, row['percent_at_numerator']),


            # but we want averages over genome, so do the following instead


            #NEW
            cur.execute("""UPDATE tax_stats_new set 
            modify_date = %s,
            genome_count = %s,
            score = %s,
            score_numerator = %s,
            gene_density = %s,
            gene_density_numerator = %s,
            percent_at = %s,
            percent_at_numerator = %s,
            chromosome_count = %s,
            plasmid_count = %s,
            replicon_count = %s,
            total_bp = %s,
            gene_count = %s,
            rrna_count = %s,
            trna_count = %s,
            contig_count = %s,
            nonstd_bp = %s,
            at_bp = %s
            WHERE tax_id = %s""", (
            merge_date_tax(modify_date, row['modify_date']),
            merge_count(genome_data['genomes_to_add'], row['genome_count']),
            replace_tax_entry_average(genome_data['score'], genome_data['old_score'], row['score_numerator'], row['genome_count'], genome_data['genomes_to_add']),
            replace_tax_entry_sum(genome_data['score'], genome_data['old_score'], row['score_numerator']),
            replace_tax_entry_average(genome_data['gene_density'], genome_data['old_gene_density'], row['gene_density_numerator'], row['genome_count'], genome_data['genomes_to_add']),
            replace_tax_entry_sum(genome_data['gene_density'], genome_data['old_gene_density'], row['gene_density_numerator']),
            replace_tax_entry_average(genome_data['percent_at'], genome_data['old_percent_at'], row['percent_at_numerator'], row['genome_count'], genome_data['genomes_to_add']),
            replace_tax_entry_sum(genome_data['percent_at'], genome_data['old_percent_at'], row['percent_at_numerator']),
            merge_count(chromosome_count, row['chromosome_count']),
            merge_count(plasmid_count, row['plasmid_count']),
            merge_count(replicon_count, row['replicon_count']),
            merge_count(total_bp, row['total_bp']),
            merge_count(gene_count, row['gene_count']),
            merge_count(rrna_count, row['rrna_count']),
            merge_count(trna_count, row['trna_count']),
            merge_count(contig_count, row['contig_count']),
            merge_count(nonstd_bp, row['nonstd_bp']),
            merge_count(at_bp, row['at_bp']),
            tax_id))

        else:
            #NEW
            cur.execute("""INSERT INTO tax_stats_new (
            tax_id,
            modify_date,
            genome_count,
            score,
            score_numerator,
            gene_density,
            gene_density_numerator,
            percent_at,
            percent_at_numerator,
            chromosome_count,
            plasmid_count,
            replicon_count,
            total_bp,
            gene_count,
            rrna_count,
            trna_count,
            contig_count,
            nonstd_bp,
            at_bp
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""", (
            tax_id, 
            modify_date, 
            genome_data['genomes_to_add'],
            genome_data['score'],
            genome_data['score'],
            genome_data['gene_density'],
            genome_data['gene_density'],
            genome_data['percent_at'],
            genome_data['percent_at'],
            chromosome_count, 
            plasmid_count, 
            replicon_count, 
            total_bp, 
            gene_count, 
            rrna_count, 
            trna_count, 
            contig_count, 
            nonstd_bp, 
            at_bp 
            ))

        tax_cur = db_connect_tax()
        tax_cur.execute("""SELECT name_txt from names where name_class = "scientific name" and tax_id = %s""", (tax_id))
        tax_name = tax_cur.fetchone()['name_txt']
        #NEW
        cur.execute("""UPDATE tax_stats_new SET tax_name=%s WHERE tax_id=%s""", (tax_name, tax_id))


def get_parent(cur, tax_id):

    #first, see if the tax_name is in our local table and if not, add it

    tax_cur = db_connect_tax()
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
        (db, cur) = db_connect_transaction()
        try:
            if (not genome_is_valid(get_genome_id(accession, version))):
                sys.stderr.write("genome is not valid\n")
                exit(1)

            # this assumes that the correct data is sitting in the base tables. 
            # if the base data changes, then the changing program is responsible for removing the tax_, genome_ and replicon_stats entries
            # they can be repopulated with the appropriate update_* functions

            tax_id, all_data = read_data(cur, accession, version)
            update_replicon(cur, accession, version, tax_id, all_data)
            genome_data = update_genome(cur, accession, version, tax_id, all_data)
        
            while tax_id not in (1, 68336, 2, 2157, 131550, 51290, 200795, 51290): #don't write data for parents of top level phyla; exclude
                update_tax(cur, accession, version, tax_id, all_data, genome_data)
                tax_id = get_parent(cur, tax_id)


            update_tax(cur, accession, version, 1, all_data, genome_data) #might as well update the root too (even though it's never displayed)
            db.commit()
        except:
            db.rollback()
            sys.stderr.write("something happened with accession " + accession + " version " + version + ", not writing to database\n")
            exit(1)

if __name__ == "__main__":
   main(sys.argv[1:])
