import MySQLdb
import os, sys
import inspect
import math
from datetime import *
from Bio import SeqIO
from Bio.SeqUtils import GC
import traceback

top_level_phyla = (1, 68336, 2, 2157, 131550, 51290, 200795, 51290)

env = "prod"

def get_env():
    env = "prod"
    return env

def db_connect_transaction(env):
    if env == "test":
        db = MySQLdb.connect(host="mysql", port=3306,db="steve_private", read_default_file="~/.my.cnf")
    elif env == "prod":
        db = MySQLdb.connect(host="mysql", port=3306,db="gatlas", read_default_file="~/.my.cnf")
    else: 
        sys.stderr.write("env " + env + " is not recognized; can't connect to database in dbconnect(env)")
        exit(1)
    db.autocommit(False)
    cur = db.cursor(MySQLdb.cursors.DictCursor)
    return (db, cur) 

def db_connect(env):
    #sys.stderr.write("connecting to db env " + env + "\n")
    if env == "test":
        db = MySQLdb.connect(host="mysql", port=3306,db="steve_private", read_default_file="~/.my.cnf")
    elif env == "prod":
        db = MySQLdb.connect(host="mysql", port=3306,db="gatlas", read_default_file="~/.my.cnf")
    else: 
        sys.stderr.write("env " + env + " is not recognized; can't connect to database in dbconnect(env)")
        exit(1)
    cur = db.cursor(MySQLdb.cursors.DictCursor)
    return cur 

def db_connect_tax():    
    db = MySQLdb.connect(host="mysql", port=3306,db="ncbitax", read_default_file="~/.my.cnf")
    cur = db.cursor(MySQLdb.cursors.DictCursor)
    return cur 

def parse_filename(inputfile):
    basename = os.path.basename(inputfile)
    filename, extension = os.path.splitext(basename)
    return parse_string(filename)

def parse_string(string):
    sys.stderr.write("parse string has: " + string + "\n")
    if "_" in string: # then this is a complete genome in the form accession_version
        accession = string[:string.find('_')] # everything up to the underscore is the accession
        if "." in string:
            version = string[-5]
        else:
            version = string[-1] # version is the last thing before the 3 character extension
    elif "prodigal" in string: #then this is an SRA or other genomes source
        accession = string[:12]
        version = "0"
    elif string.startswith("SRA") or string.startswith("TRD"):
        accession = string[:12]
        version = "0"
    elif "." in string: #then this is in an gbk or rnammer file and is in the form accession.version
        front = string[:string.find('.')]
        back = string[string.find('.')+1:]
        if back == "gbk": #then this is a WGS formatted accession
            accession = front
            version = string[-5]
        else:
            accession = front
            version = string[-1]
    else:
        accession = string
        version = "0"
    return (accession, version)

def get_genome_id(accession, version):
   print "for accession " + accession 
   cur = db_connect(env)
   try:
      cur.execute("""SELECT genome_id from replicon where accession = %s and version = %s""", (accession, version));
      row = cur.fetchone()
      if (row):
         genome_id = row['genome_id']
         print " have genomeid " + str(genome_id)
         return genome_id
      else:
         sys.stderr.write("no genome_id found for accession=" + accession + " and version=" + version + "\n")
         return -1
   except:
      sys.stderr.write("no genome_id found for accession=" + accession + " and version=" + version + "\n")
      return -1

def get_replicon_stats(accession, version):
    cur = db_connect(env)
    try:
        cur.execute("""SELECT * from displaygenome_replicon_stats where accession = %s and version = %s""", (accession, version))
        row = cur.fetchone()
        if (row):
            return row
        else:
            return None
    except:
        return None

def get_genome_validity(genome_id):
   cur = db_connect(env)
   cur.execute("""SELECT genome_validity from genome where genome_id = %s""", (genome_id))
   row = cur.fetchone()
   genome_validity = row['genome_validity']
   return genome_validity

def genome_is_valid(genome_id):
   tag = get_genome_validity(genome_id)
   #if (tag == "WARNINGS" or tag == "INVALID"):
   if (tag == "INVALID"):
      return False
   else:
      return True

def get_replicons(cur, genome_id):
    cur.execute("""SELECT * FROM replicon WHERE genome_id = %s""", (genome_id))
    replicons = cur.fetchall()
    return replicons;

def get_tax_parent(cur, env, tax_id):
    tax_cur = db_connect_tax()
    tax_cur.execute("""SELECT parent_tax_id FROM nodes WHERE tax_id = %s""", (tax_id))
    return tax_cur.fetchone()['parent_tax_id']
    
def up_tax_tree(cur, accession, version):
    #returns current tax_id, and all parents up to root
    print "getting parents for accession = " + accession + " version = " + str(version)
    cur.execute("""SELECT tax_id FROM displaygenome_replicon_stats WHERE accession = %s and version = %s""", (accession, version))
    row = cur.fetchone()
    tax_id = row['tax_id']
    tax_ids = [tax_id]
    new_tax_id = get_tax_parent(cur, "prod", tax_id)
    while new_tax_id not in (1, 68336, 2, 2157, 131550, 51290, 200795, 51290):
        tax_ids.append(new_tax_id)
        new_tax_id = get_tax_parent(cur, "prod", new_tax_id)
        print "got list: " + str(tax_ids)
    tax_ids.append(1)
    return tax_ids

def get_tax_id(cur, accession, version):
    cur.execute("""SELECT tax_id FROM displaygenome_replicon_stats WHERE accession = %s and version = %s""", (accession, version))
    row = cur.fetchone()
    tax_id = row['tax_id']
    return tax_id

def fix_one(cur, env, accession, version):
    # we fix the values by removing the entry from the dg* tables (which uses data in the dg_replicon table only), then re-adding it (which re-reads data from the base replicon tables)
    
    genome_id = get_genome_id(accession, version)
    parents = up_tax_tree(cur, accession, version)

    replicon_data = remove_accession_from_dgreplicon(cur, env, accession, version)
    genome_data = remove_accession_from_dggenome(cur, env, accession, version, genome_id)

    for tax_id in parents:
        remove_accession_from_dgtax(cur, env, tax_id, accession, version, replicon_data, genome_data)

    tax_id, new_replicon_data = read_base_replicon_data(cur, accession, version)

    add_accession_to_dgreplicon(cur, env, accession, version, tax_id, new_replicon_data)
    new_genome_data = add_accession_to_dggenome(cur, env, accession, version, tax_id, new_replicon_data)
    add_accession_to_dgtax(cur, env, accession, version, tax_id, new_replicon_data, new_genome_data)

    for tax_id in parents:
        add_accession_to_dgtax(cur, env, accession, version, tax_id, new_replicon_data, new_genome_data)

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


def read_base_replicon_data(cur, accession, version):
    # reads data out of replicon table

    cur.execute("""SELECT genome_id, stat_number_nonstd_bases, stat_number_of_contigs, stat_size_bp, stat_perc_at, stat_number_of_genes, replicon_type FROM replicon WHERE accession = %s and version = %s""", (accession, version))
    row = cur.fetchone()
    if row:
        genome_id = row['genome_id']
        total_bp = row['stat_size_bp']
        nonstd_bp = row['stat_number_nonstd_bases']
        percent_at = row['stat_perc_at']
        gene_count = row['stat_number_of_genes']
        contig_count = row['stat_number_of_contigs']
        replicon_type = row['replicon_type']
        
        cur.execute("""SELECT bioproject_id, modify_date FROM bioproject WHERE bioproject_id IN (select bioproject_id from genome where genome_id = %s)""", (genome_id))
        row = cur.fetchone()
        if row:
            bioproject_id = row['bioproject_id']
            release_date = row['modify_date']
        else:
            bioproject_id = 0
            release_date = 0
    
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

        all_data = (release_date, genome_id, bioproject_id, genome_name, chromosome_count, plasmid_count, replicon_count, contig_count, total_bp, nonstd_bp, gene_count, at_bp, rrna_count, trna_count, replicon_type, score, gene_density, percent_at)

        return tax_id, all_data
    else:
        return None











def get_table(table, env):
    if env == 'test':
        if table == 'dgreplicon':
            return 'test_dg_replicon_stats'
        elif table == 'dggenome':
            return 'test_dg_genome_stats'
        elif table == 'dgtax':
            return 'test_dg_tax_stats'
        elif table == 'genome_path':
            return 'test_genome_path'
        elif table == 'tax_path':
            return 'test_tax_path'
    elif env == 'prod':
        if table == 'dgreplicon':
            return 'displaygenome_replicon_stats'
        elif table == 'dggenome':
            return 'displaygenome_genome_stats'
        elif table == 'dgtax':
            return 'displaygenome_tax_stats'
        elif table == 'genome_path':
            return 'genome_path'
        elif table == 'tax_path':
            return 'tax_path'
    sys.stderr.write("could not resolve table name " + table + " with environment " + env + "\n")
    exit(1)

def add_accession_to_dgreplicon(cur, env, accession, version, tax_id, all_data):
    (release_date, genome_id, bioproject_id, genome_name, chromosome_count, plasmid_count, replicon_count, contig_count, total_bp, nonstd_bp, gene_count, at_bp, rrna_count, trna_count, replicon_type, score, gene_density, percent_at) = all_data

    sys.stderr.write( "ADDING ACCESSION " + accession + " TO REPLICON" + "\n")
    
    table = get_table('dgreplicon', env)
    cur.execute("""SELECT * FROM %s where accession = %%s and version = %%s""" % table, (accession, version))
    row = cur.fetchone()
    if row:
        if (genome_id != row['genome_id']):
            sys.stderr.write("genome_id does not match genome_id in db.  Stopping.\n");

    else:
        table = get_table('dgreplicon', env)
        cur.execute("""INSERT INTO %s (
        tax_id,
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
        ) VALUES (%%s, %%s, %%s, %%s, %%s, %%s, %%s, %%s, %%s, %%s, %%s, %%s, %%s, %%s, %%s)""" 
        % table,
        (tax_id,
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

def add_accession_to_dggenome(cur, env, accession, version, tax_id, all_data):
    # first check if this accession has been included in the genome before
    # if included, then don't add it again
    # otherwise add it

    (release_date, genome_id, bioproject_id, genome_name, chromosome_count, plasmid_count, replicon_count, contig_count, total_bp, nonstd_bp, gene_count, at_bp, rrna_count, trna_count, replicon_type, score, gene_density, percent_at) = all_data

    sys.stderr.write( "ADDING " + accession + " TO GENOME" + "\n")

    table = get_table('genome_path', env)
    cur.execute("""SELECT genome_id FROM %s WHERE accession = %%s and version = %%s""" % table, (accession, version));
    row = cur.fetchone()

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
        table = get_table('genome_path', env)
        cur.execute("""INSERT INTO %s (genome_id, accession, version) VALUES (%%s, %%s, %%s)""" % table, (genome_id, accession, version))
        sys.stderr.write( "ADDING " + accession + " TO GENOME PATH" + "\n")


        # get the current values from genome_stats
        table = get_table('dggenome', env)
        cur.execute("""SELECT 
        release_date,
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
        FROM %s where genome_id = %%s""" % table, (genome_id))
        row = cur.fetchone()

        if (row):
            # save some values to pass to add_replicon_to_tax
            old_score = row['score']
            genome_score = merge_score_genome(merge_count(nonstd_bp, row['nonstd_bp']), merge_count(total_bp, row['total_bp']), merge_count(contig_count, row['contig_count']), merge_count(replicon_count, row['replicon_count']))

            old_gene_density = row['gene_density']
            genome_gene_density = merge_gene_density_genome(gene_count, row['gene_count'], total_bp, row['total_bp'])

            old_percent_at = row['percent_at']
            genome_percent_at = merge_percent_genome(at_bp, row['at_bp'], total_bp, row['total_bp'])

            genome_count = 0

            # then combine the current and old values appropropriately for each field and update the db
            table = get_table('dggenome', env)
            cur.execute("""UPDATE %s SET
            release_date = %%s,
            tax_id = %%s,
            bioproject_id = %%s,
            genome_name = %%s,
            score = %%s,
            gene_density = %%s,
            percent_at = %%s,
            chromosome_count = %%s,
            plasmid_count = %%s,
            replicon_count = %%s,
            contig_count = %%s,
            total_bp = %%s,
            gene_count = %%s,
            rrna_count = %%s,
            trna_count = %%s,
            at_bp = %%s,
            nonstd_bp = %%s,
            percent_nonstd_bp = %%s
            WHERE genome_id = %%s""" 
            % table,
            (merge_date_genome(release_date, row['release_date']),
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
            table = get_table('dggenome', env)
            cur.execute("""INSERT INTO %s (
            genome_id,
            release_date,
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
            ) VALUES (%%s,%%s,%%s,%%s,%%s,%%s,%%s,%%s,%%s,%%s,%%s,%%s,%%s,%%s,%%s,%%s,%%s,%%s,%%s)"""
            % table,
            (genome_id,
            release_date,
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
    
    sys.stderr.write( "******set genomes_to_add: " + str(genome_count) + "\n")
    
    genome_data = { 'score': genome_score, 'old_score': old_score, 'gene_density': genome_gene_density, 'old_gene_density': old_gene_density, 'percent_at': genome_percent_at, 'old_percent_at': old_percent_at, 'genomes_to_add': genome_count}
    return genome_data


def replace_tax_entry_average(new_value, old_value, cur_value_in_db_num, cur_value_in_db_den, genomes_to_add):
    updated_value = (float(cur_value_in_db_num) - float(old_value) + float(new_value)) / (float(cur_value_in_db_den) + float(genomes_to_add));
    return updated_value

def replace_tax_entry_sum(new_value, old_value, cur_value_in_db):
    updated_value = float(cur_value_in_db) - float(old_value) + float(new_value)
    return updated_value


def add_accession_to_dgtax(cur, env, accession, version, tax_id, all_data, genome_data):
    # adds only one entry to the given tax_id
    (release_date, genome_id, bioproject_id, genome_name, chromosome_count, plasmid_count, replicon_count, contig_count, total_bp, nonstd_bp, gene_count, at_bp, rrna_count, trna_count, replicon_type, score, gene_density, percent_at) = all_data

    sys.stderr.write( "ADDING " + accession + " to TAX " + str(tax_id) + "\n")

    table = get_table('tax_path', env)
    cur.execute("""SELECT tax_id from %s where tax_id = %%s and accession = %%s and version = %%s""" % table, (tax_id, accession, version));
    row = cur.fetchone()

    if row:
        sys.stderr.write("create_tax_entries: row already exists for accession = " + accession + ", version = " + str(version) + ", tax_id = " + str(tax_id) + "\n")
        # nothing to do

    else:
        table = get_table('tax_path', env)
        cur.execute("""INSERT INTO %s (tax_id, accession, version) VALUES (%%s, %%s, %%s)""" % table, (tax_id, accession, version))
        # get the current values from tax_stats
        table = get_table('dgtax', env)
        cur.execute("""SELECT 
        release_date, 
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
        genome_count FROM %s where tax_id = %%s""" % table, (tax_id))
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


            table = get_table('dgtax', env)
            cur.execute("""UPDATE %s set 
            release_date = %%s,
            genome_count = %%s,
            score = %%s,
            score_numerator = %%s,
            gene_density = %%s,
            gene_density_numerator = %%s,
            percent_at = %%s,
            percent_at_numerator = %%s,
            chromosome_count = %%s,
            plasmid_count = %%s,
            replicon_count = %%s,
            total_bp = %%s,
            gene_count = %%s,
            rrna_count = %%s,
            trna_count = %%s,
            contig_count = %%s,
            nonstd_bp = %%s,
            at_bp = %%s
            WHERE tax_id = %%s"""
            % table,
            (merge_date_tax(release_date, row['release_date']),
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
            table = get_table('dgtax', env)
            cur.execute("""INSERT INTO %s (
            tax_id,
            release_date,
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
            ) VALUES (%%s,%%s,%%s,%%s,%%s,%%s,%%s,%%s,%%s,%%s,%%s,%%s,%%s,%%s,%%s,%%s,%%s,%%s,%%s)"""
            % table,
            (tax_id, 
            release_date, 
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
        table = get_table('dgtax', env)
        cur.execute("""UPDATE %s SET tax_name=%%s WHERE tax_id=%%s""" % table, (tax_name, tax_id))





def remove_accession_from_dgreplicon(cur, env, accession, version):
    # must be called before removing from genome
    sys.stderr.write( "REMOVING ACCESSION " + accession + " FROM REPLICON" + "\n")
    table = get_table('dgreplicon', env)
    cur.execute("""SELECT * FROM %s WHERE accession = %%s and version = %%s""" % table, (accession, version))
    replicon_data = cur.fetchone()
    if not replicon_data:
        sys.stderr.write("no replicon data for accession " + accession + " version " + version + "\n")
        exit(1)
    
    cur.execute("""DELETE FROM %s WHERE accession = %%s and version = %%s""" % table, (accession, version))
    
    #all_data
    table = get_table('dggenome', env)
    cur.execute("""SELECT * FROM %s WHERE genome_id = %%s""" % table, (replicon_data['genome_id']))
    genome_data = cur.fetchone()
    if not genome_data:
        sys.stderr.write("no genome data for " + str(replicon_data['genome_id']) + "\n")
    
    if replicon_data['replicon_type'] == 'CHROMOSOME':
        chromosome = 1
        plasmid = 0
    else:
        chromosome = 0
        plasmid = 1
    return (genome_data['release_date'], replicon_data['genome_id'], genome_data['bioproject_id'], genome_data['genome_name'], chromosome, plasmid, 1, replicon_data['contig_count'], replicon_data['total_bp'], replicon_data['nonstd_bp'], replicon_data['gene_count'], replicon_data['at_bp'], replicon_data['rrna_count'], replicon_data['trna_count'], replicon_data['replicon_type'], replicon_data['score'], replicon_data['gene_density'], replicon_data['percent_at']) 




def remove_accession_from_dggenome(cur, env, accession, version, genome_id):
    sys.stderr.write( "REMOVING ACCESSION " + accession + " FROM GENOME" + "\n")
    remove_accession_from_genome_path(cur, env, accession, version, genome_id)
    table = get_table('dggenome', env)
    cur.execute("""SELECT * FROM %s WHERE genome_id = %%s""" % table, (genome_id))
    old_genome_data = cur.fetchone()
    new_genome_data = recalculate_genome(cur, env, genome_id)
    return { 'score': new_genome_data['score'], 'old_score': old_genome_data['score'], 'gene_density': new_genome_data['gene_density'], 'old_gene_density': old_genome_data['gene_density'], 'percent_at': new_genome_data['percent_at'], 'old_percent_at': old_genome_data['percent_at']}

def remove_accession_from_genome_path(cur, env, accession, version, genome_id):
    table = get_table('genome_path', env)
    cur.execute("""DELETE FROM %s WHERE genome_id = %%s and accession = %%s and version = %%s""" % table, (genome_id, accession, version))

def recalculate_genome(cur, env, genome_id):
    table = get_table('dgreplicon', env)
    cur.execute("""SELECT * FROM %s WHERE genome_id = %%s""" % table, (genome_id))
    rows = cur.fetchall()

    if rows:
        tax_id = 0
        genome_score = 0
        gene_density = 0
        genome_percent_at = 0
        chromosome_count = 0
        plasmid_count = 0
        replicon_count = 0
        contig_count = 0
        total_bp = 0
        gene_count = 0
        rrna_count = 0
        trna_count = 0
        at_bp = 0
        nonstd_bp = 0
        percent_nonstd_bp = 0
        for row in rows:
            if row['replicon_type'] == 'CHROMOSOME':
                add_chromosome = 1
                add_plasmid = 0
            if row['replicon_type'] == 'PLASMID':
                add_chromosome = 0
                add_plasmid = 1
            tax_id = row['tax_id']
            genome_gene_density = merge_gene_density_genome(gene_count, row['gene_count'], total_bp, row['total_bp'])
            genome_percent_at = merge_percent_genome(at_bp, row['at_bp'], total_bp, row['total_bp'])
            chromosome_count = merge_count(chromosome_count, add_chromosome)
            plasmid_count = merge_count(plasmid_count, add_plasmid)
            replicon_count = merge_count(replicon_count, 1)
            contig_count = merge_count(contig_count, row['contig_count'])
            total_bp = merge_count(total_bp, row['total_bp'])
            gene_count = merge_count(gene_count, row['gene_count'])
            rrna_count = merge_count(rrna_count, row['rrna_count'])
            trna_count = merge_count(trna_count, row['trna_count'])
            at_bp = merge_count(at_bp, row['at_bp'])
            nonstd_bp = merge_count(nonstd_bp, row['nonstd_bp'])
            percent_nonstd_bp = merge_percent_genome(nonstd_bp, row['nonstd_bp'], total_bp, row['total_bp'])


        genome_score = merge_score_genome(nonstd_bp, total_bp, contig_count, replicon_count)

        table = get_table('dggenome', env)
        # release_date, bioproject_id, genome_name are missing, but won't be modified so it doesn't matter
        cur.execute("""UPDATE %s SET
        tax_id = %%s,
        score = %%s,
        gene_density = %%s,
        percent_at = %%s,
        chromosome_count = %%s,
        plasmid_count = %%s,
        replicon_count = %%s,
        contig_count = %%s,
        total_bp = %%s,
        gene_count = %%s,
        rrna_count = %%s,
        trna_count = %%s,
        at_bp = %%s,
        nonstd_bp = %%s,
        percent_nonstd_bp = %%s
        WHERE genome_id = %%s"""
        % table,
        (tax_id,
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
        percent_nonstd_bp,
        genome_id))

        genome_data = { 'score': genome_score, 'gene_density': genome_gene_density, 'percent_at': genome_percent_at}

    else:
        table = get_table('dggenome', env)
        cur.execute("""DELETE from %s where genome_id = %%s""" % table, (genome_id))
        genome_data = { 'score': 0, 'gene_density': 0, 'percent_at': 0}

    return genome_data


def remove_accession_from_dgtax(cur, env, tax_id, accession, version, all_data, genome_data):
    (release_date, genome_id, bioproject_id, genome_name, chromosome_count, plasmid_count, replicon_count, contig_count, total_bp, nonstd_bp, gene_count, at_bp, rrna_count, trna_count, replicon_type, score, gene_density, percent_at) = all_data

    sys.stderr.write( "REMOVING ACCESSION " + accession + " FROM TAX " + str(tax_id) + "\n")
    
    remove_accession_from_tax_path(cur, env, tax_id, accession, version)
    table = get_table('dgtax', env)
    cur.execute("""SELECT genome_count from %s where tax_id = %%s""" % table, (tax_id))
    genome_count = cur.fetchone()['genome_count']
    table = get_table('genome_path', env)
    cur.execute("""SELECT * from %s where genome_id = %%s""" % table, (genome_id))
    accession_count = len(cur.fetchall())
    genomes_to_remove = 0
    if accession_count == 0:
        genomes_to_remove = 1  # if this is the last accession, decrement the number of genomes by 1
    if genome_count == 1 and accession_count == 0:  
        # then there is just one row, the one we are removing, so just delete the row
        # the accession should have already been removed from the replicon and genome tables (in that order) at this point
        print "deleting row " + str(tax_id) + " in tax stats"
        table = get_table('dgtax', env)
        cur.execute("""DELETE from %s where tax_id = %%s""" % table, (tax_id))
    else:
        # modify_date is always going to be stuck at the max because we don't save previous values
        table = get_table('dgtax', env)
        cur.execute("""SELECT * from %s where tax_id = %%s""" % table, (tax_id))
        current_tax_value = cur.fetchone()
        table = get_table('dgtax', env)
        print "score: " + str(genome_data['score'])
        print "old_score: " + str(genome_data['old_score'])
        print "current num: " + str(current_tax_value['score_numerator'])
        print "current genome count: " + str(current_tax_value['genome_count'])
        print "genomes to remove: " + str(genomes_to_remove)
        cur.execute("""UPDATE %s set
            genome_count = %%s,
            score = %%s,
            score_numerator = %%s,
            gene_density = %%s,
            gene_density_numerator = %%s,
            percent_at = %%s,
            percent_at_numerator = %%s,
            chromosome_count = %%s,
            plasmid_count = %%s,
            replicon_count = %%s,
            total_bp = %%s,
            gene_count = %%s,
            rrna_count = %%s,
            trna_count = %%s,
            contig_count = %%s,
            nonstd_bp = %%s,
            at_bp = %%s
            WHERE tax_id = %%s"""
            % table,
            (merge_count(current_tax_value['genome_count'], genomes_to_remove),
            replace_tax_entry_average(genome_data['score'], genome_data['old_score'], current_tax_value['score_numerator'], current_tax_value['genome_count'], genomes_to_remove),
            replace_tax_entry_sum(genome_data['score'], genome_data['old_score'], current_tax_value['score_numerator']),
            replace_tax_entry_average(genome_data['gene_density'], genome_data['old_gene_density'], current_tax_value['gene_density_numerator'], current_tax_value['genome_count'], genomes_to_remove),
            replace_tax_entry_sum(genome_data['gene_density'], genome_data['old_gene_density'], current_tax_value['gene_density_numerator']),
            replace_tax_entry_average(genome_data['percent_at'], genome_data['old_percent_at'], current_tax_value['percent_at_numerator'], current_tax_value['genome_count'], genomes_to_remove),
            replace_tax_entry_sum(genome_data['percent_at'], genome_data['old_percent_at'], current_tax_value['percent_at_numerator']),
            merge_count(current_tax_value['chromosome_count'], 0 - chromosome_count),
            merge_count(current_tax_value['plasmid_count'], 0 - plasmid_count),
            merge_count(current_tax_value['replicon_count'], 0 - replicon_count),
            merge_count(current_tax_value['total_bp'], 0 - total_bp),
            merge_count(current_tax_value['gene_count'], 0 - gene_count),
            merge_count(current_tax_value['rrna_count'], 0 - rrna_count),
            merge_count(current_tax_value['trna_count'], 0 - trna_count),
            merge_count(current_tax_value['contig_count'], 0 - contig_count),
            merge_count(current_tax_value['nonstd_bp'], 0 - nonstd_bp),
            merge_count(current_tax_value['at_bp'], 0 - at_bp),
            tax_id))
            
def remove_accession_from_tax_path(cur, env, tax_id, accession, version):
    table = get_table('tax_path', env)
    cur.execute("""DELETE FROM %s WHERE tax_id = %%s AND accession = %%s AND version = %%s""" % table, (tax_id, accession, version))

