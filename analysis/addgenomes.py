import getopt
import uuid
import sys
from subprocess import call
from genomeanalysis.common import *

debug = True
env = "prod"

def usage():
    sys.stderr.write('This program only adds genomes with one replicon and is very naive.\n')
    sys.stderr.write('Accession numbers for assembled SRA: SRASRR077750\n')
    sys.stderr.write('Assession numbers for other sources: TRDA00000004\n')
    sys.stderr.write('usage:\n\t' + sys.argv[0] + ' --accession=SRASRR077750 [--version=0] [--genome_name=accession] [--genome_validity=VALID] --source=sra|other\n') 
    sys.exit(2)

def add_genome(genome_name, genome_validity, source):
    cur = db_connect(env)
    genome_id = 0
    print "adding genome"
    if source == "sra":
        assembled_sra_id = 1
        cur.execute("""SELECT * FROM genome WHERE genome_name = %s and assembled_sra_id = %s""", (genome_name, assembled_sra_id))
        row = cur.fetchone()
        if row:
            print "not re-adding genome"
        else:
            cur.execute("""INSERT INTO genome (genome_name, genome_validity, assembled_sra_id) VALUES (%s, %s, %s)""", (genome_name, genome_validity, assembled_sra_id))
            genome_id = cur.lastrowid
    elif source == "other":
        other_source_id = 1
        cur.execute("""SELECT * FROM genome WHERE genome_name = %s and other_source_id = %s""", (genome_name, other_source_id))
        row = cur.fetchone()
        if row:
            print "not re-adding genome"
        else:
            cur.execute("""INSERT INTO genome (genome_name, genome_validity, other_source_id) VALUES (%s, %s, %s)""", (genome_name, genome_validity, other_source_id))
            genome_id = cur.lastrowid
    else:
        sys.stderr.write("unknown source " + source + "\n")
        exit(1)

    return genome_id

def add_replicon(genome_id, accession, version):
    print "adding replicon"
    cur = db_connect(env)
    cur.execute("""SELECT * FROM replicon where accession = %s""", (accession))
    row = cur.fetchone()
    if row:
        print "not re-adding replicon"
    else:
        cur.execute("""INSERT INTO replicon (genome_id, accession, version, replicon_type) VALUES (%s, %s, %s, %s)""", (genome_id, accession, version, "ASSEMBLY"))

def main(argv):
    try:
        opts, args = getopt.getopt(sys.argv[1:], "h", ["help", "accession=", "version=", "genome_name=", "genome_validity=", "source="])
    except getopt.GetoptError as err:
        sys.stderr.write(str(err) + "\n")
        usage()
    accession = ""
    version = 0
    genome_name = ""
    genome_validity = "VALID"
    source = ""
    for o, a in opts:
        if o in ("-h", "--help"):
            usage()
        elif o in ("--accession"):
            accession = a
        elif o in ("--version"):
            version = a
        elif o in ("--genome_name"):
            genome_name = a
        elif o in ("--genome_validity"):
            genome_validity = a
        elif o in ("--source"):
            source = a
        else:
            sys.stderr.write("Unregognized option " + o + "\n");
            usage()
    if genome_name == "":
        genome_name = accession
    if accession == "" or source == "":
        usage()
    else:
        genome_id = add_genome(genome_name, genome_validity, source)
        add_replicon(genome_id, accession, version)

if __name__ == "__main__":
    main(sys.argv[1:])

