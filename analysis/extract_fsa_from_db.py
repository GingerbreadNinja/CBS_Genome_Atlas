from Bio import SeqIO
from Bio.SeqUtils import GC
import os, sys, getopt;
import inspect
from genomeanalysis.common import *

env = get_env()

debug = True

# this writes a fsa file for the selected set of proteins that can be used to generate pancore plots from

def usage():
    sys.stderr.write(sys.argv[0] + ' [--accession=accession] [--genome_id=genome_id]\n')
    sys.exit(2)

def get_genome_id_cheap(cur, accession):
    try:
        cur.execute("""SELECT genome_id from replicon where accession = %s""", (accession))
        row = cur.fetchone()
        if row:
            genome_id = row['genome_id']
            return genome_id
        else:
            return -1
    except:
        return -1

def get_proteins(accession):
    cur = db_connect("prod")
    cur.execute("""select * from replicon where accession = %s""", (accession))
    row = cur.fetchone()
    if row['replicon_type'] == "PLASMID":
        sys.stderr.write("not writing genome information for plasmid\n")
        exit(0)
    else:
        genome_id = get_genome_id_cheap(cur, accession)
        get_proteins_genome(genome_id)


def get_proteins_genome(genome_id):
    cur = db_connect("prod")
    cur.execute("""select genome_id, protein_id, sigma, seq from protein where sigma is not null and genome_id = %s order by protein_id""", (genome_id))
    rows = cur.fetchall()
    for row in rows:
        print ">" + str(row['genome_id']) + "_" + str(row['protein_id']) + "_" + row['sigma'].replace(" ", "_").replace("'","").replace("[","").replace("]","")
        print row['seq'].replace("*", "")

def main(argv):
    try:
        opts, args = getopt.getopt(argv,"h",["accession=", "genome_id="])
    except getopt.GetoptError:
        usage()
    accession = ""
    genome_id = ""
    for opt, arg in opts:
        if opt == '-h':
            usage()
        elif opt in ("--accession"):
            accession = arg
        elif opt in ("--genome_id"):
            genome_id = arg
        else:
            sys.stderr.write("unrecognized option " + opt + "\n")
            usage()
    if accession == "" and genome_id == "":
        sys.stderr.write("accession or genome_id argument must be specified\n")
        usage()
    if "_" in accession:
        accession = accession.split("_")[0]
    if accession:
        get_proteins(accession)
    if genome_id:
        get_proteins_genome(genome_id)

if __name__ == "__main__":
    main(sys.argv[1:])
