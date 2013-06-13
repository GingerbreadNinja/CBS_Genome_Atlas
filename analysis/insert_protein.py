from Bio import SeqIO
from Bio.SeqUtils import GC
import os, sys, getopt;
import inspect
from genomeanalysis.common import *

env = get_env()

def usage():
    sys.stderr.write(sys.argv[0] + ' --fasta=<fastafile>\n')
    sys.exit(2)

def loadproteins(fastafile):
    input_handle = open(fastafile, "rU")
    sequences= SeqIO.parse(input_handle, "fasta")
    cur = db_connect("prod")
    filejunk = os.path.basename(fastafile).split(".")[0]
    version = 0
    if "_" in filejunk:
        accession, version = filejunk.split("_")
    else:
        accession = filejunk
    genome_id = get_genome_id(accession, version)
    print "accession: " + accession + " version: " + str(version) + " genome_id: " + str(genome_id)
    for record in sequences:
        seq = record.seq
        length = len(record.seq)
        name = record.name
        #print "name: " + name + " length: " + str(length) + " seq: " + seq
        cur.execute("""SELECT * from protein where accession = %s and name = %s and seq = %s""", (accession, name, seq))
        row = cur.fetchone()
        if row is None:
            cur.execute("""INSERT INTO protein (seq, length, name, genome_id, accession) VALUES (%s, %s, %s, %s, %s)""", (seq, length, name, genome_id, accession))
        #else:
            #print "not re-inserting seq"
        


def main(argv):
    try:
        opts, args = getopt.getopt(argv,"h",["fasta="])
    except getopt.GetoptError:
        usage()
    fastafile = ""
    for opt, arg in opts:
        if opt == '-h':
            usage()
        elif opt in ("--fasta"):
            fastafile = arg
        else:
            sys.stderr.write("unrecognized option " + opt + "\n")
            usage()
    if fastafile == "":
        usage()
    loadproteins(fastafile)

if __name__ == "__main__":
    main(sys.argv[1:])
