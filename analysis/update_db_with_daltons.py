from Bio import SeqIO
from Bio.SeqUtils import GC
import os, sys, getopt;
import inspect
from genomeanalysis.common import *

env = get_env()

def usage():
    sys.stderr.write(sys.argv[0] + ' --genome_id=<genome_id>\n')
    sys.exit(2)

def daltons(seq, genome_id):
    daltons = 0


    convert = {
    'I': 131.1736, 
    'L': 131.1736,
    'K': 146.1882,
    'M': 149.2124,
    'F': 165.1900,
    'T': 119.1197,
    'W': 204.2262,
    'V': 117.1469,
    'R': 174.2017,
    'H': 155.1552,
    'A': 89.0935,
    'N': 132.1184,
    'D': 133.1032,
    'C': 121.1590,
    'E': 147.1299,
    'Q': 146.1451,
    'G': 75.0669,
    'P': 115.1310,
    'S': 105.0930,
    'Y': 181.1894,
    'water': 18.0153,
    '*': 0,
    }

    
    if len(seq) == 0:
        sys.stderr.write("sequence is 0 length for genome_id " + genome_id + " " + seq)
        exit(2)
    for amino_acid in seq:
        daltons += convert[amino_acid] - convert['water']
    daltons = daltons + convert['water'] # the peptide loses a water foreach amino acid except the last, so add it back in
    return daltons

def update_genome(genome_id):
    cur = db_connect(env)
    cur.execute("""SELECT protein_id, seq FROM protein where genome_id=%s""", (genome_id))
    rows = cur.fetchall()
    if rows:
        for row in rows:
            weight = daltons(row['seq'], genome_id)
            #print row['seq']
            #print "has daltons: " + str(weight)
            cur.execute("""UPDATE protein set daltons=%s where protein_id=%s""", (weight, row['protein_id']))
    else:
        print "no proteins found for genome_id " + str(genome_id)
        exit(1)

def main(argv):
    try:
        opts, args = getopt.getopt(argv,"h",["genome_id="])
    except getopt.GetoptError:
        usage()
    genome_id = ""
    for opt, arg in opts:
        if opt == '-h':
            usage()
        elif opt in ("--genome_id"):
            genome_id = arg
        else:
            sys.stderr.write("unrecognized option " + opt + "\n")
            usage()
    if genome_id == "":
        sys.stderr.write("genome_id argument must be specified")
        usage()
    update_genome(genome_id)

if __name__ == "__main__":
    main(sys.argv[1:])
