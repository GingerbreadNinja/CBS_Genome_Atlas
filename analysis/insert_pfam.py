from Bio import SeqIO
from Bio.SeqUtils import GC
import os, sys, getopt;
import inspect
from genomeanalysis.common import *

env = get_env()

def usage():
    sys.stderr.write(sys.argv[0] + ' --pfam=<pfamfile>\n')
    sys.exit(2)

def get_protein_id(cur, name, accession):
    cur.execute("""SELECT protein_id FROM protein where accession=%s and name=%s""", (accession, name))
    row = cur.fetchone()
    return row['protein_id']

def parse_line(line):
    #print "line: " + line
    things = line.split()
    pfam_id = things[5].split(".")[0]
    #       protein_name pfam_id clan_id    pos
    return (things[0], pfam_id, things[14], things[1]) #we only need a reasonable subset of the things

def load_pfam(pfamfile):
    print "loading pfam"
    input_handle = open(pfamfile, "rU")
    f = open(pfamfile, "rU")
    cur = db_connect("prod")

    filejunk = os.path.basename(pfamfile).split(".")[0]
    version = 0
    if "_" in filejunk:
        accession, version = filejunk.split("_")
    else:
        accession = filejunk
    genome_id = get_genome_id(accession, version)
    print "accession: " + accession + " version: " + str(version) + " genome_id: " + str(genome_id)
    
    for line in f:
        if line.startswith("#"):
            next
        elif line == "\n":
            next
        else:
            name, pfam_id, clan_id, pos = parse_line(line)
            #print line
            #print "name: " + name + " pfam_id: " + pfam_id + " clan_id: " + clan_id + " pos: " + pos
            protein_id = get_protein_id(cur, name, accession)
            #print "protein_id: " + str(protein_id)
            cur.execute("""SELECT * from protein_has_pfam where protein_id = %s and pfam_id = %s and pos = %s""", (protein_id, pfam_id, pos))
            row = cur.fetchone()
            if not row:
                #print "inserting pfam"
                cur.execute("""INSERT INTO protein_has_pfam (protein_id, pfam_id, clan_id, pos, genome_id, accession) VALUES (%s, %s, %s, %s, %s, %s)""", (protein_id, pfam_id, clan_id, pos, genome_id, accession))
            #else:
            #    print "not re-inserting pfam"
        


def main(argv):
    try:
        opts, args = getopt.getopt(argv,"h",["pfam="])
    except getopt.GetoptError:
        usage()
    pfamfile = ""
    for opt, arg in opts:
        if opt == '-h':
            usage()
        elif opt in ("--pfam"):
            pfamfile = arg
        else:
            sys.stderr.write("unrecognized option " + opt + "\n")
            usage()
    if pfamfile == "":
        sys.stderr.write("pfam argument must be specified")
        usage()
    load_pfam(pfamfile)

if __name__ == "__main__":
    main(sys.argv[1:])
