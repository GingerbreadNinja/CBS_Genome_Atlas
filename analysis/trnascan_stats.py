import MySQLdb;
import sys;
import getopt;
import re;
import os;
from genomeanalysis.common import *


debug = 0
env = "prod"

def usage():
    sys.stderr.write('usage: ' + sys.argv[0] + ' CP000020_2.trna');
    sys.exit(2)

def write_stats(accession, version, start_location, end_location, complementary_strand, amino_acid, anti_codon, sequence):
    if accession == "" or version == "" or start_location == "" or end_location == "" or complementary_strand == "" or amino_acid == "" or anti_codon == "" or sequence == "":
        sys.stderr.write("trnascan-stats: missing one of\n\taccession: " + accession + "\n\tversion: " + version + "\n\tstart_location: " + start_location + "\n\tend_location: " + end_location + "\n\tcomplementary_strand: " + complementary_strand + "\n\tamino_acid: " + amino_acid + "\n\tanti_codon " + anti_codon + "\n\tsequence: " + sequence + "\n")
        sys.exit(2)
    cur = db_connect(env)
    #TODO the database should probably enforce the non duplicate rows as well
    row = cur.execute("""SELECT * FROM trna WHERE accession = %s and version = %s and start_location = %s and end_location = %s and complementary_strand = %s""", (accession, version, start_location, end_location, complementary_strand))
    rows = cur.fetchall()
    if rows:
        cur.execute("""UPDATE trna SET amino_acid = %s, anti_codon = %s, sequence = %s WHERE accession = %s and version = %s and start_location = %s and end_location = %s and complementary_strand = %s""", (amino_acid, anti_codon, sequence, accession, version, start_location, end_location, complementary_strand))
    else:
        cur.execute("""INSERT INTO trna (accession, version, start_location, end_location, complementary_strand, amino_acid, anti_codon, sequence) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""", (accession, version, start_location, end_location, complementary_strand, amino_acid, anti_codon, sequence))

def parse_predictions(lines, filename):

    accession = ""
    version = ""
    complementary_strand = "no"
    start_location = ""
    end_location = ""
    amino_acid = ""
    anti_codon = ""
    sequence = ""

    for line in lines:
        r_first_line = re.compile(r"[A-Z]+[0-9]+.[0-9]")
        r_start = re.compile(r"[0-9]+")
        r_seq = re.compile(r"[acgt][acgt]+")
        r_aa = re.compile(r"[A-Z][a-z][a-z]")
        r_ac = re.compile(r"[actg]{3}")

        if line.startswith("number of base pairing in the anticodon stem"): #this is the last line in the block, so reset all values
            complementary_strand = "no"
            start_location = ""
            end_location = ""
            amino_acid = ""
            anti_codon = ""
            sequence = ""

        if line.startswith("complementary strand"):
            complementary_strand = "yes"

        if line.startswith("sequence name="):
            accession = r_first_line.findall(line)[0]
            accession_test = accession.replace(".", "_")
            if filename.endswith("_0"):
                sys.stderr.write("Skipping check that accession in file is same as accession in filename for WGS file\n")
                accession, version = filename.split("_")
            else:
                if accession_test != filename:
                    sys.stderr.write("Accession number in file (" + accession + ") doesn't match accession number in filename (" + filename + ").  Aborting.\n")
                    sys.exit(3)
                accession, version = parse_string(accession)
            continue; # skip first line

        if line.startswith("start position="):
            #format: start position= 31800 end position= 31873
            matches = r_start.findall(line)
            start_location = matches[0]
            end_location = matches[1]

        if line.startswith("potential tRNA sequence="):
            #potential tRNA sequence= aggggtgtagctccaattggcagagcagcggattccaaatccgcgtgttgggagttcgaatctctccacccctg
            sequence = r_seq.findall(line)[0]
            
        if line.startswith("tRNA predict as a tRNA-"):
            #tRNA predict as a tRNA- Trp : anticodon acc
            amino_acid = r_aa.findall(line)[0]
            anti_codon = r_ac.findall(line)[0]

            if debug:
                print "name: " + accession
                print "version: " + version
                print "complementary_strand: " + complementary_strand
                print "start_location: " + start_location
                print "end_location: " + end_location
                print "amino_acid: " + amino_acid
                print "anti_codon: " + anti_codon
                print "sequence: " + sequence
                print "------------------------------------------------"

            write_stats(accession, version, start_location, end_location, complementary_strand, amino_acid, anti_codon, sequence)

        else:
            continue;

def main(argv):
    trna = argv[0]
    if trna == "":
        usage()
    try:
        f = open(trna,"r")
        parse_predictions(f.readlines(), os.path.basename(trna[:-5]))
    except IOError:
        sys.stderr.write("Opening trna file: " + trna + " failed");
        sys.exit(3)

if __name__ == "__main__":
    main(sys.argv[1:])
