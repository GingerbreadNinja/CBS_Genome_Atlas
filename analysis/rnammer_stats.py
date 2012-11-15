import MySQLdb;
import sys;
import getopt;
import re;
import os;


debug = 0

def usage():
    sys.stderr.write('usage: ' + sys.argv[0] + ' CP000020_2.rrna2' + "\n");
    sys.exit(2)

def db_connect():
    #TODO pull this out into a module -- I think Steve has one
    db = MySQLdb.connect(host="mysql", port=3306,db="steve_private", read_default_file="~/.my.cnf")
    cur = db.cursor()
    return cur

def write_stats(accession, version, start_location, end_location, complementary_strand, molecule, score, sequence):
    if accession == "" or version == "" or start_location == "" or end_location == "" or complementary_strand == "" or molecule == "" or score == "" or sequence == "":
        sys.stderr.write("trnascan-stats: missing one of\n\taccession: " + accession + "\n\tversion: " + version + "\n\tstart_location: " + start_location + "\n\tend_location: " + end_location + "\n\tcomplementary_strand: " + complementary_strand + "\n\tmolecule: " + molecule + "\n\tscore " + score + "\n\tsequence: " + sequence + "\n")
        sys.exit(2)

    cur = db_connect()
    #TODO the database should probably enforce the non duplicate rows as well
    row = cur.execute("""SELECT * FROM rrna WHERE accession = %s and version = %s and start_location = %s and end_location = %s and complementary_strand = %s""", (accession, version, start_location, end_location, complementary_strand))
    rows = cur.fetchall()
    if rows:
        cur.execute("""UPDATE rrna SET molecule = %s, score = %s, sequence = %s WHERE accession = %s and version = %s and start_location = %s and end_location = %s and complementary_strand = %s""", (molecule, score, sequence, accession, version, start_location, end_location, complementary_strand))
    else:
        cur.execute("""INSERT INTO rrna (accession, version, start_location, end_location, complementary_strand, molecule, score, sequence) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""", (accession, version, start_location, end_location, complementary_strand, molecule, score, sequence))


def parse_rrna_sequences(lines, filename):

    accession = ""
    version = ""
    start_location = ""
    end_location = ""
    complementary_strand = ""
    molecule = ""
    score = ""
    sequence = ""

    # >rRNA_CP000094.2_5396757-5398281_DIR- /molecule=16s_rRNA /score=2004.6
    # AGAGTTTGATCATGGCTCAGATTGAACGCTGGCGGCAGGCCTAACACATGCAAGTCGAGC

    for line in lines:
        r_line = re.compile(r">rRNA_([A-Z]{2}[0-9]{6})\.([0-9]+)_([0-9]+)-([0-9]+)_DIR([-\+]) /molecule=(.*)_rRNA /score=([0-9]+.?[0-9]*)")

        if line.startswith(">"): 

            if (sequence != ""):
                if debug:
                    print "name: " + accession
                    print "version: " + version
                    print "start_location: " + start_location
                    print "end_location: " + end_location
                    print "complementary_strand: " + complementary_strand
                    print "molecule: " + molecule
                    print "score: " + score
                    print "sequence: " + sequence
                    print "------------------------------------------------"
                write_stats(accession, version, start_location, end_location, complementary_strand, molecule, score, sequence)


            sequence = "" #this is the first line of the new block, so reset the sequence

            m = r_line.match(line)
            accession = m.group(1)
            version = m.group(2)
            start_location = m.group(3)
            end_location = m.group(4)
            complementary_strand = m.group(5)
            molecule = m.group(6)
            score = m.group(7)

            if (complementary_strand == "+"):
                complementary_strand = "no"
            elif (complementary_strand == "-"):
                complementary_strand = "yes"

            av = accession + "_" + version
            if av != filename:
                sys.stderr.write("Accession number in file (" + av + ") doesn't match accession number in filename (" + filename + ").  Aborting.\n")
                sys.exit(3)

        else: # this is part of the sequence for the previous line
            sequence += re.sub(r'\n', "", line).lower();

    # then write the last row
    if debug:
        print "name: " + accession
        print "version: " + version
        print "start_location: " + start_location
        print "end_location: " + end_location
        print "complementary_strand: " + complementary_strand
        print "molecule: " + molecule
        print "score: " + score
        print "sequence: " + sequence
        print "------------------------------------------------"
    write_stats(accession, version, start_location, end_location, complementary_strand, molecule, score, sequence)



def main(argv):
    try:
        rrna = argv[0]
    except:
        usage()
    if rrna == "":
        usage()
    try:
        f = open(rrna,"r")
        parse_rrna_sequences(f.readlines(), os.path.basename(rrna[:-6]))
    except IOError:
        sys.stderr.write("Opening rrna file: " + rrna + " failed");
        sys.exit(3)

if __name__ == "__main__":
    main(sys.argv[1:])
