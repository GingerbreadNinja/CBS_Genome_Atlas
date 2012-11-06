import MySQLdb;
import sys;
import getopt;
import re;
import os;


debug = 0

def usage():
    sys.stderr.write('usage: ' + sys.argv[0] + ' CP000020_2.trna');
    sys.exit(2)

def db_connect():
    #TODO pull this out into a module -- I think Steve has one
    db = MySQLdb.connect(host="mysql", port=3306,db="steve_private", read_default_file="~/.my.cnf")
    cur = db.cursor()
    return cur

def write_stats(accession, version, start_location, end_location, amino_acid, anti_codon, sequence):
    cur = db_connect()
    #TODO the database should probably enforce the non duplicate rows as well
    row = cur.execute("""SELECT * FROM trna WHERE accession = %s and version = %s and start_location = %s and end_location = %s""", (accession, version, start_location, end_location))
    rows = cur.fetchall()
    if rows:
        cur.execute("""UPDATE trna SET amino_acid = %s, anti_codon = %s, sequence = %s WHERE accession = %s and version = %s and start_location = %s and end_location = %s""", (amino_acid, anti_codon, sequence, accession, version, start_location, end_location))
    else:
        cur.execute("""INSERT INTO trna (accession, version, start_location, end_location, amino_acid, anti_codon, sequence) VALUES (%s, %s, %s, %s, %s, %s, %s)""", (accession, version, start_location, end_location, amino_acid, anti_codon, sequence))

def parse_predictions(lines, filename):

    accession = ""
    version = ""
    start_location = ""
    end_location = ""
    amino_acid = ""
    anti_codon = ""
    sequence = ""

    for line in lines:
        r_first_line = re.compile(r"[A-Z]{2}[0-9]{6}.[0-9]")
        r_start = re.compile(r"[0-9]+")
        r_seq = re.compile(r"[acgt][acgt]+")
        r_aa = re.compile(r"[A-Z][a-z][a-z]")
        r_ac = re.compile(r"[actg]{3}")

        if line.startswith("sequence name="):
            accession = r_first_line.findall(line)[0]
            accession = accession.replace(".", "_")
            if accession != filename:
                sys.stderr.write("Accession number in file (" + accession + ") doesn't match accession number in filename (" + filename + ").  Aborting.\n")
                sys.exit(3)
            version = accession[-1] #TODO this will break if the version is > 9.  as of Nov 2012, the max version is 5.
            accession = accession[0:8]
            continue; # skip first line

        if line.startswith("start position="):
            #this is the first line in a block to be parsed, so reset all values
            start_location = ""
            end_location = ""
            amino_acid = ""
            anti_codon = ""
            sequence = ""

            #start position= 31800 end position= 31873
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
                print "start_location: " + start_location
                print "end_location: " + end_location
                print "amino_acid: " + amino_acid
                print "anti_codon: " + anti_codon
                print "sequence: " + sequence
                print "------------------------------------------------"

            write_stats(accession, version, start_location, end_location, amino_acid, anti_codon, sequence)

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
        print "Opening " + trna + " failed";
        sys.exit(3)

if __name__ == "__main__":
    main(sys.argv[1:])
