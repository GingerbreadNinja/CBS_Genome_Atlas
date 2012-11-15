import MySQLdb;
import sys;
import getopt;
import re;
import os;

# This program will read in a csv file in the format and some column names
# organism name \t data1 \t data2 ...
#
# The column names should correspond to columns in the database
# It will do some processing on the organism name (strip out whitespace, etc) and try to match it into the database
# If it finds an exact match, it will go ahead and add the data to the database
# if it finds a partial match, it will interactively ask the user if the match is good, and if so will update the db, otherwise it will skip the row


debug = 1

def usage():
    sys.stderr.write('usage: python ' + sys.argv[0] + ' data.csv\n');
    sys.exit(2)

def db_connect():
    #TODO pull this out into a module -- I think Steve has one
    db = MySQLdb.connect(host="mysql", port=3306,db="steve_private", read_default_file="~/.my.cnf")
    cur = db.cursor()
    return cur


def load_data(genome_id, doubling_time, optimal_growth_temp):
    if genome_id == "":
        sys.stderr.write("rnammer_stats: missing genome_id")
        sys.exit(2)

    cur = db_connect()

    row = cur.execute("""SELECT * FROM genome_external_data WHERE genome_id = %s""", (genome_id))
    rows = cur.fetchall()
    if rows:
        cur.execute("""UPDATE genome_external_data SET doubling_time_minutes = %s, optimal_growth_temp = %s WHERE genome_id = %s""", (doubling_time, optimal_growth_temp, genome_id))
    else:
        cur.execute("""INSERT INTO genome_external_data (genome_id, doubling_time_minutes, optimal_growth_temp) VALUES (%s, %s, %s)""", (genome_id, doubling_time, optimal_growth_temp))

def ask_user_about_match(organism_name, partial):
    cur = db_connect()
    print "-- checking for matches against " + partial
    cur.execute("""SELECT genome_id, genome_name FROM genome where genome_name LIKE %s""", (partial))
    rows = cur.fetchall()
    if rows:
        for r in rows:
            genome_id, genome_name = r
            input = ""
            while input != 'y' or input != 'n':
                input = raw_input("Does\t" + organism_name + "\t=\t" + genome_name + "\ny/n> ")
                if input == "y":
                    print "writing data to db"
                    return [genome_id]
                if input == "n":
                    print "skipping"
                    break
                if input == "a":
                    print "skipping all"
                    return None
    else:
        return None

def get_genome_ids(organism_name):
    #return list of all matching genome_ids for this name from the database

    # first try an exact match
    cur = db_connect()
    cur.execute("""SELECT genome_id, genome_name FROM genome where genome_name = %s""", (organism_name))
    rows = cur.fetchall()
    if rows:
        print "Found exact match on " + organism_name
        ids = []
        for genome_id, genome_name in rows:
            ids.append(genome_id)
        return ids;
    else: 
        print "No exact match, trying partial match"
        # replace with wildcards all spaces, dashes, variant spellings of "species", 
        partial = organism_name
        partial = re.sub(r'\s', "%", partial)
        partial = re.sub(r'-', "%", partial)
        partial = re.sub(r'Species', "%", partial)
        partial = re.sub(r'species', "%", partial)
        partial = re.sub(r'Sp.', "%", partial)
        partial = re.sub(r'sp.', "%", partial)
        partial += "%"

        print "name to match on: " + partial

        result = ask_user_about_match(organism_name, partial)
        if result is not None:
            return result
        else:
            # try stripping off the last block and try to match
            r = re.compile(r'(.*) [^ ]')
            m = r.match(organism_name)
            if m is not None:
                partial = m.group(1) + "%"
                print "name trimmed to " + partial
                result = ask_user_about_match(organism_name, partial)
                if result is not None:
                    return result;
                else: 
                    print "nothing looks at all similar"
                    return None
                
                    
def parse_data(lines):
    for line in lines:
        genome_id = ""
        print "line: " + line

        r_first_line = re.compile(r"(.*)\t([0-9]+)\t([0-9]+)\n")

        m = r_first_line.match(line)
        if m is not None:
            organism_name = m.group(1)
            doubling_time = m.group(2)
            optimal_growth_temp = m.group(3)

            if debug:
                print "name: " + organism_name
                print "doubling_time: " + doubling_time
                print "optimal_growth_temp: " + optimal_growth_temp
    
            genome_ids = get_genome_ids(organism_name)
            if genome_ids is not None:
                for genome_id in genome_ids:
    
                    if debug:
                        print "genome_id: " + str(genome_id)
    
                    if genome_id:
                        load_data(genome_id, doubling_time, optimal_growth_temp)
    
                    if debug:
                        print "------------------------------------------------"
        else:
            print "NO MATCH on " + line

def main(argv):
    try:
        data = argv[0]
    except:
        usage()
    try:
        f = open(data,"r")
        parse_data(f.readlines())
    except IOError:
        sys.stderr.write("Opening data file: " + data + " failed");
        sys.exit(3)

if __name__ == "__main__":
    main(sys.argv[1:])
