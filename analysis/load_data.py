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


def load_data(genome_id, alt_genome_name):
    if genome_id == "":
        sys.stderr.write("rnammer_stats: missing genome_id")
        sys.exit(2)

    cur = db_connect()

    row = cur.execute("""SELECT * FROM helen_paper_data WHERE genome_id = %s""", (genome_id))
    rows = cur.fetchall()
    if rows:
        cur.execute("""UPDATE helen_paper_data SET genome_id = %s, alt_genome_name = %s WHERE genome_id = %s""", (genome_id, alt_genome_name, genome_id))
    else:
        cur.execute("""INSERT INTO helen_paper_data (genome_id, alt_genome_name) VALUES (%s, %s)""", (genome_id, alt_genome_name))

def ask_user_about_match(organism_name, partial):
    cur = db_connect()
    print "-- checking for matches against " + partial + "\n"
    cur.execute("""SELECT genome_id, genome_name FROM genome where genome_name LIKE %s order by genome_name""", (partial))
    rows = cur.fetchall()
    if rows:
        gs = []
        for r in rows:
            genome_id, genome_name = r
            gs.append(genome_id)
            print str(genome_id) + "   " + genome_name + "                  " + organism_name

        input = ""
        #while input != 'y' or input != 'n':
        input = raw_input("\nSelect genome_ids that match or <enter> for all, 'none' for none> ")
        
        if input == 'none':
            return None
        #TODO elif input =~ [A-z] return None
        elif input != '':
            selected_genome_ids = input.split(' ')
            return selected_genome_ids
        else:
            return gs
            

                #input = raw_input("Does\t" + organism_name + "\t=\t" + genome_name + "\ny/n/a> ")
                #if input == "y":
                #    print "writing data to db"
                #    return [genome_id]
                #if input == "n":
                #    print "skipping"
                #    break
                #if input == "a":
                #    print "skipping all"
                #    return None
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
        print "No exact match"
        # replace with wildcards all spaces, dashes, variant spellings of "species", 
        partial = organism_name
        partial = re.sub(r'\s', "%", partial)
        partial = re.sub(r'-', "%", partial)
        partial = re.sub(r'Species', "%", partial)
        partial = re.sub(r'species', "%", partial)
        partial = re.sub(r'Sp.', "%", partial)
        partial = re.sub(r'sp.', "%", partial)
        partial += "%"

        result = ask_user_about_match(organism_name, partial)
        if result is not None:
            return result
        else:
            # try stripping off the last block successively and try to match
            m = ""
            r = re.compile(r'(.*) [^ ]')
            partial = organism_name
            while m is not None:
                m = r.match(partial)
                if m is not None:
                    partial = m.group(1) + "%"
                    print "name trimmed to " + partial
                    result = ask_user_about_match(organism_name, partial)
                    if result is not None:
                        return result;
            print "nothing looks at all similar"
            return None
                
                    
def parse_data(lines):
    for line in lines:
        genome_id = ""
        print "line: " + line

        r_first_line = re.compile(r"(.*)\t([0-9\.]+)\t([0-9]+)\n")

        m = r_first_line.match(line)
        if m is not None:
            organism_name = m.group(1)
            doubling_time = m.group(2)
            optimal_growth_temp = m.group(3)

            genome_ids = get_genome_ids(organism_name)
            if genome_ids is not None:
                for genome_id in genome_ids:
    
                    if debug:
                        print "genome_id: " + str(genome_id)
    
                    if genome_id:
                        load_data(genome_id, organism_name)
    
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
