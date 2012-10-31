import MySQLdb;
import sys;
import getopt;



def usage():
    sys.stderr.write('usage: ' + sys.argv[0] + ' CP000230.trna');
    sys.exit(2)

def db_connect():
    #TODO pull this out into a module -- I think Steve has one
    db = MySQLdb.connect(host="mysql", port=3306,db="steve_private", read_default_file="~/.my.cnf")
    cur = db.cursor()

def write_stats():
    cur.execute("""INSERT INTO trna_stats (start_time, status, job_id, jobstep_id, accession) VALUES (now(), %s, (SELECT job_id FROM job WHERE name=%s), (SELECT jobstep_id FROM jobstep WHERE name=%s), %s)""", ('In Progress', job, jobstep, accession))

def parse_prediction():

def main(argv):


if __name__ == "__main__":
    main(sys.argv[1:])
