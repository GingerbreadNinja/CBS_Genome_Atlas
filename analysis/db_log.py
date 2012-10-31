import MySQLdb;
import sys;
import getopt;


def usage():
    sys.stderr.write('usage:\n\t' + sys.argv[0] + ' --start --job=single_genome --jobstep=rnammer --accession=CP000230\n\t' + sys.argv[0] + ' --finish --logid=23\n');
    sys.exit(2)

def db_connect():
    #TODO pull this out into a module -- I think Steve has one
    db = MySQLdb.connect(host="mysql", port=3306,db="steve_private", read_default_file="~/.my.cnf")
    cur = db.cursor()
    return cur

def start(job, jobstep, accession):
    cur = db_connect()
    cur.execute("""INSERT INTO jobstep_log (start_time, status, job_id, jobstep_id, accession) VALUES (now(), %s, (SELECT job_id FROM job WHERE name=%s), (SELECT jobstep_id FROM jobstep WHERE name=%s), %s)""", ('In Progress', job, jobstep, accession))
    print cur.lastrowid

def finish(logid):
    cur = db_connect()
    cur.execute("""UPDATE jobstep_log SET status='Successful' WHERE log_id=%s""", (logid))

def main(argv):
    try:
        opts, args = getopt.getopt(sys.argv[1:], "h", ["help", "start", "finish", "job=", "jobstep=", "accession=", "logid="])
    except getopt.GetoptError as err:
        sys.stderr.write(str(err) + "\n")
        usage()
    state = ""
    job = ""
    jobstep = ""
    accession = ""
    logid = ""
    for o, a in opts:
        if o in ("-h", "--help"):
            usage()
        elif o in ("--job"):
            job = a
        elif o in ("--jobstep"):
            jobstep = a
        elif o in ("--accession"):
            accession = a
        elif o in ("--logid"):
            logid = a
        elif o in ("--start"):
            state = "start"
        elif o in ("--finish"):
            state = "finish"
        else:
            sys.stderr.write("Unregognized option " + o + "\n");
            usage()
    if state == "start":
        if job == "" or jobstep == "" or accession == "":
            sys.stderr.write("Must specify all of job, jobstep and accession with the start option\n")
            usage()
        else:
            start(job, jobstep, accession)
    if state == "finish":
        if logid == "":
            sys.stderr.write("Must specify logid with the finish option\n")
            usage()
        else:
            finish(logid)

if __name__ == "__main__":
    main(sys.argv[1:])
