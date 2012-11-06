import MySQLdb;
import sys;
import getopt;


def usage():
    sys.stderr.write('usage:\n\t' + sys.argv[0] + ' --start --job=single_genome --jobstep=rnammer --accession=CP000020 --version=2\n\t' + sys.argv[0] + ' --finish --logid=23\n\t' + sys.argv[0] + ' --remove --job_uuid=job_uuid\n');
    sys.exit(2)

def db_connect():
    #TODO pull this out into a module -- I think Steve has one
    db = MySQLdb.connect(host="mysql", port=3306,db="steve_private", read_default_file="~/.my.cnf")
    cur = db.cursor()
    return cur

def start(job, job_uuid, jobstep, accession, version):
    cur = db_connect()
    cur.execute("""INSERT INTO jobstep_log (start_time, status, job_id, job_uuid, jobstep_id, accession, version) VALUES (now(), %s, (SELECT job_id FROM job WHERE name=%s), %s, (SELECT jobstep_id FROM jobstep WHERE name=%s), %s, %s)""", ('In Progress', job, job_uuid, jobstep, accession, version))
    #TODO error checking
    print cur.lastrowid

def finish(logid):
    cur = db_connect()
    cur.execute("""UPDATE jobstep_log SET status='Successful' WHERE log_id=%s""", (logid))
    #TODO error checking

def remove(job_uuid):
    cur=db_connect()
    cur.execute("""DELETE from active_jobs WHERE job_uuid=%s""", (job_uuid))
    #TODO error checking

def main(argv):
    try:
        opts, args = getopt.getopt(sys.argv[1:], "h", ["help", "start", "finish", "remove", "job=", "job_uuid=", "jobstep=", "accession=", "version=", "logid="])
    except getopt.GetoptError as err:
        sys.stderr.write(str(err) + "\n")
        usage()
    state = ""
    job = ""
    job_uuid = ""
    jobstep = ""
    accession = ""
    version = ""
    logid = ""
    for o, a in opts:
        if o in ("-h", "--help"):
            usage()
        elif o in ("--job"):
            job = a
        elif o in ("--job_uuid"):
            job_uuid = a
        elif o in ("--jobstep"):
            jobstep = a
        elif o in ("--accession"):
            accession = a
        elif o in ("--version"):
            version = a
        elif o in ("--logid"):
            logid = a
        elif o in ("--start"):
            state = "start"
        elif o in ("--finish"):
            state = "finish"
        elif o in ("--remove"):
            state = "remove"
        else:
            sys.stderr.write("Unregognized option " + o + "\n");
            usage()
    if state == "start":
        if job == "" or job_uuid == "" or jobstep == "" or accession == "" or version == "":
            sys.stderr.write("Must specify all of job, job_uuid, jobstep, accession and version with the start option.\n")
            usage()
        else:
            start(job, job_uuid, jobstep, accession, version)
    if state == "finish":
        if logid == "":
            sys.stderr.write("Must specify logid with the finish option.\n")
            usage()
        else:
            finish(logid)
    if state == "remove":
        remove()

if __name__ == "__main__":
    main(sys.argv[1:])
