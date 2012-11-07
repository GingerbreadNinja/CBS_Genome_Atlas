import MySQLdb;
import sys;
import getopt;

debug = False

def usage():
    sys.stderr.write('usage:\n\t' + sys.argv[0] + ' --start --job=single_genome --jobstep=rnammer --accession=CP000020 --version=2\n\t' + sys.argv[0] + ' --finish=success --logid=23\n\t' + sys.argv[0] + ' --finish=failure --logid=23\n\t' + sys.argv[0] + ' --remove_job --job_uuid=job_uuid\n');
    sys.exit(2)

def db_connect():
    #TODO pull this out into a module -- I think Steve has one
    db = MySQLdb.connect(host="mysql", port=3306,db="steve_private", read_default_file="~/.my.cnf")
    cur = db.cursor()
    return cur

def start(job, job_uuid, jobstep, accession, version):
    if debug:
        print "logging start of jobstep"
    cur = db_connect()
    cur.execute("""INSERT INTO jobstep_log (start_time, status, job_id, job_uuid, jobstep_id, accession, version) VALUES (now(), %s, (SELECT job_id FROM job WHERE job_name=%s), %s, (SELECT jobstep_id FROM jobstep WHERE jobstep_name=%s), %s, %s)""", ('In Progress', job, job_uuid, jobstep, accession, version))
    #TODO error checking
    print cur.lastrowid

def finish(logid, success):
    if debug:
        print "logging end of jobstep"
    cur = db_connect()
    if success == "success":
        cur.execute("""UPDATE jobstep_log SET status='Success' WHERE log_id=%s""", (logid))
    elif success == "failure":
        cur.execute("""UPDATE jobstep_log SET status='Failure' WHERE log_id=%s""", (logid))

def remove_job(job_uuid):
    if debug:
        print "removing job"
    cur=db_connect()
    #TODO check that success passed in was really successful!
    cur.execute("""SELECT * FROM jobstep_log WHERE job_uuid=%s AND status='Failure'""", (job_uuid))
    success = True
    for row in cur.fetchall():
        success = False
        # TODO add in reporting code here
    if success:
        cur.execute("""UPDATE active_job SET status="Success" WHERE job_uuid=%s""", (job_uuid))
    else:
        cur.execute("""UPDATE active_job SET status="Failure" WHERE job_uuid=%s""", (job_uuid))

def main(argv):
    try:
        opts, args = getopt.getopt(sys.argv[1:], "h", ["help", "start", "finish=", "remove_job", "job=", "job_uuid=", "jobstep=", "accession=", "version=", "logid="])
    except getopt.GetoptError as err:
        sys.stderr.write(str(err) + "\n")
        usage()
    state = ""
    job = ""
    job_uuid = ""
    jobstep = ""
    accession = ""
    version = ""
    success = ""
    job_success = ""
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
            success = a
        elif o in ("--remove_job"):
            state = "remove_job"
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
            if success == "":
                sys.stderr.write("Must specify success or failure with the finish option.\n")
            else:
                finish(logid, success)
    if state == "remove_job":
        remove_job(job_uuid)

if __name__ == "__main__":
    main(sys.argv[1:])
