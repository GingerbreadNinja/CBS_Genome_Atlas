import MySQLdb;
import getopt
import uuid
import sys
from subprocess import call

# this script can be run from cron.hourly and also as a one off, if an accession number is given

# If running from cron, it does the following:
# - registers this run of the script in the cron_log table
# - processes any new genomes since last run date
# - re-processes any genomes that failed and send permanent failures for manual review

# if backfill is specified, it will look for a date in the cron_job with the backfill flag set, and refill from that date
# TODO maybe it should backfill from a date specified

# Otherwise, it just processes the accession number given

# Processing a genome means
# - register the job in the database active_jobs table
# - enqueue the job on the job system
# the job is a makefile that contains all the processing steps needed to analyze the genome
# the job is removed from the database by the last instruction in the makefile

debug = True

def usage():
    sys.stderr.write('usage:\n\t' + sys.argv[0] + ' --accession=CP000020 --version=2\n\t' + sys.argv[0] + ' --av=CP000020_2\n\t' + sys.argv[0] + ' --all [--backfill]\n');
    sys.exit(2)

def db_connect():
    #TODO pull this out into a module -- I think Steve has one
    db = MySQLdb.connect(host="mysql", port=3306,db="steve_private", read_default_file="~/.my.cnf")
    cur = db.cursor()
    return cur 

def register_job(accession, version):
    job_id = 1 #TODO do not hardcode this
    job_uuid = uuid.uuid1()
    cur=db_connect()
    cur.execute("""INSERT INTO active_job (job_id, submission_time, job_uuid, accession, version, status) VALUES (%s, now(), %s, %s, %s, 'In Progress')""", (job_id, job_uuid, accession, version))
    return str(job_uuid)

def process_one_genome(accession, version):
    # TODO check that accession file exists on disk
    # TODO if not passed version, get latest version for this accession number

    # write to db active_jobs table that we are submitting to the queueing system
    job_uuid = register_job(accession, version)

    # TODO get this information from a config file
    logging_dir = "/home/panfs/cbs/projects/cge/data/public/genome_sync/log/"
    call(["xmsub", "-l", "mem=4gb,walltime=3600,procs=8,partition=cge-cluster", "-de", "-ro", logging_dir + accession + "_" + job_uuid + ".log", "-re", logging_dir + accession + "_" + job_uuid + ".out", "-N", job_uuid, "-r", "y", "make -i -k", "ACCESSION=" + accession, "VERSION=" + str(version), "JOB_UUID=" + job_uuid])
 
def get_last_runtime(backfill):
    # check in db for latest runtime
    cur = db_connect()
    if backfill:
        cur.execute("""SELECT start_time FROM cron_log WHERE responsible = 'backfill' ORDER BY start_time DESC LIMIT 1""")
    else:
        cur.execute("""SELECT start_time FROM cron_log ORDER BY start_time DESC LIMIT 1""")
    time = 0
    for row in cur.fetchall():
        time = row[0]
        if debug:
            print "last run time is " + str(time)
    return time

def log_new_run():
    cur=db_connect()
    cur.execute("""INSERT INTO cron_log (start_time, responsible) VALUES (now(), "cron")""")
    #TODO error checking

def new_genomes(time):
    # get list of all new genomes added since last runtime
    cur=db_connect()
    cur.execute("""SELECT accession, version FROM bioproject, genome, replicon WHERE genome.bioproject_id = bioproject.bioproject_id AND genome.genome_id = replicon.genome_id AND modify_date >= %s""", (time)) 
    return cur.fetchall()

def process_new_genomes(backfill):
    last_runtime = get_last_runtime(backfill)
    log_new_run()
    for accession, version in new_genomes(last_runtime):
        if debug:
            print "accession = " + accession + "\tversion = " + str(version)
        process_one_genome(accession, version)
    
def failed_genomes():
    # get list of all genomes to reprocess;
    return []

def process_failed_genomes():
    for accession in failed_genomes():
        process_one_genome(accession)

def main(argv):
    try:
        opts, args = getopt.getopt(sys.argv[1:], "h", ["help", "all", "accession=", "version=", "av=", "backfill"])
    except getopt.GetoptError as err:
        sys.stderr.write(str(err) + "\n")
        usage()
    plan = ""
    accession = ""
    version = ""
    backfill = False
    for o, a in opts:
        if o in ("-h", "--help"):
            usage()
        elif o in ("--all"):
            plan = "all"
        elif o in ("--accession"):
            plan = "one"
            accession = a
        elif o in ("--version"):
            version = a
        elif o in ("--av"):
            plan = "one"
            accession=a[0:8]
            version=a[-1]
        elif o in ("--backfill"):
            backfill = True
        else:
            sys.stderr.write("Unregognized option " + o + "\n");
            usage()
    if plan == "all":
        process_new_genomes(backfill)
        process_failed_genomes()
    elif plan == "one":
        if version == "" or accession == "":
            usage()
        else:
            process_one_genome(accession, version)
    else:
        usage()

if __name__ == "__main__":
    main(sys.argv[1:])

