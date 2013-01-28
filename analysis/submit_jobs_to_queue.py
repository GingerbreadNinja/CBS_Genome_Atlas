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
    sys.stderr.write('usage:\n\t' + sys.argv[0] + ' --accession=CP000020 --version=2\n\t' + sys.argv[0] + ' --av=CP000020_2\n\t' + sys.argv[0] + ' --latest\t(run on everything since script last ran)\n\t' + sys.argv[0] + ' --backfill\t(run on everything)\noptional:\t--maketag=populate_tax\n\t\t--cron (if running from cron)\n');
    sys.exit(2)

def db_connect():
    #TODO pull this out into a module -- I think Steve has one
    db = MySQLdb.connect(host="mysql", port=3306,db="steve_private", read_default_file="~/.my.cnf")
    cur = db.cursor()
    return cur 

def register_job(accession, version, maketag, cronid):
    job_id = 0
    if maketag == "single_genome":
        job_id = 1 
    if maketag == "populate_tax":
        job_id = 2
    if job_id == 0:
        sys.stderr.write("maketag " + maketag + " is invalid.\n")
        usage()
    job_uuid = uuid.uuid1()
    cur=db_connect()
    cur.execute("""INSERT INTO active_job (job_id, submission_time, job_uuid, accession, version, status, cron_id) VALUES (%s, now(), %s, %s, %s, 'In Progress', %s)""", (job_id, job_uuid, accession, version, cronid))
    return str(job_uuid)

def process_one_genome(accession, version, maketag, cronid):
    # TODO check that accession file exists on disk
    # TODO if not passed version, get latest version for this accession number

    # write to db active_jobs table that we are submitting to the queueing system
    job_uuid = register_job(accession, version, maketag, cronid)

    # TODO get this information from a config file
    logging_dir = "/home/panfs/cbs/projects/cge/data/public/genome_sync/log/"
    filename = "./command" + "_" + accession + "_" + job_uuid
    f = open(filename, 'w')
    string = "make -i -k" + maketag + "ACCESSION=" + accession + "VERSION=" + str(version) + "JOB_UUID=" + job_uuid
    f.write(string)
    f.close()
    call(["qsub", "-l", "mem=4gb,walltime=3600,ncpus=1", "-N", accession + "_" + str(version) + "_" + job_uuid, "-r", "y", filename])
    #call(["xmsub", "-l", "mem=4gb,walltime=3600,procs=4,partition=cge-cluster", "-q cge", "-de", "-d", logging_dir, "-ro", logging_dir + accession + "_" + job_uuid + ".log", "-re", logging_dir + accession + "_" + job_uuid + ".out", "-N", accession + "_" + str(version) + "_" + job_uuid, "-r", "y", "make -i -k", maketag, "ACCESSION=" + accession, "VERSION=" + str(version), "JOB_UUID=" + job_uuid])
 
def get_last_runtime(backfill):
    # check in db for latest runtime
    cur = db_connect()
    if backfill:
        cur.execute("""SELECT start_time FROM cron_log WHERE runas = 'backfill' ORDER BY start_time DESC LIMIT 1""")
    else:
        cur.execute("""SELECT start_time FROM cron_log ORDER BY start_time DESC LIMIT 1""")
    time = 0
    for row in cur.fetchall():
        time = row[0]
        if debug:
            print "last run time is " + str(time)
    return time

def log_new_run(runas):
    cur=db_connect()
    cur.execute("""INSERT INTO cron_log (start_time, runas) VALUES (now(), %s)""", runas)
    #TODO error checking
    cronid = cur.lastrowid
    return cronid

def new_genomes(time):
    # get list of all new genomes added since last runtime
    cur=db_connect()
    cur.execute("""SELECT accession, version FROM bioproject, genome, replicon WHERE genome.bioproject_id = bioproject.bioproject_id AND genome.genome_id = replicon.genome_id AND modify_date >= %s""", (time)) 
    return cur.fetchall()

def process_new_genomes(backfill, maketag, cronid):
    last_runtime = get_last_runtime(backfill)
    for accession, version in new_genomes(last_runtime):
        if debug:
            print "accession = " + accession + "\tversion = " + str(version)
        process_one_genome(accession, version, maketag, cronid)
    
def failed_genomes():
    # get list of all genomes to reprocess;
    return []

def process_failed_genomes(maketag, cronid):
    for accession in failed_genomes():
        process_one_genome(accession, version, maketag, cronid)

def main(argv):
    try:
        opts, args = getopt.getopt(sys.argv[1:], "h", ["help", "latest", "accession=", "version=", "av=", "backfill", "maketag=", "cron"])
    except getopt.GetoptError as err:
        sys.stderr.write(str(err) + "\n")
        usage()
    plan = ""
    accession = ""
    version = ""
    backfill = False
    maketag = "single_genome"
    runas = "oneoff"
    for o, a in opts:
        if o in ("-h", "--help"):
            usage()
        elif o in ("--latest"):
            plan = "all"
        elif o in ("--accession"):
            plan = "one"
            accession = a
        elif o in ("--version"):
            version = a
        elif o in ("--av"):
            plan = "one"
            accession=a[0:8] #XXX this breaks for badly formatted accession numbers
            version=a[-1]
        elif o in ("--backfill"):
            plan = "all"
            backfill = True
            runas = "backfill"
        elif o in ("--maketag"):
            maketag = a
        elif o in ("--cron"):
            runas = "cron"
        else:
            sys.stderr.write("Unregognized option " + o + "\n");
            usage()
    if plan == "all":
        cronid = log_new_run(runas)
        process_new_genomes(backfill, maketag, cronid)
        #process_failed_genomes(maketag, cronid) TODO
    elif plan == "one":
        if version == "" or accession == "":
            usage()
        else:
            cronid = log_new_run(runas)
            process_one_genome(accession, version, maketag, cronid)
    else:
        usage()

if __name__ == "__main__":
    main(sys.argv[1:])

