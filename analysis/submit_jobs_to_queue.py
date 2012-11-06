import MySQLdb;
import getopt
import uuid
import sys
from subprocess import call

# this script can be run from cron.hourly and also as a one off, if an accession number is given

# If running from cron, it does the following:
# - processes any new genomes since last run date
# - re-processes any genomes that failed and send permanent failures for manual review

# Otherwise, it just processes the accession number given

# Processing a genome means
# - register the job in the database
# - enqueue the job on the job system
# the job is a makefile that contains all the processing steps needed to analyze the genome
# the job is removed from the database by the last instruction in the makefile

def usage():
    sys.stderr.write('usage:\n\t' + sys.argv[0] + ' --accession=CP000020 --version=2\n\t' + sys.argv[0] + ' --all\n');
    sys.exit(2)

def db_connect():
    #TODO pull this out into a module -- I think Steve has one
    db = MySQLdb.connect(host="mysql", port=3306,db="steve_private", read_default_file="~/.my.cnf")
    cur = db.cursor()
    return cur 

def last_runtime():
    # check in db for latest runtime
    cur = db_connect()
    cur.execute("""SELECT start_time FROM cron_log ORDER BY id DESC LIMIT 1""")
    time = 0
    for row in cur.fetchall():
        time = row[0]
        print "last run time is " + time
    return time

def new_genomes(time):
    # get list of all new genomes added since last runtime
    cur=db_connect()
    cur.execute("""SELECT accession, version, replicon.genome_id FROM bioproject, genome, replicon WHERE genome.bioproject_id = bioproject.bioproject_id AND genome.genome_id = replicon.genome_id AND modify_date >= %s""", (time)) 
    accesions = []
    for r in cur.fetchall():
        av = r[0] + "_" + r[1] # accession is actually accession_version
        accession.append(av)
    return accessions

def failed_genomes():
    # get list of all genomes to reprocess;
    return []

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

    logging_dir = "/home/people/helen/CBS_Genome_Atlas/analysis/"
    #command = "xmsub -l nodes=1:ppn=2 -de -ro " + logging_dir + "out -re " + logging_dir + "err -N " + job_uuid + " -r y make ACCESSION=" + accession + " VERSION=" + version + " JOB_UUID=testuuid"
    call(["xmsub", "-l", "nodes=1:ppn=2", "-de", "-ro", logging_dir + "out", "-re", logging_dir + "err", "-N", job_uuid, "-r", "y", "make", "ACCESSION=" + accession, "VERSION=" + version, "JOB_UUID=" + job_uuid])
    
def process_new_genomes():
    for accession in new_genomes(last_runtime()):
        process_one_genome(accession)
    
def process_failed_genomes():
    for accession in failed_genomes():
        process_one_genome(accession)

def main(argv):
    try:
        opts, args = getopt.getopt(sys.argv[1:], "h", ["help", "all", "accession=", "version="])
    except getopt.GetoptError as err:
        sys.stderr.write(str(err) + "\n")
        usage()
    plan = ""
    accession = ""
    version = ""
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
        else:
            sys.stderr.write("Unregognized option " + o + "\n");
            usage()
    if plan == "all":
        process_new_genomes()
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

