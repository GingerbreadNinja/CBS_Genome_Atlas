import MySQLdb;
import getopt
import uuid
import sys
from subprocess import call
from genomeanalysis.common import *

# this script can be run from cron.hourly and also as a one off, if an accession number is given

# If running from cron, it does the following:
# - registers this run of the script in the cron_log table
# - processes any new genomes since last run date
# - re-processes any genomes that failed and send permanent failures for manual review TODO

# if backfill is specified, it will look for a date in the cron_job with the backfill flag set, and refill from that date
# TODO maybe it should backfill from a date specified

# Otherwise, it just processes the accession number given

# Processing a genome means
# - register the job in the database active_jobs table
# - enqueue the job on the job system
# the job is a makefile that contains all the processing steps needed to analyze the genome
# the job is removed from the database by the last instruction in the makefile

debug = True
env = "prod"

def usage():
    sys.stderr.write('usage:\n\t' + sys.argv[0] + ' --accession=CP000020 --version=2\n\t' + sys.argv[0] + ' --av=CP000020_2\n\t' + sys.argv[0] + ' --latest\t(run on everything since script last ran)\n\t' + sys.argv[0] + ' --backfill\t(run on everything since last backfill)\n\t' + sys.argv[0] + ' --date=2012-11-05\t(run on everything modified since date)\n\toptional:\t--maketag=populate_tax\n\t\t--cron (if running from cron)\n\t--missing\t (everything that has a replicon that has not been added to tax tables)\n');
    sys.exit(2)

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
    cur=db_connect(env)
    cur.execute("""INSERT INTO active_job (job_id, submission_time, job_uuid, accession, version, status, cron_id) VALUES (%s, now(), %s, %s, %s, 'In Progress', %s)""", (job_id, job_uuid, accession, version, cronid))
    return str(job_uuid)

def process_one_genome(accession, version, maketag, cronid):
    # TODO check that accession file exists on disk
    # TODO if not passed version, get latest version for this accession number

    # write to db active_jobs table that we are submitting to the queueing system
    job_uuid = register_job(accession, version, maketag, cronid)

    # TODO get this information from a config file
    logging_dir = "/home/panfs/cbs/projects/cge/data/public/genome_sync/log/"
    makefile = "/home/people/helen/CBS_Genome_Atlas/analysis/Makefile"
    #filename = "./command" + "_" + accession + "_" + job_uuid
    #f = open(filename, 'w')
    #string = "make -i -k " + maketag + " ACCESSION=" + accession + " VERSION=" + str(version) + " JOB_UUID=" + job_uuid
    #f.write(string)
    #f.close()
    #call(["qsub", "-l", "mem=4gb,walltime=3600,ncpus=1", "-N", accession + "_" + str(version) + "_" + job_uuid, "-r", "y", filename])
    # am I supposed to add "-q cge" here?  
    string = "xmsub -l mem=4gb,walltime=3600,procs=4,partition=cge-cluster -de -d " + logging_dir + " -ro " + logging_dir + accession + "_" + job_uuid + ".log -re " + logging_dir + accession + "_" + job_uuid + ".out -N " + accession + "_" + str(version) + "_" + job_uuid + " -r y make -i -k --file=" + makefile + " " + maketag + " ACCESSION=" + accession + " VERSION=" + str(version) + " JOB_UUID=" + job_uuid
    print string
    #call(["xmsub", "-l", "mem=4gb,walltime=3600,procs=4,partition=cge-cluster", "-de", "-d", logging_dir, "-ro", logging_dir + accession + "_" + job_uuid + ".log", "-re", logging_dir + accession + "_" + job_uuid + ".out", "-N", accession + "_" + str(version) + "_" + job_uuid, "-r", "y", "make -i -k --file=" + makefile, maketag, "ACCESSION=" + accession, "VERSION=" + str(version), "JOB_UUID=" + job_uuid])
    os.system(string)
 
def get_last_runtime(backfill, cron_id):
    # check in db for latest runtime
    cur = db_connect(env)
    if backfill:
        cur.execute("""SELECT start_time FROM cron_log WHERE runas = 'backfill' and id != %s ORDER BY start_time DESC LIMIT 1""", cron_id)
    else:
        cur.execute("""SELECT start_time FROM cron_log WHERE id != %s ORDER BY start_time DESC LIMIT 1""", cron_id)
    time = 0
    rows = cur.fetchall()
    if rows:
        for row in rows:
            time = row['start_time']
            if debug:
                print "last run time is " + str(time)
        return time
    else:
        return "1970-01-01"

def log_new_run(runas):
    cur=db_connect(env)
    cur.execute("""INSERT INTO cron_log (start_time, runas) VALUES (now(), %s)""", runas)
    #TODO error checking
    cronid = cur.lastrowid
    return cronid

def new_genomes(time):
    # get list of all new genomes added since last runtime
    cur=db_connect(env)
    if time == "all":
        cur.execute("""SELECT accession, version FROM bioproject, genome, replicon WHERE genome.bioproject_id = bioproject.bioproject_id AND genome.genome_id = replicon.genome_id""")
    else:
        cur.execute("""SELECT accession, version FROM bioproject, genome, replicon WHERE genome.bioproject_id = bioproject.bioproject_id AND genome.genome_id = replicon.genome_id AND modify_date >= %s""", (time)) 
    return cur.fetchall()

def process_old_genomes(date, maketag, cronid):
    for row in new_genomes(date):
        accession = row['accession']
        version = row['version']
        if debug:
            print "accession = " + accession + "\tversion = " + str(version)
        process_one_genome(accession, version, maketag, cronid)
    
def process_new_genomes(backfill, maketag, cronid):
    last_runtime = get_last_runtime(backfill, cronid)
    process_old_genomes(last_runtime, maketag, cronid)
    
def missing_genomes():
    # get list of all replicons not in the displaygenome_* tables
    cur = db_connect(env)
    #cur.execute("""SELECT accession, version FROM replicon WHERE accession NOT IN (SELECT accession FROM displaygenome_replicon_stats) and replicon_type = 'CHROMOSOME' or replicon_type = 'PLASMID'""") #original
    #cur.execute("""SELECT accession, version FROM replicon JOIN genome USING (genome_id) WHERE stat_size_bp IS NULL AND replicon_type = 'CHROMOSOME' OR replicon_type = 'PLASMID' AND genome_validity = 'VALID'""") # get all complete genomes
    #cur.execute("""SELECT accession, version FROM replicon JOIN genome USING (genome_id) WHERE stat_size_bp IS NULL AND replicon_type = 'WGS' AND genome_validity != 'VALID' and accession not in (select accession from active_job)""") # get all *valid* WGS genomes that have not been processed before
    cur.execute("""SELECT accession, version from replicon join genome using (genome_id) where genome_id not in (select genome_id from displaygenome_genome_stats) and accession not in (select accession from active_job)""")
    return cur.fetchall()

def process_missing_genomes(maketag, cronid):
    max = 500
    acc = 0
    for row in missing_genomes():
        process_one_genome(row['accession'], row['version'], maketag, cronid)
        acc = acc + 1
        if acc >= max:
            break

def main(argv):
    try:
        opts, args = getopt.getopt(sys.argv[1:], "h", ["help", "latest", "missing", "accession=", "version=", "av=", "backfill", "date=", "maketag=", "cron"])
    except getopt.GetoptError as err:
        sys.stderr.write(str(err) + "\n")
        usage()
    plan = ""
    accession = ""
    version = ""
    backfill = False
    date = False
    maketag = "single_genome"
    runas = "oneoff"
    for o, a in opts:
        if o in ("-h", "--help"):
            usage()
        elif o in ("--latest"):
            plan = "all"
        elif o in ("--missing"):
            plan = "missing"
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
        elif o in ("--date"):
            plan = "all"
            runas = "oneoff"
            date = a
        elif o in ("--maketag"):
            maketag = a
        elif o in ("--cron"):
            runas = "cron"
        else:
            sys.stderr.write("Unregognized option " + o + "\n");
            usage()
    if plan == "all":
        cronid = log_new_run(runas)
        if date:
            process_old_genomes(date, maketag, cronid)
        else:
            process_new_genomes(backfill, maketag, cronid)
    elif plan == "missing":
        cronid = log_new_run(runas)
        process_missing_genomes(maketag, cronid)
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

