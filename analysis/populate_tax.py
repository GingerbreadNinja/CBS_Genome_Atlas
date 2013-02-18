from Bio import SeqIO
from Bio.SeqUtils import GC
import os, sys, getopt;
import MySQLdb
import inspect
import math
from datetime import *
from genomeanalysis.common import *
import traceback

debug = 0

def usage():
   sys.stderr.write(sys.argv[0] + ' accession version\n')
   sys.exit(2)


def main(argv):
    env = 'prod'
    try:
        accession = argv[0]
        version = argv[1]
    except:
        usage()
    if accession == "" or version == "":
        usage()
    else:
        (db, cur) = db_connect_transaction()
        try:
            if (not genome_is_valid(get_genome_id(accession, version))):
                sys.stderr.write("genome is not valid\n")
                exit(1)

            # this assumes that the correct data is sitting in the base tables. 
            # if the base data changes, then the changing program is responsible for removing the tax_, genome_ and replicon_stats entries
            # they can be repopulated with the appropriate update_* functions

            data = read_base_replicon_data(cur, accession, version)
            if data:
                tax_id, all_data = data
                add_accession_to_dgreplicon(cur, env, accession, version, tax_id, all_data)
                genome_data = add_accession_to_dggenome(cur, env, accession, version, tax_id, all_data)
            
                while tax_id not in (1, 68336, 2, 2157, 131550, 51290, 200795, 51290): #don't write data for parents of top level phyla; exclude
                    add_accession_to_dgtax(cur, env, accession, version, tax_id, all_data, genome_data)
                    tax_id = get_tax_parent(cur, env, tax_id)

                add_accession_to_dgtax(cur, env, accession, version, 1, all_data, genome_data) #might as well update the root too (even though it's never displayed)
                db.commit()
            else:
                sys.stderr.write("no data available for accession " + accession + " version " + version + " so skipping.\n")
                
        except Exception, e:
            db.rollback()
            msg = traceback.format_exc()
            sys.stderr.write(msg + "\n" + e.message + "\nsomething happened with accession " + accession + " version " + version + ", not writing to database\n")
            exit(1)

if __name__ == "__main__":
   main(sys.argv[1:])
