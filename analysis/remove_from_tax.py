import MySQLdb;
import getopt
import uuid
import sys
from subprocess import call
from genomeanalysis.common import *

env = "prod"

def usage():
   sys.stderr.write(sys.argv[0] + ' accession version\n')
   sys.exit(2)


# this script will safely remove an accession or genome from the tax table.  
# the values that we want to remove come from the base genome, replicon etc tables.

def main(argv):
    try:
        accession = argv[0]
        version = argv[1]
    except:
        usage()
    if accession == "" or version == "":
        usage()
    else:
        remove_all_replicons_for_genome = False #TODO add this switch
        db, cur = db_connect_transaction(env)
        try:
            genome_id = get_genome_id(accession, version)
            if (not genome_is_valid(genome_id)):
                sys.stderr.write("genome is not valid\n")
                exit(1)

            # this assumes that the correct data is sitting in the base tables. 
            # if the base data changes, then the changing program is responsible for removing the tax_, genome_ and replicon_stats entries using the appropriate functions in genomeanalysis.common

            if remove_all_replicons_for_genome:
                replicons_to_remove = get_replicons(cur, genome_id)
            else:
                replicons_to_remove = {'accession': accession, 'version': version}

            for r in replicons_to_remove:
                accession = r['accession']
                version = r['version']
                tax_id, all_data = read_base_replicon_data(cur, accession, version) #gets current data from base replicon table

                remove_accession_from_replicon(cur, accession, version)
                genome_data = remove_accession_from_genome(cur, accession, version, genome_id)

                while tax_id not in top_level_phyla: #don't write data for parents of top level phyla; exclude
                    remove_accession_from_tax(cur, tax_id, accession, version, all_data, genome_data)
                    tax_id = get_parent(cur, tax_id)

                remove_accession_from_tax(cur, 1, accession, version, all_data, genome_data) #might as well update the root too (even though it's never displayed)

            db.commit()
        except:
            db.rollback()
            sys.stderr.write("something happened with accession " + accession + " version " + version + ", not writing to database\n")
            exit(1)

if __name__ == "__main__":
   main(sys.argv[1:])
