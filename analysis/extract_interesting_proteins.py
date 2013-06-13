from Bio import SeqIO
from Bio.SeqUtils import GC
import os, sys, getopt;
import inspect
from genomeanalysis.common import *

env = get_env()

debug = True

def usage():
    sys.stderr.write(sys.argv[0] + ' --accession accession\n')
    sys.exit(2)

def get_genome_id_cheap(cur, accession):
    try:
        cur.execute("""SELECT genome_id from replicon where accession = %s""", (accession))
        row = cur.fetchone()
        if row:
            genome_id = row['genome_id']
            return genome_id
        else:
            return -1
    except:
        return -1

def get_proteins(accession):
    cur = db_connect("prod")


    genome_id = get_genome_id_cheap(cur, accession)

    cur.execute("""select genome_id, accession, protein_id, pfam_id from protein_has_pfam where protein_id in (select protein_id from protein join protein_has_pfam using(protein_id) where protein.genome_id = %s and pfam_id in ('PF00140', 'PF04539', 'PF04542', 'PF04545', 'PF04546', 'PF08281', 'PF03979', 'PF00309', 'PF04963', 'PF04552', 'PF07638', 'PB000208') order by pos) and genome_id = %s order by protein_id, pos""", (genome_id, genome_id))

    #cur.execute("""SELECT * FROM protein_has_pfam where protein_id in (select protein_id from protein_has_pfam join protein using(protein_id) where pfam_id IN ('PF00140', 'PF04539', 'PF04542', 'PF04545', 'PF04546', 'PF08281', 'PF03979', 'PF00309', 'PF04963', 'PF04552', 'PF07638', 'PB000208') and genome_id in (select genome_id from replicon where accession = %s))""", (accession))
    #cur.execute("""SELECT * FROM protein_has_pfam where protein_id in (SELECT protein_id FROM protein_has_pfam WHERE protein_id IN (SELECT protein_id FROM protein WHERE accession = %s) AND pfam_id IN ('PF00140', 'PF04539', 'PF04542', 'PF04545', 'PF04546', 'PF08281', 'PF03979', 'PF00309', 'PF04963', 'PF04552', 'PF07638', 'PB000208')) ORDER BY protein_id, pos""", (accession)) 
    #cur.execute("""SELECT * FROM protein_has_pfam WHERE protein_id IN (SELECT protein_id FROM protein WHERE accession = %s) AND pfam_id IN ('PF00140', 'PF04539', 'PF04542', 'PF04545', 'PF04546', 'PF08281', 'PF03979', 'PF00309', 'PF04963', 'PF04552', 'PF07638', 'PB000208') ORDER BY protein_id, pos""", (accession)) #fast but doesn't get everytihng
    rows = cur.fetchall()
    found = {}
    reference = {'rpoD': ['PF03979', 'PF00140', 'PF04546', 'PF04542', 'PF04539', 'PF04545'],
                 'rpoS': ['PF00140', 'PF04542', 'PF04539', 'PF04545'],
                 'rpoE or fecI': ['PF04542', 'PF08281'],
                 'rpoH': ['PF04542', 'PB000208', 'PF04545'],
                 'fliA': ['PF04542', 'PF04539', 'PB000208', 'PF04545'],
                 'rpoN': ['PF00309', 'PF04963', 'PF04552'],
                }

    if debug:
        print "Accession: " + accession
    for r in rows:
        #if debug:
            #print "found row: " + str(r)
        if not r['protein_id'] in found:
            found[r['protein_id']] = []
        found[r['protein_id']].append(r['pfam_id'])

    #print "FOUND: " + str(found)
    for protein in found:
        arch = found[protein]
        #if debug:
            #print "checking protein " + str(protein) + " in found"
        is_ref = False
        for ref_protein in reference:
            ref_arch = reference[ref_protein]
            if ref_arch == arch:
                print "\tHas " + str(protein) + " with " + ref_protein
                is_ref = True
                cur.execute("""UPDATE protein set sigma=%s where protein_id=%s""", (ref_protein, protein))
        if not is_ref:
            print "\tHas extra " + str(protein) + " with " + str(arch)
            cur.execute("""UPDATE protein set sigma=%s where protein_id=%s""", ("extra " + str(arch), protein))
            

        


def main(argv):
    try:
        opts, args = getopt.getopt(argv,"h",["accession="])
    except getopt.GetoptError:
        usage()
    accession = ""
    for opt, arg in opts:
        if opt == '-h':
            usage()
        elif opt in ("--accession"):
            accession = arg
        else:
            sys.stderr.write("unrecognized option " + opt + "\n")
            usage()
    if accession == "":
        sys.stderr.write("accession argument must be specified")
        usage()
    if "_" in accession:
        accession = accession.split("_")[0]
    get_proteins(accession)

if __name__ == "__main__":
    main(sys.argv[1:])
