from Bio import SeqIO
from Bio.SeqUtils import GC
import os, sys, getopt;
import inspect
from genomeanalysis.common import *

env = get_env()

def usage():
    sys.stderr.write(sys.argv[0] + ' -i <inputfile> [-o <outputfile>] [--write_stats]\n')
    sys.exit(2)

def get_length(record):
    print "length " + str(len(record))
    return len(record)

def get_nonstd_bases(record):
    number_a = record.seq.count("A")
    number_c = record.seq.count("C")
    number_g = record.seq.count("G")
    number_t = record.seq.count("T")
    nonstd_bases = get_length(record) - (number_a + number_c + number_g + number_t) 
    print "nonstd " + str(nonstd_bases)
    return nonstd_bases

def get_number_genes(record):
    number_genes = 0
    for annotation in record.features:
        if annotation.type == "gene":
             number_genes = number_genes + 1
    return number_genes

def get_number_contigs(record, inputfile):
    number_contigs = 1
    if record.annotations['data_file_division'] == 'CON':
        try:
             sys.stderr.write("have contigs, opening contig file\n")
             contig_handle = open(inputfile + ".contig", "rU")
             contig_sequences = SeqIO.parse(contig_handle, "genbank")
             contig_records = 0
             for contig_record in contig_sequences:
                 contig_records += 1
                 contigs = contig_record.annotations.get('contig',None)
                 if contigs != None:
                     number_contigs = contigs.count(",") + 1  # contigs are separated by commas
                 else:
                     sys.stderr.write('convert_to_fasta: no contigs found in contig file\n')
                     sys.exit(1)
             if contig_records > 1:
                 sys.stderr.write('convert_to_fasta: Found more than 1 locus in contig file\n')
                 sys.exit(1)

        except IOError as e:
            sys.exit(1)
    return number_contigs

def get_at_bp(record):
    percent_gc = GC(record.seq)
    percent_at = 100 - percent_gc
    at_bp = percent_at * get_length(record)
    return at_bp

def convert(inputfile, outputfile, write_stats_db):
    sys.stderr.write('Input file is ' + inputfile + "\n")
    sys.stderr.write('Output file is ' + outputfile + "\n")
    
    (accession, version) = parse_filename(inputfile)
    genome_id = get_genome_id(accession, version)
    if (genome_id > 0):
        if (not genome_is_valid(genome_id)):
            sys.stderr.write("genome " + str(genome_id) + " has warnings or is invalid\n")
            exit(1)
    else:
        sys.stderr.write("genome_id not found\n")
        exit(1)

    try:
        input_handle = open(inputfile, "rU")
        output_handle = open(outputfile, "w")
        sequences = SeqIO.parse(input_handle, "genbank")

        number_records = 0

        replicon_length = 0
        nonstd_bases = 0
        at_bp = 0
        number_genes = 0
        number_contigs = 0

        for record in sequences:
            number_records += 1 #number of records in file (different from number of contigs)
            if write_stats_db:
                (accession, version) = parse_string(record.id)
                replicon_length += get_length(record) 
                nonstd_bases += get_nonstd_bases(record)
                at_bp += get_at_bp(record)
                number_genes += get_number_genes(record)
                number_contigs += get_number_contigs(record, inputfile) #number of contigs; a single record may hold multiple contigs

        if write_stats_db:
            sys.stderr.write("replicon_length: " + str(replicon_length) + "\n")
            sys.stderr.write("nonstd_bases: " + str(nonstd_bases) + "\n")
            percent_at = '{:.2f}'.format(at_bp / replicon_length)
            sys.stderr.write("percent_at: " + str(percent_at) + "\n")
            sys.stderr.write("number of genes: " + str(number_genes) + "\n")
            sys.stderr.write("number of contigs: " + str(number_contigs) + "\n")
            sys.stderr.write("accession: " + accession + "\n")
            sys.stderr.write("version: " + str(version) + "\n")

            cur = db_connect(env)
            try: 
                sys.stderr.write("writing all that to db\n")
                cur.execute("""UPDATE replicon SET stat_size_bp = %s, stat_number_nonstd_bases = %s, stat_perc_at = %s, stat_number_of_genes = %s, stat_number_of_contigs = %s WHERE accession = %s and version = %s""", (replicon_length, nonstd_bases, percent_at, number_genes, number_contigs, accession, version))
                sys.stderr.write("ok\n")
            except:
                sys.stderr.write("could not write stats to db\n")


        if number_records > 1:
            sys.stderr.write("cannot write FASTA output for multilocus gbk file\n")
        else:
            SeqIO.write(record, output_handle, "fasta") 

         
        output_handle.close()
        input_handle.close()
    except IOError as e:
        # if one replicon is not present on disk, then the entire genome is invalid, so mark it as so in the database
        if write_stats_db:
            cur = db_connect(env)
            cur.execute("""UPDATE genome SET genome_validity = 'MISSING_CONTENT' where genome_id = %s""", (genome_id));
            print "genome set to missing content"

        sys.stderr.write("convert_to_fasta: ioerror: " + str(e) + "\n")
        sys.exit(1)

def main(argv):
    inputfile = ''
    outputfile = ''
    write_stats_db = False
    try:
        opts, args = getopt.getopt(argv,"hi:o:",["ifile=","ofile=","write_stats"])
    except getopt.GetoptError:
        usage()
    for opt, arg in opts:
        if opt == '-h':
            usage()
        elif opt in ("-i", "--ifile"):
            inputfile = arg
        elif opt in ("-o", "--ofile"):
            outputfile = arg
        elif opt in ("--write_stats"):
            write_stats_db = True
        else:
            sys.stderr.write("unrecognized option " + opt + "\n")
            usage()
    if inputfile == "":
        usage()
    if outputfile == "":
        if inputfile.endswith(".gbk"):
            filename, fileextension = os.path.splitext(inputfile)
            outputfile = filename + ".fna";
        else:
            outputfile = inputfile + ".fna";
    convert(inputfile, outputfile, write_stats_db)

if __name__ == "__main__":
    main(sys.argv[1:])
