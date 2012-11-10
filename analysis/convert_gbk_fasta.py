from Bio import SeqIO
from Bio.SeqUtils import GC
import os, sys, getopt;
import MySQLdb
import inspect


def usage():
   sys.stderr.write(sys.argv[0] + ' -i <inputfile> [-o <outputfile>]\n')
   sys.exit(2)

def db_connect():
    #TODO pull this out into a module -- I think Steve has one
    db = MySQLdb.connect(host="mysql", port=3306,db="steve_private", read_default_file="~/.my.cnf")
    cur = db.cursor()
    return cur 

def convert(inputfile, outputfile):
   sys.stderr.write('Input file is ' + inputfile + "\n")
   sys.stderr.write('Output file is ' + outputfile + "\n")
   try:
      input_handle = open(inputfile, "rU")
      output_handle = open(outputfile, "w")
      sequences = SeqIO.parse(input_handle, "genbank")

      records = 0
      replicon_length = 0
      percent_gc = 0
      number_contigs = 1
      
      for record in sequences:

         name = record.id
         accession = name[0:8]
         version = name[-1]
         replicon_length = len(record)
         percent_gc = GC(record.seq)
         percent_at = 100 - percent_gc
         percent_at = '{:.2f}'.format(percent_at)
         number_genes = 0
         #score = 1

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
                      n = number_contigs
                      #while n - (25 * replicon_length / 1000000) > 0:
                      #  score = score - 0.2 #score reduces by 0.2 for every 25 contigs per megabase
                   else:
                      sys.stderr.write('convert_to_fasta: no contigs found in contig file\n')
                      sys.exit(1)
                if contig_records > 1:
                   sys.stderr.write('convert_to_fasta: Found more than 1 locus in contig file\n')
                   sys.exit(1)

            except IOError as e:
               sys.exit(1)

         for annotation in record.features:
            if annotation.type == "gene":
                number_genes = number_genes + 1

         number_a = record.seq.count("A")
         number_c = record.seq.count("C")
         number_g = record.seq.count("G")
         number_t = record.seq.count("T")

         nonstd_bases = replicon_length - (number_a + number_c + number_g + number_t) 
         fraction_nonstandard = nonstd_bases / replicon_length

         # TODO this is broken, but should be calculated on the genome level anyway
         #score = score - 10 * (fraction_nonstandard) 
         #if score < 0.1:
         #   score = 0.1
         #sys.stderr.write("score is: " + str(score) + "\n")

         if len(record.seq) < 1:  #TODO this doesn't actually work to get the length of teh nucleotide sequence 
            sys.stderr.write("convert_to_fasta: Found record with length less than 1:" + str(len(record.seq)) + "\n")
            sys.exit(1)

         if percent_gc < 0.01:
            sys.stderr.write("convert_to_fasta: Percent gc is obviously wrong at " + str(percent_gc) + "\n")
            sys.exit(1)

         SeqIO.write(record, output_handle, "fasta")

         sys.stderr.write("record id: " + record.id + "\n")
         sys.stderr.write("record length: " + str(len(record)) + "\n")
         sys.stderr.write("accession: " + accession + "\n")
         sys.stderr.write("version: " + version + "\n")
         sys.stderr.write("number of genes: " + str(number_genes) + "\n")
         sys.stderr.write("number of contigs: " + str(number_contigs) + "\n")
         sys.stderr.write("nonstd_bases: " + str(nonstd_bases) + "\n")
         sys.stderr.write("percent_at: " + str(percent_at) + "\n")

         cur = db_connect()
         cur.execute("""UPDATE replicon SET stat_size_bp = %s, stat_number_nonstd_bases = %s, stat_perc_at = %s, stat_number_of_genes = %s, stat_number_of_contigs = %s WHERE accession = %s and version = %s""", (replicon_length, nonstd_bases, percent_at, number_genes, number_contigs, accession, version))


      if records > 1:
         sys.stderr.write('convert_to_fasta: Found more than 1 locus\n')
         sys.exit(1)
       
      output_handle.close()
      input_handle.close()
      #SeqIO.convert(inputfile, "genbank", outputfile, "fasta") # silently converts empty files to crap!
   except IOError as e:
      sys.stderr.write("convert_to_fasta: ioerror: " + str(e) + "\n")
      sys.exit(1)

def main(argv):
   inputfile = ''
   outputfile = ''
   try:
      opts, args = getopt.getopt(argv,"hi:o:",["ifile=","ofile="])
   except getopt.GetoptError:
      usage()
   for opt, arg in opts:
      if opt == '-h':
         usage()
      elif opt in ("-i", "--ifile"):
         inputfile = arg
      elif opt in ("-o", "--ofile"):
         outputfile = arg
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
   convert(inputfile, outputfile)

if __name__ == "__main__":
   main(sys.argv[1:])
