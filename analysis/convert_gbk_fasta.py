from Bio import SeqIO
from Bio.SeqUtils import GC
import os, sys, getopt;
import MySQLdb


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
      for record in sequences:
         name = record.id
         accession = name[0:8]
         version = name[-1]
         replicon_length = len(record)
         percent_gc = GC(record.seq)
         percent_at = 100 - percent_gc

         sys.stderr.write("record id: " + record.id + "\n")
         sys.stderr.write("record length: " + str(len(record)) + "\n")
         sys.stderr.write("accession: " + accession + "\n")
         sys.stderr.write("version: " + version + "\n")

         if len(record.seq) < 1:  #TODO this doesn't actually work to get the length of teh nucleotide sequence 
            sys.stderr.write("convert_to_fasta: Found record with length less than 1:" + str(len(record.seq)) + "\n")
            sys.exit(1)

         if percent_gc < 0.01:
            sys.stderr.write("convert_to_fasta: Percent gc is obviously wrong at " + str(percent_gc) + "\n")
            sys.exit(1)

         SeqIO.write(record, output_handle, "fasta")

         cur = db_connect()
         cur.execute("""UPDATE replicon SET stat_size_bp = %s, stat_perc_at = %s WHERE accession = %s and version = %s""", (replicon_length, percent_at, accession, version))


      if records > 1:
         sys.stderr.write('convert_to_fasta: Found more than 1 locus\n')
         sys.exit(1)
      output_handle.close()
      input_handle.close()
      #SeqIO.convert(inputfile, "genbank", outputfile, "fasta") # silently converts empty files to crap!
   except IOError as e:
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
