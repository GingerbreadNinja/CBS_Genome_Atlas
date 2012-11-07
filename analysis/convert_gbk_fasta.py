from Bio import SeqIO
import os, sys, getopt;


def usage():
   sys.stderr.write(sys.argv[0] + ' -i <inputfile> [-o <outputfile>]\n')
   sys.exit(2)

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
   if inputfile == "":
      usage()
   if outputfile == "":
      if inputfile.endswith(".gbk"):
         filename, fileextension = os.path.splitext(inputfile)
         outputfile = filename + ".fasta";
      else:
         outputfile = inputfile + ".fasta";
   sys.stderr.write('Input file is ' + inputfile + "\n")
   sys.stderr.write('Output file is ' + outputfile + "\n")
   try:
      input_handle = open(inputfile, "rU")
      output_handle = open(outputfile, "w")
      sequences = SeqIO.parse(input_handle, "genbank")
      count = SeqIO.write(sequences, output_handle, "fasta")
 
      output_handle.close()
      input_handle.close()
 
      sys.stderr.write("Found " + str(count) + " records\n")
      if count <= 1: 
         sys.exit(1)
      else:
         sys.exit(0)

      #SeqIO.convert(inputfile, "genbank", outputfile, "fasta") # silently converts empty files to crap!
   except IOError as e:
      sys.exit(1)

if __name__ == "__main__":
   main(sys.argv[1:])
