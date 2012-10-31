from Bio import SeqIO
import os, sys, getopt;


def main(argv):
   inputfile = ''
   outputfile = ''
   try:
      opts, args = getopt.getopt(argv,"hi:o:",["ifile=","ofile="])
   except getopt.GetoptError:
      print sys.argv[0] + ' -i <inputfile> [-o <outputfile>]'
      sys.exit(2)
   for opt, arg in opts:
      if opt == '-h':
         print sys.argv[0] + ' -i <inputfile> [-o <outputfile>]'
         sys.exit()
      elif opt in ("-i", "--ifile"):
         inputfile = arg
      elif opt in ("-o", "--ofile"):
         outputfile = arg
   if inputfile == "":
      print sys.argv[0] + ' -i <inputfile> [-o <outputfile>]'
      sys.exit()
   if outputfile == "":
      if inputfile.endswith(".gbk"):
         filename, fileextension = os.path.splitext(inputfile)
         outputfile = filename + ".fasta";
      else:
         outputfile = inputfile + ".fasta";
   print 'Input file is ', inputfile
   print 'Output file is ', outputfile
   SeqIO.convert(inputfile, "genbank", outputfile, "fasta")

if __name__ == "__main__":
   main(sys.argv[1:])
