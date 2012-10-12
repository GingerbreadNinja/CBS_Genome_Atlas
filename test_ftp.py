
import ftpget;
import unittest;

class TestNCBIProkaryotes(unittest.TestCase):

    def setUp(self):
        print "setup\n";

    def test_fetchftp(self):
        prokaryotes = ftpget.getNCBIProkaryotesIndex();
        data = prokaryotes.read();
        self.assertTrue(len(data) > 0); #stream contains values
        self.assertTrue(data.find('Organism') >= 0); 
        # TODO: better checks that the header is correct
        # #Organism/Name	BioProject Accession	BioProject ID	Group	SubGroup	Size (Mb)	GC%	Chromosomes/RefSeq	Chromosomes/INSDC	Plasmids/RefSeq	Plasmids/INSDC	WGS	Scaffolds	Genes	Proteins	Release Date	Modify Date	Status	Center

    def test_exception_on_file_that_dne(self):
        print "testing file that does not exist, expect error\n";
        emptyfile = ftpget.getFile('ftp.ncbi.nlm.nih.gov', '', '', '', 180, 'genomes/GENOME_REPORTS/', 'READM', 'ncbi/READM');
        self.assertTrue(emptyfile == 1); # error returned from ftpget
        
if __name__ == '__main__':
    unittest.main()
