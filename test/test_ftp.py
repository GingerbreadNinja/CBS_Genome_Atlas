import settings
import ftpget
import unittest
import socket
from ftpget import FTP_Get
import logging

class TestNCBIProkaryotes(unittest.TestCase):

    ftp = None

    def setUp(self):
        self.ftp = ftpget.FTP_Get(logging.getLogger())
        self.ftp.connect('ftp.ncbi.nlm.nih.gov', '', '', '', 180)
        self.ftp.cwd('genomes/GENOME_REPORTS/')
    
    def tearDown(self):
        if(self.ftp): self.ftp.close()
    
    def test_illegal_host(self):
        f = FTP_Get()
        self.assertRaises(socket.error, f.connect('ftp://fakehost.fake', '', '', '', 180))
        self.assertFalse(f.ftp_conn)
    
    def test_fetchftp(self):
        prokaryotes = self.ftp.getNCBIProkaryotesIndex();
        data = prokaryotes.read();
        self.assertTrue(len(data) > 0); #stream contains values
        self.assertTrue(data.find('Organism') >= 0); 
        # TODO: better checks that the header is correct
        # #Organism/Name	BioProject Accession	BioProject ID	Group	SubGroup	Size (Mb)	GC%	Chromosomes/RefSeq	Chromosomes/INSDC	Plasmids/RefSeq	Plasmids/INSDC	WGS	Scaffolds	Genes	Proteins	Release Date	Modify Date	Status	Center

    def test_exception_on_file_that_dne(self):
        print "testing file that does not exist, expect error\n"
        self.assertRaises(Exception, self.ftp.getFile, 'READM', 'ncbi/READM')
    
    def test_prokaryotes_from_settings(self):
        s = settings.Settings.get_settings('config/debug.cfg')
        res = FTP_Get.getNCBISettings(s)
        self.assertTrue(res)
        self.assertIsInstance(res, file)
                
if __name__ == '__main__':
    unittest.main()
