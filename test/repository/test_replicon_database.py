'''
Created on Oct 16, 2012

@author: Steven
'''

import unittest
from repository import replicon_database
import os
from settings import Settings

class Test(unittest.TestCase):
    
    __gbk_mysql = None
    
    def setUp(self):
        s = Settings.get_settings('../config/cge-dev.cfg')
        logger = Settings.init_logger(s)
        self.__gbk_mysql = replicon_database.RepliconDB(logger.getChild('TestRepliconDB'))
        self.__gbk_mysql.connect( **s['mysql_replicon'] )

    def tearDown(self):
        self.__gbk_mysql.close()

    def testSelectOneNoVersion(self):
        result = self.__gbk_mysql.get_genbank_accession(['CM000855'], 'test/accessions/')
        self.assertEqual(len(result), 1)
        self.assertTrue(os.path.exists('test/accessions/CM000855.gbk'))
        stat_res = os.stat('test/accessions/CM000855.gbk')
        self.assertGreater(stat_res.st_size, 0)
        os.remove('test/accessions/CM000855.gbk')
    
    def testSelectOneNoVersionAssureMax(self):
        result = self.__gbk_mysql.get_genbank_accession(['A59058'], 'test/accessions/')
        self.assertEqual(len(result), 1)
        self.assertTrue(os.path.exists('test/accessions/A59058.gbk'))
        stat_res = os.stat('test/accessions/A59058.gbk')
        self.assertGreater(stat_res.st_size, 0)
        os.remove('test/accessions/A59058.gbk')
        
    def testSelectOneInvalidNoVersion(self):
        result = self.__gbk_mysql.get_genbank_accession(['CP000000'], 'test/accessions/')
        self.assertEqual(len(result), 0)
        self.assertFalse(os.path.exists('test/accessions/CP000000.gbk'))
    
    #I expect this to fail because I don't think CBS stores out-dated genbank files
    @unittest.expectedFailure
    def testSelectOneWithVersion(self):
        result = self.__gbk_mysql.get_genbank_accession_version([('A59058',1)], 'test/accessions/')
        self.assertEqual(len(result), 1)
        self.assertTrue(os.path.exists('test/accessions/A59058_1.gbk'))
        stat_res = os.stat('test/accessions/A59058_1.gbk')
        self.assertGreater(stat_res.st_size, 0)
        os.remove('test/accessions/A59058_1.gbk')
        
    def testSelectOneInvalidWithVersion(self):
        result = self.__gbk_mysql.get_genbank_accession_version([('CP000000',1)], 'test/accessions/')
        self.assertEqual(len(result), 0)
        self.assertFalse(os.path.exists('test/accessions/CP000000_1.gbk'))
        
    def testSelectTwoNoVersion(self):
        result = self.__gbk_mysql.get_genbank_accession(['CM000855','A59058'], 'test/accessions/')
        self.assertEqual(len(result), 2)
        self.assertTrue(os.path.exists('test/accessions/CM000855.gbk'))
        stat_res = os.stat('test/accessions/CM000855.gbk')
        self.assertGreater(stat_res.st_size, 0)
        os.remove('test/accessions/CM000855.gbk')
        self.assertTrue(os.path.exists('test/accessions/A59058.gbk'))
        stat_res = os.stat('test/accessions/A59058.gbk')
        self.assertGreater(stat_res.st_size, 0)
        os.remove('test/accessions/A59058.gbk')
        
    def testSelectTwoWithVersion(self):
        result = self.__gbk_mysql.get_genbank_accession_version([('CM000855',1),('A59058',2)], 'test/accessions/')
        self.assertEqual(len(result), 2)
        self.assertTrue(os.path.exists('test/accessions/CM000855_1.gbk'))
        stat_res = os.stat('test/accessions/CM000855_1.gbk')
        self.assertGreater(stat_res.st_size, 0)
        os.remove('test/accessions/CM000855_1.gbk')
        self.assertTrue(os.path.exists('test/accessions/A59058_2.gbk'))
        stat_res = os.stat('test/accessions/A59058_2.gbk')
        self.assertGreater(stat_res.st_size, 0)
        os.remove('test/accessions/A59058_2.gbk')
        
if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()
