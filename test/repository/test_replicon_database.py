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
        result = self.__gbk_mysql.get_genbank_accession(['CP000002'], 'test/accessions/')
        self.assertEqual(len(result), 1)
        self.assertTrue(os.path.exists('test/accessions/CP000002.gbk'))
        stat_res = os.stat('test/accessions/CP000002.gbk')
        self.assertGreater(stat_res.st_size, 0)
        os.remove('test/accessions/CP000002.gbk')
        
    def testSelectOneInvalidNoVersion(self):
        result = self.__gbk_mysql.get_genbank_accession(['CP000000'], 'test/accessions/')
        self.assertEqual(len(result), 0)
        self.assertFalse(os.path.exists('test/accessions/CP000000.gbk'))
        
    def testSelectOneWithVersion(self):
        result = self.__gbk_mysql.get_genbank_accession_version([('CP000002',1)], 'test/accessions/')
        self.assertEqual(len(result), 1)
        self.assertTrue(os.path.exists('test/accessions/CP000002_1.gbk'))
        stat_res = os.stat('test/accessions/CP000002_1.gbk')
        self.assertGreater(stat_res.st_size, 0)
        os.remove('test/accessions/CP000002_1.gbk')
        
    def testSelectOneInvalidWithVersion(self):
        result = self.__gbk_mysql.get_genbank_accession_version([('CP000000',1)], 'test/accessions/')
        self.assertEqual(len(result), 0)
        self.assertFalse(os.path.exists('test/accessions/CP000000_1.gbk'))
        
    def testSelectTwoNoVersion(self):
        result = self.__gbk_mysql.get_genbank_accession(['CP000001','CP000002'], 'test/accessions/')
        self.assertEqual(len(result), 2)
        self.assertTrue(os.path.exists('test/accessions/CP000001.gbk'))
        stat_res = os.stat('test/accessions/CP000001.gbk')
        self.assertGreater(stat_res.st_size, 0)
        os.remove('test/accessions/CP000001.gbk')
        self.assertTrue(os.path.exists('test/accessions/CP000002.gbk'))
        stat_res = os.stat('test/accessions/CP000002.gbk')
        self.assertGreater(stat_res.st_size, 0)
        os.remove('test/accessions/CP000002.gbk')
        
    def testSelectTwoWithVersion(self):
        result = self.__gbk_mysql.get_genbank_accession_version([('CP000001',1),('CP000002',1)], 'test/accessions/')
        self.assertEqual(len(result), 2)
        self.assertTrue(os.path.exists('test/accessions/CP000001_1.gbk'))
        stat_res = os.stat('test/accessions/CP000001_1.gbk')
        self.assertGreater(stat_res.st_size, 0)
        os.remove('test/accessions/CP000001_1.gbk')
        self.assertTrue(os.path.exists('test/accessions/CP000002_1.gbk'))
        stat_res = os.stat('test/accessions/CP000002_1.gbk')
        self.assertGreater(stat_res.st_size, 0)
        os.remove('test/accessions/CP000002_1.gbk')
        
if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()