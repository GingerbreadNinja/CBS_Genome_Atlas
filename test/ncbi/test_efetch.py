'''
Created on Oct 17, 2012

@author: Steven
'''
import unittest
import settings
from ncbi import efetch_manager
from ncbi.bioproject import BioProjectSaxHandler, CSVBioProjectParser

class Test(unittest.TestCase):

    logger = None
    setting = None
    efetch = None
    
    test_list = {'PRJEA162063' : 162063, 
                 'PRJNA13130' : 13130,
                 'PRJNA13131' : 13131,
                 'PRJNA39147' : 39147,
                 'PRJNA39155' : 39155,
                 'PRJNA39157' : 39157,
                 'PRJNA39163' : 39163,
                 'PRJNA39177' : 39177,
                 'PRJNA45959' : 45959,
                 'PRJNA45961' : 45961,
                 'PRJNA57613' : 57613,
                 'PRJNA57615' : 57615,
                 'PRJNA57617' : 57617,
                 'PRJNA57691' : 57691,
                 'PRJNA57695' : 57695,
                 'PRJNA57697' : 57697,
                 'PRJNA57777' : 57777,
                 'PRJNA57793' : 57793,
                 'PRJNA57801' : 57801,
                 'PRJNA57961' : 57961,
                 'PRJNA61411' : 61411,
                 'PRJNA61565' : 61565,
                 'PRJNA61567' : 61567,
                 'PRJNA61569' : 61569,
                 'PRJNA61573' : 61573,
                 'PRJNA61581' : 61581,
                 'PRJNA61583' : 61583,       
                 'PRJNA61585' : 61585,
                 'PRJNA61589' : 61589,
                 'PRJNA61591' : 61591,
                 'PRJNA61593' : 61593,
                 'PRJNA61611' : 61611,
                 'PRJNA62901' : 62901,
                 'PRJNA62903' : 62903,
                 'PRJNA62911' : 62911,
                 'PRJNA62923' : 62923,
                 'PRJNA62971' : 62971,
                 'PRJNA80923' : 80923
                 }
    
    invalid_bioprojects = [1234512345]

    def setUp(self):
        self.setting = settings.Settings.get_settings('../test/debug.cfg')
        self.logger = settings.Settings.init_logger(self.setting).getChild('test_efetch')
        self.logger.info( self.setting )
        self.efetch = efetch_manager.EFetchManager(self.logger, self.setting['entrez'])

    def testFetch(self):
        handler = BioProjectSaxHandler(self.logger)
        results, errors = self.efetch.fetch_uids(handler, self.test_list.values())
        self.assertTrue(not errors)
        for value in self.test_list.values():
            self.assertTrue(results.has_key(value))
            
    def testFetchWithInvalid(self):
        handler = BioProjectSaxHandler(self.logger)
        testlist = self.test_list.values() + self.invalid_bioprojects
        results, errors = self.efetch.fetch_uids(handler, testlist)
        for value in self.test_list.values():
            if(not results.has_key(value)):
                self.assertIn(value, errors)
                self.assertIn(value, self.invalid_bioprojects)
                
    def testFetchWithArtificiallyLargeList(self):
        handler = BioProjectSaxHandler(self.logger)
        testlist = self.test_list.values() * 5
        results, errors = self.efetch.fetch_uids(handler, testlist)
        self.assertEquals(len(results) + len(errors), len(self.test_list.values()))
        for value in self.test_list.values():
            self.assertTrue(results.has_key(value))
            
    def testFetchWithLargeList(self):
        f = open('prokaryotes.txt')
        bioprojects = CSVBioProjectParser.csv_parse_stream(f)
        bioprojects = bioprojects[1:200]
        f.close()
        testlist = map(lambda bioproject: bioproject.bioproject_id, bioprojects)
        
        handler = BioProjectSaxHandler(self.logger)
        results, errors = self.efetch.fetch_uids(handler, testlist)
        # Removed because the CSV file potentially contains duplicates
        #self.assertEquals(len(results) + len(errors), len(testlist))
        for value in testlist:
            if( not results.has_key(value) ):
                self.assertIn(value, errors)

if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()