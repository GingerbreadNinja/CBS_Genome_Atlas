'''
Created on Oct 15, 2012

@author: Steven
'''
import unittest
from ncbi.bioproject import BioProject, CSVBioProjectParser

class Test(unittest.TestCase):


    def setUp(self):
        pass


    def tearDown(self):
        pass

    def testParseCSV(self):
        f = open('test.csv', 'wb')
        #I apologize for the ugliness here...
        test_str = '''#Organism/Name\tBioProject Accession\tBioProject ID\tGroup\tSubGroup\tSize (Mb)\tGC%\tChromosomes/RefSeq\tChromosomes/INSDC\tPlasmids/RefSeq\tPlasmids/INSDC\tWGS\tScaffolds\tGenes\tProteins\tRelease Date\tModify Date\tStatus\tCenter
Campylobacter jejuni subsp. jejuni DFVF1099\tPRJNA41639\t41639\tProteobacteria\tdelta/epsilon subdivisions\t1.73386\t30.4\t-\t-\t-\t-\tADHK01\t71\t1964\t1920\t2011/01/03\t2011/09/16\tScaffolds or contigs\tFaculty of Life Sciences, Department of Food Science, University of Copenhagen
Campylobacter jejuni subsp. jejuni 305\tPRJNA41641\t41641\tProteobacteria\tdelta/epsilon subdivisions\t1.80827\t30.4\t-\t-\t-\t-\tADHL01\t333\t2268\t2138\t2011/01/03\t2011/09/16\tScaffolds or contigs\tFaculty of Life Sciences, Department of Food Science, University of Copenhagen
Campylobacter jejuni subsp. jejuni 327\tPRJNA41643\t41643\tProteobacteria\tdelta/epsilon subdivisions\t1.61861\t30.5\t-\t-\t-\t-\tADHM01\t48\t1786\t1711\t2011/01/03\t2011/05/12\tScaffolds or contigs\tFaculty of Life Sciences, Department of Food Science, University of Copenhagen
Campylobacter jejuni subsp. jejuni 414\tPRJNA43389\t43389\tProteobacteria\tdelta/epsilon subdivisions\t1.71012\t30\tNZ_CM000855.1\tCM000855.1\t-\t-\tADGM01\t1\t1840\t1800\t2010/01/12\t2010/06/16\tComplete\tUniversity of Liverpool'''
        f.write(test_str)
        f.close()
        t = open('test.csv', 'rb')
        bioprojects = CSVBioProjectParser.csv_parse_stream(t)
        t.close()
        self.assertEqual( len(bioprojects) , 4)
        self.assertIsInstance( bioprojects[0], BioProject)
        for bioproject in bioprojects:
            self.assertIn( bioproject.bioproject_id, [41639, 41641, 41643, 43389] )

if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testParseCSV']
    unittest.main()