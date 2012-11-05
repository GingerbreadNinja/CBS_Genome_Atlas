
import unittest
import datetime
import MySQLdb
from repository import genome_database
from settings import Settings
from ncbi.bioproject import BioProject

class Test(unittest.TestCase):
    
    genome_db = None
    
    def setUp(self):
        self.genome_db = genome_database.GenomeDB()
        s = Settings.get_settings('../config/cge-dev.cfg')
        logger = Settings.init_logger(s)
        
        conn = MySQLdb.connect( **s['mysql_genome_sync'] )
        curs = conn.cursor()
        curs.execute('TRUNCATE genome')
        curs.execute('TRUNCATE bioproject')
        curs.close()
        conn.close()
        
        self.genome_db.connect(**s['mysql_genome_sync'])
        
    def tearDown(self):
        self.genome_db.close()
        
    
    def testInsertBioProject(self):
        bioproject_list = [ BioProject(bioproject_id=1, status='COMPLETE', release_date=datetime.datetime.now(), modify_date=datetime.datetime.now()),
                            BioProject(bioproject_id=2, status='COMPLETE', release_date=datetime.datetime.now(), modify_date=datetime.datetime.now()),
                            BioProject(bioproject_id=3, status='COMPLETE', release_date=datetime.datetime.now(), modify_date=datetime.datetime.now()),
                            BioProject(bioproject_id=3, status='COMPLETE', release_date=datetime.datetime.now(), modify_date=datetime.datetime.now()),
                            BioProject(bioproject_id=4, status='NO DATA', release_date=datetime.datetime.now(), modify_date=datetime.datetime.now()) ]
        res = self.genome_db.insert_bioprojects(bioproject_list)
        self.assertEquals( res, 4L )
        
    def testSelectBioProject(self):
        bioproject_list = [ BioProject(bioproject_id=1, status='COMPLETE', release_date=datetime.datetime.now(), modify_date=datetime.datetime.now()),
                            BioProject(bioproject_id=2, status='COMPLETE', release_date=datetime.datetime.now(), modify_date=datetime.datetime.now()),
                            BioProject(bioproject_id=3, status='COMPLETE', release_date=datetime.datetime.now(), modify_date=datetime.datetime.now()),
                            BioProject(bioproject_id=3, status='COMPLETE', release_date=datetime.datetime.now(), modify_date=datetime.datetime.now()),
                            BioProject(bioproject_id=4, status='NO DATA', release_date=datetime.datetime.now(), modify_date=datetime.datetime.now()) ]
        self.genome_db.insert_bioprojects(bioproject_list)
        res = self.genome_db.get_bioprojects()
        self.assertEquals( len(res), 4L )
        
        res = self.genome_db.get_bioprojects_by_status( status='COMPLETE' )
        self.assertEquals( len(res), 3L )
    
    def testInsertBioProjectGenome(self):
        bioproject_list = [ BioProject(bioproject_id=1, status='COMPLETE', release_date=datetime.datetime.now(), modify_date=datetime.datetime.now(), taxon_id=1, organism='test'),
                            BioProject(bioproject_id=2, status='COMPLETE', release_date=datetime.datetime.now(), modify_date=datetime.datetime.now(), taxon_id=1, organism='test'),
                            BioProject(bioproject_id=3, status='COMPLETE', release_date=datetime.datetime.now(), modify_date=datetime.datetime.now(), taxon_id=1, organism='test'),
                            BioProject(bioproject_id=3, status='COMPLETE', release_date=datetime.datetime.now(), modify_date=datetime.datetime.now(), taxon_id=1, organism='test'),
                            BioProject(bioproject_id=4, status='NO DATA', release_date=datetime.datetime.now(), modify_date=datetime.datetime.now(), taxon_id=1, organism='test') ]
        self.genome_db.insert_bioprojects(bioproject_list)
        res = self.genome_db.insert_bioprojects_to_genome( bioproject_list )
        self.assertEquals( res, 4L )
    
    def testSelectGenomeByBioProjectID(self):
        bioproject_list = [ BioProject(bioproject_id=1, status='COMPLETE', release_date=datetime.datetime.now(), modify_date=datetime.datetime.now(), taxon_id=1, organism='test'),
                            BioProject(bioproject_id=2, status='COMPLETE', release_date=datetime.datetime.now(), modify_date=datetime.datetime.now(), taxon_id=1, organism='test'),
                            BioProject(bioproject_id=3, status='COMPLETE', release_date=datetime.datetime.now(), modify_date=datetime.datetime.now(), taxon_id=1, organism='test'),
                            BioProject(bioproject_id=3, status='COMPLETE', release_date=datetime.datetime.now(), modify_date=datetime.datetime.now(), taxon_id=1, organism='test'),
                            BioProject(bioproject_id=4, status='NO DATA', release_date=datetime.datetime.now(), modify_date=datetime.datetime.now(), taxon_id=1, organism='test') ]
        self.genome_db.insert_bioprojects(bioproject_list)
        self.genome_db.insert_bioprojects_to_genome( bioproject_list )
        res = self.genome_db.get_genome_by_bioproject_ids([1,2])
        self.assertEquals(len(res), 2L)
        
    def testSelectGenomeByBioProject(self):
        bioproject_list = [ BioProject(bioproject_id=1, status='COMPLETE', release_date=datetime.datetime.now(), modify_date=datetime.datetime.now(), taxon_id=1, organism='test'),
                            BioProject(bioproject_id=2, status='COMPLETE', release_date=datetime.datetime.now(), modify_date=datetime.datetime.now(), taxon_id=1, organism='test'),
                            BioProject(bioproject_id=3, status='COMPLETE', release_date=datetime.datetime.now(), modify_date=datetime.datetime.now(), taxon_id=1, organism='test'),
                            BioProject(bioproject_id=3, status='COMPLETE', release_date=datetime.datetime.now(), modify_date=datetime.datetime.now(), taxon_id=1, organism='test'),
                            BioProject(bioproject_id=4, status='NO DATA', release_date=datetime.datetime.now(), modify_date=datetime.datetime.now(), taxon_id=1, organism='test') ]
        self.genome_db.insert_bioprojects(bioproject_list)
        self.genome_db.insert_bioprojects_to_genome( bioproject_list )
        test_list = bioproject_list[0:2]
        res = self.genome_db.get_genome_by_bioprojects(test_list)
        self.assertEquals(len(res), len(test_list))
                            
if __name__ == '__main__':
    unittest.main()
    
