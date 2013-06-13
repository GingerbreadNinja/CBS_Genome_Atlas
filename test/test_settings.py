
import unittest
import settings
import os
import logging
import yaml

class Test(unittest.TestCase):
    
    default_settings = settings.Settings._default_settings 

    @classmethod
    def setUpClass(cls):
        if(not os.path.exists('../config')):
            os.makedirs('../config')
    
    def setUp(self):
        unittest.TestCase.setUp(self)
        if(os.path.exists('../config/default.cfg')):
            os.remove('../config/default.cfg')
        if(os.path.exists('../config/test.cfg')):
            os.remove('../config/test.cfg')

    @classmethod
    def tearDownClass(cls):
        super(Test, cls).tearDownClass()
        os.removedirs('../config')
    
    def tearDown(self):
        if(os.path.exists('../config/default.cfg')):
            os.remove('../config/default.cfg')
        if(os.path.exists('../config/test.cfg')):
            os.remove('../config/test.cfg')
    
    def test_make_defaults(self):
        settings.Settings.make_defaults()
        self.assertTrue( os.path.exists('../config/default.cfg') )
        
    def test_load_defaults(self):
        results = settings.Settings.get_defaults()
        self.assertDictEqual(results, self.default_settings)
        
    def test_load_others(self):
        new_setting = {'mysql_genbank_files':{'compress':True}}
        new_file = '../config/test.cfg'
        f = open(new_file, 'wb')
        yaml.dump(new_setting, f, default_flow_style=False)
        f.close()
        s = settings.Settings.get_settings(new_file)
        self.assertTrue( s['mysql_genbank_files'].pop('compress') )
        self.assertTrue( s == self.default_settings ) #TODO: Make more comprehensive tests for merge_settings
    
    def test_logger(self):
        s = settings.Settings.get_defaults()
        logger = settings.Settings.init_logger(s)
        self.assertIsNotNone(logger)
        self.assertIsInstance(logger, logging.Logger)
        logger.warn('this is a test')
        self.assertTrue(os.path.exists('../genomesync.log'))
        self.assertEqual( len(logger.handlers), 2)

if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.test_load_defaults']
    unittest.main()