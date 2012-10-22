
import os
import yaml
#import logging
import logging.config
import copy

class Settings():    
    _default_filename = '../config/default.cfg'
    _default_settings = {'entrez': {
                          'max_attempts': 10,
                          'max_lookups': 50,
                          'email':'steve@cbs.dtu.dk',
                          'timeout': 180
                         },
                         'mysql_genome_sync': {
                           'passwd': None, 
                           'host': 'mysql', 
                           'port': 3306, 
                           'user': None, 
                           'db': 'steve_private'
                          },
                          'mysql_replicon': {
                           'passwd': None, 
                           'host':'mysql', 
                           'port': 3306, 
                           'user': None, 
                           'db': 'genbank_xml'
                          },
                          'mysql_taxonomy': {
                           'passwd': None, 
                           'host':'mysql', 
                           'port': 3306, 
                           'user': None, 
                           'db': 'ncbitax'
                          },
                          'ftp_ncbi': {
                           'saveas': 'ncbi/prokaryotes.txt', 
                           'host': 'ftp.ncbi.nlm.nih.gov',
                           'user': None,
                           'passwd': None,
                           'acct': None,
                           'timeout': 180, 
                           'dirname': 'genomes/GENOME_REPORTS/', 
                           'filename': 'prokaryotes.txt'
                          },
                          'logging': {
                           'version': 1,
                           'formatters': {
                            'brief': {
                             'format': '%(message)s'
                            },
                            'precise': {
                             'format': '%(asctime)s %(levelname)-8s %(name)-15s %(message)s',
                             'datefmt': '%Y-%d-%m %H:%M:%S'
                            }
                           },
                           'handlers': {
                            'console': {
                             'class': 'logging.StreamHandler',
                             'formatter': 'brief',
                             'level':'INFO',
                             'stream': 'ext://sys.stdout'
                            },
                            'file':{
                             'class': 'logging.FileHandler',
                             'formatter': 'precise',
                             'level':'INFO',
                             'filename': '../genomesync.log'
                            }
                           },
                           'loggers': {
                            'genome_sync':{
                             'handlers':['console','file'],
                             'level':'INFO'
                            }
                           }
                          },
                          'logger':'genome_sync'
                         }
    
    @classmethod
    def make_defaults(cls):
        d = os.path.dirname(cls._default_filename)
        if(d and not os.path.exists(d)): os.makedirs(d)
        default_file = open(cls._default_filename, 'wb')
        yaml.dump(cls._default_settings, default_file, default_flow_style=False)
        default_file.close()

    @classmethod
    def get_defaults(cls):
        if(not os.path.exists(cls._default_filename)):
            cls.make_defaults()
        default_file = open(cls._default_filename)
        settings = yaml.load(default_file)
        default_file.close()
        return settings
        
    @classmethod
    def get_settings(cls, filename):
        settings = cls.get_defaults()
        if(os.path.exists(filename)):
            new_file = open(filename)
            new_settings = yaml.load(new_file)
            new_file.close()
            settings = Settings.merge_settings(settings, new_settings)
        else:
            raise IOError('The settings file could not be found at %s!', os.path.abspath(filename))
        return settings
    
    @staticmethod
    def merge_settings(s1, s2):
        '''
        Non-intrusively merge two settings dictionaries recursively
        '''
        if( not isinstance(s2, dict)):
            return s2
        
        s3 = copy.deepcopy(s1)
        if( not isinstance(s3, dict)):
            s3 = {}
        
        for k, v in s2.iteritems():
            s3[k] =  Settings.merge_settings(s3.get(k), v)
        return s3
            
    @classmethod 
    def init_logger(cls, settings):
        logging.config.dictConfig(settings['logging'])
        return logging.getLogger(settings['logger'])