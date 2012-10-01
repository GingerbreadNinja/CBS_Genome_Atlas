'''
Created on Aug 30, 2012

@author: Steven J. Otto
'''

import os
import sys
import yaml

class Settings():
    
    
    
    def __init__(self):
        self.base_path = os.path.dirname(sys.argv[0])
        self.repository_db_file = os.path.join(self.base_path, 'repo.sqlite')
        self.ftp_host = 'ftp.ncbi.nlm.nih.gov'
        self.ftp_dir = 'genomes/GENOME_REPORTS/'
        self.ftp_file = 'prokaryotes.txt'
        self.ftp_saveas = os.path.join(self.base_path, 'ncbi' ,'prokaryotes.csv')
        self.ftp_acct = ''
        self.ftp_user = ''
        self.ftp_pass = ''
        self.mysql_host = 'interaction.cbs.dtu.dk'
        self.mysql_user = ''
        self.mysql_passwd = ''
        self.mysql_db = 'genbank_xml'
        self.genbank_save_path = os.path.join(self.base_path, 'accessions')
        
    def load_yaml(self, filename):
        if(not os.path.exists(filename)):
            return
        try:
            f = open(filename, 'r')
            #This is kind of a workaround method... not sure it's really necessary
            self.__dict__.update(yaml.load(f))
        finally:
            if(f != None): f.close()
            
    def __repr__(self):
        result = 'Settings:'
        for k in sorted(vars(self)):
            result += '\n\t' + k + ': ' + getattr(self, k)
        return result
        
    def save_yaml(self, filename):
        try:
            f = open(filename, 'w')
            yaml.dump(vars(self), f, default_flow_style = False)
        finally:
            if(f != None): f.close()
    
def main():
    s = Settings()
    s.save_yaml('test.cfg')
    print s
    s.ftp_acct = 'test'
    s.load_yaml('cbs.cfg')
    print s
    print os.path.abspath(s.base_path)
    
if __name__ == '__main__':
    main()