
import MySQLdb
from settings import Settings
from Bio import Entrez
from xml import sax

class TaxXMLHandler(sax.ContentHandler):
    
    __in_id_list = False
    __in_link = False
    __chrs = ''
    
    __id = -1
    __link = -1
    
    __values = []
    
    def startElement(self, name, attrs):
        if(name == 'IdList'):
            self.__id = -1
            self.__in_id_list = True
        elif(name == 'Link'):
            self.__link = -1
            self.__in_link = True
        elif(name == 'Id'):
            self.__chrs = ''
            
    def endElement(self, name):
        if(name == 'IdList'):
            self.__in_id_list = False
        elif(name == 'Link'):
            self.__in_link = False
        elif(name == 'Id'):
            if( self.__in_id_list ):
                self.__id = int(self.__chrs)
            elif( self.__in_link):
                self.__link = int(self.__chrs)
        elif(name == 'LinkSet'):
            if(self.__link > -1 and self.__id > -1):
                self.__values.append( (self.__id, self.__link) )
                
    def characters(self, content):
        self.__chrs += content
        
    def get_results(self):
        return self.__values
    
def lookup_taxonomy(logger, bioproject_id_list, email):
    t = TaxXMLHandler()
    Entrez.email = email
    for bioproject_id in bioproject_id_list:
        handle = None
        id_string = '%i' % int(bioproject_id[0])
        logger.info('Downloading bioproject %s', bioproject_id)
        try:
            handle = Entrez.elink(dbFrom='bioproject',db='taxonomy',id=id_string)
            sax.parse(handle, t)
        except Exception, e:
            logger.warn('Failed to download taxonomy for %s', bioproject_id)
            logger.info(e)
        finally:
            if(handle):
                handle.close()
    return t.get_results()


def update_taxonomy():
    s = Settings.get_settings('../config/cge-dev.cfg')
    logger = Settings.init_logger(s)
    logger.info('Beginning taxonomy lookup')
    
    conn = MySQLdb.connect( **s['mysql_genome_sync'] )
    curs = conn.cursor()
    statement = """SELECT bioproject_id FROM genome WHERE tax_id IS NULL"""
    res = curs.execute(statement)
    id_list = curs.fetchall()
    logger.info('Found %i taxonomies to lookup', len( id_list ))
    #id_list = id_list[1:50] #test
    id_tax = lookup_taxonomy(logger, id_list, s['entrez']['email'])
    logger.info('Looked up %i taxonomies', len( id_tax ))
    update_statement = """UPDATE genome SET tax_id = %s WHERE bioproject_id = %s"""
    tax_id = [(tax, id) for id, tax in id_tax]
    res = curs.executemany(update_statement, tax_id)
    conn.commit()
    logger.info('Finished updating taxonomies')
    

if __name__ == '__main__':
    update_taxonomy()
