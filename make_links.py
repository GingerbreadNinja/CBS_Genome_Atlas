import MySQLdb
from settings import Settings
import os

def make_links():
    s = Settings.get_settings('../config/cge-dev.cfg')
    logger = Settings.init_logger(s)
    logger.info('Making links')
    conn = MySQLdb.connect( **s['mysql_genome_sync'] )
    curs = conn.cursor()
    logger.info('Looking up accession to tax-id translations')
    statement = """SELECT accession, version, tax_id FROM replicon JOIN genome USING (genome_id) WHERE tax_id IS NOT NULL"""
    res = curs.execute(statement)
    if( not res ):
        return -1
    
    accession_dir = s['replicon']['savedir']
    taxon_dir = s['replicon']['taxonomy']

    for row in curs.fetchall():
        link_path = os.path.join( taxon_dir, str(row[2]) ) #.../taxonomy/123456/
        filename = str(row[0])+'_'+str(row[1]) + '.gbk'
        link_file = os.path.join( link_path, filename )
        source = os.path.join( accession_dir, filename)
        if (not os.path.exists( source )):
            continue
        if (not os.path.exists(link_path)):
            os.makedirs( link_path )
        if (not os.path.exists( link_file )):
            os.symlink(source, link_file)
    logger.info('Created SymLinks for Taxonomy')
    
if __name__ == '__main__':
    make_links()
    

