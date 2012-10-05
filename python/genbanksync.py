'''
Created on Aug 28, 2012

@author: Steven J. Otto
'''

import getopt
import os
import sys
import logging
import re

import ftpget
import genbank_mysql_fork

from config import Settings
from ncbi import bioproject, efetch_bioproject
#from repository import SQLite3Repository
import itertools
import time

repo_rebuild = False
sleep_time = 5 * 60 #5 minutes
sleep_count = 100 #100 items

def bioproject_to_accessions(bp):
    return (map(lambda acc: (bp.bioproject_id, bp.bioproject_accession, acc, 'INSDC/CHROMOSOME'), bp.insdc_chromosomes) +
            map(lambda acc: (bp.bioproject_id, bp.bioproject_accession, acc, 'INSDC/PLASMID'), bp.insdc_plasmids))

def genbank_sync(settings = Settings()):
    logger = logging.getLogger('ncbi_sync')
    hdlr = logging.FileHandler('ncbi_sync.log')
    formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
    hdlr.setFormatter(formatter)
    logger.addHandler(hdlr) 
    
    logger.debug(settings)
    #download the ftp file from NCBI
    logger.debug( 'Downoading FTP Genome Report' )
    logger.debug('FTP file downloaded to: %s' % os.path.join(settings.base_path, settings.ftp_saveas))
    r = ftpget.getFile(settings.ftp_host,
                       settings.ftp_user,
                       settings.ftp_pass,
                       settings.ftp_acct,
                       180, 
                       settings.ftp_dir, 
                       settings.ftp_file, 
                       os.path.join(settings.base_path, settings.ftp_saveas))
    
    if(r==0):
        logger.info('FTP download success')
    else:
        logger.warn('An error occured during FTP download, continuing with current list')
    
    #Parse FTP as CSV file
    logger.debug( 'Parsing FTP Genome Report from CSV to BioProjects' )
    csv_bioproject_stream = open(os.path.join(settings.base_path, settings.ftp_saveas))
    bioprojects = bioproject.CSVBioProjectParser.csv_parse_stream(csv_bioproject_stream)
    logger.info( 'Read %i BioProjects from CSV File' % len(bioprojects) )
    csv_bioproject_stream.close()
    
    #Load the SQLite Repository
    """
    logger.debug( 'Opening SQLite repository' )
    logger.debug('SQLite3 DB at: %s' % os.path.join(settings.base_path, settings.repository_db_file))
    repo = SQLite3Repository(os.path.join(settings.base_path, settings.repository_db_file))
    if(repo_rebuild):
        logger.info('Repository rebuild requested')
        repo.drop_tables()
    
    #Make sure basic data is in there (the different kinds of statuses, and accession types
    logger.debug( 'Building tables (if not exist)' )
    repo.build_tables()
    repo.fill_tables_ncbi()
    repo.fill_tables_accessiontype()
    
    #Update the repository with current information
    logger.debug( 'Combining parsed BioProjects with repository' )
    repo.insert_bioprojects(bioprojects)
    
    #Search for 'illegal' bioprojects - no insdc accession numbers
    logger.debug( 'Finding completed bioprojects with no INSDC accession numbers' )
    incomplete = repo.select_complete_no_accessions()
    logger.info( 'Found %i bioprojects with no accession numbers' % len(incomplete))
    #If there are incomplete bioprojects
    if(len(incomplete) > 0):
        #Find their associated accession numbers 
        bplist = map(lambda btuple: btuple[0], incomplete)
        logger.debug('Searching for bioproject information for incomplete bioprojects')
        found_bioprojects = efetch_bioproject.fetch_accessions(bplist)
        #Insert accessions into DB
        logger.debug('Combining found bioproject information with repository')
        repo.insert_accessions_from_bioprojects(found_bioprojects)
    
    #Find accessions which haven't been downloaded from the MySQL DB
    logger.debug( 'Finding complete accession numbers needing download' )
    rows = repo.select_complete_accessions_no_downloads()
    logger.info( 'Found %i complete accession numbers needing download' % len(rows))
    #There is no REGEXP function in SQLite by default so we filter them here:
    reg = re.compile('^[a-zA-Z]{1,2}[0-9]{5,6}')
    valid = filter(lambda r: reg.match(r[1])!=None, rows)
    invalid = filter(lambda r: reg.match(r[1])==None, rows)
    l = len(invalid)
    #Warn about invaild accession numbers (these are mistakes by NCBI and shouldn't be in the list!)
    if(l > 0):
        logger.warn('Found %i invalid accession numbers!' % l)
    for row in invalid:
        logger.warn('Ignoring invalid accession number \'%s\' from bioproject \'%s\'' % (row[1], row[0]))
    
    logger.info('Downloading %i accession files from MySQL', len(valid))
    logger.debug('Placing accession files in: %s' % os.path.join(settings.base_path, settings.genbank_save_path))
    """
    
    valid = filter(lambda bp: bp.status == bioproject.BioProject.STATUS_COMPLETE, bioprojects)
    accessions = map(bioproject_to_accessions, valid)
    acc = list(itertools.chain.from_iterable(accessions))
    i = 0
    for row in acc:
        if(i==sleep_count):
            time.sleep(sleep_time)
            i=0
        saveas = os.path.join(settings.base_path, settings.genbank_save_path, row[1])
        base = os.path.join(settings.base_path, settings.genbank_save_path)
        if(not os.path.exists(base)):
            os.mkdir(base)
        if(not os.path.exists(saveas)):
            os.mkdir(saveas)
        s = genbank_mysql_fork.get_filename(saveas, row[2])
        if(not os.path.exists(s)):
            i += 1
            r = genbank_mysql_fork.mysql_fork_fetch(accession = row[2], savedir=saveas, user=settings.mysql_user, passwd=settings.mysql_passwd)
            if(r == 0 and os.stat(s).st_size > 5):
                logger.info( 'Successfully downloaded accession %s' % row[2] )
                #repo.mark_downloaded(row[1])
            else:
                if(os.path.exists(s)):
                    os.remove(s)
                logger.warn( 'An error occurred while downloading accession %s' % row[2] )
        else:
            logger.info('Accession already downloaded %s' % row[2])
        
        """
        logger.info('Downloading accession %s' % row[1])
        saveas = os.path.join(settings.base_path, settings.genbank_save_path, row[0])
        os.mkdir(os.path.join(settings.base_path, settings.genbank_save_path))
        os.mkdir(saveas)
        r = genbank_mysql_fork.mysql_fork_fetch(row[1], saveas)
        if(r == 0):
            logger.info( 'Successfully downloaded accession %s' % row[1] )
            repo.mark_downloaded(row[1])
        else:
            logger.warn( 'An error occurred while downloading accession %s' % row[1] )
        break #this is just so we can test one first...
        """
    #logger.debug('Closing Repository')
    #repo.close()

def usage():
    print """genbanksync.py [-chv] [--config=FILE] [--verbose] [--help]
        [-c],[--config]=FILE    Specify config file to use
        [-v],[--verbose]        Use verbose logging
        [-h],[--help]           Display this help-text"""

def main(argv):
    try:
        opts, args = getopt.getopt(argv[1:], 'vhc:', ['--config=','--verbose', '--help'])
    except getopt.GetoptError, err:
        print str(err) # "Option -? not recognized"
        usage()
    
    cfg = None #It is probably not a good idea to proceed if the settings are 'none'
    logger = logging.getLogger('ncbi_sync')
    logger.setLevel(logging.INFO)
    for o, a in opts:
        if o == "-v":
            logger.setLevel(logging.DEBUG)
        elif o in ("-h", "--help"):
            usage()
            sys.exit()
        elif o in ("-c", "--config"):
            cfg = a
        else:
            assert False, "unhandled option"
    
    settings = Settings()
    if(cfg != None):
        settings.load_yaml(cfg)
    else:
        #Abandon Ship?
        logging.warn('No configuration specified!')
        
    try:
        f = open('.my.cnf')
        
    except:
        """No local mysql configuration"""
    genbank_sync(settings)

if __name__ == '__main__':
    main(sys.argv)