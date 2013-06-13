from settings import Settings
from ftpget import FTP_Get
from ncbi import bioproject, efetch_manager, get_gbk
from repository import genome_database, replicon_database
import os
import argparse

def download_bioproject_csv( bioproject_list, logger, s, genome_db ):
    db_logger = logger.getChild('BioprojectDownloads')
    db_logger.info('Inserting accessions for found genomes')
    errors = genome_db.insert_accessions( bioproject_list )

    savedir = s['replicon']['savedir']
    replicon_db = replicon_database.RepliconDB( logger.getChild('RepliconDB') )
    replicon_db.connect(**s['mysql_replicon'])
    
    logger.info('Downloading accessions')
    online_gbk = get_gbk.GetGBK( logger.getChild('NCBI-Replicon'), s['entrez']['email'])

    for bp in bioproject_list:
        for p in bp.insdc_plasmids:
            if(replicon_db.get_genbank_accession_version( [ p.split('.',1) ], savedir )):
                logger.info('Downloaded accession %s for bioproject %i', p, bp.bioproject_id )
            elif(not online_gbk.get_gbk(p.split('.',1), savedir)):
                logger.warn('Error downloading accession %s for bioproject %i', p, bp.bioproject_id)
                genome_db.mark_genome_as_warning_by_bioproject_ids((bp.bioproject_id,))
        for c in bp.insdc_chromosomes:
            if(replicon_db.get_genbank_accession_version( [ c.split('.',1) ], savedir )):
                logger.info('Downloaded accession %s for bioproject %i', c, bp.bioproject_id )
            elif(not online_gbk.get_gbk(c.split('.',1), savedir)):
                logger.warn('Error downloading accession %s for bioproject %i', c, bp.bioproject_id)
                genome_db.mark_genome_as_warning_by_bioproject_ids((bp.bioproject_id ,))
        if( not bp.insdc_chromosomes and not bp.insdc_plasmids ):
            for w in bp.wgs:
                res = 0
                try:
                    res = online_gbk.get_wgs(w, savedir)
                except Exception, e:
                    logger.error("Exception while downloading WGS %s", w)
                    logger.debug(e)
                if(res):
                    logger.info('Downloaded accession %s for bioproject %i' % (w, bp.bioproject_id))
                else:
                    logger.warn('Error downloading accession %s for bioproject %i' % (w, bp.bioproject_id))
                    genome_db.mark_genome_as_warning_by_bioproject_ids((bp.bioproject_id,))
        logger.info('Deleting bioproject download %i' % bp.bioproject_id)
        genome_db.delete_download_bioproject( bp.bioproject_id )
    replicon_db.close()

def download_bioprojects( bioproject_list, logger, s, genome_db ):
    logger.info('Looking up complete bioprojects in NCBI')
    manager = efetch_manager.EFetchManager( logger.getChild('Efetch'), s['entrez'])
    handler = bioproject.BioProjectSaxHandler( logger.getChild('Efetch Parser') )
    
    full_res, errors = manager.fetch_uids( handler, bioproject_list )
    
    db_logger = logger.getChild('BioprojectDownloads')
    db_logger.info('Inserting accessions for found genomes')
    errors = genome_db.insert_accessions( full_res.values() )
    
    if( errors ):
        db_logger.warn('Errors inserting accessions into db for bioprojects: %s', errors )
        genome_db.mark_genome_as_warning_by_bioproject_ids_where_not_validated( errors )
        for k in errors:
            full_res.pop( k, 1 ) #so no key errors are thrown
            
    
    savedir = s['replicon']['savedir']
    replicon_db = replicon_database.RepliconDB( logger.getChild('RepliconDB') )
    replicon_db.connect(**s['mysql_replicon'])
    
    logger.info('Downloading accessions')
    online_gbk = get_gbk.GetGBK( logger.getChild('NCBI-Replicon'), s['entrez']['email'])

    for bp in full_res.values():
        for p in bp.insdc_plasmids:
            if(replicon_db.get_genbank_accession_version( [ p.split('.',1) ], savedir )):
                logger.info('Downloaded accession %s for bioproject %i', p, bp.bioproject_id )
            elif(not online_gbk.get_gbk(p.split('.',1), savedir)):
                logger.warn('Error downloading accession %s for bioproject %i', p, bp.bioproject_id)
                genome_db.mark_genome_as_warning_by_bioproject_ids((bp.bioproject_id,))
        for c in bp.insdc_chromosomes:
            if(replicon_db.get_genbank_accession_version( [ c.split('.',1) ], savedir )):
                logger.info('Downloaded accession %s for bioproject %i', c, bp.bioproject_id )
            elif(not online_gbk.get_gbk(c.split('.',1), savedir)):
                logger.warn('Error downloading accession %s for bioproject %i', c, bp.bioproject_id)
                genome_db.mark_genome_as_warning_by_bioproject_ids((bp.bioproject_id ,))
        if( not bp.insdc_chromosomes and not bp.insdc_plasmids ):
            for w in bp.wgs:
                if(online_gbk.get_wgs(w, savedir)):
                    logger.info('Downloaded accession %s for bioproject %i' % (w, bp.bioproject_id))
                else:
                    logger.warn('Error downloading accession %s for bioproject %i' % (w, bp.bioproject_id))
                    genome_db.mark_genome_as_warning_by_bioproject_ids((bp.bioproject_id,))
        logger.info('Deleting bioproject download %i' % bp.bioproject_id)
        genome_db.delete_download_bioproject( bp.bioproject_id )
    replicon_db.close()

def slice_iterator( lst, slice_length ):
    for i in xrange(0, len(lst), slice_length):
        slice = lst[i:i+slice_length]
        yield slice

def download_genomes_csv( bioproject_list, logger, s, genome_db ):
    db_logger = logger.getChild('GenomeCSVDownloads')
    db_logger.info('Fetching Download List')
    download_list = genome_db.get_download_list()
    db_logger.info('Found %i downloads' % len(download_list))

    bp_filter = set()
    for download in download_list:
        # genome_id, tax_id, genome_name, genome_validity, bioproject_id
        genome = genome_db.get_genome_by_id( download[0] )
        
        if not genome:
            db_logger.warn('Genome ID "%i" in DL list does not have a matching genome' % download)
            genome_db.delete_download( download )
            continue
        if not genome[1]:
            # Download taxonomy if not found
            pass
        
        # If it is a bioproject:
        if genome[4]:
            bp_filter.add( genome[4] )
    
    bioproject_downloads = [ bp for bp in bioproject_list if bp.bioproject_id in bp_filter ]
    db_logger.info('Found %i bioprojects which need to be downloaded' % len(bioproject_downloads) )
    bioproject_complete = [ bp for bp in bioproject_downloads if bp.status == 'COMPLETE']
    bioproject_other    = [ bp for bp in bioproject_downloads if bp.status != 'COMPLETE']
    db_logger.info('Downloading %i Complete Genomes' % len(bioproject_complete))
    download_bioproject_csv( bioproject_complete, logger, s, genome_db )
    db_logger.info('Downloading %i Incomplete Genoems' % len(bioproject_other))
    download_bioproject_csv( bioproject_other, logger, s, genome_db )

def download_genomes( logger, s, genome_db ):
    db_logger = logger.getChild('GenomeDownloads')
    db_logger.info('Fetching Download List')
    download_list = genome_db.get_download_list()
    db_logger.info('Found %i downloads' % len(download_list))
    
    bioproject_list = []
    # in future, add checks for other project types
    # limitting to 1000 downloads because it is a big process sadly
    for download in download_list[:1000]:
        # genome_id, tax_id, genome_name, genome_validity, bioproject_id
        genome = genome_db.get_genome_by_id( download[0] )
        
        if not genome:
            db_logger.warn('Genome ID "%i" in DL list does not have a matching genome' % download)
            genome_db.delete_download( download )
            continue
        if not genome[1]:
            # Download taxonomy if not found
            pass
        
        # If it is a bioproject:
        if genome[4]:
            bioproject_list.append( genome[4] )

    for bp_slice in slice_iterator( bioproject_list, 50 ):
        download_bioprojects( bp_slice, logger, s, genome_db )       

def syncronize():
    parser = argparse.ArgumentParser(description="Synchronize NCBI Genomes with Database")
    parser.add_argument('-c', '--config',help='Use the specified settings file', default='../config/cge-dev.cfg')
    args = parser.parse_args()
    
    s = Settings.get_settings( args.config )
    logger = Settings.init_logger(s)
    logger.info('Beginning Genome Sync')
    
    ftp_settings = s['ftp_ncbi']
    
    old_filename = ftp_settings['saveas'] + '.old'
    if( os.path.exists( ftp_settings['saveas'] ) ):
        os.rename( ftp_settings['saveas'], old_filename )

    ftp = FTP_Get(logger.getChild('NCBI-FTP'))
    ftp.connect(host=ftp_settings['host'],
                user=ftp_settings['user'],
                passwd=ftp_settings['passwd'],
                acct=ftp_settings['acct'],
                timeout=ftp_settings['timeout'])
    
    csv_file = None            
    if(ftp_settings.get('dirname',None)):
        ftp.cwd( ftp_settings['dirname'] )
    try:
        csv_file = ftp.getFile( ftp_settings['filename'], ftp_settings['saveas'])
        ftp.close()
        
    except Exception, e:
        if( csv_file ):
            csv_file.close()
        if( os.path.exists( ftp_settings['saveas'] ) ):
            os.remove( ftp_settings['saveas'] )
        logger.error('Error durring FTP download of %s' % ftp_settings['filename'])
        logger.debug( e.message )
        logger.info( 'Using Old FTP File instead' )
        if( os.path.exists( old_filename )):
            csv_file = open( old_filename, 'rb' )
    
    bioprojects = bioproject.CSVBioProjectParser.csv_parse_stream(csv_file, logger.getChild('CSVBioParse'))
    csv_file.close()
    
    db_logger = logger.getChild('GenomeDB')
    db_logger.info('Connecting to Genome Database')
    genome_db = genome_database.GenomeDB()
    genome_db.connect( **s['mysql_genome_sync'] )

#    download_genomes(logger, s, genome_db)
  
#    genome_db.close()
#    return 0
    
    db_logger.info('Checking which bioprojects need to be downloaded')
    
    for bp in bioprojects:
        tup = genome_db.get_bioproject_by_id(bp.bioproject_id)
        if(tup and bp.modify_date): # if there's already a bioproject
            if( (not tup[3]) or (tup[3] and bp.modify_date.date() > tup[3]) ):
                # Update with newer information
                db_logger.info("Bioproject %i modified" % bp.bioproject_id )
                genome_db.update_bioproject( bp )
                if( not genome_db.get_genome_by_bioproject_ids( [ bp.bioproject_id] )):
                    db_logger.info("Inserting bioproject %i to genome table" % bp.bioproject_id )
                    genome_db.insert_bioprojects_to_genome( [bp] )
                genome_db.insert_download_bioproject( bp.bioproject_id )
            elif not genome_db.get_genome_by_bioproject_ids( [ bp.bioproject_id] ):
                db_logger.info("Bioproject %i has no genome, creating genome" % bp.bioproject_id)
                genome_db.insert_bioprojects_to_genome( [ bp ] )
                genome_db.insert_download_bioproject( bp.bioproject_id )
            else:
                db_logger.info("Bioproject %i unchanged" % bp.bioproject_id)
        elif( not tup ):
            db_logger.info("Bioproject %i added to database" % bp.bioproject_id)
            genome_db.insert_bioprojects( [ bp ] )
            if not genome_db.get_genome_by_bioproject_ids( [ bp.bioproject_id] ):
                genome_db.insert_bioprojects_to_genome( [ bp ] )
            genome_db.insert_download_bioproject( bp.bioproject_id )
        elif not genome_db.get_genome_by_bioproject_ids( [ bp.bioproject_id] ):
            db_logger.info("Bioproject %i has no genome, creating genome" % bp.bioproject_id )
            genome_db.insert_bioprojects_to_genome( [ bp ] )
            genome_db.insert_download_bioproject( bp.bioproject_id )

    download_genomes_csv(bioprojects, logger, s, genome_db)
    #download_genomes(logger, s, genome_db)
  
    genome_db.close()
    
if __name__ == '__main__':
    syncronize()
