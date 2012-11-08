from settings import Settings
from ftpget import FTP_Get
from ncbi import bioproject, efetch_manager, get_gbk
from repository import genome_database, replicon_database

def syncronize():
    s = Settings.get_settings('../config/cge-dev.cfg')
    logger = Settings.init_logger(s)
    logger.info('Beginning Genome Sync')
    
    ftp_settings = s['ftp_ncbi']
    ftp = FTP_Get(logger.getChild('NCBI-FTP'))
    ftp.connect(host=ftp_settings['host'],
                user=ftp_settings['user'],
                passwd=ftp_settings['passwd'],
                acct=ftp_settings['acct'],
                timeout=ftp_settings['timeout'])
                
    if(ftp_settings.get('dirname',None)):
        ftp.cwd( ftp_settings['dirname'] )
    csv_file = ftp.getFile( ftp_settings['filename'], ftp_settings['saveas'])
    ftp.close()
    
    bioprojects = bioproject.CSVBioProjectParser.csv_parse_stream(csv_file, logger.getChild('CSVBioParse'))
    csv_file.close()
    
    complete_list = [bp for bp in bioprojects if (bp.status == bioproject.BioProject.STATUS_COMPLETE)]
    logger.info('Found %i complete bioprojects in list', len(complete_list))
    
    db_logger = logger.getChild('GenomeDB')
    db_logger.info('Connecting to Genome Database')
    genome_db = genome_database.GenomeDB()
    genome_db.connect( **s['mysql_genome_sync'] )

    warnings = genome_db.get_not_in_by_bioprojects_and_status( complete_list )
    if(warnings):
        warning_ids = [warning[0] for warning in warnings]
        logger.warn('The following bioprojects are not in the ftp complete list and will be marked as warnings: %s', warning_ids)
        genome_db.mark_genome_as_warning_by_bioproject_ids_where_not_validated( warning_ids )
        del warning_ids
    
    db_logger.info('Checking which bioprojects need to be downloaded')
    
    dl_list = []
    for bp in complete_list:
        tup = genome_db.get_bioproject_by_id(bp.bioproject_id)
        if(tup and bp.modify_date.date() > tup[3]):
            dl_list.append(bp)
        elif( not tup ):
            dl_list.append(bp)
    
    if( not dl_list ):
        logger.info('No new bioprojects found. Terminating')
        return
        
    db_logger.info('Inserting bioprojects into Genome Database')
    genome_db.insert_bioprojects( dl_list )
    
    db_logger.info('Adding Genome IDs for complete Genomes')
    genome_db.insert_bioprojects_to_genome( dl_list )
    
    #TODO: Add complete genomes without taxonomy_id's to the download list
    
    logger.info('Looking up complete bioprojects in NCBI')
    manager = efetch_manager.EFetchManager( logger.getChild('Efetch'), s['entrez'])
    handler = bioproject.BioProjectSaxHandler( logger.getChild('Efetch Parser') )
    full_res, errors = manager.fetch_uids( handler, [bp.bioproject_id for bp in dl_list])
    if(errors):
        logger.warn('Could not lookup %i bioprojects from NCBI, leaving blank taxonomy for these items', len(errors))
        
    db_logger.info('Updating taxonomy for found genomes')
    genome_db.update_genomes_taxonomy_by_bioprojects( full_res.values() )
    
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
                genome_db.mark_genome_as_warnings_by_accession(p)
        for c in bp.insdc_chromosomes:
            if(replicon_db.get_genbank_accession_version( [ c.split('.',1) ], savedir )):
                logger.info('Downloaded accession %s for bioproject %i', c, bp.bioproject_id )
            elif(not online_gbk.get_gbk(c.split('.',1), savedir)):
                logger.warn('Error downloading accession %s for bioproject %i', c, bp.bioproject_id)
                genome_db.mark_genome_as_warnings_by_accession(c)
    genome_db.close()
    replicon_db.close()
    
if __name__ == '__main__':
    syncronize()
