
import MySQLdb
import copy
from ncbi import bioproject

class GenomeDB():
    
    def __init__(self):
        self.__conn = None
        self.__curs = None
        
    def connect(self, **mysqlargs):
        self.__conn = MySQLdb.connect( **mysqlargs )
        self.__curs = self.__conn.cursor()
        
    def close(self):
        if( self.__curs ):
            self.__curs.close()
        if( self.__conn ):
            self.__conn.close()
            
    def __expand(self, size):
        return ('%s,' * (size - 1)) + '%s'
            
    def get_bioprojects(self):
        #Get all bioprojects
        self.__curs.execute('''SELECT bioproject_id, bioproject_status, release_date, modify_date FROM bioproject''')
        return self.__curs.fetchall()
        
    def get_bioproject_by_id(self, bioproject_id):
        self.__curs.execute('''SELECT bioproject_id, bioproject_status, release_date, modify_date FROM bioproject WHERE bioproject_id=%s''', (bioproject_id,) )
        return self.__curs.fetchone()
    
    def get_bioprojects_by_status(self, status=bioproject.BioProject.STATUS_COMPLETE):
        #Get all bioprojects which have status
        statement = '''SELECT bioproject_id, bioproject_status, release_date, modify_date FROM bioproject WHERE bioproject_status=%s'''
        self.__curs.execute(statement, status)
        return self.__curs.fetchall()
    
    def get_not_in_by_id(self, bioproject_id_list):
        # Get all complete bioprojects not in the list
        # This is useful to see if anything was dropped from NCBI's DB
        base_statement = '''SELECT bioproject_id, bioproject_status, release_date, modify_date FROM bioproject WHERE bioproject_id NOT IN (%s)'''
        expansion = self.__expand( len(bioproject_id_list) )
        self.__curs.execute( ( base_statement % expansion ), bioproject_id_list )
        return self.__curs.fetchall()
        
    def get_not_in_by_bioprojects(self, bioproject_list):
        as_id_list = [ bioproject.bioproject_id for bioproject in bioproject_list]
        return self.get_not_in_by_id(as_id_list)
    
    def get_not_in_by_id_and_status(self, bioproject_id_list, status=bioproject.BioProject.STATUS_COMPLETE):
        base_statement = '''SELECT bioproject_id, bioproject_status, release_date, modify_date FROM bioproject WHERE bioproject_status=%s AND bioproject_id NOT IN (%s)'''
        expansion = self.__expand( len(bioproject_id_list) )
        self.__curs.execute( ( base_statement % ('%s', expansion) ), [status] + bioproject_id_list)
        return self.__curs.fetchall()
    
    def get_not_in_by_bioprojects_and_status(self, bioproject_list, status=bioproject.BioProject.STATUS_COMPLETE):
        as_id_list = [ bioproject.bioproject_id for bioproject in bioproject_list]
        return self.get_not_in_by_id_and_status(as_id_list, status)
    
    def insert_bioprojects(self, bioproject_list):
        #Remember to insert the bioproject into the genome table as well...
        as_tuple_list = [ (bioproject.bioproject_id, bioproject.status, bioproject.release_date, bioproject.modify_date) for bioproject in bioproject_list]
        statement = '''INSERT IGNORE INTO bioproject(bioproject_id, bioproject_status, release_date, modify_date) VALUES (%s, %s, %s, %s)'''
        #We would hope the returned number is equal to the number of bioprojects you submitted, but no duplicates are allowed
        try:
            res = self.__curs.executemany( statement, as_tuple_list )
        except:
            self.__conn.rollback()
            return 0
        else:
            self.__conn.commit()
            return res

    def insert_bioprojects_to_genome(self, bioproject_list):
        #Field bioproject_id of genome table is unique so this should raise warnings on key errors
        statement = '''INSERT IGNORE INTO genome(genome_id, tax_id, genome_name, genome_validity, bioproject_id) VALUES (DEFAULT, %s, %s, 'VALID', %s)'''
        as_tuple_list = [ (bioproject.taxon_id, bioproject.organism, bioproject.bioproject_id) for bioproject in bioproject_list]
        try:
            res = self.__curs.executemany( statement, as_tuple_list)
        except:
            self.__conn.rollback()
            return 0
        else:
            self.__conn.commit()
            return res
            
    def get_genome_by_bioproject_ids(self, bioproject_id_list):
        statement = '''SELECT genome_id, tax_id, genome_name, genome_validity, bioproject_id FROM genome WHERE bioproject_id IN (%s)'''
        expansion = self.__expand( len(bioproject_id_list) )
        self.__curs.execute( (statement % expansion), bioproject_id_list )
        #Returns all the genomes from that list... should consider un-tupling these to a standard format
        return self.__curs.fetchall()

    def update_bioproject(self, bp):
        statement = '''UPDATE bioproject SET release_date=%s, modify_date=%s, bioproject_status=%s WHERE bioproject_id=%s'''
        tup = ( bp.release_date, bp.modify_date, bp.status, bp.bioproject_id)
        self.__curs.execute( statement, tup )
        self.__conn.commit()
            
    def get_genome_by_bioprojects(self, bioproject_list):
        as_id_list = [ bioproject.bioproject_id for bioproject in bioproject_list]
        return self.get_genome_by_bioproject_ids(as_id_list)

    def get_genomes_without_bioprojects(self):
        statement = '''SELECT genome_id, tax_id, genome_name, genome_validity, bioproject_id FROM genome WHERE bioproject_id IS NULL'''
        self.__curs.execute(statement)
        return self.__curs.fetchall()

    def mark_genome_as_warning_by_bioproject_ids(self, bioproject_id_list):
        statement = '''UPDATE genome SET genome_validity='WARNINGS' WHERE bioproject_id IN (%s)'''
        expansion = self.__expand( len(bioproject_id_list) )
        try:
            res = self.__curs.execute( (statement % expansion), bioproject_id_list )
        except:
            self.__conn.rollback()
            return 0
        else:
            self.__conn.commit()
            return res
        
    def mark_genome_as_warning_by_bioproject_ids_where_not_validated(self, bioproject_id_list):
        statement = '''UPDATE genome SET genome_validity='WARNINGS' WHERE genome_validity != 'RECONCILED' bioproject_id IN (%s)'''
        expansion = self.__expand( len(bioproject_id_list) )
        try:
            res = self.__curs.execute( (statement % expansion), bioproject_id_list )
        except:
            self.__conn.rollback()
            return 0
        else:
            self.__conn.commit()
            return res
    
    def update_genomes_taxonomy_by_bioprojects(self, bioproject_list):
        statement = '''UPDATE genome SET tax_id = %s WHERE bioproject_id = %s'''
        as_tuple_list = [ (bp.taxon_id, bp.bioproject_id) for bp in bioproject_list]
        try:
            res = self.__curs.execute( statement, as_tuple_list )
        except:
            self.__conn.rollback()
            return 0
        else:
            self.__conn.commit()
            return res
            
    def insert_accession(self, bioproject_id, accession, version, replicon_type='CHROMOSOME'):
        statement = '''INSERT IGNORE INTO replicon (accession, version, genome_id, replicon_type) VALUES (%s, %s, (SELECT genome_id FROM genome WHERE bioproject_id=%s), %s)'''
        t = (accession, version, bioproject_id, replicon_type);
        self.__conn.execute( statement, t );

    def insert_accessions(self, bioproject_list):
        statement = '''INSERT IGNORE INTO replicon (accession, version, genome_id, replicon_type) VALUES (%s, %s, (SELECT genome_id FROM genome WHERE bioproject_id=%s), %s)'''
        t_list = []
        errors = []
        for bp in bioproject_list:
            for p in bp.insdc_plasmids:
                temp = p.split('.',1)
                if( len(temp) > 1 ):
                    t_list.append( (temp[0], temp[1], bp.bioproject_id, 'PLASMID') )
                else:
                    errors.append(bp.bioproject_id)
            for c in bp.insdc_chromosomes:
                temp = c.split('.',1)
                if( len(temp) > 1 ):
                    t_list.append( (temp[0], temp[1], bp.bioproject_id, 'CHROMOSOME') )
                else:
                    errors.append(bp.bioproject_id)
            for w in bp.wgs:
                t_list.append( (w, 0, bp.bioproject_id, 'WGS') )
        self.__curs.executemany(statement, t_list)
        self.__conn.commit()
        return errors
        
    def insert_download(self, genome_id):
        statement = '''INSERT IGNORE INTO downloads( genome_id ) VALUES %s'''
        tup = (genome_id,)
        try:
            s = self.__curs.execute(statement, tup)
            self.__conn.commit()
            return s
        except Exception, e:
            self.__conn.rollback()
            return 0

    def insert_download_bioproject(self, bioproject_id):
        statement = '''INSERT IGNORE INTO downloads( genome_id ) SELECT genome_id FROM genome WHERE bioproject_id=%s'''
        tup = (bioproject_id,)
        try:
            s = self.__curs.execute(statement, tup)
            self.__conn.commit()
            return s
        except Exception, e:
            print e.message
            self.__conn.rollback()
            return 0
        	
    def get_download_list(self):
        statement = '''SELECT genome_id FROM downloads'''
        s = self.__curs.execute(statement)
        if(s != 0):
            return self.__curs.fetchall()
        else:
            return []

    def delete_download(self, genome_id):
        statement = '''DELETE FROM downloads WHERE genome_id = %s'''
        tup = (genome_id, )
        try:
            s = self.__curs.execute(statement, tup)
            self.__conn.commit()
            return s
        except Exception, e:
            self.__conn.rollback()
            return 0

    def delete_download_bioproject(self, bioproject_id):
        statement = '''DELETE FROM downloads WHERE genome_id IN (SELECT genome_id FROM genome WHERE bioproject_id = %s)'''
        tup = (bioproject_id, )
        try:
            s = self.__curs.execute(statement, tup)
            self.__conn.commit()
            return s
        except Exception, e:
            self.__con.rollback()
            return 0
            
    def get_genome_by_id(self, genome_id):
        statement = '''SELECT genome_id, tax_id, genome_name, genome_validity, bioproject_id FROM genome WHERE genome_id = %s'''
        s = self.__curs.execute(statement, (genome_id,) )
        if(s != 0):
            return self.__curs.fetchone()
        else:
            return None
		
