
import MySQLdb
import os

class RepliconDB():
    
    def __init__(self, logger):
        self.__logger = logger
        self.__conn = None
        self.__curs = None
        
    def connect(self, **mysqlargs):
        self.__logger.info('Connecting to MySQL server')
        self.__conn = MySQLdb.connect(**mysqlargs)
        self.__curs = self.__conn.cursor()
    
    def close(self):
        self.__logger.info('Closing MySQL connection')
        if(self.__curs): self.__curs.close()
        if(self.__conn): self.__conn.close()
    
    def __write_output(self, savedir, name, contents):
        if(not os.path.exists(savedir)):
            os.makedirs(savedir)
        decoded = str.decode(contents) #Replace \N with the newline-character
        filename = name + '.gbk'
        if(savedir):
            filename = os.path.join(savedir, filename)
        self.__logger.info('Writing genbank file %s', filename)
        o = open(filename, 'wb')
        o.write(decoded)
        o.close()
        self.__logger.debug('Finished writing genbank file %s', filename)
    
    def __generate_sql_1(self, list_length):
        sql = 'SELECT locus, uncompress(data_z) FROM raw_ff WHERE (locus, version) IN (SELECT locus, MAX(version) FROM raw_ff GROUP BY locus) AND locus IN ( %s )'
        #sql = 'SELECT locus, uncompress(data_z) FROM raw_ff WHERE (locus, version) IN (SELECT locus, MAX(version) FROM raw_ff WHERE locus IN ( %s ) GROUP BY locus)'
        marker = '%s'
        separator = ', '
        in_string = (marker + separator) * (list_length - 1) + marker
        return sql % in_string
    
    def __generate_sql_2(self, list_length):
        sql = 'SELECT locus, version, uncompress(data_z) FROM raw_ff WHERE (locus, version) in ( %s )'
        marker = '(%s, %s)'
        separator = ', '
        in_string = (marker + separator) * (list_length - 1) + marker
        return sql % in_string
    
    def __get_genbank_1(self, accession_number, savedir):
        sql = self.__generate_sql_1(1)
        self.__logger.debug('Performing SQL Query: \'%s\'', sql)
        self.__logger.debug('Accession: %s', accession_number)
        status = self.__curs.execute(sql, (accession_number,))
        self.__logger.debug('SQL Query result: %i', status)
        if(not status): return []
        result = self.__curs.fetchone()
        try:
            self.__write_output(savedir, result[0], result[1])
        except Exception, e:
            self.__logger.warn('Error while writing output for accession %s', result[0])
            self.__logger.debug(e)
            return []
        else:
            return [result[0]]
        
    def __get_genbank_2(self, accession_number_version, savedir):
        sql = self.__generate_sql_2(1)
        self.__logger.debug('Performing SQL Query: \'%s\'', sql)
        status = self.__curs.execute(sql, accession_number_version)
        if(not status): return []
        result = self.__curs.fetchone()
        self.__logger.debug('SQL Query result: %i', status)
        try:
            self.__write_output(savedir, str(result[0])+'_'+str(result[1]), result[2])
        except Exception, e:
            self.__logger.warn('Error while writing output for accession %s', str(result[0])+'.'+str(result[1]))
            self.__logger.debug(e)
            return []
        else:
            return [(result[0],result[1])]
    
    def __get_genbank_1_many(self, accession_number_list, savedir):
        result_set = []
        accession_numbers = tuple(accession_number_list)
        sql = self.__generate_sql_1(len(accession_numbers))
        self.__logger.debug('Performing SQL Query: \'%s\'', sql) 
        status = self.__curs.execute(sql, accession_numbers)
        self.__logger.debug('SQL Query result: %i', status)
        result = self.__curs.fetchone()
        while(result):
            try:
                self.__write_output(savedir, str(result[0]), result[1])
            except Exception, e:
                self.__logger.warn('Error while writing output for accession %s', result[0])
                self.__logger.debug(e)
            else:
                result_set.append(result[0])
            result = self.__curs.fetchone()
        return result_set
    
    def __get_genbank_2_many(self, accession_number_version_list, savedir):
        result_set = []
        # I know this is unreadable but it's (one of) the fastest way(s) to flatten these things =\
        items = [item for accession_number_version in accession_number_version_list for item in accession_number_version]
        sql = self.__generate_sql_2(len(accession_number_version_list))
        self.__logger.debug('Performing SQL Query: \'%s\'', sql)
        status = self.__curs.execute(sql, items)
        self.__logger.debug('SQL Query result: %i', status)
        result = self.__curs.fetchone()
        while(result):
            try:
                self.__write_output(savedir, str(result[0])+'_'+str(result[1]), result[2])
            except Exception, e:
                self.__logger.warn('Error while writing output for accession %s', str(result[0])+'.'+str(result[1]))
                self.__logger.debug(e)
            else:
                result_set.append((str(result[0]),result[1]))
            result = self.__curs.fetchone()
        return result_set

    def get_genbank_accession(self, accession_list, savedir):
        if(len(accession_list)==1):
            return self.__get_genbank_1(accession_list[0], savedir)
        return self.__get_genbank_1_many(accession_list, savedir)
    
    def get_genbank_accession_version(self, accession_version_list, savedir):
        if(len(accession_version_list)==1):
            return self.__get_genbank_2(accession_version_list[0], savedir)
        return self.__get_genbank_2_many(accession_version_list, savedir)
        