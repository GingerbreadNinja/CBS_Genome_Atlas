import logging
import ftplib
import socket
import os
from ftplib import FTP

class FTP_Get():
    
    def __init__(self, logger):
        self.logger = logger
        self.ftp_conn = None
    
    def connect(self, host, user, passwd, acct, timeout):
        try:
            self.logger.info('Connecting to FTP host @ %s', host)
            self.ftp_conn = FTP(host, timeout=timeout)
            self.logger.info('Attempting to log in...')
            self.ftp_conn.login(user, passwd, acct)
        except socket.error, e:
            self.logger.error('An error occurred connecting to FTP host @ %s', host)
            self.logger.debug(str(e))
            if(self.ftp_conn):
                self.ftp_conn.quit()
                self.ftp_conn = None
            raise
        except ftplib.Error, e:
            self.logger.error('An FTP error occurred while connecting and logging in.')
            self.logger.debug(str(e))
            if(self.ftp_conn):
                self.ftp_conn.quit()
                self.ftp_conn = None
            raise
    
    def close(self):
        if(self.ftp_conn):
            self.logger.info('Closing FTP connection')
            self.ftp_conn.close()
    
    def cwd(self, dirname):
        self.logger.info('Changing FTP working directory to %s', dirname)
        self.ftp_conn.cwd(dirname)
    
    def getStream(self, filename):
        self.logger.info('Building stream from FTP File %s', filename)
        return self.ftp_conn.ntransfercmd('RETR '+filename)
        
    def getFile(self, filename, saveas):
        d = os.path.dirname(saveas)
        if(d and not os.path.exists(d)): os.makedirs(d)
        try:
            self.logger.info('Opening destination file for FTP download: %s', saveas)
            output = open(saveas, 'wb')
            self.logger.info('Downloading file \'%s\' from FTP into \'%s\'', filename, saveas)
            self.ftp_conn.retrbinary('RETR '+filename, output.write)
            return open(saveas, 'rb')
        finally:
            if(output): output.close()
    
    @staticmethod
    def getNCBIProkaryotesIndex():
        ftp_get = FTP_Get()
        ftp_get.connect('ftp.ncbi.nlm.nih.gov', '', '', '', 180)
        ftp_get.cwd('genomes/GENOME_REPORTS/')
        f = ftp_get.getFile('prokaryotes.txt', 'ncbi/prokaryotes.txt')
        ftp_get.close()
        return f
    
    @staticmethod
    def getNCBISettings(settings):
        logger = logging.getLogger(settings['logger']).getChild('FTP_Get')
        if(not settings.get('ftp_ncbi')):
            logger.warn('Attempting to get from NCBI but no settings found!')
            return
        ftps = settings['ftp_ncbi']
        ftp_get = FTP_Get(logger)
        #Opted to use get(key) instead of [key] to avoid Key Errors
        ftp_get.connect(host=ftps.get('host'), user=ftps.get('user'), 
                        passwd=ftps.get('passwd'), acct=ftps.get('acct'), 
                        timeout=ftps.get('timeout'))
        if(ftps.get('dirname')):
            ftp_get.cwd(ftps['dirname'])
        
        try:
            if(ftps.get('saveas')):
                return ftp_get.getFile(ftps['filename'], ftps['saveas'])
            else:
                return ftp_get.getStream(ftps['filename'])
        finally:
            ftp_get.close()
            