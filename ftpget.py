'''
Created on Aug 27, 2012

@author: Steven J. Otto
'''

import ftplib
import socket
import sys
from ftplib import FTP

def getNCBIProkaryotesIndex():
    getFile('ftp.ncbi.nlm.nih.gov', '', '', '', 180, 'genomes/GENOME_REPORTS/', 'prokaryotes.txt', 'ncbi/prokaryotes.txt')
    return open('ncbi/prokaryotes.txt')

def getFile(host, user, passwd, acct, timeout, dirname, filename, save_as):
    """
    Gets a single file from the ftp host specified, and saves it as 'saveas'
    """
    try:
        ftp_connection = FTP(host=host)#, timeout=timeout)
        ftp_connection.login(user, passwd, acct)
        ftp_connection.cwd(dirname)
        output = open(save_as, 'wb')
        ftp_connection.retrbinary('RETR '+filename, output.write)
        output.close()
        ftp_connection.quit()
    except socket.error:
        #error in the underlying connection
        print >> sys.stderr, 'FTPLIB: Error in underlying socket connection'
        return 1
    except ftplib.Error, e:
        print >> sys.stderr, 'FTPLIB: Error during ftp transfer: ' + e.message 
        return 1
    return 0
    
    
        
