'''
Created on Aug 29, 2012

Implements fetching and writing the Genbank File directly through OS forks and pipes

@author: Steven J. Otto
'''

import subprocess
import os

database='genbank_xml'

def get_filename(savedir, accession):
    return os.path.join(savedir, accession + '.gbk')

def mysql_fork_fetch(accession, savedir='', host='', user='',passwd=''):
    """
    Using a subprocess, open a connection to mysql, query for the genbank file,
    and push it's output into the appropriate file.
    """
    
    #Make sure we have an accession to check
    if(accession==None or accession==''):
        return 1
    
    opath = get_filename(savedir, accession)
    saveas = open(opath, 'w+')
    
    args = ['mysql','--skip-column-names']
    if(host != None and host != ''):
        args.append( '--host='+host )
    if(user != None and user != ''):
        args.append( '--user='+user )
    if(passwd != None and passwd != ''):
        args.append( '--password='+passwd )
        
    args.append('-D')
    args.append(database)
    args.append('--max_allowed_packet=64M')  
    locus = accession.split('.', 2)
    query = 'SELECT uncompress(data_z) FROM raw_ff WHERE locus=\''+locus[0]+'\''
    if(len(locus)==2):
        query += ' AND version='+locus[1]
    
    args.append( '-e' )
    args.append(query)
    #print args;
    p = subprocess.call(args, stdout=saveas)
    
    if(p != 0):
        #os.remove(opath)
        return 1
    return 0

def main():
    exit(mysql_fork_fetch(accession='CP003386.1', savedir='', user='steve', passwd='EasyAs123'))
    
if __name__ == '__main__':
    main()