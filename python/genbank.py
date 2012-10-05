'''
Created on Aug 27, 2012

@author: admin
'''

import MySQLdb
        
def get_genbank(self, accession_number, saveas, host='interaction.cbs.dtu.dk', user='', passwd='', database='genbank_xml'):
    conn = MySQLdb.connnect(host, user, passwd, database)
    curs = self.conn.cursor()
    accession = accession_number.split('.',1)
    if(len(accession)==1):
        curs.execute("""SELECT uncompress(data_z) FROM raw_ff WHERE locus=?""", (accession[0],))
    elif(len(accession)==2):
        curs.execute("""SELECT uncompress(data_z) FROM raw_ff WHERE locus=? AND version=?""", (accession[0],int(accession[1])))
    else:
        return False
    result = self.curs.fetchone()
    if(result == None):
        return False
    decoded = str.decode(result[0])
    o = open(saveas, 'wb')
    o.write(decoded)
    o.close()
    conn.close()
    return True
        
def main():
    """
    Test-Case
    """
    get_genbank("CP000025.1", "/home/panfs/cbs/projects/cge/data/public/ncbi_completed/CP000025.1.gbk")
    
if __name__ == '__main__':
    main()