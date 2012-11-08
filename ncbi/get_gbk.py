import os
from Bio import Entrez

class GetGBK():
    
    def __init__(self, logger, email):
        self.__logger = logger
        Entrez.email = email
    
    def __write_output(self, savedir, name, handle):
        if(savedir and not os.path.exists(savedir)):
            os.makedirs(savedir)
        filename = name + '.gbk'
        if(savedir):
            filename = os.path.join(savedir, filename)
        self.__logger.info('Writing genbank file %s', filename)
        o = open(filename, 'wb')
        o.write(handle.read())
        o.close()
        self.__logger.debug('Finished writing genbank file %s', filename)
    
    def get_gbk(self, accession_version, savedir):
        handle = None
        filename = str(accession_version[0])+'_'+str(accession_version[1])
        print filename
        try:
            handle = Entrez.efetch(db='nuccore', rettype='gbwithparts',retmode='text',id=accession_version[0])
            self.__write_output(savedir, filename, handle);
        except Exception, e:
            self.__logger.warn('Error downloading accession')
            return None
        finally:
            if (handle):
                handle.close()
        return filename

