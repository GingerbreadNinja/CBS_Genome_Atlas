'''
Created on Sep 14, 2012

@author: admin
'''

from Bio import Entrez
from xml import sax
import string

class _esummary_parser(sax.handler.ContentHandler):
    
    def __init__(self):
        self._from_list = []
        self._to_list = []
        self.__taxid = None
        self.__projectid = None
        self.__chars = ''
        
    def startElement(self, name, attrs):
        if(name == 'DocumentSummary'):
            self._has_projectid = None
            self._has_taxid = None
        elif(name == 'TaxId'):
            self.__chars = ''
        elif(name == 'Project_Id'):
            self.__chars = ''
            
    def characters(self, content):
        self.__chars += content
        
    def endElement(self, name):
        if(name == 'TaxId' and self.__taxid == None):
            self.__taxid = self.__chars
        elif(name == 'Project_Id' and self.__projectid == None):
            self.__projectid = self.__chars
        elif(name == 'DocumentSummary'):
            if(self.__taxid != None and self.__projectid != None):
                self._from_list.append(self.__projectid)
                self._to_list.append(self.__taxid)
            self.__projectid = None
            self.__taxid = None
            
    def getResults(self):
        result = dict()
        print "FROM: %i" % len(self._from_list)
        print "TO: %i" % len(self._to_list)
        #assert len(self._from_list) == len(self._to_list)
        for i in range(len(self._from_list)) :
            result[self._from_list[i]] = self._to_list[i]
        return result 

class _elink_parser(sax.handler.ContentHandler):
    
    def __init__(self):
        self._from_list = []
        self._to_list = []
        self.__in_from = False
        self.__in_to = False
        self.__chars = ''
    
    def startElement(self, name, attrs):
        if(name == 'IdList'):
            self.__in_from = True
            self.__in_to = False
        elif(name == 'LinkSetDb'):
            self.__in_from = False
            self.__in_to = True
        elif(name == 'Id'):
            self.__chars = ''
    
    def characters(self, content):
        self.__chars += content
    
    def endElement(self, name):
        if(name == 'Id'):
            if(self.__in_from):
                self._from_list.append(self.__chars)
            elif(self.__in_to):
                self._to_list.append(self.__chars)
        elif(name== 'IdList'):
            self.__in_from = False
        elif(name== 'LinkSetDb'):
            self.__in_to = False
        
    def getResults(self):
        result = dict()
        print "FROM: %i" % len(self._from_list)
        print "TO: %i" % len(self._to_list)
        assert len(self._from_list) == len(self._to_list)
        for i in range(len(self._from_list)) :
            result[self._from_list[i]] = self._to_list[i]
        return result

def elink(email, dbFrom, db, id_list):
    Entrez.email = email;
    id_string = string.join(id_list, ',')
    handle = Entrez.elink(dbFrom=dbFrom, db=db, id=id_string)
    handler = _elink_parser()
    sax.parse(handle, handler)
    handle.close()
    try:
        return handler.getResults()
    except:
        print 'Error fetching elink, trying esummary instead'
    handle = Entrez.esummary(db=dbFrom, id=id_string)
    handler = _esummary_parser()
    sax.parse(handle, handler)
    handle.close
    return handler.getResults()
    
def __main__():
    try:
        print elink('st_otto@hotmail.com', 'bioproject', 'taxonomy', ['175110'])
    except Exception, e:
        print e
        return 1
    return 0
    
if __name__ == '__main__':
    exit(__main__())