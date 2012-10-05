'''
Created on Aug 29, 2012

@author: admin
'''

import string
from urllib2 import urlopen
from xml import sax
from bioproject import BioProject
from Bio import Entrez

efetch_url = 'http://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi?'

efetch_email_key = 'email='
efetch_email_value = 'st_otto@hotmail.com'

efetch_database_key = 'db='
efetch_database_value = 'bioproject'

efetch_id_key = 'id='
efetch_id_value = ''

class BioProjectXMLHandler(sax.handler.ContentHandler):
    
    results = []
    bioproject = None
    chrs = ''
    in_project = False
    in_assemblyrepliconset = False
    in_replicon = False
    accn_type = ''
    
    def startElement(self, name, attrs):
        if(name=='ArchiveID'):
            self.bioproject = BioProject(attrs['id'], attrs['accession'])
        elif(name=='Project'):
            self.in_project = True
        elif(self.in_project and name=='Organism'):
            self.bioproject.taxon_id = string.atoi(attrs['taxID'])
        elif(name=='OrganismName'):
            self.chrs = ''
        elif(name=='AssemblyRepliconSet'):
            self.in_assemblyrepliconset = True
            self.bioproject.insdc_chromosomes = []
            self.bioproject.insdc_plasmids = []
            self.bioproject.refseq_chromosomes = []
            self.bioproject.refseq_plasmids = []
            self.chrs = ''
        elif(self.in_assemblyrepliconset and name=='Replicon'):
            self.in_replicon = True
        elif(self.in_replicon and name=='Type'):
            self.chrs = ''
        elif(self.in_replicon and name=='RSaccn'):
            self.chrs = ''
        elif(self.in_replicon and name=='GBaccn'):
            self.chrs = ''
        

    def characters(self, content):
        self.chrs += content
    
    def endElement(self, name):
        if(name=='Project'):
            self.in_project = False
        elif(name=='OrganismName'):
            self.bioproject.organism = self.chrs
        elif(name=='AssemblyRepliconSet'):
            self.in_assemblyrepliconset = False
        elif(self.in_assemblyrepliconset == True and name=='Assemblies'):
            self.bioproject.wgs = self.chrs
        elif(name=='Replicon'):
            self.in_replicon = False
        elif(self.in_replicon and name=='Type'):
            self.accn_type = self.chrs 
        elif(self.in_replicon and name=='RSaccn'):
            if(self.accn_type=='eChromosome'):
                self.bioproject.refseq_chromosomes.append(self.chrs)
                self.chrs = ''
            elif(self.accn_type=='ePlasmid'):
                self.bioproject.refseq_plasmids.append(self.chrs)
                self.chrs = ''
        elif(self.in_replicon and name=='GBaccn'):
            if(self.accn_type=='eChromosome'):
                self.bioproject.insdc_chromosomes.append(self.chrs)
                self.chrs = ''
            elif(self.accn_type=='ePlasmid'):
                self.bioproject.insdc_plasmids.append(self.chrs)
                self.chrs = ''
        elif(name=='DocumentSummary'):
            self.results.append(self.bioproject)

def fetch_accessions(project_list):
    efetch_id_value = string.join(map(str, project_list), ',')
    #print efetch_id_value;
    #cannot fetch an empty list
    if(len(efetch_id_value)==0 or efetch_id_value[0]==''): return
    Entrez.email = 'st_otto@hotmail.com'
    handle = Entrez.efetch(db='bioproject', id=efetch_id_value)
    handler = BioProjectXMLHandler()
    sax.parse(handle, handler)
    return handler.results
    

project_list = { 'PRJEA162063' : 162063, 
                 'PRJNA13130' : 13130,
                 'PRJNA13131' : 13131,
                 'PRJNA39147' : 39147,
                 'PRJNA39155' : 39155,
                 'PRJNA39157' : 39157,
                 'PRJNA39163' : 39163,
                 'PRJNA39177' : 39177,
                 'PRJNA45959' : 45959,
                 'PRJNA45961' : 45961,
                 'PRJNA57613' : 57613,
                 'PRJNA57615' : 57615,
                 'PRJNA57617' : 57617,
                 'PRJNA57691' : 57691,
                 'PRJNA57695' : 57695,
                 'PRJNA57697' : 57697,
                 'PRJNA57777' : 57777,
                 'PRJNA57793' : 57793,
                 'PRJNA57801' : 57801,
                 'PRJNA57961' : 57961,
                 'PRJNA61411' : 61411,
                 'PRJNA61565' : 61565,
                 'PRJNA61567' : 61567,
                 'PRJNA61569' : 61569,
                 'PRJNA61573' : 61573,
                 'PRJNA61581' : 61581,
                 'PRJNA61583' : 61583,       
                 'PRJNA61585' : 61585,
                 'PRJNA61589' : 61589,
                 'PRJNA61591' : 61591,
                 'PRJNA61593' : 61593,
                 'PRJNA61611' : 61611,
                 'PRJNA62901' : 62901,
                 'PRJNA62903' : 62903,
                 'PRJNA62911' : 62911,
                 'PRJNA62923' : 62923,
                 'PRJNA62971' : 62971,
                 'PRJNA80923' : 80923
}

def main():
    handler = BioProjectXMLHandler()
    sax.parse(open('/Users/admin/Documents/rnammer_errors_bioprojects.xml', 'rb'), handler)
    bioprojects = handler.results;
    print 'Bioproject_Id\tBioproject\tINSDC/Chromosomes\tINSDC/Plasmids'
    for bioproject in bioprojects:
        print str(bioproject.bioproject_id) + '\t' + str(bioproject.bioproject_accession) + '\t' + str(bioproject.insdc_chromosomes) + '\t' + str(bioproject.insdc_plasmids)
    
if __name__ == '__main__':
    main()