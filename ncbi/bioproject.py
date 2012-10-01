'''
Created on Aug 27, 2012

@author: Steven J. Otto
'''

from datetime import datetime
from csv import DictReader
#from genbank_sync.ncbi import efetch_bioproject

class BioProject():
    
    STATUS_NO_DATA = 'NO DATA'
    STATUS_TRACES = 'SRA OR TRACES'
    STATUS_SCAFFOLDS = 'SCAFFOLDS OR CONTIGS'
    STATUS_COMPLETE = 'COMPLETE'
        
    def __init__(self, bioproject_id, bioproject_accession = "",
                 organism = "", group="", subgroup="", taxon_id = None, size=None, gc_percent=None,
                refseq_chromosomes=[], insdc_chromosomes=[], refseq_plasmids = [],
                insdc_plasmids = [], wgs = "", scaffolds = None, genes = None, proteins = None,
                release_date = None, modify_date = None, status=STATUS_NO_DATA, center=''):
        """
        Initialize a bioproject instance based on the data provided (this data comes from
        the NCBI FTP site's genome report for the bioprojects. The NCBI has a history of
        modifying the layout/structure of this list, so modification may be needed at some
        point in order to keep this working nicely. Most of the fields in here probably will not
        end up in the database anyway.
        """
        if(status == None): status = self.STATUS_NO_DATA
        self.bioproject_id = bioproject_id
        self.bioproject_accession = bioproject_accession
        self.organism = organism
        self.group = group
        self.subgroup = subgroup
        self.size = size
        self.gc_percent = gc_percent
        self.refseq_chromosomes = refseq_chromosomes
        self.insdc_chromosomes = insdc_chromosomes
        self.refseq_plasmids = refseq_plasmids
        self.insdc_plasmids = insdc_plasmids
        self.wgs = wgs
        self.scaffolds = scaffolds
        self.genes = genes
        self.proteins = proteins
        self.release_date = release_date
        self.modify_date = modify_date
        self.status = status
        self.center = center
        self.taxon_id = taxon_id;
        
    def to_dict(self):
        return self.__dict__
        
class CSVBioProjectParser():
    _csv_organism = '#Organism/Name'
    _csv_bioproject_accession = 'BioProject Accession'
    _csv_bioproject_id = 'BioProject ID'
    _csv_group = 'Group'
    _csv_subgroup = 'SubGroup'
    _csv_size = 'Size (Mb)'
    _csv_gc_percent = 'GC%'
    _csv_refseq_chromosomes = 'Chromosomes/RefSeq'
    _csv_insdc_chromosomes = 'Chromosomes/INSDC'
    _csv_refseq_plasmids = 'Plasmids/RefSeq'
    _csv_insdc_plasmids = 'Plasmids/INSDC'
    _csv_wgs = 'WGS'
    _csv_scaffolds = 'Scaffolds'
    _csv_genes = 'Genes'
    _csv_proteins = 'Proteins'
    _csv_release_date = 'Release Date'
    _csv_modify_date = 'Modify Date'
    _csv_status = 'Status'
    _csv_center = 'Center'
    
    _csv_header_list = [_csv_organism, _csv_bioproject_accession, _csv_bioproject_id,
                        _csv_group, _csv_subgroup, _csv_size, _csv_gc_percent, 
                        _csv_refseq_chromosomes, _csv_insdc_chromosomes,
                        _csv_refseq_plasmids, _csv_insdc_plasmids, _csv_wgs,
                        _csv_scaffolds, _csv_genes, _csv_proteins,
                        _csv_release_date, _csv_modify_date, _csv_status, _csv_center]
    
    @staticmethod
    def csv_parse_stream(parseable):
        """
        Parses a whole CSV stream of NCBI-BioProjects. If you want this to parse a string in memory,
        use stringio to wrap your string for processing.
        """
        reader = DictReader(parseable, dialect='excel-tab')#We do not pass the header_list because we expect it to already be in the file
        bioprojects = []
        for row in reader:
            #parse a single row of information
            bioproject = CSVBioProjectParser._csv_parse_row(row)
            bioprojects.append(bioproject)        
        return bioprojects
    
    @staticmethod
    def _csv_parse_row(csv_dict_row):
        """
        Pulls all the data from a bioproject row in the csv file. Then, formats all the data
        so that it fits into our bioproject class's structure and expected types.
        """
        bioproject_id = csv_dict_row[CSVBioProjectParser._csv_bioproject_id]
        bioproject_accession = csv_dict_row[CSVBioProjectParser._csv_bioproject_accession]
        organism = csv_dict_row[CSVBioProjectParser._csv_organism]
        group = csv_dict_row[CSVBioProjectParser._csv_group]
        subgroup = csv_dict_row[CSVBioProjectParser._csv_subgroup]
        size = csv_dict_row[CSVBioProjectParser._csv_size]
        gc_percent = csv_dict_row[CSVBioProjectParser._csv_gc_percent]
        refseq_chromosomes = csv_dict_row[CSVBioProjectParser._csv_refseq_chromosomes]
        insdc_chromosomes = csv_dict_row[CSVBioProjectParser._csv_insdc_chromosomes]
        refseq_plasmids = csv_dict_row[CSVBioProjectParser._csv_refseq_plasmids]
        insdc_plasmids = csv_dict_row[CSVBioProjectParser._csv_insdc_plasmids]
        wgs = csv_dict_row[CSVBioProjectParser._csv_wgs]
        scaffolds = csv_dict_row[CSVBioProjectParser._csv_scaffolds]
        genes = csv_dict_row[CSVBioProjectParser._csv_genes]
        proteins = csv_dict_row[CSVBioProjectParser._csv_proteins]
        release_date = csv_dict_row[CSVBioProjectParser._csv_release_date]
        modify_date = csv_dict_row[CSVBioProjectParser._csv_modify_date]
        status=csv_dict_row[CSVBioProjectParser._csv_status]
        center=csv_dict_row[CSVBioProjectParser._csv_center]
        
        if(bioproject_id != None and bioproject_id != '-'):
            bioproject_id = int(bioproject_id) #bioproject_id is expected as an integer value
        else:
            #this seems a little dangerous as bioproject accession starts with letters...
            bioproject_id = int(bioproject_accession)
        
        if(organism == '-'):
            organism = None
        
        if(group == '-'):
            group = None
        
        if(subgroup == '-'):
            subgroup = None
        
        if(size != None and size != '-'):
            size = float(size) #size is expected to be a floating point number
        else:
            size = None
        
        if(gc_percent != None and gc_percent != '-'):
            gc_percent = float(gc_percent) #gc_percent is expected to be a floating point number
        else:
            gc_percent = None
            
        #split refseq chromosomes into individual strings
        if(refseq_chromosomes != None and refseq_chromosomes != '-'):
            refseq_chromosomes = str.split(refseq_chromosomes, ',')
        else:
            refseq_chromosomes = []
            
        #split insdc chromosomes into individual strings
        if(insdc_chromosomes != None and insdc_chromosomes != '-'):
            insdc_chromosomes = str.split(insdc_chromosomes, ',')
        else:
            insdc_chromosomes = []
            
        #split refseq plasmids into individual strings
        if(refseq_plasmids != None and refseq_plasmids != '-'):
            refseq_plasmids = str.split(refseq_plasmids, ',')
        else:
            refseq_plasmids = []
            
        #split insdc plasmids into individual strings
        if(insdc_plasmids != None and insdc_plasmids != '-'):
            insdc_plasmids = str.split(insdc_plasmids, ',')
        else:
            insdc_plasmids = []
        
        if(wgs != None and wgs == '-'):
            wgs = None
            
        if(scaffolds != None and scaffolds != '-'):
            scaffolds = int(scaffolds) #scaffolds is expected to be an integer number
        else:
            scaffolds = None
        
        if(genes != None and genes != '-'):
            genes = int(genes) #proteins is expected to be an integer number
        else:
            genes = None
        
        if(proteins != None and proteins != '-'):
            proteins = int(proteins) #proteins is expected to be an integer number
        else:
            proteins = None
        
        #parse the release_date as a datetime
        if(release_date != None and release_date != '-'):
            release_date = datetime.strptime(release_date, '%Y/%m/%d')
        else:
            release_date = None
        
        #parse the modify_date as a datetime
        if(modify_date != None and modify_date != '-'):
            modify_date = datetime.strptime(modify_date, '%Y/%m/%d')
        else:
            modify_date = None
        
        #parse the status into one of the accepted formats    
        if(status == None or status == '-' or status == 'No data'): #TODO-Move this to own variable
            status = BioProject.STATUS_NO_DATA
        elif(status == 'SRA or Traces'): #TODO-Move this to own variable
            status = BioProject.STATUS_TRACES
        elif(status == 'Scaffolds or contigs'): #TODO-Move this to own variable
            status = BioProject.STATUS_SCAFFOLDS
        elif(status == 'Complete'): #TODO-Move this to own variable
            status = BioProject.STATUS_COMPLETE
        else:
            status = BioProject.STATUS_NO_DATA
            
        if(center == '-'):
            center = None
            
        return BioProject(bioproject_id, bioproject_accession, organism, group, subgroup, None,
                          size, gc_percent, refseq_chromosomes, insdc_chromosomes,
                          refseq_plasmids, insdc_plasmids, wgs, scaffolds, genes, proteins,
                          release_date, modify_date, status, center)
        
        
def main():
    """
    This is mostly for testing
    """
    from genbank_sync import ftpget
    ftpget.getFile('ftp.ncbi.nlm.nih.gov', '', '', '', 180, 'genomes/GENOME_REPORTS/', 'prokaryotes.txt', 'prokaryotes.txt')
    csv_bioproject_stream = open('prokaryotes.txt')
    bioprojects = CSVBioProjectParser.csv_parse_stream(csv_bioproject_stream)
    csv_bioproject_stream.close()
    printData = map(lambda bioproject: (bioproject.bioproject_id, bioproject.refseq_chromosomes), bioprojects)
    print printData
    
    complete = filter(lambda bioproject: bioproject.status==BioProject.STATUS_COMPLETE, bioprojects)
    print len(complete)
    
    
if __name__ == '__main__':
    main()