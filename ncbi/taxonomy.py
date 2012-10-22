class TaxonomyNode():
    
    def __init__(self, tax_id, parent_tax_id, rank):
        self.tax_id = tax_id
        self.parent_tax_id = parent_tax_id
        if(isinstance(rank, int)):
            self.rank = rank
        else:
            self.rank = Rank.parse_enum_string(rank)
          
    def __repr__(self):
        return '{\'tax_id\': %i, \'parent_tax_id\': %i, \'rank\': \'%s\'}' % (self.tax_id, self.parent_tax_id, Rank.string_representation(self.rank)) 
        
class TaxonomyName():
            
    def __init__(self, tax_id, name_txt, unique_name, name_class):
        self.tax_id = tax_id
        self.name_txt = name_txt
        self.unique_name = unique_name #It doesn't seem like this field is acutally used...
        if(isinstance(name_class, int)):
            self.name_class = name_class
        else:
            self.name_class = NameClass.parse_enum_string(name_class)
            
    def __repr__(self):
        return '{\'tax_id\': %i, \'name_txt\': \'%s\', \'unique_name\': \'%s\', \'name_class\': \'%s\'}' % (self.tax_id, self.name_txt, self.unique_name, NameClass.string_representation(self.name_class))
        
class NameClass:
    acronym = 0
    anamorph = 1
    authority = 2
    blast_name = 3
    common_name = 4
    equivalent_name = 5
    genbank_acronym = 6
    genbank_anamorph = 7
    genbank_common_name = 8
    genbank_synonym = 9
    includes = 10
    in_part = 11
    misnomer = 12
    misspelling = 13
    scientific_name = 14
    synonym = 15
    teleomorph = 16
    unpublished_name = 17
    
    __list_form = ['acronym','anamorph','authority','blast name','common name','equivalent name','genbank acronym','genbank anamorph','genbank common name','genbank synonym','includes','in-part','misnomer','misspelling','scientific name','synonym','teleomorph','unpublished name']
    
    @classmethod
    def parse_enum_string(cls, enum):
        enum_string = str( enum )
        name = enum_string.replace(' ','_').replace('-','_').lower()
        try:
            return getattr(cls, name)
        except:
            return None #Error value
        
    @classmethod
    def string_representation(cls, name_class):
        if(0 <= name_class < len(cls.__list_form)):
            return cls.__list_form[name_class]
        return None 
   
class Rank():
    
    __list_form = ['class','family','forma','genus','infraclass','infraorder','kingdom','no rank','order',
                   'parvorder','phylum','species','species group','species subgroup','subclass',
                   'subfamily','subgenus','suborder','subphylum','subspecies','subtribe','superclass','superfamily','superkingdom','superorder','superphylum','tribe','varietas']
    
    cls = 0
    family = 1
    forma = 2
    genus = 3
    infraclass = 4
    infraorder = 5
    kingdom = 6
    no_rank = 7
    order = 8
    parvorder = 9
    phylum = 10
    species = 11
    species_group = 12
    species_subgroup = 13
    subclass = 14
    subfamily = 15
    subgenus = 16
    suborder = 17
    subphylum = 18
    subspecies = 19
    subtribe = 20
    superclass = 21
    superfamily = 22
    superkingdom = 23
    superorder = 24
    superphylum = 25
    tribe = 26
    varietas = 27
    
    @classmethod
    def parse_enum_string(cls, enum):
        enum_string = str( enum )
        name = enum_string.replace(' ','_').replace('-','_').lower()
        if(name == 'class'): return cls.cls
        try:
            return getattr(cls, name)
        except:
            return None #Error value
        
    @classmethod
    def string_representation(cls, name_class):
        if(0 <= name_class < len(cls.__list_form)):
            return cls.__list_form[name_class]
        return None 