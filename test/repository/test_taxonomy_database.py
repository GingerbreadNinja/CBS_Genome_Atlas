'''
Created on Oct 19, 2012

@author: Steven
'''
import unittest

from repository import taxonomy_database
from ncbi import taxonomy
from settings import Settings

class Test(unittest.TestCase):

    '''
    <Taxon>
    <TaxId>683083</TaxId>
    <ScientificName>Campylobacter jejuni subsp. jejuni 414</ScientificName>
    <OtherNames>
    <EquivalentName>Campylobacter jejuni subsp. jejuni strain 414</EquivalentName>
    <EquivalentName>Campylobacter jejuni subsp. jejuni str. 414</EquivalentName>
    <Name>
    <ClassCDE>misspelling</ClassCDE>
    <DispName>Campylobacter jejuni 414</DispName>
    </Name>
    </OtherNames>
    <ParentTaxId>32022</ParentTaxId>
    <Rank>no rank</Rank>
    <Division>Bacteria</Division>
    <GeneticCode>
    <GCId>11</GCId>
    <GCName>Bacterial, Archaeal and Plant Plastid</GCName>
    </GeneticCode>
    <MitoGeneticCode>
    <MGCId>0</MGCId>
    <MGCName>Unspecified</MGCName>
    </MitoGeneticCode>
    <Lineage>
    cellular organisms; Bacteria; Proteobacteria; delta/epsilon subdivisions; Epsilonproteobacteria; Campylobacterales; Campylobacteraceae; Campylobacter; Campylobacter jejuni; Campylobacter jejuni subsp. jejuni
    </Lineage>
    <LineageEx>
    <Taxon>
    <TaxId>131567</TaxId>
    <ScientificName>cellular organisms</ScientificName>cellular_organisms
    <Rank>no rank</Rank>
    </Taxon>
    <Taxon>
    <TaxId>2</TaxId>
    <ScientificName>Bacteria</ScientificName>
    <Rank>superkingdom</Rank>
    </Taxon>
    <Taxon>
    <TaxId>1224</TaxId>
    <ScientificName>Proteobacteria</ScientificName>
    <Rank>phylum</Rank>
    </Taxon>
    <Taxon>
    <TaxId>68525</TaxId>
    <ScientificName>delta/epsilon subdivisions</ScientificName>
    <Rank>subphylum</Rank>
    </Taxon>
    <Taxon>
    <TaxId>29547</TaxId>
    <ScientificName>Epsilonproteobacteria</ScientificName>
    <Rank>class</Rank>
    </Taxon>
    <Taxon>
    <TaxId>213849</TaxId>
    <ScientificName>Campylobacterales</ScientificName>
    <Rank>order</Rank>
    </Taxon>
    <Taxon>
    <TaxId>72294</TaxId>
    <ScientificName>Campylobacteraceae</ScientificName>
    <Rank>family</Rank>
    </Taxon>
    <Taxon>
    <TaxId>194</TaxId>
    <ScientificName>Campylobacter</ScientificName>
    <Rank>genus</Rank>
    </Taxon>
    <Taxon>
    <TaxId>197</TaxId>
    <ScientificName>Campylobacter jejuni</ScientificName>
    <Rank>species</Rank>
    </Taxon>
    <Taxon>
    <TaxId>32022</TaxId>
    <ScientificName>Campylobacter jejuni subsp. jejuni</ScientificName>
    <Rank>subspecies</Rank>
    </Taxon>
    </LineageEx>
    <CreateDate>2009/10/13 10:18:01</CreateDate>
    <UpdateDate>2010/01/11 13:14:30</UpdateDate>
    <PubDate>2009/10/15 18:00:17</PubDate>
    </Taxon>
    '''

    test_id = 683083
    test_parent = 32022
    scientific_name = taxonomy.TaxonomyName(test_id, 'Campylobacter jejuni subsp. jejuni 414', '', taxonomy.NameClass.scientific_name)
    equivalent_name1 = taxonomy.TaxonomyName(test_id, 'Campylobacter jejuni subsp. jejuni strain 414', '', taxonomy.NameClass.equivalent_name)
    equivalent_name2 = taxonomy.TaxonomyName(test_id, 'Campylobacter jejuni subsp. jejuni str. 414', '', taxonomy.NameClass.equivalent_name)
    misspelling = taxonomy.TaxonomyName(test_id, 'Campylobacter jejuni 414', '', taxonomy.NameClass.misspelling)
    test_names = [ scientific_name, equivalent_name1, equivalent_name2, misspelling ]
    test_node = taxonomy.TaxonomyNode(test_id, test_parent, taxonomy.Rank.no_rank)
    test_full_nodes = [ taxonomy.TaxonomyNode(1,1,taxonomy.Rank.no_rank),
                        taxonomy.TaxonomyNode(131567,1,taxonomy.Rank.no_rank),
                        taxonomy.TaxonomyNode(2,131567,taxonomy.Rank.superkingdom),
                        taxonomy.TaxonomyNode(1224,2,taxonomy.Rank.phylum),
                        taxonomy.TaxonomyNode(68525,1224,taxonomy.Rank.subphylum),
                        taxonomy.TaxonomyNode(29547,68525,taxonomy.Rank.cls),
                        taxonomy.TaxonomyNode(213849,29547,taxonomy.Rank.order),
                        taxonomy.TaxonomyNode(72294,213849,taxonomy.Rank.family),
                        taxonomy.TaxonomyNode(194,72294,taxonomy.Rank.genus),
                        taxonomy.TaxonomyNode(197,194,taxonomy.Rank.species),
                        taxonomy.TaxonomyNode(32022,197,taxonomy.Rank.subspecies),
                        taxonomy.TaxonomyNode(683083,32022,taxonomy.Rank.no_rank)]
    
    taxondb = None
    
    def setUp(self):
        self.taxondb = taxonomy_database.TaxonDB()
        self.taxondb.connect(Settings.get_settings('config/cge-dev.cfg')['mysql_taxonomy'])
        
    def tearDown(self):
        self.taxondb.close()

    def testTaxonomyNode(self):
        node = self.taxondb.getTaxonNode(self.test_id)
        self.assertEqual(node.tax_id, self.test_node.tax_id)
        self.assertEqual(node.parent_tax_id, self.test_node.parent_tax_id)
        self.assertEqual(node.rank, self.test_node.rank)
        
    def testTaxonomyName(self):
        name = self.taxondb.getTaxonName(self.test_id, taxonomy.NameClass.scientific_name)
        self.assertEqual(self.scientific_name.tax_id, name.tax_id)
        self.assertEqual(self.scientific_name.name_txt, name.name_txt)
        self.assertEqual(self.scientific_name.unique_name, name.unique_name)
        self.assertEqual(self.scientific_name.name_class, name.name_class)
        
    def testTaxonomyNodeWithName(self):
        node, name = self.taxondb.getTaxonNodeWithName(self.test_id, taxonomy.NameClass.scientific_name)
        self.assertEqual(node.tax_id, self.test_node.tax_id)
        self.assertEqual(node.parent_tax_id, self.test_node.parent_tax_id)
        self.assertEqual(node.rank, self.test_node.rank)
        self.assertEqual(self.scientific_name.tax_id, name.tax_id)
        self.assertEqual(self.scientific_name.name_txt, name.name_txt)
        self.assertEqual(self.scientific_name.unique_name, name.unique_name)
        self.assertEqual(self.scientific_name.name_class, name.name_class)
        
    def testTaxonomyNames(self):
        names = self.taxondb.getTaxonNames(self.test_id)
        nameStrings = map(str, names)
        testStrings = map(str, self.test_names)
        for name in nameStrings:
            self.assertIn(name, testStrings)
            
    def testFullTaxonomy(self):
        fullList = self.taxondb.getFullTaxonomyNodeWithName(self.test_id, taxonomy.NameClass.scientific_name)
        fullNodes = map(lambda x: str(x[0]), fullList)
        for node in self.test_full_nodes:
            self.assertIn(str(node), fullNodes)
            
    def testPartialTaxonomy(self):
        fullList = self.taxondb.getFullTaxonomyNodeWithName(self.test_id, taxonomy.NameClass.scientific_name, 5)
        fullNodes = map(lambda x: str(x[0]), fullList)
        testNodes = map(str, self.test_full_nodes)
        self.assertLessEqual(len(fullNodes), 5)
        for node in fullNodes:
            self.assertIn(node, testNodes)
        
if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testTaxonomyNode']
    unittest.main()