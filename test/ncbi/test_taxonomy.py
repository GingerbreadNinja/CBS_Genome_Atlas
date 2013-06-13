'''
Created on Oct 18, 2012

@author: Steven
'''
import unittest
from ncbi.taxonomy import TaxonomyName, NameClass, Rank, TaxonomyNode

class Test(unittest.TestCase):

    def testNameClassStrings(self):
        t = TaxonomyName(0, 'test', 'test', 'acronym')
        self.assertEquals(t.name_class, NameClass.acronym)
        t = TaxonomyName(0, 'test', 'test', 'authority')
        self.assertEquals(t.name_class, NameClass.authority)
        t = TaxonomyName(0, 'test', 'test', 'blast name')
        self.assertEquals(t.name_class, NameClass.blast_name)
        t = TaxonomyName(0, 'test', 'test', 'common name')
        self.assertEquals(t.name_class, NameClass.common_name)
        t = TaxonomyName(0, 'test', 'test', 'equivalent name')
        self.assertEquals(t.name_class, NameClass.equivalent_name)
        t = TaxonomyName(0, 'test', 'test', 'genbank acronym')
        self.assertEquals(t.name_class, NameClass.genbank_acronym)
        t = TaxonomyName(0, 'test', 'test', 'genbank anamorph')
        self.assertEquals(t.name_class, NameClass.genbank_anamorph)
        t = TaxonomyName(0, 'test', 'test', 'genbank common name')
        self.assertEquals(t.name_class, NameClass.genbank_common_name)
        t = TaxonomyName(0, 'test', 'test', 'genbank synonym')
        self.assertEquals(t.name_class, NameClass.genbank_synonym)
        t = TaxonomyName(0, 'test', 'test', 'includes')
        self.assertEquals(t.name_class, NameClass.includes)
        t = TaxonomyName(0, 'test', 'test', 'in-part')
        self.assertEquals(t.name_class, NameClass.in_part)
        t = TaxonomyName(0, 'test', 'test', 'misnomer')
        self.assertEquals(t.name_class, NameClass.misnomer)
        t = TaxonomyName(0, 'test', 'test', 'misspelling')
        self.assertEquals(t.name_class, NameClass.misspelling)
        t = TaxonomyName(0, 'test', 'test', 'scientific name')
        self.assertEquals(t.name_class, NameClass.scientific_name)
        t = TaxonomyName(0, 'test', 'test', 'synonym')
        self.assertEquals(t.name_class, NameClass.synonym)
        t = TaxonomyName(0, 'test', 'test', 'teleomorph')
        self.assertEquals(t.name_class, NameClass.teleomorph)
        t = TaxonomyName(0, 'test', 'test', 'unpublished name')
        self.assertEquals(t.name_class, NameClass.unpublished_name)
        
    def testNameClassValues(self):
        t = NameClass.string_representation(0)
        self.assertEquals(t, 'acronym')
        t = NameClass.string_representation(1)
        self.assertEquals(t, 'anamorph')
        t = NameClass.string_representation(2)
        self.assertEquals(t, 'authority')
        t = NameClass.string_representation(3)
        self.assertEquals(t, 'blast name')
        t = NameClass.string_representation(4)
        self.assertEquals(t, 'common name')
        t = NameClass.string_representation(5)
        self.assertEquals(t, 'equivalent name')
        t = NameClass.string_representation(6)
        self.assertEquals(t, 'genbank acronym')
        t = NameClass.string_representation(7)
        self.assertEquals(t, 'genbank anamorph')
        t = NameClass.string_representation(8)
        self.assertEquals(t, 'genbank common name')
        t = NameClass.string_representation(9)
        self.assertEquals(t, 'genbank synonym')
        t = NameClass.string_representation(10)
        self.assertEquals(t, 'includes')
        t = NameClass.string_representation(11)
        self.assertEquals(t, 'in-part')
        t = NameClass.string_representation(12)
        self.assertEquals(t, 'misnomer')
        t = NameClass.string_representation(13)
        self.assertEquals(t, 'misspelling')
        t = NameClass.string_representation(14)
        self.assertEquals(t, 'scientific name')
        t = NameClass.string_representation(15)
        self.assertEquals(t, 'synonym')
        t = NameClass.string_representation(16)
        self.assertEquals(t, 'teleomorph')
        t = NameClass.string_representation(17)
        self.assertEquals(t, 'unpublished name')
        t = NameClass.string_representation(18)
        self.assertIsNone(t)

    def testTaxonomyName(self):
        t = TaxonomyName(0, 'Test Name', 'Test Unique', 'teleomorph')
        self.assertEquals(0, t.tax_id)
        self.assertEquals('Test Name', t.name_txt)
        self.assertEquals('Test Unique', t.unique_name)
        self.assertEquals(16, t.name_class)
        
    def testTaxonomyNumber(self):
        t = TaxonomyName(0, 'Test Name', 'Test Unique', 16)
        self.assertEquals(0, t.tax_id)
        self.assertEquals('Test Name', t.name_txt)
        self.assertEquals('Test Unique', t.unique_name)
        self.assertEquals(16, t.name_class)
        
    def testTaxonomyNone(self):
        t = TaxonomyName(0, 'Test Name', 'Test Unique', None)
        self.assertEquals(0, t.tax_id)
        self.assertEquals('Test Name', t.name_txt)
        self.assertEquals('Test Unique', t.unique_name)
        self.assertEquals(None, t.name_class)
        
    def testTaxonomyRepresentation(self):
        t = TaxonomyName(0, 'Test Name', 'Test Unique', 'teleomorph')
        rep = str(t)
        ans = '{\'tax_id\': 0, \'name_txt\': \'Test Name\', \'unique_name\': \'Test Unique\', \'name_class\': \'teleomorph\'}'
        self.assertEquals(rep, ans)
        
    def testRank(self):
        for i in range(28):
            before = Rank.string_representation(i)
            after = Rank.parse_enum_string(before)
            self.assertEquals(i, after)
            
    def testTaxonomyNode(self):
        for i in range(28):
            a1 = TaxonomyNode(3, 4, Rank.string_representation(i))
            a2 = TaxonomyNode(4, 3, i)
            self.assertEquals(a1.tax_id, 3)
            self.assertEquals(a1.parent_tax_id, 4)
            self.assertEquals(a2.tax_id, 4)
            self.assertEquals(a2.parent_tax_id, 3)
            self.assertEquals(a1.rank, a2.rank)
        
if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testStringParse']
    unittest.main()