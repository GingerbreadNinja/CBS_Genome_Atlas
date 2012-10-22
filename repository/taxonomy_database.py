
import MySQLdb
from ncbi import taxonomy

class TaxonDB():
    
    __conn = None
    __curs = None
    
    def connect(self, **mysqlargs):
        self.__conn = MySQLdb.connect(**mysqlargs)
        self.__curs = self.__conn.cursor()
        
    def close(self):
        if(self.__curs):
            self.__curs.close()
        if(self.__conn):
            self.__conn.close()
        
    def getTaxonNode(self, tax_id):
        self.__curs.execute('SELECT tax_id, parent_tax_id, rank FROM nodes WHERE tax_id = %s', (tax_id,))
        res = self.__curs.fetchone()
        if(res):
            return taxonomy.TaxonomyNode(res[0], res[1], res[2]) 
        else:
            return None
        
    def getTaxonNames(self, tax_id):
        self.__curs.execute('SELECT tax_id, name_txt, unique_name, name_class FROM names WHERE tax_id = %s', (tax_id,))
        results = self.__curs.fetchall()
        return [taxonomy.TaxonomyName(tax_id, name_txt, unique_name, name_class) for tax_id, name_txt, unique_name, name_class in results]
        
    def getTaxonName(self, tax_id, name_class):
        self.__curs.execute('SELECT tax_id, name_txt, unique_name, name_class FROM names WHERE tax_id = %s and name_class = %s', (tax_id, taxonomy.NameClass.string_representation(name_class)))
        res = self.__curs.fetchone()
        if(res):
            return taxonomy.TaxonomyName(res[0], res[1], res[2], res[3])
        else:
            return None
        
    def getTaxonNodeWithName(self, tax_id, name_class):
        self.__curs.execute('SELECT tax_id, parent_tax_id, rank, name_txt, unique_name, name_class FROM nodes JOIN names USING (tax_id) WHERE tax_id = %s and name_class = %s', (tax_id, taxonomy.NameClass.string_representation(name_class)))
        res = self.__curs.fetchone()
        if(res):
            return taxonomy.TaxonomyNode(res[0], res[1], res[2]), taxonomy.TaxonomyName(res[0], res[3], res[4], res[5]) 
        else:
            return None, None
        
    def getFullTaxonomyNodeWithName(self, tax_id, name_class, max_depth = 15):
        current_id = None
        parent_id = tax_id
        tax_list = []
        d = 0
        while(current_id != parent_id and d < max_depth):
            tax_node, tax_name = self.getTaxonNodeWithName(parent_id, name_class)
            if(tax_node and tax_name):
                tax_list.append( (tax_node, tax_name) )
            else:
                break
            current_id = tax_node.tax_id
            parent_id = tax_node.parent_tax_id
            d += 1
        
        tax_list.reverse()
        return tax_list

    def getTaxonomyByName(self, name_txt):
        '''
        Would this end up causing security issues? I'm not so sure because 
        the connector is supposed to escape all the strings but I still 
        worry about sql injection. Especially if this is how we open it to 
        the web-front...
        '''
        #ADDED TO QUESTIONS FOR PROJECT SUPERVISORS
        #self._curs.execute('SELECT tax_id, parent_tax_id, rank, name_txt, unique_name, name_class FROM nodes JOIN names USING (tax_id) WHERE name_txt LIKE %s', ('%' + self.__conn.escape(name_txt) + '%'))