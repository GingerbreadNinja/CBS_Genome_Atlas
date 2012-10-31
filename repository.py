'''
Created on Aug 27, 2012

@author: admin
'''

import sqlite3
import csv
from genbank_sync.ncbi.bioproject import BioProject
from timeit import itertools
import ftpget
import time
from ncbi import elink
from csv import DictWriter

class SQLite3Repository(object):

    def __init__(self, database):
        '''
        Constructor
        '''
        self.connect(database)
        
    
    def connect(self, database):
        self.conn = sqlite3.connect(database)
        self.curs = self.conn.cursor()
    
    def close(self):
        self.conn.close()
    
    def drop_tables(self):
        self.curs.execute("""DROP TABLE IF EXISTS bioproject;""")
        self.curs.execute("""DROP TABLE IF EXISTS accession;""")
        self.curs.execute("""DROP TABLE IF EXISTS bioproject_status;""")
        self.curs.execute("""DROP TABLE IF EXISTS accession_type;""")
        self.curs.execute("""DROP TABLE IF EXISTS project;""")
        self.curs.execute("""DROP VIEW IF EXISTS insdc_accession""")
        
            
    def build_tables(self):
        self.curs.execute("""CREATE TABLE IF NOT EXISTS project
                                (project_id INTEGER PRIMARY KEY AUTOINCREMENT,
                                 name VARCHAR(255),
                                 organism VARCHAR(255),
                                 UNIQUE (name, organism));""")
        self.curs.execute("""CREATE TABLE IF NOT EXISTS bioproject_status
                                (status_id INTEGER PRIMARY KEY AUTOINCREMENT,
                                 name VARCHAR(50) UNIQUE);""")
        self.curs.execute("""CREATE TABLE IF NOT EXISTS bioproject
                                (bioproject_id INTEGER PRIMARY KEY,
                                 project_id INTEGER NOT NULL REFERENCES project(project_id),
                                 name VARCHAR(255) NOT NULL,
                                 taxon_id INTEGER,
                                 status_id INTEGER NOT NULL REFERENCES bioproject_status(status_id),
                                 release_date DATE,
                                 modify_date DATE);""")
        self.curs.execute("""CREATE TABLE IF NOT EXISTS accession_type
                                (type_id INTEGER PRIMARY KEY AUTOINCREMENT,
                                name VARCHAR(50) UNIQUE);""")
        self.curs.execute("""CREATE TABLE  IF NOT EXISTS accession
                                (accession_id INTEGER PRIMARY KEY AUTOINCREMENT,
                                 project_id INTEGER NOT NULL REFERENCES project(project_id),
                                 accession VARCHAR(50) NOT NULL,
                                 type_id INTEGER NOT NULL REFERENCES accession_type(type_id),
                                 download_date DATE,
                                 UNIQUE (project_id, accession));""")
        self.curs.execute("""CREATE VIEW IF NOT EXISTS insdc_accession AS SELECT * FROM accession WHERE type_id IN (SELECT type_id FROM accession_type WHERE name='INSDC/CHROMOSOME' OR name='INSDC/PLASMID')""")
        
    def fill_tables_ncbi(self):
        statuses = [(BioProject.STATUS_NO_DATA,),
                    (BioProject.STATUS_SCAFFOLDS,),
                    (BioProject.STATUS_TRACES,),
                    (BioProject.STATUS_COMPLETE,)]
        self.curs.executemany("""INSERT OR IGNORE INTO bioproject_status(name)
                                    VALUES (?);""", statuses)
        self.conn.commit()
        
    def fill_tables_accessiontype(self):
        accession_types = [('REFSEQ/CHROMOSOME',),
                           ('INSDC/CHROMOSOME',),
                           ('REFSEQ/PLASMID',),
                           ('INSDC/PLASMID',)]
        self.curs.executemany("""INSERT OR IGNORE INTO accession_type(name)
                                    VALUES (?);""", accession_types)
        self.conn.commit()
    
    def bioproject_to_project(self, bioproject):
        return (bioproject.bioproject_accession, bioproject.organism)
    
    def bioproject_to_tuple(self, bioproject):
        project = self.bioproject_to_project(bioproject)
        return (bioproject.bioproject_id, project[0], project[1], bioproject.bioproject_accession,
                bioproject.status, bioproject.release_date, bioproject.modify_date)
    
    def bioproject_tuple1(self, bioproject):
        project = self.bioproject_to_project(bioproject)
        return (bioproject.bioproject_id, project[0], project[1], bioproject.bioproject_accession)
    
    def bioproject_tuple2(self, bioproject):
        return (bioproject.status, bioproject.release_date, bioproject.modify_date, bioproject.bioproject_id)
    
    def bioproject_to_accessions(self, bioproject):
        return (map(lambda acc: (bioproject.bioproject_id, acc, 'REFSEQ/CHROMOSOME'), bioproject.refseq_chromosomes) +
                map(lambda acc: (bioproject.bioproject_id, acc, 'INSDC/CHROMOSOME'), bioproject.insdc_chromosomes) +
                map(lambda acc: (bioproject.bioproject_id, acc, 'REFSEQ/PLASMID'), bioproject.refseq_plasmids) +
                map(lambda acc: (bioproject.bioproject_id, acc, 'INSDC/PLASMID'), bioproject.insdc_plasmids))

    def insert_bioproject(self, bioproject):
        if(bioproject == None or not isinstance(bioproject, BioProject)): return
        project = self.bioproject_to_project(bioproject);
        self.curs.execute("""INSERT OR IGNORE INTO project(name, organism) VALUES(?,?);""",
                          project)
        self.curs.commit()
        self.curs.execute("""INSERT OR IGNORE INTO bioproject
                            (bioproject_id, project_id, name, status_id)
                            VALUES( ?,
                                    (SELECT project_id FROM project WHERE name=? AND organism=?),
                                    ?,
                                    (SELECT status_id FROM bioproject_status WHERE name='NO DATA'));""", self.bioproject_tuple1(bioproject))
        self.conn.commit()
        self.curs.execute("""UPDATE bioproject SET status_id=((SELECT status_id FROM bioproject_status WHERE name = ?),
                                                   release_date=?, modify_date=?
                                                   WHERE bioproject_id = ?""", self.bioproject_tuple2(bioproject))
        self.conn.commit()
        self.curs.executemany("""INSERT IGNORE INTO accession(project_id, accession, type_id)
                                 VALUES( (SELECT project_id FROM bioproject WHERE bioproject=?),
                                         ?,
                                         (SELECT type_id FROM accession_type WHERE name=?));""",
                              self.bioproject_to_accessions(bioproject));
        self.conn.commit()
        
    def insert_bioprojects(self, bioprojects):
        project_list = map(self.bioproject_to_project, bioprojects)
        self.curs.executemany("""INSERT OR IGNORE INTO project(name, organism) VALUES(?,?);""",
                          project_list)
        self.conn.commit()
        bioproject_list = map(self.bioproject_tuple1, bioprojects)
        self.curs.executemany("""INSERT OR IGNORE INTO bioproject
                            (bioproject_id, project_id, name, status_id)
                            VALUES( ?,
                                    (SELECT project_id FROM project WHERE name=? AND organism=?),
                                    ?,
                                    (SELECT status_id FROM bioproject_status WHERE name='NO DATA'));""", bioproject_list)
        self.conn.commit()
        update_list = map(self.bioproject_tuple2, bioprojects)
        self.curs.executemany("""UPDATE bioproject SET status_id=(SELECT status_id FROM bioproject_status WHERE name = ?),
                                                   release_date=?, modify_date=? WHERE bioproject_id=?;""", update_list)
        self.conn.commit()
        accession_list = map(self.bioproject_to_accessions, bioprojects)
        accession_list_proper = list(itertools.chain.from_iterable(accession_list))
        self.curs.executemany("""INSERT OR IGNORE INTO accession(project_id, accession, type_id)
                                 VALUES( (SELECT project_id FROM bioproject WHERE bioproject_id=?),
                                         ?,
                                         (SELECT type_id FROM accession_type WHERE name=?));""",
                              accession_list_proper);
        self.conn.commit()
        
    def insert_accessions_from_bioprojects(self, bioprojects):
        accession_list = map(self.bioproject_to_accessions, bioprojects)
        accession_list_proper = list(itertools.chain.from_iterable(accession_list))
        self.curs.executemany("""INSERT OR IGNORE INTO accession(project_id, accession, type_id)
                                 VALUES( (SELECT project_id FROM bioproject WHERE bioproject_id=?),
                                         ?,
                                         (SELECT type_id FROM accession_type WHERE name=?));""",
                              accession_list_proper);
        self.conn.commit()
        
    def select_complete_bioprojects(self):
        self.curs.execute("""SELECT b.bioproject_id, p.organism, b.name, s.name, b.taxon_id, b.release_date, b.modify_date FROM bioproject b JOIN bioproject_status s ON b.status_id=s.status_id JOIN project p on b.project_id=p.project_id WHERE s.name = 'COMPLETE';""")
        rows = self.curs.fetchall()
        return map(self.row_to_bioproject, rows)
    
    def select_complete_no_accessions(self):
        self.curs.execute("""SELECT bioproject_id FROM bioproject WHERE status_id=(SELECT status_id FROM bioproject_status WHERE name='COMPLETE') AND project_id NOT IN (SELECT project_id FROM insdc_accession);""")
        rows = self.curs.fetchall()
        return rows
    
        self.curs.execute("""SELECT b.name, a.accession, a.download_date FROM insdc_accession a JOIN bioproject b ON a.project_id=b.project_id WHERE b.status_id = (SELECT status_id FROM bioproject_status WHERE name='COMPLETE');""")
    def select_complete_accessions(self):
        rows = self.curs.fetchall()
        return rows
        
    def select_complete_accessions_no_downloads(self):
        self.curs.execute("""SELECT b.name, a.accession, a.download_date FROM insdc_accession a JOIN bioproject b ON a.project_id=b.project_id WHERE b.status_id = (SELECT status_id FROM bioproject_status WHERE name='COMPLETE') AND a.download_date IS NULL;""")
        rows = self.curs.fetchall()
        return rows
    
    def select_complete_accessions_with_download(self):
        self.curs.execute("""SELECT b.name, a.accession, a.download_date FROM insdc_accession a JOIN bioproject b ON a.project_id=b.project_id WHERE b.status_id = (SELECT status_id FROM bioproject_status WHERE name='COMPLETE') AND a.download_date IS NOT NULL;""")
        rows = self.curs.fetchall()
        return rows
        
    def row_to_bioproject(self, row):
        return BioProject(bioproject_id=row[0], bioproject_accession=row[2],
                          organism=row[1], taxon_id=row[4], release_date=row[5], modify_date=row[6], status=row[3])
        
    def mark_downloaded(self, accession):
        self.curs.execute("""UPDATE accession SET download_date=date('now') WHERE accession=?""",(accession,))
    
    def select_accession(self, accession):
        self.curs.execute("""SELECT * FROM accession WHERE accession=?""", (accession,))
        return self.curs.fetchone()
    
    def select_no_taxon_id(self):
        self.curs.execute("""SELECT bioproject_id FROM bioproject WHERE taxon_id IS NULL;""")
        return self.curs.fetchall()
    
    def select_no_taxon_id_complete(self):
        self.curs.execute("""SELECT bioproject_id FROM bioproject WHERE taxon_id IS NULL AND status_id = (SELECT status_id FROM bioproject_status WHERE name='COMPLETE');""")
        return self.curs.fetchall()
    
    def update_taxonomy(self, bp_taxons):
        reverse_tuples = map(lambda bp_taxon: (bp_taxon[1],bp_taxon[0]), bp_taxons)
        self.curs.executemany("""UPDATE bioproject SET taxon_id=? WHERE bioproject_id=?""", reverse_tuples)
        self.conn.commit()


        
def main():
    """
    Test Function
    """
    #import ftpget
    from ncbi import bioproject
    csv_bioproject_stream = ftpget.getNCBIProkaryotesIndex();
    bioprojects = bioproject.CSVBioProjectParser.csv_parse_stream(csv_bioproject_stream)
    csv_bioproject_stream.close()
    
    repo = SQLite3Repository('test.db')
    #repo.drop_tables()
    repo.build_tables()
    repo.fill_tables_ncbi()
    repo.fill_tables_accessiontype()
    repo.insert_bioprojects(bioprojects)
    complete = repo.select_complete_bioprojects()
    printData = map(lambda bioproject: (bioproject.bioproject_id, bioproject.status), complete)
    print repo.select_complete_no_accessions()
    #repo.mark_downloaded('CP003386.1')
    print repo.select_accession('CP003386.1')
    print repo.select_complete_accessions_with_download()
    print len(repo.select_complete_accessions())
    repo.curs.execute("SELECT COUNT(*) FROM insdc_accession;")
    print repo.curs.fetchone()
    print len(repo.select_complete_accessions_no_downloads())
    print printData
    print len(complete)
    
    no_taxon = repo.select_no_taxon_id_complete()
    words = map(lambda bp: str(bp[0]), no_taxon)
    gap = 1
    
    for i in range(len(words)/gap+1):
        start = i * gap
        end = start + gap
        print words[start:end]
        mapping = elink.elink('st_otto@hotmail.com', 'bioproject', 'taxonomy', words[start:end])
        print mapping
        if(len(mapping)>0):
            repo.update_taxonomy(mapping.items())
        time.sleep(1)
        
    bioprojects =  repo.select_complete_bioprojects()
    print bioprojects[0].to_dict()
    
    with open('taxonomy_list.csv', 'wb') as write_file:
        tax_writer = csv.DictWriter(write_file, bioprojects[0].to_dict().keys(), dialect=csv.excel_tab)
        d = dict()
        for k in bioprojects[0].to_dict().keys():
            d[k] = k
        tax_writer.writerow(d)
        for bioproject in bioprojects:
            tax_writer.writerow(bioproject.to_dict())
    repo.close()
    
if __name__ == '__main__':
    main()
