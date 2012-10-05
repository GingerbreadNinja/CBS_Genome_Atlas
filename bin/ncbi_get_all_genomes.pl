#!/usr/bin/perl -w

use strict;
use warnings;

use lib "../lib";
use NCBIParser;
use GenomeAtlasDB;
use GenomeAtlasDB::Genome;


# pull in data from NCBI and write new data into the database

#skip this since while debugging
#my @prokaryotes = get_prokaryotes_index();
#parse_prokaryotes(@prokaryotes);

my @batch = ('41639', '41641'); #'41643'; '43389'; '43391'; '52015'; '52017'; '52847'; '52849'; '52853'; '52857'; '52859'; '52861'; '52863'; '52865'; '52867');
fetch_batch_accession(\@batch);

# run heuristics to validate data
# TODO

# write new data to the database

GenomeAtlasDB::Genome->new();

