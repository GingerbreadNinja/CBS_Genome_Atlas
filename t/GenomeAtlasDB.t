#!/usr/bin/perl -w

use strict;
use warnings;

use Test::More tests => 3;
use lib "../lib";
use Class::DBI;

BEGIN { 
    use_ok( 'GenomeAtlasDB' ); 
    use_ok( 'GenomeAtlasDB::Genome' ); 
    use_ok( 'GenomeAtlasDB::Taxonomy' ); 
}

