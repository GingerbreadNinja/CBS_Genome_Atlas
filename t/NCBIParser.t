#!/usr/bin/perl -w

use strict;
use warnings;

use Test::More tests => 5;
use lib "../lib";

my $use_network = false; #skip network tests

BEGIN { use_ok( 'NCBIParser' ); }


SKIP: {
    skip "tests require network", 1 unless $use_network;
    ok(get_prokaryotes_index(), "Fetch index from NCBI");
}

#define some test data
my $bioproject = "";
my $accession = "";
my $modifydate = "";

my my @batch = ('52015', '52017');
ok(fetch_batch_accession(\@batch), "Fetch as batch");

TODO: {
    local $TODO = "not written yet";
    ok(write_genome_db();
    ok(is_up_to_date($bioproject, $accession, $modifydate));
}
