package NCBIParser;

use strict;
use warnings;

our $VERSION = '1.00';
use base 'Exporter';

our @EXPORT = qw(get_prokaryotes_index parse_prokaryotes fetch_batch_accession);

use Bio::DB::GenBank;
 

use HTTP::Request;
use LWP::UserAgent;
use XML::SAX;
use Bio::DB::GenBank;
use Data::Dumper;


=head1 NAME
 
NCBIParser - A set of functions to fetch and parse NCBI genome data
 
=head1 DESCRIPTION

  Fetches and parses NCBI genome data.
 
=head2 Functions
 
  The following functions are exported by default.
 
=cut

=head3 get_prokaryotes_index();
  
 Gets the index of all prokaryote (includes archaea) genomes available on NCBI.
 Returns an array of lines, one for each organism.

=cut

sub get_prokaryotes_index {
	my $request = HTTP::Request->new(GET => 'ftp://ftp.ncbi.nlm.nih.gov/genomes/GENOME_REPORTS/prokaryotes.txt');
	my $ua = LWP::UserAgent->new;
    print "getting prokaryotes from NCBI\n";
	my $response = $ua->request($request);
	if ($response->is_success) {
        return split "\n", $response->decoded_content;
	}
    else {
        die $response->status_line;
    }
}

#unused
sub fetch_genome() {
    my ($accession) = @_;
    my $gb = Bio::DB::GenBank->new;
    my $seq_obj = $gb->get_Seq_by_acc($accession);
}

=head3 is_up_to_date()

  Checks whether we have current information on this bioprojectid.
  Returns true if it is up to date.

=cut

sub write_genome_db {
    my ($seq) = @_;
    #TODO
    return 1;
}

sub is_up_to_date {
    my ($bioprojectid, $accession, $modifydate) = @_;
    #look up whether this bioproject has been entered into the database before, and whether it needs to be updated
    #TODO
    warn "bioproject $bioprojectid with accession number $accession with modify date $modifydate is not up to date\n";
    return 0;
}

=head3 fetch_batch_accession

  fetch_batch_accession(\@batch);
  fetch_batch_accession(\@batch, $gb);

  Fetches data about all accenssion numbers passed in with arrayref @batch.
  If a genbank object handle is passed in as $gb, this will be used instead of a new one created.

=cut

sub fetch_batch_accession {
    my ($batch, $gb) = @_;
    my @batch = @$batch;
    $gb = Bio::DB::GenBank->new() if not defined $gb;

    my $seqio = $gb->get_Stream_by_acc(@batch);
    while (my $seq = $seqio->next_seq) {
        print "fetched data\n" . Dumper($seq) . "\n";
        # push data to db
        write_genome_db($seq);
    }
}

=head3 parse_prokaryotes

  parse_prokaryotes(@prokaryotes);

  Parses the index file, checks whether we already have the entry in the database, and builds a queue of accession numbers that need to be retrieved.

=cut

sub parse_prokaryotes {
    my (@prokaryotes) = @_;

    # go through each row in the index file and see if our data is up to date in the local database
    # push the accession numbers we need onto into a batch, then get data for the batch when we have enough

    my $gb = Bio::DB::GenBank->new;

    my @batch;
    my $first = 0;
    foreach my $organism (@prokaryotes) {
        my ($orgname, $bioprojectname, $accession, $bioprojectid, @rest) = split "\t", $organism;

        next if $orgname eq "#Organism/Name";

        print "orgname: $orgname\n";
        print "bioprojectname: $bioprojectname\n";
        print "accession: $accession\n";
        print "bioprojectid: $bioprojectid\n";
        my $modifydate = $rest[12];

        unless (is_up_to_date($bioprojectid, $accession, $modifydate)) { 
            push @batch, $accession;
            print "adding accession $accession to batch\n";
            if (scalar @batch == 16) {
                print "process batch: " . Dumper(@batch) . "\n";
                fetch_batch_accession(\@batch, $gb);
                die;
                @batch = ();
            }
        }
    }
    # take care of any last guys in the batch
    if (length @batch > 0) {
        fetch_batch_accession($gb, @batch);
    }

}


1;
