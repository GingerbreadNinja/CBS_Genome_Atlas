package GenomeAtlasDB::Genome;
use base 'GenomeAtlasDB::DBI';
GenomeAtlasDB::Genome->table('genome');
GenomeAtlasDB::Genome->columns(All => qw/genome_id tax_id genome_name genome_validity/);
GenomeAtlasDB::Genome->has_a(tax_id => 'GenomeAtlasDB::Taxonomy');

1;
