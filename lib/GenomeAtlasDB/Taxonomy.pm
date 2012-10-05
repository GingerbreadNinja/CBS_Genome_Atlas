package GenomeAtlasDB::Taxonomy;
use base 'GenomeAtlasDB::DBI';
GenomeAtlasDB::Taxonomy->table('taxonomy');
GenomeAtlasDB::Taxonomy->columns(All => qw/tax_id taxon_name left_id depth left_id/); 

1;
