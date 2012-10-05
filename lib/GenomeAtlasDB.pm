package GenomeAtlasDB::DBI;
use base 'Class::DBI';
my $dsn = "DBI:mysql:database=genbank_xml:host=mysql";
my $user = "steve";
my $password = "9r3fVKf2";
my $dbh = GenomeAtlasDB::DBI->connection($dsn, $user, $password) or die "can't connect to $dsn with user $user";

1;
