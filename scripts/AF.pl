#!/usr/bin/perl 
# 
# 
# 
# 
# Kim Brugger (04 Dec 2013), contact: kim.brugger@easih.ac.uk

use strict;
use warnings;
use Data::Dumper;


use lib '/software/packages/ExomeVCFdb/modules';
use CTRU::ExomeVCFdb;

use Getopt::Std;

my $dbhost = 'mgsrv01';
my $dbname = 'ExomeVCFdb';

my $dbi = CTRU::ExomeVCFdb::connect($dbname, $dbhost, "easih_admin", "easih");

my $opts = 'r:a:c:p:';
my %opts;
getopts($opts, \%opts);

my $chr   = $opts{c};
my $pos   = $opts{p};
my $ref   = $opts{r};
my $alt   = $opts{a};

my @samples  =  CTRU::ExomeVCFdb::fetch_sample_ids( );
#print "There are " . int( @samples ) . " samples in the database\n";


if ( $chr && $pos && $ref && $alt) {


  my $vid = CTRU::ExomeVCFdb::fetch_variant_id( $chr, $pos, $ref, $alt );
  my $AF = analyse_variant( $vid );
  print join("\t", $chr, $pos, $ref, $alt, $AF) . "\n";

}
elsif ( $chr && $pos) {

#  print "By Chr ($chr) and Pos ($pos) \n";

  my $vids = CTRU::ExomeVCFdb::fetch_variant_ids_by_position( $chr, $pos);

  foreach my $var ( @{$vids } ) {
#    print Dumper( $var );
    my $AF = analyse_variant( $$var{vid} );
    print join("\t", $chr, $pos, $$var{ref}, $$var{alt}, $AF) . "\n";
  }

}
else {

  my @variants = sort{ $$a{ chr } cmp $$b{ chr } ||
		       $$a{ pos } <=> $$b{ pos }} CTRU::ExomeVCFdb::fetch_variants();

  foreach my $variant ( @variants ) {
    
    my $AF = analyse_variant( $$variant{ vid } );
    print join("\t", $$variant{chr}, $$variant{pos}, $$variant{ref}, $$variant{alt}, $AF) . "\n";
#    die Dumper( $variant );

  }


}



# 
# 
# 
# Kim Brugger (11 Feb 2014)
sub analyse_variant {
  my ( $vid ) = @_;

  
  my @variants = CTRU::ExomeVCFdb::fetch_sample_variants( $vid );

  my $alt_count = 0;
  foreach my $variant ( @variants ) {
    $alt_count += $$variant{ allele_count };
  }

#  return sprintf("%.2f\t%.2f\t\%d\t%d\t%d", $alt_count/(int(@samples)*2), int(@variants)/int(@samples), int( @samples), int(@variants), $alt_count);
  return sprintf("%.2f", $alt_count/(int(@samples)*2));
  return sprintf("%.2f\t%.2f\t\%d\t%d\t%d", $alt_count/(int(@samples)*2), int(@variants)/int(@samples), int( @samples), int(@variants), $alt_count);
}
