#!/usr/bin/perl 
# 
# 
# 
# 
# Kim Brugger (04 Dec 2013), contact: kim.brugger@easih.ac.uk

use strict;
use warnings;
use Data::Dumper;


use lib '/software/packages/VCFdb/modules';
use CTRU::VCFdb;

use Getopt::Std;

my $dbhost = 'mgsrv01';
my $dbname = 'VCFdb';

my $dbi = CTRU::VCFdb::connect($dbname, $dbhost, "easih_admin", "easih");

my $test = shift;

my @tests_done = CTRU::VCFdb::fetch_sample_ids_by_test($test);
my $tests_done = int(@tests_done);


my @variants = CTRU::VCFdb::variants_from_test( $test );
foreach my $variant (  @variants ) {

#  print Dumper( $variant );

  my $times_seen = samples_w_pass_variant( $$variant{ vid } );

  next if ($times_seen == 0);

  my $variant_hash = CTRU::VCFdb::fetch_variant_hash( $$variant{ vid } );

  printf("VID:$$variant{vid}\t$$variant_hash{chr}:$$variant_hash{pos}\t$$variant_hash{'ref'}>$$variant_hash{alt}\tcCAF: %.4f\t%d\t%d\n", $times_seen/$tests_done, $times_seen, $tests_done);
#  exit;
}


# 
# 
# 
# Kim Brugger (05 Dec 2013)
sub samples_w_pass_variant {
  my ( $vid ) = @_;
  
  my @ss_variants = CTRU::VCFdb::fetch_sample_variants($vid);


  my @pass_ssids;
  foreach my $ss_variant ( @ss_variants ) {
    
    next if ( $$ss_variant{depth} < 30 );
    push @pass_ssids, $$ss_variant{ssid};
    
  }

  my %sid_hash;
  
  foreach my $ssid (@pass_ssids ) {
    
    my $sample_sequence_hash = CTRU::VCFdb::fetch_sample_sequence_hash( $ssid );
    $sid_hash{ $$sample_sequence_hash{ sid }}++;


  }

  return keys %sid_hash;
}
