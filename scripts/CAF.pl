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

my $opts = 'r:a:c:p:';
my %opts;
getopts($opts, \%opts);

my $chr   = $opts{c};
my $pos   = $opts{p};
my $ref   = $opts{r};
my $alt   = $opts{a};

my $vid = CTRU::VCFdb::fetch_variant_id( $chr, $pos, $ref, $alt );
my $rid = CTRU::VCFdb::fetch_region_id_by_position( $chr, $pos );

#print "VID:$vid :: $chr, $pos, $ref, $alt\n";

my $region_hash = CTRU::VCFdb::fetch_region_hash( $rid );

#print "RID:$rid $$region_hash{chr}:$$region_hash{start}-$$region_hash{end} :: $chr, $pos\n";


my $times_seen = samples_w_pass_variant( $vid );
my $test = vid2test( $vid );

my @tests = CTRU::VCFdb::fetch_sample_ids_by_test($test);
printf("cCAF: %.4f\t%d\t%d\n", $times_seen/int(@tests), $times_seen, int(@tests));

#print "$times_seen Samples have the variant, a total of ".@tests ." have been performed\n";

if ( $rid ) {
  my @coverages = CTRU::VCFdb::fetch_coverages_by_rid( $rid );
  my $pass_coverages = 0;
  foreach my $coverage ( @coverages) {
    $pass_coverages++ if ( $$coverage{min} && $$coverage{min} > 30);
  }
  printf("pCAF: %.4f\t%d\t%d\n", $times_seen/$pass_coverages, $times_seen, $pass_coverages);
  
#  print "$times_seen Samples have the variant, a total of $pass_coverages pass samples\n";
}




# 
# 
# 
# Kim Brugger (05 Dec 2013)
sub vid2test {
  my ( $vid ) = @_;
  
  my @ss_variants = CTRU::VCFdb::fetch_sample_variants($vid);
  return "" if ( !@ss_variants );
  my $sample_sequence_hash = CTRU::VCFdb::fetch_sample_sequence_hash( $ss_variants[0]{ssid} );
  my $sample_name = $$sample_sequence_hash{ name };
  $sample_name = substr( $sample_name, 0,3);

  return $sample_name;
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
