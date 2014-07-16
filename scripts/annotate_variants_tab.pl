#!/usr/bin/perl 
# 
# 
# 
# 
# Kim Brugger (19 Nov 2013), contact: kim.brugger@easih.ac.uk

use strict;
use warnings;
use Data::Dumper;

use lib '/software/packages/VCFdb/modules';
use CTRU::VCFdb;

use Getopt::Std;
my $opts = 'v:';
my %opts;
getopts($opts, \%opts);


my $dbhost = 'mgsrv01';
my $dbname = 'VCFdb';

my $dbi = CTRU::VCFdb::connect($dbname, $dbhost, "easih_admin", "easih");
open( my $in, $opts{v}) || die "Could not open '$opts{v}': $!\n";
while(<$in>) {
  next if (/#/);
  next if (/^\z/);
  chomp;
  my ($chr, $pos, $ref, $alt, $comment) = split("\t", $_);
  
  my $vid = CTRU::VCFdb::fetch_variant_id( $chr, $pos, $ref, $alt );

  if ( ! $vid ) {
    $ref =~ tr/[ACGT]/[TGCA]/;
    $alt =~ tr/[ACGT]/[TGCA]/;

    $vid = CTRU::VCFdb::fetch_variant_id( $chr, $pos, $ref, $alt );
  }
  
  print "VID::$vid, $chr, $pos, $ref, $alt -- $comment\n";

  CTRU::VCFdb::update_variant($vid, undef, undef, undef, undef, $comment);
  
}
