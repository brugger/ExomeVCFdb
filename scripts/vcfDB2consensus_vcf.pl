#!/usr/bin/perl 
# 
# 
# 
# 
# Kim Brugger (22 Apr 2014), contact: kim.brugger@addenbrookes.nhs.uk

use strict;
use warnings;
use Data::Dumper;

use lib '/software/packages/ExomeVCFdb/modules';
use CTRU::ExomeVCFdb;
use lib "/software/packages/vcftools_0.1.11/lib/perl5/site_perl";
use Vcf;

use Getopt::Std;
my $opts = 'tv:RG:e:X:';
my %opts;
getopts($opts, \%opts);


my $dbhost = 'mgsrv01';
my $dbname = 'ExomeVCFdb';

my $dbi = CTRU::ExomeVCFdb::connect($dbname, $dbhost, "easih_admin", "easih");


my $vcf_out = Vcf->new( version=>"4.0");
$vcf_out->add_columns('GEMINI_MERGED');

print $vcf_out->format_header();

#exit;
my $variants = CTRU::ExomeVCFdb::fetch_variants();

#print Dumper( $variants );

foreach my $variant ( @$variants ) {

    my ($vid, $chr, $pos, $ref, $alt ) = @{$variant};

    my @line = [$chr, $pos, ".", $ref, $alt, "999", "PASS"];
#    print Dumper( \@line);

    print $vcf_out->format_line(@line);
}
