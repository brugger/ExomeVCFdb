#!/usr/bin/perl 
# 
# 
# 
# 
# Kim Brugger (19 Nov 2013), contact: kim.brugger@easih.ac.uk

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

my $vcf_file = $opts{v} || usage();

my $vcf = Vcf->new(file=>$vcf_file);
$vcf->parse_header();

my $sample_file = $vcf_file;

$sample_file =~ s/\..*//;

my $sample_name;

$sample_name = $vcf_file;
$sample_name =~ s/.*\///;
$sample_name =~ s/\..*//;
$sample_name =~ s/.bam//;
$sample_name =~ s/\_mem//;

#print " $sample_name => $sample_name\n";

#exit;
my $sid = CTRU::ExomeVCFdb::fetch_sample_id( $sample_name );


exit if ( $sid );

$sid = CTRU::ExomeVCFdb::add_sample( $sample_name );
readin_stats("$sample_file.bam.flagstat", "$sample_file.bam.isize");

print "Sample : $sample_name :: $sid\n";

my @sample_variants;


while (my $entry = $vcf->next_data_hash()) {
  
  foreach my $alt ( @{$$entry{ ALT}}) {
    my $vid = CTRU::ExomeVCFdb::add_variant($$entry{CHROM}, $$entry{POS}, $$entry{REF}, $alt, $$entry{INFO}{CSQ});

#    print "VID :: $vid\n";

    my ($ref_freq, $alt_freq) = split(",", $$entry{gtypes}{$sample_name}{AD})  ;

    next if  ( ! $alt_freq );

    if ( ! $alt_freq ) {
      print "    my AAF = $alt_freq/($ref_freq+$alt_freq) \n";
      print "$$entry{CHROM}  $$entry{POS}  $$entry{REF}  $alt $$entry{gtypes}{$sample_name}{AD} \n";
    }

    my $AAF = $alt_freq/($ref_freq+$alt_freq);
    push @sample_variants, [ $sid, $vid, $$entry{gtypes}{$sample_name}{DP}, $AAF, $$entry{QUAL}, $$entry{INFO}{AC}];

  }


}

CTRU::ExomeVCFdb::add_sample_variants( @sample_variants);


# 
# 
# 
# Kim Brugger (04 Dec 2013)
sub readin_stats {
  my ($flagstat_file, $isize_file) = @_;

  my %res;
  open (my $in, $flagstat_file) || die "Could not open '$flagstat_file': $!\n";
  while(<$in>) {
#    print;
    if ( /(\d+) .*total/) {
      $res{total_reads} = $1;
    }
    elsif ( /^(\d+) .*duplicates/) {
      $res{dup_reads} = $1;
    }
    elsif ( /(\d+) .*mapped \((.*?)%/) {
      $res{mapped_reads} = $1;
      $res{mapped_perc} = $2;
    }
    elsif ( /(\d+) .*properly paired/) {
      $res{properly_paired} = $1;
    }
    elsif ( /(\d+) .*singletons/) {
      $res{singletons} = $1;
    }
  }
  close( $in );

  open(  $in, $isize_file) || die "Could not open '$isize_file': $!\n";
  while(<$in>) {
    chomp;
    if ( /^MEDIAN_INSERT_SIZE/) {
      my @rows = split("\t");
      $_ = <$in>;
      chomp;
      my @fields = split("\t");

      for( my $i=0;$i< @rows;$i++) {
	$res{ lc($rows[ $i ])} = $fields[ $i ];
      }
      last;
    }
  }
  close( $in );


#  print Dumper(\%res);

  
  CTRU::ExomeVCFdb::add_sample_stats( $sid, $res{total_reads}, $res{mapped_reads}, $res{dup_reads}, $res{mean_insert_size});

}
