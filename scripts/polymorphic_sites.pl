#!/usr/bin/perl 
# 
# Script to identify polymorphic sites from the VCF database
# 
# 
# Kim Brugger (09 Jan 2014), contact: kim.brugger@easih.ac.uk

use strict;
use warnings;
use Data::Dumper;


use lib '/software/packages/VCFdb/modules';
use CTRU::VCFdb;

my $dbhost = 'mgsrv01';
my $dbname = 'VCFdb';

my $dbi = CTRU::VCFdb::connect($dbname, $dbhost, "easih_admin", "easih");

my $variants = CTRU::VCFdb::fetch_variants();

my %grouped_variants;

foreach my $variant ( @$variants ) {
  
#  push @{$grouped_variants{ $$variant{ chr } }{ $$variant{ pos } }}, [$$variant{ alt }, $$variant{ ref }]
  push @{$grouped_variants{ $$variant{ chr } }{ $$variant{ pos } }}, $$variant{ vid };

}

foreach my $chr ( keys %grouped_variants ) { 
  foreach my $pos ( keys %{$grouped_variants{ $chr }} ) { 
    if ( @{$grouped_variants{ $chr }{ $pos }} == 1 ) {
      delete $grouped_variants{ $chr }{ $pos };
    }
    else {
      
      my @passed_vids;
      foreach my $vid ( @{$grouped_variants{ $chr }{ $pos }} ) {
	my $seq_vars = CTRU::VCFdb::fetch_sample_variants( $vid );
	
	my @passed_seq_vars;
	foreach my $seq_var ( @$seq_vars ) {
	  push @passed_seq_vars, $seq_var if ( $$seq_var{depth} > 30 );
	}
	
	if ( @passed_seq_vars && @passed_seq_vars > 5 ) {
	  
#	  push @passed_vids, \@passed_seq_vars;
	  push @passed_vids, int(@passed_seq_vars);

	}

      }
      
      if ( @passed_vids && @passed_vids > 2 ) {
	$grouped_variants{ $chr }{ $pos } = \@passed_vids;
      }
      else {
	delete $grouped_variants{ $chr }{ $pos };
      }

      
    }
  }
}

#print Dumper( \%grouped_variants );

foreach my $chr ( keys %grouped_variants ) { 
  foreach my $pos ( keys %{$grouped_variants{ $chr }} ) { 

    print join("\t", $chr, $pos, "Sequence Artefact (auto)",  int(@{$grouped_variants{ $chr }{ $pos }})) . "\n";

  }
}    
