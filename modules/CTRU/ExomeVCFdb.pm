package CTRU::ExomeVCFdb;
# 
# 
# 
# 
# Kim Brugger (19 Nov 2013), contact: kim.brugger@easih.ac.uk

use strict;
use warnings;
use Data::Dumper;
use POSIX qw( strftime );


use EASIH::DB;

my $dbi;

my @samples;


# 
# 
# 
# Kim Brugger (11 Feb 2014)
sub AF_by_vid {
  my ( $vid ) = @_;

  if ( ! @samples ) {
     @samples  =  CTRU::ExomeVCFdb::fetch_sample_ids( );
  }

  my @variants = CTRU::ExomeVCFdb::fetch_sample_variants( $vid );

  my $alt_count = 0;
  foreach my $variant ( @variants ) {
    $alt_count += $$variant{ allele_count } if ( $$variant{ allele_count } );
  }



#  return sprintf("%.2f\t%.2f\t\%d\t%d\t%d", $alt_count/(int(@samples)*2), int(@variants)/int(@samples), int( @samples), int(@variants), $alt_count);
  return sprintf("%.2f", $alt_count/(int(@samples)*2));
}



# 
# 
# 
# Kim Brugger (11 Feb 2014)
sub AF {
  my ( $chr, $pos, $ref, $alt ) = @_;
  
  my $vid = CTRU::ExomeVCFdb::fetch_variant_id( $chr, $pos, $ref, $alt );

  return AF_by_vid( $vid );
}


# 
# 
# 
# Kim Brugger (11 Feb 2014)
sub AFs {
  my ( $chr, $pos, $ref, $alt ) = @_;
  
  my $vid = CTRU::ExomeVCFdb::fetch_variant_id( $chr, $pos, $ref, $alt );

  return AF_by_vid( $vid );
}



# 
# 
# 
# Kim Brugger (11 Feb 2014)
sub raw_frequency_by_vid {
  my ( $vid ) = @_;

  if ( ! @samples ) {
     @samples  =  CTRU::ExomeVCFdb::fetch_sample_ids( );
  }

  my @variants = CTRU::ExomeVCFdb::fetch_sample_variants( $vid );

  my $alt_count = 0;
  foreach my $variant ( @variants ) {
    $alt_count += $$variant{ allele_count } if ( $$variant{ allele_count } );
  }

  return sprintf("%d", $alt_count, int(@samples));



  return sprintf("%.2f\t%.2f\t\%d\t%d\t%d", $alt_count/(int(@samples)*2), int(@variants)/int(@samples), int( @samples), int(@variants), $alt_count);
  return sprintf("%.2f", $alt_count/(int(@samples)*2));
}



# 
# 
# 
# Kim Brugger (11 Feb 2014)
sub raw_frequency {
  my ( $chr, $pos, $ref, $alt ) = @_;
  
  my $vid = CTRU::ExomeVCFdb::fetch_variant_id( $chr, $pos, $ref, $alt );

  return raw_frequency_by_vid( $vid );
}




# 
# 
# 
# Kim Brugger (11 Feb 2014)
sub sample_frequency_by_vid {
  my ( $vid ) = @_;

  if ( ! @samples ) {
     @samples  =  CTRU::ExomeVCFdb::fetch_sample_ids( );
  }

  my @variants = CTRU::ExomeVCFdb::fetch_sample_variants( $vid );

  my $alt_count = 0;
  foreach my $variant ( @variants ) {
    $alt_count += $$variant{ allele_count };
  }



#  return sprintf("%.2f\t%.2f\t\%d\t%d\t%d", $alt_count/(int(@samples)*2), int(@variants)/int(@samples), int( @samples), int(@variants), $alt_count);
  return sprintf("%.2f", int(@variants)/int(@samples));
}



# 
# 
# 
# Kim Brugger (11 Feb 2014)
sub sample_frequency {
  my ( $chr, $pos, $ref, $alt ) = @_;
  
  my $vid = CTRU::ExomeVCFdb::fetch_variant_id( $chr, $pos, $ref, $alt );

  return sample_frequency_by_vid( $vid );
}


# 
# 
# 
# Kim Brugger (20 Nov 2013)
sub connect {
  my ($dbname, $dbhost, $db_user, $db_pass) = @_;
  $dbhost  ||= "mgsrv01";
  $db_user ||= 'easih_ro';

  $dbi = EASIH::DB::connect($dbname,$dbhost, $db_user, $db_pass);
  return $dbi;
}


# 
# 
# 
# Kim Brugger (20 Nov 2013)
sub add_sample {
  my ($name) = @_;


  if ( ! $name ) { 
    print STDERR "add_sequence: No sequence name provided\n";
    return -3;
  }
  
  my $sid = fetch_sample_id( $name );
  return $sid if ( $sid );
     
  my %call_hash = ( name => $name);

  return (EASIH::DB::insert($dbi, "sample", \%call_hash));
}



# 
# 
# 
# Kim Brugger (3 Dec 2013)
sub add_sample_stats {
  my ($sid, $total_reads, $mapped_reads, $duplicate_reads, $mean_isize, ) = @_;

  my $ss_name = fetch_sample_name($sid);
  if ( ! $ss_name  ) {
    print STDERR "add_sample_variant: Unknown sequence_sample-id $sid '$ss_name'\n";
    return -6;
  }
     
  my %call_hash = ( sid => $sid);
  $call_hash{ total_reads }     = $total_reads     if ( $total_reads );
  $call_hash{ mapped_reads }    = $mapped_reads    if ( $mapped_reads );
  $call_hash{ duplicate_reads } = $duplicate_reads if ( $duplicate_reads );
  $call_hash{ mean_isize }      = $mean_isize      if ( $mean_isize );

  return (EASIH::DB::update($dbi, "sample", \%call_hash, "sid"));
}


# 
# 
# 
# Kim Brugger (20 Nov 2013)
sub fetch_sample_id {
  my ( $name ) = @_;
  if ( ! $name ) { 
    print STDERR "fetch_sample_id: No sample name provided\n";
    return -1;
  }
  my $q    = "SELECT sid FROM sample WHERE name = ?";
  my $sth  = EASIH::DB::prepare($dbi, $q);
  my @line = EASIH::DB::fetch_array( $dbi, $sth, $name );
  return $line[0] || undef;
}

# 
# 
# 
# Kim Brugger (20 Nov 2013)
sub fetch_sample_name {
  my ( $sid ) = @_;

  if ( ! $sid ) { 
    print STDERR "fetch_sample_name: No sample id provided\n";
    return "";
  }

  my $q    = "SELECT name FROM sample WHERE sid = ?";
  my $sth  = EASIH::DB::prepare($dbi, $q);
  my @line = EASIH::DB::fetch_array( $dbi, $sth, $sid );
  return $line[0] || undef;
}

# 
# 
# 
# Kim Brugger (20 Nov 2013)
sub fetch_sample_hash {
  my ( $sid ) = @_;
  if ( ! $sid ) { 
    print STDERR "fetch_sample_hash: No sample id provided\n";
    return {};
  }
  my $q    = "SELECT * FROM sample WHERE sid = ?";
  my $sth  = EASIH::DB::prepare($dbi, $q);
  return( EASIH::DB::fetch_hash( $dbi, $sth, $sid ));
}


# 
# 
# 
# Kim Brugger (20 Nov 2013)
sub fetch_sample_ids {

  my $q    = "SELECT sid FROM sample";
  my $sth  = EASIH::DB::prepare($dbi, $q);
  return( EASIH::DB::fetch_array_array( $dbi, $sth ));
}


# 
# 
# 
# Kim Brugger (20 Nov 2013)
sub update_sample {
  my ($sid, $name, $total_reads, $mapped_reads, $duplicate_reads, $mean_isize, ) = @_;


  if ( ! $sid ) { 
    print STDERR "update_sample: No sample sequence id provided\n";
    return -1;
  }

  if ( ! $name ) { 
    print STDERR "update_sample: No name provided\n";
    return -1;
  }

  my %call_hash;
  $call_hash{sid}              = $sid;
  $call_hash{name}              = $name            if ($name);
  $call_hash{ total_reads }     = $total_reads     if ( $total_reads );
  $call_hash{ mapped_reads }    = $mapped_reads    if ( $mapped_reads );
  $call_hash{ duplicate_reads } = $duplicate_reads if ( $duplicate_reads );
  $call_hash{ mean_isize }      = $mean_isize      if ( $mean_isize );
  return (EASIH::DB::update($dbi, "sample", \%call_hash, "sid"));
}


#================== variant functions =========================

# 
# 
# 
# Kim Brugger (20 Nov 2013)
sub add_variant {
  my ($chr, $pos, $ref, $alt, $annotation, $comment) = @_;

  if ( ! $chr ) { 
    print STDERR "add_variant: No chr provided\n";
    return -1;
  }

  if ( ! $pos ) { 
    print STDERR "add_variant: No variant position provided\n";
    return -2;
  }

  if ( ! $ref ) { 
    print STDERR "add_variant: No variant ref base(s) provided\n";
    return -3;
  }

  if ( ! $alt ) { 
    print STDERR "add_variant: No variant alt base(s) provided\n";
    return -4;
  }

  
  my $vid = fetch_variant_id( $chr, $pos, $ref, $alt );
  return $vid if ( $vid );
     
  my %call_hash = ( chr  => $chr,
		    pos  => $pos,
		    ref  => $ref,
		    alt  => $alt);

  $call_hash{ comment   } = $comment     if ( $comment    );
  $call_hash{ annotation } = $annotation if ( $annotation );

  return (EASIH::DB::insert($dbi, "variant", \%call_hash));
}

# 
# 
# 
# Kim Brugger (20 Nov 2013)
sub fetch_variant_id {
  my ( $chr, $pos, $ref, $alt ) = @_;

  if ( ! $chr || !$pos || !$ref || ! $alt ) { 
    print STDERR "fetch_variant_id: requires 4 paramters: chr, pos, ref and alt\n";
    return -1;
  }

  my $q    = "SELECT vid FROM variant WHERE chr = ? AND pos = ? AND ref = ? AND alt = ?";
  my $sth  = EASIH::DB::prepare($dbi, $q);
  my @line = EASIH::DB::fetch_array( $dbi, $sth, $chr, $pos, $ref, $alt );
  return $line[0] || undef;
}


# 
# 
# 
# Kim Brugger (20 Nov 2013)
sub fetch_variant_ids_by_position {
  my ( $chr, $pos ) = @_;

  if ( ! $chr || !$pos ) { 
    print STDERR "fetch_variant_ids_by_position: requires 2 paramters: chr, pos\n";
    return -1;
  }

  my $q    = "SELECT * FROM variant WHERE chr = ? AND pos = ?";
  my $sth  = EASIH::DB::prepare($dbi, $q);
  return EASIH::DB::fetch_array_hash( $dbi, $sth, $chr, $pos );
}

# 
# 
# 
# Kim Brugger (20 Nov 2013)
sub fetch_variants {

  my $q    = "SELECT * FROM variant order by (chr) limit 10";
  $q    = "SELECT vid, chr, pos, ref, alt FROM variant order by (chr)";
  my $sth  = EASIH::DB::prepare($dbi, $q);
  return (EASIH::DB::fetch_array_array( $dbi, $sth));
}



# 
# 
# 
# Kim Brugger (20 Nov 2013)
sub fetch_variant_hash {
  my ( $vid ) = @_;
  if ( ! $vid ) { 
    print STDERR "fetch_variant_hash: No variant id provided\n";
    return {};
  }
  my $q    = "SELECT * FROM variant WHERE vid = ?";
  my $sth  = EASIH::DB::prepare($dbi, $q);
  return( EASIH::DB::fetch_hash( $dbi, $sth, $vid ));
}

# 
# 
# 
# Kim Brugger (20 Nov 2013)
sub update_variant {
  my ($vid, $chr, $pos, $ref, $alt, $comment, $annotation) = @_;

  if ( ! $vid ) { 
    print STDERR "update_variant: No variant id provided\n";
    return -1;
  }

  my %call_hash;
  $call_hash{vid}        = $vid        if ( $vid        );
  $call_hash{chr}        = $chr        if ( $chr        );
  $call_hash{pos}        = $pos        if ( $pos        );
  $call_hash{ref}        = $ref        if ( $ref        );
  $call_hash{alt}        = $alt        if ( $alt        );
  $call_hash{comment}    = $comment    if ( $comment    );
  $call_hash{annotation} = $annotation if ( $annotation );

  return (EASIH::DB::update($dbi, "variant", \%call_hash, "vid"));
}


#================== sample_variant functions =========================

# 
# 
# 
# Kim Brugger (20 Nov 2013)
sub add_sample_variant {
  my ($sid, $vid, $depth, $AAF, $quality, $allele_count) = @_;

  if ( ! $sid ) { 
    print STDERR "add_sample_variant: No sample id provided\n";
    return -1;
  }

  if ( ! $vid ) { 
    print STDERR "add_sample_variant: No variant id provided\n";
    return -2;
  }

  if ( ! $depth ) { 
    print STDERR "add_sample_variant: No depth provided\n";
    return -3;
  }

  if ( ! $AAF ) { 
    print STDERR "add_sample_variant: No Alternative Allele Freq (AAF) provided\n";
    return -4;
  }

  if ( ! $quality ) { 
    print STDERR "add_sample_variant: No quality provided\n";
    return -5;
  }

  if ( ! $allele_count ) {
    print STDERR "add_sample_variant: No allele count provided\n";
    return -8;
  }
    
  my $ss_name = fetch_sample_name($sid);
  if ( ! $ss_name  ) {
    print STDERR "add_sample_variant: Unknown sequence_sample-id $sid '$ss_name'\n";
    return -6;
  }

  my $v_hash = fetch_variant_hash($vid);
  if ( ! $v_hash || keys %{$v_hash} == 0) {
    print STDERR "add_sample_variant: Unknown variant-id $vid $v_hash\n";
    return -7;
  }

  my $sv_hash = fetch_sample_variant_hash( $sid, $vid );
  return 1 if ( $sv_hash && keys %{$sv_hash} > 0 );

  my %call_hash = ( sid          => $sid,
		    vid          => $vid,
		    depth        => $depth,
		    AAF          => $AAF,
		    quality      => $quality,
		    allele_count => $allele_count);


  return (EASIH::DB::insert($dbi, "sample_variant", \%call_hash));
}



# 
# 
# 
# Kim Brugger (12 Jun 2014), contact: kim.brugger
sub add_sample_variants {
  my (@sample_variants) = @_;


  if ( ! @sample_variants ) { 
    print STDERR "add_sample_variants: No sample_variants(s) provided\n";
    return -1;
  }

  my @new_sample_variants;
  foreach my $entry ( @sample_variants ) {
    my ($sid, $vid, $depth, $AAF, $quality, $allele_count) = @$entry;

#    print "    my ($sid, $vid, $depth, $AAF, $quality, $allele_count) = \n";

    my $sv_hash = fetch_sample_variant_hash( $sid, $vid );
    next if ( $sv_hash && keys %{$sv_hash} > 0 );


    if ( ! $sid ) { 
      print STDERR "add_sample_variants: No sample id provided\n";
      exit;
      next;
    }
    
    if ( ! $vid ) { 
      print STDERR "add_sample_variants: No variant id provided\n";
      exit; 
      next;
    }

    my $ss_name = fetch_sample_name($sid);
    if ( ! $ss_name  ) {
      print STDERR "add_sample_variant: Unknown sequence_sample-id $sid '$ss_name'\n";
      exit;
      next;
    }


    my $v_hash = fetch_variant_hash($vid);
    if ( ! $v_hash || keys %{$v_hash} == 0) {
      print STDERR "add_sample_variant: Unknown variant-id $vid $v_hash\n";
      exit;
      next;
    }


    my %call_hash = ( sid          => $sid,
		      vid          => $vid,
		      depth        => $depth,
		      AAF          => $AAF,
		      quality      => $quality,
		      allele_count => $allele_count);
    push @new_sample_variants, \%call_hash;

    if ( int( @new_sample_variants ) == 500 ) {
      (EASIH::DB::insert($dbi, "sample_variant", \@new_sample_variants));
      undef @new_sample_variants;
    }

  }

  if ( int(@new_sample_variants)) {
    (EASIH::DB::insert($dbi, "sample_variant", \@new_sample_variants));
  }
}




# 
# 
# 
# Kim Brugger (20 Nov 2013)
sub fetch_sample_variant_hash {
  my ($sid,  $vid ) = @_;
  if ( ! $vid || ! $sid ) { 
    print STDERR "fetch_sample_variant_hash: No variant and/or sample-sequence id provided\n";
    return {};
  }
  my $q    = "SELECT * FROM sample_variant WHERE sid = ? AND vid = ?";
  my $sth  = EASIH::DB::prepare($dbi, $q);
  return( EASIH::DB::fetch_hash( $dbi, $sth, $sid, $vid ));
}

# 
# 
# 
# Kim Brugger (20 Nov 2013)
sub fetch_sample_variants {
  my ($vid ) = @_;
  if ( ! $vid ) { 
    print STDERR "fetch_sample_variant_hash: No variant and/or sample-sequence id provided\n";
    return {};
  }
  my $q    = "SELECT * FROM sample_variant WHERE vid = ?";
  my $sth  = EASIH::DB::prepare($dbi, $q);
  return( EASIH::DB::fetch_array_hash( $dbi, $sth, $vid ));
}


# 
# 
# 
# Kim Brugger (20 Nov 2013)
sub update_sample_variant {
  my ($sid, $vid, $depth, $AAF, $quality, $allele_count) = @_;


  if ( ! $sid ) { 
    print STDERR "add_sample_variant: No sample id provided\n";
    return -1;
  }

  if ( ! $vid ) { 
    print STDERR "add_sample_variant: No variant id provided\n";
    return -2;
  }

  my $ss_name = fetch_sample_name($sid);
  if ( ! $ss_name  ) {
    print STDERR "add_sample_variant: Unknown sequence_sample-id $sid $ss_name\n";
    return -3;
  }

  my $v_hash = fetch_variant_hash($vid);
  if ( ! $v_hash || keys %{$v_hash} == 0) {
    print STDERR "add_sample_variant: Unknown variant-id $vid $v_hash\n";
    return -4;
  }

  my $sv_hash = fetch_sample_variant_hash( $sid, $vid );
  if ( !$sv_hash || keys %{$sv_hash} == 0 ) {
    print "update_sample_variant: unknown entry\n";
    return -5;
  }


  my %call_hash;
  $call_hash{sid}       = $sid    if ( $sid    );
  $call_hash{vid}        = $vid     if ( $vid     );
  $call_hash{depth}      = $depth   if ( $depth   );
  $call_hash{AAF}        = $AAF     if ( $AAF     );
  $call_hash{quality}    = $quality if ( $quality );
  $call_hash{allele_count} = $allele_count if ($allele_count);

  return (EASIH::DB::update($dbi, "sample_variant", \%call_hash, "sid", "vid"));
}


#================== Complex functions =========================


1;
