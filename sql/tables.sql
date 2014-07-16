drop database ExomeVCFdb;
create database ExomeVCFdb;
use ExomeVCFdb;




CREATE TABLE sample (

  sid                 INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  name                VARCHAR(80) NOT NULL,
  total_reads	      INT,
  mapped_reads	      INT,
  duplicate_reads     INT,
  mean_isize	      float,

  KEY sid_idx   (sid),
  KEY name_idx  (name)
);


CREATE TABLE variant (

  vid                 INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  chr                 VARCHAR(8) NOT NULL ,
  pos		      INT NOT NULL,
  ref                 VARCHAR(100) NOT NULL ,
  alt                 VARCHAR(100) NOT NULL ,
  comment	      VARCHAR(200),
  annotation	      VARCHAR(500),


  KEY pos_idx  (chr, pos, ref, alt)
);

CREATE TABLE sample_variant (

  sid                INT NOT NULL,
  vid                INT NOT NULL,

  depth		     INT,    
  AAF		     varchar(200),	  
  quality	     FLOAT,  
  allele_count	     INT NOT NULL,


  KEY sid_idx  (sid),
  KEY vid_idx  (vid)
);


