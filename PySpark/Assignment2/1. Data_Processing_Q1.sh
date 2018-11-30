# STAT478-18S1 Assignment 2
# Howard Low, 53626262

#*********************************** Q1a ***********************************

#@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@ Q1 @@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@

#============================ File Structure ==========================================
#Audio

#*********************************** Q1a ***********************************
hdfs dfs -ls -R hdfs:/data/msd/audio | awk '{print $8}' | sed -e 's/[^-][^\/]*\//--/g' -e 's/^/ /' -e 's/-/|/'
<<'COMMENT'
 |------audio
	 |---------attributes
		 |-----------msd-jmir-area-of-moments-all-v1.0.attributes.csv
		 |-----------msd-jmir-lpc-all-v1.0.attributes.csv
		 |-----------msd-jmir-methods-of-moments-all-v1.0.attributes.csv
		 |-----------msd-jmir-mfcc-all-v1.0.attributes.csv
		 |-----------msd-jmir-spectral-all-all-v1.0.attributes.csv
		 |-----------msd-jmir-spectral-derivatives-all-all-v1.0.attributes.csv
		 |-----------msd-marsyas-timbral-v1.0.attributes.csv
		 |-----------msd-mvd-v1.0.attributes.csv
		 |-----------msd-rh-v1.0.attributes.csv
		 |-----------msd-rp-v1.0.attributes.csv
		 |-----------msd-ssd-v1.0.attributes.csv
		 |-----------msd-trh-v1.0.attributes.csv
		 |-----------msd-tssd-v1.0.attributes.csv
	 |---------features
		 |-----------msd-jmir-area-of-moments-all-v1.0.csv
			 |-------------part-00000.csv.gz
			 |-------------part-00001.csv.gz
			 |-------------part-00002.csv.gz
			 |-------------part-00003.csv.gz
			 |-------------part-00004.csv.gz
			 |-------------part-00005.csv.gz
			 |-------------part-00006.csv.gz
			 |-------------part-00007.csv.gz
		 |-----------msd-jmir-lpc-all-v1.0.csv
			 |-------------part-00000.csv.gz
			 |-------------part-00001.csv.gz
			 |-------------part-00002.csv.gz
			 |-------------part-00003.csv.gz
			 |-------------part-00004.csv.gz
			 |-------------part-00005.csv.gz
			 |-------------part-00006.csv.gz
			 |-------------part-00007.csv.gz
		 |-----------msd-jmir-methods-of-moments-all-v1.0.csv
			 |-------------part-00000.csv.gz
			 |-------------part-00001.csv.gz
			 |-------------part-00002.csv.gz
			 |-------------part-00003.csv.gz
			 |-------------part-00004.csv.gz
			 |-------------part-00005.csv.gz
			 |-------------part-00006.csv.gz
			 |-------------part-00007.csv.gz
		 |-----------msd-jmir-mfcc-all-v1.0.csv
			 |-------------part-00000.csv.gz
			 |-------------part-00001.csv.gz
			 |-------------part-00002.csv.gz
			 |-------------part-00003.csv.gz
			 |-------------part-00004.csv.gz
			 |-------------part-00005.csv.gz
			 |-------------part-00006.csv.gz
			 |-------------part-00007.csv.gz
		 |-----------msd-jmir-spectral-all-all-v1.0.csv
			 |-------------part-00000.csv.gz
			 |-------------part-00001.csv.gz
			 |-------------part-00002.csv.gz
			 |-------------part-00003.csv.gz
			 |-------------part-00004.csv.gz
			 |-------------part-00005.csv.gz
			 |-------------part-00006.csv.gz
			 |-------------part-00007.csv.gz
		 |-----------msd-jmir-spectral-derivatives-all-all-v1.0.csv
			 |-------------part-00000.csv.gz
			 |-------------part-00001.csv.gz
			 |-------------part-00002.csv.gz
			 |-------------part-00003.csv.gz
			 |-------------part-00004.csv.gz
			 |-------------part-00005.csv.gz
			 |-------------part-00006.csv.gz
			 |-------------part-00007.csv.gz
		 |-----------msd-marsyas-timbral-v1.0.csv
			 |-------------part-00000.csv.gz
			 |-------------part-00001.csv.gz
			 |-------------part-00002.csv.gz
			 |-------------part-00003.csv.gz
			 |-------------part-00004.csv.gz
			 |-------------part-00005.csv.gz
			 |-------------part-00006.csv.gz
			 |-------------part-00007.csv.gz
		 |-----------msd-mvd-v1.0.csv
			 |-------------part-00000.csv.gz
			 |-------------part-00001.csv.gz
			 |-------------part-00002.csv.gz
			 |-------------part-00003.csv.gz
			 |-------------part-00004.csv.gz
			 |-------------part-00005.csv.gz
			 |-------------part-00006.csv.gz
			 |-------------part-00007.csv.gz
		 |-----------msd-rh-v1.0.csv
			 |-------------part-00000.csv.gz
			 |-------------part-00001.csv.gz
			 |-------------part-00002.csv.gz
			 |-------------part-00003.csv.gz
			 |-------------part-00004.csv.gz
			 |-------------part-00005.csv.gz
			 |-------------part-00006.csv.gz
			 |-------------part-00007.csv.gz
		 |-----------msd-rp-v1.0.csv
			 |-------------part-00000.csv.gz
			 |-------------part-00001.csv.gz
			 |-------------part-00002.csv.gz
			 |-------------part-00003.csv.gz
			 |-------------part-00004.csv.gz
			 |-------------part-00005.csv.gz
			 |-------------part-00006.csv.gz
			 |-------------part-00007.csv.gz
		 |-----------msd-ssd-v1.0.csv
			 |-------------part-00000.csv.gz
			 |-------------part-00001.csv.gz
			 |-------------part-00002.csv.gz
			 |-------------part-00003.csv.gz
			 |-------------part-00004.csv.gz
			 |-------------part-00005.csv.gz
			 |-------------part-00006.csv.gz
			 |-------------part-00007.csv.gz
		 |-----------msd-trh-v1.0.csv
			 |-------------part-00000.csv.gz
			 |-------------part-00001.csv.gz
			 |-------------part-00002.csv.gz
			 |-------------part-00003.csv.gz
			 |-------------part-00004.csv.gz
			 |-------------part-00005.csv.gz
			 |-------------part-00006.csv.gz
			 |-------------part-00007.csv.gz
		 |-----------msd-tssd-v1.0.csv
			 |-------------part-00000.csv.gz
			 |-------------part-00001.csv.gz
			 |-------------part-00002.csv.gz
			 |-------------part-00003.csv.gz
			 |-------------part-00004.csv.gz
			 |-------------part-00005.csv.gz
			 |-------------part-00006.csv.gz
			 |-------------part-00007.csv.gz
		 |---------statistics
		 	 |-----------sample_properties.csv.gz
COMMENT

#Genre
hdfs dfs -ls -R hdfs:/data/msd/genre | awk '{print $8}' | sed -e 's/[^-][^\/]*\//--/g' -e 's/^/ /' -e 's/-/|/'
<<'COMMENT'
 |------genre
	 |---------msd-MAGD-genreAssignment.tsv
	 |---------msd-MASD-styleAssignment.tsv
	 |---------msd-topMAGD-genreAssignment.tsv
COMMENT

#Main
hdfs dfs -ls -R hdfs:/data/msd/main | awk '{print $8}' | sed -e 's/[^-][^\/]*\//--/g' -e 's/^/ /' -e 's/-/|/'
<<'COMMENT'
 |------main
	 |---------summary
		 |-----------analysis.csv.gz
		 |-----------metadata.csv.gz
COMMENT

 #Taste Profile
 hdfs dfs -ls -R hdfs:/data/msd/tasteprofile | awk '{print $8}' | sed -e 's/[^-][^\/]*\//--/g' -e 's/^/ /' -e 's/-/|/'
<<'COMMENT'
 |------tasteprofile
	 |---------mismatches
	 	|-----------sid_matches_manually_accepted.txt
	 |-----------sid_mismatches.txt
	 	|---------triplets.tsv
			 |-----------part-00000.tsv.gz
			 |-----------part-00001.tsv.gz
			 |-----------part-00002.tsv.gz
			 |-----------part-00003.tsv.gz
			 |-----------part-00004.tsv.gz
			 |-----------part-00005.tsv.gz
			 |-----------part-00006.tsv.gz
			 |-----------part-00007.tsv.gz
COMMENT


#============================ No of Partitions  ==========================================

#check no. of blocks

#Audio
hdfs fsck hdfs:/data/msd/audio/attributes -blocks
<<'COMMENT'
Replicated Blocks:
 Total size:    105513 B
 Total files:   13
 Total blocks (validated):      13 (avg. block size 8116 B)
 Minimally replicated blocks:   13 (100.0 %)
 Over-replicated blocks:        0 (0.0 %)
 Under-replicated blocks:       0 (0.0 %)
 Mis-replicated blocks:         0 (0.0 %)
 Default replication factor:    8
 Average block replication:     8.0
 Missing blocks:                0
 Corrupt blocks:                0
 Missing replicas:              0 (0.0 %)
COMMENT

hdfs fsck hdfs:/data/msd/audio/features -blocks
<<'COMMENT'
Replicated Blocks:
 Total size:    13125542239 B
 Total files:   104
 Total blocks (validated):      175 (avg. block size 75003098 B)
 Minimally replicated blocks:   175 (100.0 %)
 Over-replicated blocks:        0 (0.0 %)
 Under-replicated blocks:       0 (0.0 %)
 Mis-replicated blocks:         0 (0.0 %)
 Default replication factor:    8
 Average block replication:     8.0
 Missing blocks:                0
 Corrupt blocks:                0
 Missing replicas:              0 (0.0 %)
COMMENT

hdfs fsck hdfs:/data/msd/audio/statistics -blocks
<<'COMMENT'
Replicated Blocks:
 Total size:    42224669 B
 Total files:   1
 Total blocks (validated):      1 (avg. block size 42224669 B)
 Minimally replicated blocks:   1 (100.0 %)
 Over-replicated blocks:        0 (0.0 %)
 Under-replicated blocks:       0 (0.0 %)
 Mis-replicated blocks:         0 (0.0 %)
 Default replication factor:    8
 Average block replication:     8.0
 Missing blocks:                0
 Corrupt blocks:                0
 Missing replicas:              0 (0.0 %)
COMMENT

#Genre
hdfs fsck hdfs:/data/msd/genre/msd-MAGD-genreAssignment.tsv -blocks
<<'COMMENT'
Replicated Blocks:
 Total size:    11625230 B
 Total files:   1
 Total blocks (validated):      1 (avg. block size 11625230 B)
 Minimally replicated blocks:   1 (100.0 %)
 Over-replicated blocks:        0 (0.0 %)
 Under-replicated blocks:       0 (0.0 %)
 Mis-replicated blocks:         0 (0.0 %)
 Default replication factor:    8
 Average block replication:     8.0
 Missing blocks:                0
 Corrupt blocks:                0
 Missing replicas:              0 (0.0 %)
COMMENT

hdfs fsck hdfs:/data/msd/genre/msd-MASD-styleAssignment.tsv -blocks
<<'COMMENT'
Replicated Blocks:
 Total size:    8820054 B
 Total files:   1
 Total blocks (validated):      1 (avg. block size 8820054 B)
 Minimally replicated blocks:   1 (100.0 %)
 Over-replicated blocks:        0 (0.0 %)
 Under-replicated blocks:       0 (0.0 %)
 Mis-replicated blocks:         0 (0.0 %)
 Default replication factor:    8
 Average block replication:     8.0
 Missing blocks:                0
 Corrupt blocks:                0
 Missing replicas:              0 (0.0 %)
COMMENT

hdfs fsck hdfs:/data/msd/genre/msd-topMAGD-genreAssignment.tsv -blocks
<<'COMMENT'
Replicated Blocks:
 Total size:    11140605 B
 Total files:   1
 Total blocks (validated):      1 (avg. block size 11140605 B)
 Minimally replicated blocks:   1 (100.0 %)
 Over-replicated blocks:        0 (0.0 %)
 Under-replicated blocks:       0 (0.0 %)
 Mis-replicated blocks:         0 (0.0 %)
 Default replication factor:    8
 Average block replication:     8.0
 Missing blocks:                0
 Corrupt blocks:                0
 Missing replicas:              0 (0.0 %)
COMMENT

#Main
hdfs fsck hdfs:/data/msd/main/summary/analysis.csv.gz -blocks
<<'COMMENT'
Replicated Blocks:
 Total size:    58658141 B
 Total files:   1
 Total blocks (validated):      1 (avg. block size 58658141 B)
 Minimally replicated blocks:   1 (100.0 %)
 Over-replicated blocks:        0 (0.0 %)
 Under-replicated blocks:       0 (0.0 %)
 Mis-replicated blocks:         0 (0.0 %)
 Default replication factor:    8
 Average block replication:     8.0
 Missing blocks:                0
 Corrupt blocks:                0
 Missing replicas:              0 (0.0 %)
COMMENT

hdfs fsck hdfs:/data/msd/main/summary/metadata.csv.gz -blocks
<<'COMMENT'
Replicated Blocks:
 Total size:    124211304 B
 Total files:   1
 Total blocks (validated):      1 (avg. block size 124211304 B)
 Minimally replicated blocks:   1 (100.0 %)
 Over-replicated blocks:        0 (0.0 %)
 Under-replicated blocks:       0 (0.0 %)
 Mis-replicated blocks:         0 (0.0 %)
 Default replication factor:    8
 Average block replication:     8.0
 Missing blocks:                0
 Corrupt blocks:                0
 Missing replicas:              0 (0.0 %)
COMMENT

#Taste Profile
<<'COMMENT'
hdfs fsck hdfs:/data/msd/tasteprofile/mismatches -blocks
Replicated Blocks:
 Total size:    2117524 B
 Total files:   2
 Total blocks (validated):      2 (avg. block size 1058762 B)
 Minimally replicated blocks:   2 (100.0 %)
 Over-replicated blocks:        0 (0.0 %)
 Under-replicated blocks:       0 (0.0 %)
 Mis-replicated blocks:         0 (0.0 %)
 Default replication factor:    8
 Average block replication:     8.0
 Missing blocks:                0
 Corrupt blocks:                0
 Missing replicas:              0 (0.0 %)
COMMENT

hdfs fsck hdfs:/data/msd/tasteprofile/triplets.tsv -blocks
<<'COMMENT'
Replicated Blocks:
 Total size:    512139195 B
 Total files:   8
 Total blocks (validated):      8 (avg. block size 64017399 B)
 Minimally replicated blocks:   8 (100.0 %)
 Over-replicated blocks:        0 (0.0 %)
 Under-replicated blocks:       0 (0.0 %)
 Mis-replicated blocks:         0 (0.0 %)
 Default replication factor:    8
 Average block replication:     8.0
 Missing blocks:                0
 Corrupt blocks:                0
 Missing replicas:              0 (0.0 %)
COMMENT




#*********************************** Q1c ***********************************

PATHS=`hdfs dfs -find /data/msd/`
for i in $PATHS; do
  echo $i ':' `hdfs dfs -cat $i | wc -l` >> line_count.txt;
done
<<'COMMENT'
/data/msd/audio/attributes/msd-jmir-area-of-moments-all-v1.0.attributes.csv : 21
/data/msd/audio/attributes/msd-jmir-lpc-all-v1.0.attributes.csv : 21
/data/msd/audio/attributes/msd-jmir-methods-of-moments-all-v1.0.attributes.csv : 11
/data/msd/audio/attributes/msd-jmir-mfcc-all-v1.0.attributes.csv : 27
/data/msd/audio/attributes/msd-jmir-spectral-all-all-v1.0.attributes.csv : 17
/data/msd/audio/attributes/msd-jmir-spectral-derivatives-all-all-v1.0.attributes.csv : 17
/data/msd/audio/attributes/msd-marsyas-timbral-v1.0.attributes.csv : 125
/data/msd/audio/attributes/msd-mvd-v1.0.attributes.csv : 421
/data/msd/audio/attributes/msd-rh-v1.0.attributes.csv : 61
/data/msd/audio/attributes/msd-rp-v1.0.attributes.csv : 1441
/data/msd/audio/attributes/msd-ssd-v1.0.attributes.csv : 169
/data/msd/audio/attributes/msd-trh-v1.0.attributes.csv : 421
/data/msd/audio/attributes/msd-tssd-v1.0.attributes.csv : 1177

/data/msd/audio/features/msd-jmir-area-of-moments-all-v1.0.csv/part-00000.csv.gz : 33417
/data/msd/audio/features/msd-jmir-area-of-moments-all-v1.0.csv/part-00001.csv.gz : 33066
/data/msd/audio/features/msd-jmir-area-of-moments-all-v1.0.csv/part-00002.csv.gz : 33453
/data/msd/audio/features/msd-jmir-area-of-moments-all-v1.0.csv/part-00003.csv.gz : 33952
/data/msd/audio/features/msd-jmir-area-of-moments-all-v1.0.csv/part-00004.csv.gz : 33486
/data/msd/audio/features/msd-jmir-area-of-moments-all-v1.0.csv/part-00005.csv.gz : 33569
/data/msd/audio/features/msd-jmir-area-of-moments-all-v1.0.csv/part-00006.csv.gz : 33561
/data/msd/audio/features/msd-jmir-area-of-moments-all-v1.0.csv/part-00007.csv.gz : 31719

/data/msd/audio/features/msd-jmir-lpc-all-v1.0.csv/part-00000.csv.gz : 25932
/data/msd/audio/features/msd-jmir-lpc-all-v1.0.csv/part-00001.csv.gz : 25687
/data/msd/audio/features/msd-jmir-lpc-all-v1.0.csv/part-00002.csv.gz : 25395
/data/msd/audio/features/msd-jmir-lpc-all-v1.0.csv/part-00003.csv.gz : 25783
/data/msd/audio/features/msd-jmir-lpc-all-v1.0.csv/part-00004.csv.gz : 25564
/data/msd/audio/features/msd-jmir-lpc-all-v1.0.csv/part-00005.csv.gz : 25546
/data/msd/audio/features/msd-jmir-lpc-all-v1.0.csv/part-00006.csv.gz : 25240
/data/msd/audio/features/msd-jmir-lpc-all-v1.0.csv/part-00007.csv.gz : 24387

/data/msd/audio/features/msd-jmir-methods-of-moments-all-v1.0.csv/part-00000.csv.gz : 19691
/data/msd/audio/features/msd-jmir-methods-of-moments-all-v1.0.csv/part-00001.csv.gz : 19811
/data/msd/audio/features/msd-jmir-methods-of-moments-all-v1.0.csv/part-00002.csv.gz : 19886
/data/msd/audio/features/msd-jmir-methods-of-moments-all-v1.0.csv/part-00003.csv.gz : 19314
/data/msd/audio/features/msd-jmir-methods-of-moments-all-v1.0.csv/part-00004.csv.gz : 19535
/data/msd/audio/features/msd-jmir-methods-of-moments-all-v1.0.csv/part-00005.csv.gz : 19859
/data/msd/audio/features/msd-jmir-methods-of-moments-all-v1.0.csv/part-00006.csv.gz : 19527
/data/msd/audio/features/msd-jmir-methods-of-moments-all-v1.0.csv/part-00007.csv.gz : 19099

/data/msd/audio/features/msd-jmir-mfcc-all-v1.0.csv/part-00000.csv.gz : 32472
/data/msd/audio/features/msd-jmir-mfcc-all-v1.0.csv/part-00001.csv.gz : 32614
/data/msd/audio/features/msd-jmir-mfcc-all-v1.0.csv/part-00002.csv.gz : 32735
/data/msd/audio/features/msd-jmir-mfcc-all-v1.0.csv/part-00003.csv.gz : 32464
/data/msd/audio/features/msd-jmir-mfcc-all-v1.0.csv/part-00004.csv.gz : 32675
/data/msd/audio/features/msd-jmir-mfcc-all-v1.0.csv/part-00005.csv.gz : 32563
/data/msd/audio/features/msd-jmir-mfcc-all-v1.0.csv/part-00006.csv.gz : 32333
/data/msd/audio/features/msd-jmir-mfcc-all-v1.0.csv/part-00007.csv.gz : 30973

/data/msd/audio/features/msd-jmir-spectral-all-all-v1.0.csv/part-00000.csv.gz : 24959
/data/msd/audio/features/msd-jmir-spectral-all-all-v1.0.csv/part-00001.csv.gz : 25191
/data/msd/audio/features/msd-jmir-spectral-all-all-v1.0.csv/part-00002.csv.gz : 25480
/data/msd/audio/features/msd-jmir-spectral-all-all-v1.0.csv/part-00003.csv.gz : 24958
/data/msd/audio/features/msd-jmir-spectral-all-all-v1.0.csv/part-00004.csv.gz : 25380
/data/msd/audio/features/msd-jmir-spectral-all-all-v1.0.csv/part-00005.csv.gz : 25086
/data/msd/audio/features/msd-jmir-spectral-all-all-v1.0.csv/part-00006.csv.gz : 25176
/data/msd/audio/features/msd-jmir-spectral-all-all-v1.0.csv/part-00007.csv.gz : 24206

/data/msd/audio/features/msd-jmir-spectral-derivatives-all-all-v1.0.csv/part-00000.csv.gz : 24959
/data/msd/audio/features/msd-jmir-spectral-derivatives-all-all-v1.0.csv/part-00001.csv.gz : 25191
/data/msd/audio/features/msd-jmir-spectral-derivatives-all-all-v1.0.csv/part-00002.csv.gz : 25480
/data/msd/audio/features/msd-jmir-spectral-derivatives-all-all-v1.0.csv/part-00003.csv.gz : 24958
/data/msd/audio/features/msd-jmir-spectral-derivatives-all-all-v1.0.csv/part-00004.csv.gz : 25380
/data/msd/audio/features/msd-jmir-spectral-derivatives-all-all-v1.0.csv/part-00005.csv.gz : 25086
/data/msd/audio/features/msd-jmir-spectral-derivatives-all-all-v1.0.csv/part-00006.csv.gz : 25176
/data/msd/audio/features/msd-jmir-spectral-derivatives-all-all-v1.0.csv/part-00007.csv.gz : 24206

/data/msd/audio/features/msd-marsyas-timbral-v1.0.csv/part-00000.csv.gz : 211585
/data/msd/audio/features/msd-marsyas-timbral-v1.0.csv/part-00001.csv.gz : 211830
/data/msd/audio/features/msd-marsyas-timbral-v1.0.csv/part-00002.csv.gz : 212396
/data/msd/audio/features/msd-marsyas-timbral-v1.0.csv/part-00003.csv.gz : 210986
/data/msd/audio/features/msd-marsyas-timbral-v1.0.csv/part-00004.csv.gz : 211635
/data/msd/audio/features/msd-marsyas-timbral-v1.0.csv/part-00005.csv.gz : 211093
/data/msd/audio/features/msd-marsyas-timbral-v1.0.csv/part-00006.csv.gz : 211955
/data/msd/audio/features/msd-marsyas-timbral-v1.0.csv/part-00007.csv.gz : 203682

/data/msd/audio/features/msd-mvd-v1.0.csv/part-00000.csv.gz : 663899
/data/msd/audio/features/msd-mvd-v1.0.csv/part-00001.csv.gz : 665675
/data/msd/audio/features/msd-mvd-v1.0.csv/part-00002.csv.gz : 663516
/data/msd/audio/features/msd-mvd-v1.0.csv/part-00003.csv.gz : 663391
/data/msd/audio/features/msd-mvd-v1.0.csv/part-00004.csv.gz : 662462
/data/msd/audio/features/msd-mvd-v1.0.csv/part-00005.csv.gz : 663993
/data/msd/audio/features/msd-mvd-v1.0.csv/part-00006.csv.gz : 663818
/data/msd/audio/features/msd-mvd-v1.0.csv/part-00007.csv.gz : 633908

/data/msd/audio/features/msd-rh-v1.0.csv/part-00000.csv.gz : 113134
/data/msd/audio/features/msd-rh-v1.0.csv/part-00001.csv.gz : 112560
/data/msd/audio/features/msd-rh-v1.0.csv/part-00002.csv.gz : 112892
/data/msd/audio/features/msd-rh-v1.0.csv/part-00003.csv.gz : 113354
/data/msd/audio/features/msd-rh-v1.0.csv/part-00004.csv.gz : 113032
/data/msd/audio/features/msd-rh-v1.0.csv/part-00005.csv.gz : 112661
/data/msd/audio/features/msd-rh-v1.0.csv/part-00006.csv.gz : 113230
/data/msd/audio/features/msd-rh-v1.0.csv/part-00007.csv.gz : 108668

/data/msd/audio/features/msd-rp-v1.0.csv/part-00000.csv.gz : 2179745
/data/msd/audio/features/msd-rp-v1.0.csv/part-00001.csv.gz : 2181398
/data/msd/audio/features/msd-rp-v1.0.csv/part-00002.csv.gz : 2181997
/data/msd/audio/features/msd-rp-v1.0.csv/part-00003.csv.gz : 2185657
/data/msd/audio/features/msd-rp-v1.0.csv/part-00004.csv.gz : 2184739
/data/msd/audio/features/msd-rp-v1.0.csv/part-00005.csv.gz : 2183239
/data/msd/audio/features/msd-rp-v1.0.csv/part-00006.csv.gz : 2181977
/data/msd/audio/features/msd-rp-v1.0.csv/part-00007.csv.gz : 2081171

/data/msd/audio/features/msd-ssd-v1.0.csv/part-00000.csv.gz : 286349
/data/msd/audio/features/msd-ssd-v1.0.csv/part-00001.csv.gz : 285764
/data/msd/audio/features/msd-ssd-v1.0.csv/part-00002.csv.gz : 284379
/data/msd/audio/features/msd-ssd-v1.0.csv/part-00003.csv.gz : 284436
/data/msd/audio/features/msd-ssd-v1.0.csv/part-00004.csv.gz : 285610
/data/msd/audio/features/msd-ssd-v1.0.csv/part-00005.csv.gz : 286054
/data/msd/audio/features/msd-ssd-v1.0.csv/part-00006.csv.gz : 285284
/data/msd/audio/features/msd-ssd-v1.0.csv/part-00007.csv.gz : 272151

/data/msd/audio/features/msd-trh-v1.0.csv/part-00000.csv.gz : 675893
/data/msd/audio/features/msd-trh-v1.0.csv/part-00001.csv.gz : 676897
/data/msd/audio/features/msd-trh-v1.0.csv/part-00002.csv.gz : 675325
/data/msd/audio/features/msd-trh-v1.0.csv/part-00003.csv.gz : 675019
/data/msd/audio/features/msd-trh-v1.0.csv/part-00004.csv.gz : 675434
/data/msd/audio/features/msd-trh-v1.0.csv/part-00005.csv.gz : 676747
/data/msd/audio/features/msd-trh-v1.0.csv/part-00006.csv.gz : 675558
/data/msd/audio/features/msd-trh-v1.0.csv/part-00007.csv.gz : 643112

/data/msd/audio/features/msd-tssd-v1.0.csv/part-00000.csv.gz : 1860302
/data/msd/audio/features/msd-tssd-v1.0.csv/part-00001.csv.gz : 1862402
/data/msd/audio/features/msd-tssd-v1.0.csv/part-00002.csv.gz : 1862556
/data/msd/audio/features/msd-tssd-v1.0.csv/part-00003.csv.gz : 1860987
/data/msd/audio/features/msd-tssd-v1.0.csv/part-00004.csv.gz : 1863542
/data/msd/audio/features/msd-tssd-v1.0.csv/part-00005.csv.gz : 1862813
/data/msd/audio/features/msd-tssd-v1.0.csv/part-00006.csv.gz : 1861818
/data/msd/audio/features/msd-tssd-v1.0.csv/part-00007.csv.gz : 1776530

/data/msd/audio/statistics/sample_properties.csv.gz : 183262

/data/msd/genre/msd-MAGD-genreAssignment.tsv : 422714
/data/msd/genre/msd-MASD-styleAssignment.tsv : 273936
/data/msd/genre/msd-topMAGD-genreAssignment.tsv : 406427

/data/msd/main/summary/analysis.csv.gz : 239762
/data/msd/main/summary/metadata.csv.gz : 480546

/data/msd/tasteprofile/mismatches/sid_matches_manually_accepted.txt : 938
/data/msd/tasteprofile/mismatches/sid_mismatches.txt : 19094

/data/msd/tasteprofile/triplets.tsv/part-00000.tsv.gz : 210041
/data/msd/tasteprofile/triplets.tsv/part-00001.tsv.gz : 209199
/data/msd/tasteprofile/triplets.tsv/part-00002.tsv.gz : 208896
/data/msd/tasteprofile/triplets.tsv/part-00003.tsv.gz : 209050
/data/msd/tasteprofile/triplets.tsv/part-00004.tsv.gz : 209315
/data/msd/tasteprofile/triplets.tsv/part-00005.tsv.gz : 210353
/data/msd/tasteprofile/triplets.tsv/part-00006.tsv.gz : 209326
/data/msd/tasteprofile/triplets.tsv/part-00007.tsv.gz : 208540
COMMENT