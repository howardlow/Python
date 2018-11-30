# STAT478-18S1 Assignment 1
# Howard Low, 53626262

#@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@
#======================================= ANALYSIS Q3a ============================================
#@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@

#to determine the block size of 2017.csv.gz
hdfs fsck hdfs:/data/ghcnd/daily/2017.csv.gz -blocks

#to determine the block size of 2010.csv.gz
hdfs fsck hdfs:/data/ghcnd/daily/2010.csv.gz -blocks
