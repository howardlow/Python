# STAT478-18S1 Assignment 1
# Howard Low, 53626262

#@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@
#======================================= PROCESSING Q1 ===========================================
#@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@

#Q1a) print directory tree to represent hdfs:///data/ghcnd 
hdfs dfs -ls -R hdfs:/data/ghcnd | awk '{print $8}' | sed -e 's/[^-][^\/]*\//--/g' -e 's/^/ /' -e 's/-/|/'

#Q1b) to check how many files inside daily so as to determine how many years of record
hdfs dfs -ls hdfs:/data/ghcnd/daily/*.csv.gz | wc -l

#Q1c) to check total size of all of the data
hdfs dfs -du -s hdfs:/data/ghcnd

#Q1c) to check total size of all daily
hdfs dfs -du -s hdfs:/data/ghcnd/daily

# Peek at the top of each data file to check the schema is as described
hdfs dfs -cat /data/shared/ghcnd/countries | head
hdfs dfs -cat /data/shared/ghcnd/inventory | head
hdfs dfs -cat /data/shared/ghcnd/states | head
hdfs dfs -cat /data/shared/ghcnd/stations | head

hdfs dfs -cat /data/shared/ghcnd/daily/2017.csv.gz | gunzip | head