# STAT478-18S1 Assignment 1
# Howard Low, 53626262

#@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@
#======================================= ANALYSIS Q1 =============================================
#@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@

# ================================  Analysis Q1a) To understand more about stations  ==============================
#to find out how many stations
stations.count()

#to find out how many station are active in year 2017
stations.filter((stations.min_firstyear == 2017) | (stations.max_lastyear == 2017)).count()

#count how many stations is GSN, HCN or CRN
stations.filter(stations.gsn_flag == 'GSN').count()
stations.filter(stations.hcn_crn_flag == 'HCN').count()
stations.filter(stations.hcn_crn_flag == 'CRN').count()

#to find how many stations with more than one of these networks.
stations.filter((stations.gsn_flag != '') & (stations.hcn_crn_flag != '')).count()

# ================================  Analysis Q1b) To find out how many stations by countries and state are populated with the number of stations  ==============================
# to find out how many stations by country
station_country_count = stations.groupby("country_code").agg(F.count(stations.id).alias("num_of_stations"))
station_country_count.registerTempTable('station_country_count_tbl')

country_sql = """
        SELECT
            country_tbl.*,
            station_country_count_tbl.num_of_stations
        FROM 
            country_tbl        
            LEFT JOIN station_country_count_tbl ON country_tbl.code = station_country_count_tbl.country_code
"""

country = spark.sql(country_sql)
country = country.fillna(0)
country.show()

#write to csv
country.write.csv("hdfs:///user/zhl15/outputs/ghcnd/countries/")

# to find out how many stations by state
station_state_count = stations.groupby("state").agg(F.count(stations.id).alias("num_of_stations"))
station_state_count.registerTempTable('station_state_count_tbl')

# to include number of stations to state meta data
state_sql = """
        SELECT
            state_tbl.*,
            station_state_count_tbl.num_of_stations
        FROM 
            state_tbl        
            LEFT JOIN station_state_count_tbl ON state_tbl.code = station_state_count_tbl.state
"""

state = spark.sql(state_sql)

#fill all NULL value with 0
state = state.fillna(0)
state.show()

#write to csv file
state.write.csv("hdfs:///user/zhl15/outputs/ghcnd/states/")

# ================================  Analysis Q1c) To find out many stations in southern hemisphere and territories of US  ==============================
#stations in southern hemisphere
stations.filter(stations.latitude < 0).count()

#stations in  territories of US 
stations.filter(stations.country_code != 'US').select('country_name').where("country_name like '%United States%'").count()
stations.filter(stations.country_code == 'US').count()