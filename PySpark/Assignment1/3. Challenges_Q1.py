# STAT478-18S1 Assignment 1
# Howard Low, 53626262

#@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@
#======================================= CHALLENGES Q1 ===========================================
#@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@

# ================================  Challenges Q1 Investigate Quality Flag and Other Elements ==============================  
# to check the date and counts where the quality flag is not empty
check_daily_2017.filter(check_daily_2017.quality_flag != '').groupby("date").agg(F.count(check_daily_2017.date)).show()

# to check which elements has the highest and lowest count
check_daily_2017.groupby("element").agg(F.count(check_daily_2017.element).alias("count")).orderBy("count",ascending=False).show(70)

#to join daily 2017 records with stations
check_daily_2017.registerTempTable('check_daily_2017_tbl')
daily_station_2017_sql = """
        SELECT
            stations_tbl.*,
            check_daily_2017_tbl.element,
            check_daily_2017_tbl.value            
        FROM 
            check_daily_2017_tbl        
            LEFT JOIN stations_tbl ON check_daily_2017_tbl.id = stations_tbl.id
"""

#investigate the stations which contains element WT17 and DASF
daily_station_2017 = spark.sql(daily_station_2017_sql)
daily_station_2017.cache()
daily_station_2017.filter((daily_station_2017.element == 'WT17') | (daily_station_2017.element == 'DASF')).show()
