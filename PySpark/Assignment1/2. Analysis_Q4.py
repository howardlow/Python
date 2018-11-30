# STAT478-18S1 Assignment 1
# Howard Low, 53626262

#@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@
#======================================= ANALYSIS Q4 =============================================
#@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@

# ================================  Analysis Q4a)  Load all daily ==============================

check_all_daily = (
    spark.read.format("com.databricks.spark.csv")
    .option("header", "false")
    .option("inferSchema", "false")
    .option("codec", "gzip")
    .schema(schema_daily)
    .load("hdfs:///data/ghcnd/daily/*.csv.gz")
)

check_all_daily.count()


# ================================  Analysis Q4b)  Check which element has the highest count ==============================

#group elements and find the count for each elements for all daily records
check_all_daily.filter(check_all_daily.element.isin('PRCP','SNOW','SNWD','TMAX','TMIN')).groupby('element').agg(F.count(check_all_daily.element)).show()

#group elements and find the count for each elements for 2017 daily records
daily_station_2017.filter(daily_station_2017.element.isin('PRCP','SNOW','SNWD','TMAX','TMIN')).groupby('element').agg(F.count(daily_station_2017.element)).show()




# ================================  Analysis Q4c)   Determine how many observations of TMIN do not have a corresponding observation of TMAX  ==============================
#get all daily TMIN
all_daily_tmin = check_all_daily.filter(check_all_daily.element == 'TMIN').select('id','date')
#get all daily TMAX
all_daily_tmax = check_all_daily.filter(check_all_daily.element == 'TMAX').select('id','date')

#all daily with TMIN but without TMAX by all TMIN substract all TMAX
all_daily_with_tmin_without_tmax = all_daily_tmin.subtract(all_daily_tmax)
all_daily_with_tmin_without_tmax.cache()
all_daily_with_tmin_without_tmax.count()

#-- find unique stations with tmin and without tmax
stations_with_tmin_without_tmax = all_daily_with_tmin_without_tmax.select('id').distinct()
stations_with_tmin_without_tmax.cache()
stations_with_tmin_without_tmax.count()

#-- find if these stations are either gsn, hcn or crn
stations_with_tmin_without_tmax.registerTempTable('stations_with_tmin_without_tmax_tbl')
stations_with_tmin_without_tmax_sql = """
        SELECT
            stations_tbl.*        
        FROM 
            stations_with_tmin_without_tmax_tbl        
            JOIN stations_tbl on stations_with_tmin_without_tmax_tbl.id = stations_tbl.id
        WHERE
            stations_tbl.gsn_flag <> '' 
            OR stations_tbl.hcn_crn_flag <> ''
"""

stations_4c = spark.sql(stations_with_tmin_without_tmax_sql)
stations_4c.count()

# ================================  Analysis Q4d)  Filter daily to obtain all observations of TMIN and TMAX for all stations in New Zealand ==============================
check_all_daily.registerTempTable('check_all_daily_tbl')

#find out how many observations belong to NZ stations
nz_stations_daily_4d_sql = """
        SELECT
            check_all_daily_tbl.*        
        FROM 
            check_all_daily_tbl        
            JOIN stations_tbl on check_all_daily_tbl.id = stations_tbl.id
        WHERE
            stations_tbl.country_code = 'NZ' 
            AND check_all_daily_tbl.element IN ('TMIN', 'TMAX')
"""

nz_stations_daily_4d = spark.sql(nz_stations_daily_4d_sql)
nz_stations_daily_4d.cache()
nz_stations_daily_4d.count()

#create a new column year and calculate the distinct number of years
nz_stations_daily_4d = nz_stations_daily_4d.withColumn('year', nz_stations_daily_4d['date'].substr(0,4))
nz_stations_daily_4d.select('year').distinct().count()

nz_stations_daily_4d.write.csv("hdfs:///user/zhl15/outputs/ghcnd/daily/4d/")

# ================================  Analysis Q4e)  Compute the average rainfall in each year for each country ==============================  

#compute the average total rainfall for each day and multiply by 365 days to get total average rainfall for a year by each country
country_stations_prcp_avg_annual_total_sql = """
        SELECT
            stations_tbl.country_code,
            stations_tbl.country_name,           
            substr(check_all_daily_tbl.date,0,4) as year,
            AVG(check_all_daily_tbl.value) * 365 as average_annual
        FROM 
            check_all_daily_tbl        
            JOIN stations_tbl on check_all_daily_tbl.id = stations_tbl.id
        WHERE
            check_all_daily_tbl.element = 'PRCP'
        GROUP BY 
            stations_tbl.country_code,
            stations_tbl.country_name,          
            substr(check_all_daily_tbl.date,0,4)
        ORDER BY
            AVG(check_all_daily_tbl.value) * 365 DESC
"""

country_stations_prcp_avg_annual_total = spark.sql(country_stations_prcp_avg_annual_total_sql)
country_stations_prcp_avg_annual_total.cache()
country_stations_prcp_avg_annual_total.show()

country_stations_prcp_avg_annual_total.write.csv("hdfs:///user/zhl15/outputs/ghcnd/daily/4e/")