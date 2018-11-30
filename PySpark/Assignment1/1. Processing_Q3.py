# STAT478-18S1 Assignment 1
# Howard Low, 53626262

#@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@
#======================================= PROCESSING Q3 ===========================================
#@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@

# ================================  Processing Q3a) Extract the two character country code from each station code and create a new column  ==============================
station = station.withColumn('country_code',
        F.substring('id', 0, 2)
    )


# ================================  Processing Q3b) LEFT JOIN stations with countries ==============================
station.registerTempTable('station_tbl')
country.registerTempTable('country_tbl')
state.registerTempTable('state_tbl')
inventory.registerTempTable('inventory_tbl')

station_sql = """
        SELECT
            station_tbl.*,
            country_tbl.name as country_name
        FROM 
            station_tbl        
            LEFT JOIN country_tbl ON station_tbl.country_code = country_tbl.code
"""


stations = spark.sql(station_sql)
stations.show()

# ================================  Processing Q3c) LEFT JOIN stations with states ==============================
station_sql = """
        SELECT
            station_tbl.*,
            country_tbl.name as country_name,
            state_tbl.name as state_name
        FROM 
            station_tbl        
            LEFT JOIN country_tbl ON station_tbl.country_code = country_tbl.code
            LEFT JOIN state_tbl ON station_tbl.state = state_tbl.code
"""

stations = spark.sql(station_sql)
stations.show()

# ================================  Processing Q3d) Data filtering on inventory ==============================
# -- To find out what was the ﬁrst and last year that each station was active and collected any element at all
inventory_year = inventory.groupby("id").agg(F.min("firstyear").alias("min_firstyear"), F.max("lastyear").alias("max_lastyear"))
inventory_year.show()

# -- To find out how many different elements has each station collected overall
inventory_count_all_elements = inventory.groupby("id").agg(F.countDistinct(inventory.element).alias("num_all_elements"))
inventory_count_all_elements.show()

# --  To count separately the number of core elements that each station has collected overall
inventory_count_core_elements =  inventory.filter((inventory.element == 'PRCP') 
    | (inventory.element == 'SNOW')
    | (inventory.element == 'SNWD')
    | (inventory.element == 'TMAX')
    | (inventory.element == 'TMIN')    
    ).groupby("id").agg(F.countDistinct(inventory.element).alias("num_core_elements"))

# --  To count separately the number of other elements that each station has collected overall
inventory_count_other_elements = inventory.filter((inventory.element != 'PRCP') 
    & (inventory.element != 'SNOW')
    & (inventory.element != 'SNWD')
    & (inventory.element != 'TMAX')
    & (inventory.element != 'TMIN')
    ).groupby("id").agg(F.countDistinct(inventory.element).alias("num_other_elements"))


# Join all the information on the number of all elements,  the number of core elements and the number of other elements by joining the previous 3 derived dataframe against the master list of inventory ids.
inventory_station = inventory.select('id').distinct().alias("id")
inventory_station.registerTempTable('inventory_station_tbl')
inventory_year.registerTempTable('inventory_year_tbl')
inventory_count_all_elements.registerTempTable('inventory_count_all_elements_tbl')
inventory_count_core_elements.registerTempTable('inventory_count_core_elements_tbl')
inventory_count_other_elements.registerTempTable('inventory_count_other_elements_tbl')

#join all the information so produce a new RDD new_inventory
new_inventory_sql = """
        SELECT
            inventory_station_tbl.id,
            inventory_year_tbl.min_firstyear,
            inventory_year_tbl.max_lastyear,
            inventory_count_all_elements_tbl.num_all_elements,
            inventory_count_core_elements_tbl.num_core_elements,
            inventory_count_other_elements_tbl.num_other_elements            
        FROM 
            inventory_station_tbl        
            LEFT JOIN inventory_year_tbl ON inventory_station_tbl.id = inventory_year_tbl.id
            LEFT JOIN inventory_count_all_elements_tbl ON inventory_station_tbl.id = inventory_count_all_elements_tbl.id
            LEFT JOIN inventory_count_core_elements_tbl ON inventory_station_tbl.id = inventory_count_core_elements_tbl.id
            LEFT JOIN inventory_count_other_elements_tbl ON inventory_station_tbl.id = inventory_count_other_elements_tbl.id
"""

new_inventory = spark.sql(new_inventory_sql)

#fill all the NULL value with 0
new_inventory = new_inventory.na.fill(0)
new_inventory.show()

#-- To find out how many stations collect all ﬁve core elements
new_inventory.filter(new_inventory.num_core_elements == 5).count()

#--  To find out how many only collection precipitation
new_inventory.registerTempTable('new_inventory_tbl')

only_precipitation_sql = """
        SELECT
            new_inventory_tbl.*,
            inventory_tbl.element
        FROM 
            inventory_tbl        
            JOIN new_inventory_tbl ON inventory_tbl.id = new_inventory_tbl.id
        WHERE
            inventory_tbl.element = 'PRCP'
            AND new_inventory_tbl.num_core_elements = 1
            AND new_inventory_tbl.num_other_elements = 0
"""

only_precipitation = spark.sql(only_precipitation_sql)
only_precipitation.show()

#count how many inventory only collect precipitation
only_precipitation.count()

# ================================  Processing Q3e) LEFT JOIN stations and your output from part (d). ==============================
station_sql = """
        SELECT
            station_tbl.*,
            country_tbl.name as country_name,
            state_tbl.name as state_name,
            new_inventory_tbl.min_firstyear,
            new_inventory_tbl.max_lastyear,
            new_inventory_tbl.num_all_elements,
            new_inventory_tbl.num_core_elements,
            new_inventory_tbl.num_other_elements
        FROM 
            station_tbl        
            LEFT JOIN country_tbl ON station_tbl.country_code = country_tbl.code
            LEFT JOIN state_tbl ON station_tbl.state = state_tbl.code
            LEFT JOIN new_inventory_tbl ON station_tbl.id = new_inventory_tbl.id
"""

stations = spark.sql(station_sql)
stations = stations.fillna('')
stations.show()

#write csv file
stations.write.csv("hdfs:///user/zhl15/outputs/ghcnd/stations/")

# ================================  Processing Q3f) To find out how many stations in 2017 deriverd earlier on does not exist in stations ==============================
daily_2017.registerTempTable('daily_2017_tbl')
stations.registerTempTable('stations_tbl')

#left join daily 2017 records with stations meta data
daily_station_2017_sql = """
        SELECT
            daily_2017_tbl.*,
            stations_tbl.id as station_id
        FROM 
            daily_2017_tbl        
            LEFT JOIN stations_tbl ON daily_2017_tbl.id = stations_tbl.id
"""
daily_station_2017 = spark.sql(daily_station_2017_sql)
daily_station_2017.show()

#fill all the NULL value with empty string
daily_station_2017 = daily_station_2017.fillna('')

#count how many stations with empty station code
daily_station_2017.filter(daily_station_2017.station_id == '').count()

# --- better way of finding out how many stations does not exist from daily 2017
daily_2017.join(stations, "id", "left_anti").show()