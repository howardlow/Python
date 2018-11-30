# STAT478-18S1 Assignment 1
# Howard Low, 53626262

#@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@
#======================================= ANALYSIS Q2 =============================================
#@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@

# ================================  Analysis Q2a)  Create user deﬁned functions in Spark by taking native Python functions and wrapping them using pyspark.sql.functions.udf   ==============================
from math import sin, cos, sqrt, atan2, radians

#UDF function to calculate distance between 2 sets of latitude and longtitude
def calculate_distance(latitudeA, longtitudeA, latitudeB, longtitudeB):
    # approximate radius of earth in km
    R = 6373.0
    lat1 = radians(latitudeA)
    lon1 = radians(longtitudeA)
    lat2 = radians(latitudeB)
    lon2 = radians(longtitudeB)

    dlon = lon2 - lon1
    dlat = lat2 - lat1

    a = sin(dlat / 2)**2 + cos(lat1) * cos(lat2) * sin(dlon / 2)**2
    c = 2 * atan2(sqrt(a), sqrt(1 - a))

    distance = R * c

    return distance

#intialize UDF
distance_udf = F.udf(calculate_distance)

#-- For testing if UDF works --
stations_testA = stations.sample(False, 0.1, seed=0).limit(10)
stations_testB = stations.sample(False, 0.1, seed=0).limit(10)

stations_testA.registerTempTable('stations_testA_tbl')
stations_testB.registerTempTable('stations_testB_tbl')

test_sql = """
        SELECT
            stations_testA_tbl.id as station_a_code,
            stations_testA_tbl.latitude as station_a_latitude,
            stations_testA_tbl.longitude as station_a_longitude,            
            stations_testB_tbl.id as station_b_code,
            stations_testB_tbl.latitude as station_b_latitude,
            stations_testB_tbl.longitude as station_b_longitude         
        FROM 
            stations_testA_tbl        
            CROSS JOIN stations_testB_tbl
"""
test_dist = spark.sql(test_sql)

test_dist.withColumn("distance_apart", distance_udf(test_dist.station_a_latitude, test_dist.station_a_longitude, 
    test_dist.station_b_latitude, test_dist.station_b_longitude)).show()

# ================================  Analysis Q2b) calculate distance between all stations in NZ stations ==============================
#filter out only NZ stations
nz_stations = stations.filter(stations.country_code == 'NZ')

nz_stations.registerTempTable('nz_stations_tbl')

#cross join NZ stations to create cartesian product to get a pair per row for calculating the distance apart
stations_distance_nz_sql = """
        SELECT
            a.id as station_nz_codeA,
            a.latitude as station_nz_latitudeA,
            a.longitude as station_nz_longitudeA,            
            b.id as station_nz_codeB,
            b.latitude as station_nz_latitudeB,
            b.longitude as station_nz_longitudeB         
        FROM 
            nz_stations_tbl a        
            CROSS JOIN nz_stations_tbl b
        WHERE
            a.id <> b.id
"""

stations_distance_nz = spark.sql(stations_distance_nz_sql)

#to find the distance apart for each pair value using UDF
stations_distance_nz = stations_distance_nz.withColumn("distance_apart", distance_udf(
    stations_distance_nz.station_nz_latitudeA, stations_distance_nz.station_nz_longitudeA, 
    stations_distance_nz.station_nz_latitudeB, stations_distance_nz.station_nz_longitudeB).cast(DoubleType()))

#display and show which stations is nearest to each other
stations_distance_nz.orderBy("distance_apart").show()