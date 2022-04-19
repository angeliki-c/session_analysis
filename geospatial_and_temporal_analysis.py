"""
    For managing geospatial data the apache sedona external python library is used.
    Apache Sedona (formerly known as GeoSpark) extends pyspark functions and depends on libraries pyspark, attrs, shapely.
    For the most part, the geospatial operations, in the code below, will use functions from the apache sedona lib.
    
"""
sc.setLogLevel('OFF')
spark = SparkSession.builder.appName('appName').config('spark.serializer','KryoSerializer.getName').config('spark.kryo.registrator','SedonaKryoRegistrator.getName').config('spark.jars.packages','org.apache.sedona:sedona-python-adapter-3.0_2.12:1.1.1-incubating,org.apache.sedona:sedona-viz-3.0_2.12:1.0.1-incubating,org.datasyslab:geotools-wrapper:1.1.0-25.2').getOrCreate()
SparkContext.setSystemProperty('spark.executor.memory','4g')
SparkContext.setSystemProperty('spark.driver.memory','4g')
SparkContext.setSystemProperty('spark.master','local[*]')
SparkContext.setSystemProperty("spark.scheduler.mode", "FAIR")
#SparkContext.setSystemProperty('spark.executor.cores', '8')
    
from sedona.register import SedonaRegistrator
SedonaRegistrator.registerAll(spark)

import re
		
from pyspark.sql.types import StructType, StructField, LongType, DoubleType, StringType, DateType, TimestampType, IntegerType
import pyspark.sql.functions as F 

# it is good to submit the schema of the dataset when reading, as it is avoided the second pass of the data for 
# inferring the schema and the dataset in this case is big 
schema_temp = StructType([StructField(' medallion',StringType(),True),StructField(' hack_license',StringType(),True),StructField(' vendor_id',StringType(),True),StructField(' rate_code',StringType(),True),StructField(' store_and_fwd_flag',StringType(),True),StructField(' pickup_datetime',TimestampType(),True),StructField(' dropoff_datetime',TimestampType(),True),StructField(' passenger_count',StringType(),True),StructField(' trip_time_in_secs',StringType(),True),StructField(' trip_distance',DoubleType(),True),StructField(' pickup_longitude',DoubleType(),True),StructField(' pickup_latitude',DoubleType(),True),StructField(' dropoff_longitude',DoubleType(),True),StructField(' dropoff_latitude',DoubleType(),True)])
schema = StructType([StructField('medallion',StringType(),True),StructField('hack_license',StringType(),True),StructField('vendor_id',StringType(),True),StructField('rate_code',StringType(),True),StructField('store_and_fwd_flag',StringType(),True),StructField('pickup_datetime',TimestampType(),True),StructField('dropoff_datetime',TimestampType(),True),StructField('passenger_count',StringType(),True),StructField('trip_time_in_secs',StringType(),True),StructField('trip_distance',DoubleType(),True),StructField('pickup_longitude',DoubleType(),True),StructField('pickup_latitude',DoubleType(),True),StructField('dropoff_longitude',DoubleType(),True),StructField('dropoff_latitude',DoubleType(),True)])
header = sc.textFile('/user/data/trip_data_1.csv').first()
col_names_of_interest = ['hack_license','pickup_datetime','dropoff_datetime','pickup_longitude','pickup_latitude','dropoff_longitude','dropoff_latitude']
header = re.split(',\s*',header)
if  header!= schema.names:
    print("================")
    print("There is schema mismatch.")
    raise Exception    
else:
    print("================")
    print("read the taxi-trip data....")
    df_raw = spark.read.format('csv').option('header','true').schema(schema_temp).load('hdfs://localhost:9000/user/data/trip_data_1.csv')
    df_raw = df_raw.rdd.toDF(schema.names)
    df = df_raw.select(col_names_of_interest)  
    

#df.count()                          #   14776615
df = df.dropna(how = 'any')      #  14776529 remaining records 
df = df.withColumn('pickup_datetime', df.pickup_datetime.cast(LongType()))
df = df.withColumn('dropoff_datetime', df.dropoff_datetime.cast(LongType()))
#df.rdd.getNumPartitions()              #    19
df = df.repartition(100)
df.cache()
print("================")
print("Summary statistics on the data (after dropping records containing any null value):")
df.describe().show()
dfp = df.selectExpr(['hack_license', 'pickup_datetime', 'dropoff_datetime', 'ST_Point(pickup_longitude, pickup_latitude) as pickup_point', 'ST_Point(dropoff_longitude, dropoff_latitude) as dropoff_point'])
#dfp = spark.sql("select hack_license, pickup_datetime, dropoff_datetime, ST_Point(pickup_longitude, pickup_latitude) as pickup_point, ST_Point(dropoff_longitude, dropoff_latitude) as dropoff_point from trip_data")
dfp.cache()

# For further cleansing of the data, we should start thinking of other constraints that should be met for ensuring data 
# quality and avoid potential disruptions of the workflow in the future, with incoming real data.
print("================")
print("Compute the 'trip_in_hours' feature and estimate the validity of the data.")
print("A sample of the distribution of the 'trip_in_hours' values across records :")
import math

hours_udf = F.udf(lambda start,stop : math.floor((stop - start)/3600))
trip_data = dfp.withColumn('trip_in_hours',hours_udf(F.col('pickup_datetime'),F.col('dropoff_datetime')).cast('integer'))
trip_data.cache()
hist = trip_data.groupBy('trip_in_hours').count().orderBy('trip_in_hours')
hist.show() 
df.unpersist()
dfp.unpersist()
print("================")                                             #    Oh, the dataset is not clean, for sure!
print("""Since we are not aware of how long an indicative taxi trip can take in the NYC, we will use 
       common sense and the 0.97 quantile of the data for estimating a trip_in_hours cutoff value.""")     
quantiles =   trip_data.stat.approxQuantile('trip_in_hours',[0.25,0.5,0.75,0.97], 0.01)       
print(f"Quantiles : {quantiles}")
trip_data = trip_data.where('trip_in_hours >=0 and trip_in_hours < 4')                   
print("================")
print("Removing records with negative trip duration in hours and with trip duration >= 4 hours. ")


# Processing of geospatial data 
"""
      This snippet of code reads information about the geographical boundaries of boroughs. 
      
# read the spatial data for each borough in the city 
print("================")
print("read the borough spatial data ...")
boroughs = spark.read.format('csv').option('header',True).option('inferSchema', True).load('/user/data/nybb.csv')
geo_ref = boroughs.drop('BoroCode').dropna(how = 'any')
geo_ref.cache()
geo_ref = geo_ref.withColumnRenamed('BoroName', 'borough') 
geo_ref.count()                   #    5
print("================")
print(f"There is geospatial information about {geo_ref.count()} boroughs. ")
geo_ref.show()
geo_ref = geo_ref.selectExpr(['the_geom', 'borough', 'Shape_Leng', 'Shape_Area','st_geomFromWKT(the_geom) as geometry '])
geo_ref = geo_ref.selectExpr(['the_geom', 'borough', 'Shape_Leng', 'Shape_Area','geometry ','ST_Area(geometry) location_area']).orderBy(F.desc('location_area'))
"""


"""
      This snippet drows geographical information on the exact taxi-zones. In this information there is also information 
      about the borough where each taxi-zone belongs to.
      
"""
print("================")
print("read the taxi zone data ...")
taxi_zones = spark.read.format('csv').option('header',True).option('inferSchema', True).load('/user/data/taxi_zones.csv')

# Constructs a geometry with the 'the_geom' string which is in wkt format. It returns a type of geometry.
taxi_zones = taxi_zones.selectExpr(['Shape_Leng', 'the_geom', 'Shape_Area', 'zone','borough','st_geomFromWKT(the_geom) as geometry'])
taxi_zones_agg = taxi_zones.groupBy('borough').agg(F.expr('ST_Union_Aggr(geometry)').alias('total_geometry'))
taxi_zones.cache()
"""
    Here a heuristic is applied for optimizing the response time for our query.
	Most taxi rides in NYC begin and end most likely in places of greater location area than the orhers, therefore
    we sort 'taxi_zones' dataframe by location area size, as it will most probably take less time to find the borough
    where the passenger was dropped off.
    
"""

taxi_zones_agg = taxi_zones_agg.selectExpr(['borough','total_geometry','ST_Area(total_geometry) as location_area']).orderBy(F.desc('location_area'))
taxi_zones_agg.cache()
trip_data_part = trip_data.select(['hack_license','dropoff_point'])
trip_data_part.repartition(200)
# where each trip ends
tripEndDF = trip_data_part.alias('b').join(F.broadcast(taxi_zones_agg.select(['borough','total_geometry'])).alias('a'), F.expr('ST_Contains(a.total_geometry,b.dropoff_point)'))
print("================")
print("Lets now see in which borough each trip ends ...")		 
tripEndDF.select(['borough', 'hack_license']).show(10)    
from pyspark import StorageLevel
# It stores to disk only. It is useful if you are running out of memory resource.
"""tripEndDF.persist(StorageLevel(True, False, False, False, 1))	 
from pyspark.sql import SQLContext
sqc = SQLContext(spark)
sqc.clearCache()
"""
data_g = tripEndDF.groupBy('borough').count().orderBy(F.desc('count'))
data_g.cache()
print("================")
print("Distribution of the trips ending in each borough across boroughs. ")
data_g.show()


print("================")
print("""Is there any trip that doesn't end in any of the boroughs considered? If there is any that doesn't sound 
        like a place in NYC it will be removed as not valid.  """)
not_valid_boroughs = []
grouped_aux_list = data_g.select('borough').collect()
for el in grouped_aux_list:
    borough_list = [r.borough for r in taxi_zones_agg.collect()]
    if el.borough not in borough_list:
        print(f"Ending in '{el.borough}' for which we have not geospatial information.")
        not_valid_boroughs.append(el.borough)
        
#not_valid_boroughs = tuple(not_valid_boroughs)
print(not_valid_boroughs)
if len(not_valid_boroughs) != 0:
    cond = " or ".join([f"borough = '{b}'"  for b in not_valid_boroughs])
    not_valid_drp = tripEndDF.where(cond).select('dropoff_point')

    
not_valid_drp.createOrReplaceTempView('not_valid_drp')

trip_data = trip_data.where('dropoff_point not in (select dropoff_point from not_valid_drp)')


#Sessionization

										   
# Construct sessions (log of events) considering pickup and dropoff times.
# It ensures that we have data for one taxi per partition and sorted.
sessions =  trip_data.repartition(trip_data.hack_license).sortWithinPartitions(trip_data.hack_license, trip_data.pickup_datetime) 			
sessions.cache()  
# if 'sessions' is big in size better persist it to disk                         
#sessions.persist(StorageLevel(True, False, False, False, 1))

										
from pyspark.sql.window import Window

window_spec = Window.partitionBy(sessions['hack_license']).orderBy(sessions['pickup_datetime'])
lead_pickup_point = F.lead(sessions['pickup_point'],offset = 1,default = None).over(window_spec) 
lead_datetime = F.lead(sessions['pickup_datetime'],offset = 1,default = -1).over(window_spec) 
durations = lead_datetime  - sessions['dropoff_datetime']
durations_df = sessions.select([F.col('hack_license'),F.col('pickup_datetime').cast(TimestampType()),F.col('dropoff_datetime').cast(TimestampType()), F.col('pickup_point'), F.col('dropoff_point'), lead_datetime.alias('lead_datetime'), lead_pickup_point.alias('lead_pickup_point'),durations.alias('next_fare_duration')])
cdn = durations_df.where('lead_datetime = -1').count()            # 32223. These are the records marked as the last of the window that refers to a particular driver and are regarded as the last before changing shift.
time.sleep(4)

durations_df = durations_df.where('lead_datetime != -1')
durations_df = durations_df.drop('lead_datetime')										
durations_df.where("next_fare_duration < 0").count()        #   4995. Negative durations typically might occure when picking up a passenger when the taxi is already on a tarrif.
time.sleep(4) 														    #	Though it is an indicator of utilization of a taxi we will remove it from our analysis as it doesn't help us in
                                                            #   in our question of in how much time from dropping off a client the driver finds the next client related to the 
                                                            #   borough where the previous client was dropped off.  
durations_df = durations_df.where("next_fare_duration > 0")                                                            
durations_df.cache()
res = durations_df.alias('a').join(F.broadcast(taxi_zones_agg.select(['borough','total_geometry'])).alias('b'), F.expr('ST_Contains(b.total_geometry,a.lead_pickup_point)')).selectExpr(["a.hack_license","a.pickup_datetime", "a.dropoff_datetime" , "div(a.next_fare_duration,60) next_fare_duration", "a.dropoff_point",  "b.borough next_pickup_borough "])
res = res.alias('a').join(taxi_zones_agg.select(['borough','total_geometry']).alias('b'), F.expr('ST_Contains(b.total_geometry,a.dropoff_point)')).selectExpr(["a.hack_license", "a.pickup_datetime", "a.dropoff_datetime", "a.next_fare_duration", "b.borough dropoff_borough","b.total_geometry dropoff_geometry", "a.next_pickup_borough"])
#   standard deviation and average value of the duration, for each borough where the tariff ends to.
dur_stat = res.groupBy('dropoff_borough').agg(F.avg('next_fare_duration').alias('average_duration'), F.stddev('next_fare_duration').alias('deviation_duration'),F.first('dropoff_geometry').alias('dropoff_geometry')).orderBy('average_duration')          
print("Statistics of time duration between last and next fare by dropoff borough: ")
dur_stat.show()

dur_stat_pd = dur_stat.toPandas()
import geopandas as gpd

print("Heatmap of the average_duration across the boroughs of the city (stored as image.png in the working dir):")
gdf = gpd.GeoDataFrame(dur_stat_pd, geometry="dropoff_geometry")
gdf.plot(figsize=(10, 8),column="average_duration",legend=True,cmap='YlOrBr',scheme='quantiles',edgecolor='lightgray').figure.savefig('image')
