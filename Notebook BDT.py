# Databricks notebook source
# MAGIC %run "./pid_schema"

# COMMAND ----------

get_pid_schema()

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import from_json, col

# connect to broker
JAAS = 'org.apache.kafka.common.security.scram.ScramLoginModule required username="fel.student" password="FelBigDataWinter2022bflmpsvz";'

# COMMAND ----------


df_buses = spark.readStream \
  .format("kafka")\
  .option("kafka.bootstrap.servers", "b-2-public.bdffelkafka.3jtrac.c19.kafka.us-east-1.amazonaws.com:9196, b-1-public.bdffelkafka.3jtrac.c19.kafka.us-east-1.amazonaws.com:9196") \
  .option("kafka.sasl.mechanism", "SCRAM-SHA-512")\
  .option("kafka.security.protocol", "SASL_SSL") \
  .option("kafka.sasl.jaas.config", JAAS) \
  .option("subscribe", "regbuses") \
  .load()

#get schema for the stream from the function in helper notebook
schema_pid=get_pid_schema() 

select_base_buses = df_buses.select(from_json(col("value").cast("string"),schema_pid).alias("data")).select("data.*")
#lets start reading from the stream stream over casted to memory, be advised, you can ran out of it
#with option .outputMode("append") we are saving only the new data coming to the stream
#with option checkpoint, so the stream knows not to overwrite someother stream, in case we stream the same topics into two streams
#for saving into table we can add command .toTable("nameofthetable") , table will be stored in Data>hive_metastore>default>nameofthetable, this may prove usefull for some of you maybe
select_stream = select_base_buses.writeStream.option("checkpointLocation", "checkpoint/dir").toTable("busTable")

# COMMAND ----------

spark.read.table("busTable").count()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from busTable;

# COMMAND ----------


df_buses = spark.readStream \
  .format("kafka")\
  .option("kafka.bootstrap.servers", "b-2-public.bdffelkafka.3jtrac.c19.kafka.us-east-1.amazonaws.com:9196, b-1-public.bdffelkafka.3jtrac.c19.kafka.us-east-1.amazonaws.com:9196") \
  .option("kafka.sasl.mechanism", "SCRAM-SHA-512")\
  .option("kafka.security.protocol", "SASL_SSL") \
  .option("kafka.sasl.jaas.config", JAAS) \
  .option("subscribe", "regbuses") \
  .load()

#get schema for the stream from the function in helper notebook
schema_pid=get_pid_schema() 

select_base_buses = df_buses.select(from_json(col("value").cast("string"),schema_pid).alias("data")).select("data.*") \
#lets start reading from the stream stream over casted to memory, be advised, you can ran out of it
#with option .outputMode("append") we are saving only the new data coming to the stream
#with option checkpoint, so the stream knows not to overwrite someother stream, in case we stream the same topics into two streams
#for saving into table we can add command .toTable("nameofthetable") , table will be stored in Data>hive_metastore>default>nameofthetable, this may prove usefull for some of you maybe
select_stream = select_base_buses.writeStream \
        .format("memory")\
        .queryName("mem_buses")\
        .outputMode("append")\
        .start()

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table buses;

# COMMAND ----------

# MAGIC %sql
# MAGIC create table buses select * from mem_buses;

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct properties.trip.gtfs.route_short_name as lineNumber from buses order by lineNumber;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from buses;

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from pragueLines;

# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC   properties.trip.gtfs.route_short_name as lineNumber,
# MAGIC   properties.trip.gtfs.trip_headsign as direction,
# MAGIC   max(properties.last_position.last_stop.sequence) as numStops
# MAGIC from buses
# MAGIC group by
# MAGIC   properties.trip.gtfs.route_short_name,
# MAGIC   properties.trip.gtfs.trip_headsign
# MAGIC order by lineNumber;

# COMMAND ----------

# MAGIC %sql
# MAGIC select *
# MAGIC from buses a, numStops b
# MAGIC where
# MAGIC   a.properties.trip.gtfs.route_short_name = b.lineNumber
# MAGIC   and  properties.trip.gtfs.trip_headsign = b.direction
# MAGIC   and a.properties.last_position.last_stop.sequence = b.numStops
# MAGIC limit 60;

# COMMAND ----------

# MAGIC %sql
# MAGIC create table numStops
# MAGIC select
# MAGIC   properties.trip.gtfs.route_short_name as lineNumber,
# MAGIC   properties.trip.gtfs.trip_headsign as direction,
# MAGIC   max(properties.last_position.last_stop.sequence) as numStops
# MAGIC from buses
# MAGIC group by
# MAGIC   properties.trip.gtfs.route_short_name,
# MAGIC   properties.trip.gtfs.trip_headsign
# MAGIC order by lineNumber;

# COMMAND ----------

properties.trip.sequence_id as tripSequence, 
%sql
select
properties.trip.gtfs.route_short_name as lineNumber,
properties.trip.gtfs.trip_headsign as direction,
properties.trip.sequence_id as tripSequence, 
properties.last_position.last_stop.sequence as lastStopSeq,
properties.trip.gtfs.trip_id as trip_id,
properties.trip.start_timestamp as startTime, 
properties.last_position.origin_timestamp as timeNow,
properties.last_position.shape_dist_traveled as distTravelled,
properties.last_position.delay.actual as delay
from buses
order by lineNumber, tripSequence, trip_id, lastStopSeq
limit 300;

# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC properties.trip.gtfs.route_short_name as lineNumber,
# MAGIC properties.trip.gtfs.trip_headsign as direction,
# MAGIC properties.trip.sequence_id as tripSequence, 
# MAGIC properties.trip.gtfs.trip_id as trip_id,
# MAGIC properties.trip.start_timestamp as startTime
# MAGIC from buses
# MAGIC group by lineNumber, direction, trip_id, tripSequence, startTime
# MAGIC order by lineNumber, tripSequence, trip_id;

# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC properties.trip.gtfs.route_short_name, properties.trip.start_timestamp, count(*)
# MAGIC from buses
# MAGIC group by
# MAGIC   properties.trip.gtfs.route_short_name,
# MAGIC   properties.trip.start_timestamp;

# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC count(distinct properties.trip.gtfs.trip_id)
# MAGIC from buses

# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC count(distinct properties.trip.gtfs.route_short_name, properties.trip.start_timestamp, properties.trip.gtfs.trip_headsign)
# MAGIC from buses

# COMMAND ----------

# MAGIC %md
# MAGIC buses that go to Prague

# COMMAND ----------

# MAGIC %sql
# MAGIC create table pragueLines
# MAGIC select
# MAGIC   selB.properties.trip.gtfs.route_short_name as lineNumber,
# MAGIC   selB.properties.trip.gtfs.trip_headsign as directionToPrague,
# MAGIC   allB.properties.trip.gtfs.trip_headsign as directionFromPrague
# MAGIC from buses allB, buses selB
# MAGIC WHERE selB.properties.trip.gtfs.trip_headsign LIKE 'Praha%'
# MAGIC AND (selB.properties.trip.gtfs.route_short_name=allB.properties.trip.gtfs.route_short_name
# MAGIC   AND selB.properties.trip.gtfs.trip_headsign <> allB.properties.trip.gtfs.trip_headsign)
# MAGIC group by selB.properties.trip.gtfs.route_short_name, selB.properties.trip.gtfs.trip_headsign, allB.properties.trip.gtfs.route_short_name, allB.properties.trip.gtfs.trip_headsign
# MAGIC order by selB.properties.trip.gtfs.route_short_name;

# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC   selB.properties.trip.gtfs.route_short_name as lineNumber,
# MAGIC   selB.properties.trip.gtfs.trip_headsign as directionToPrague,
# MAGIC   allB.properties.trip.gtfs.trip_headsign as directionFromPrague
# MAGIC from buses allB, buses selB
# MAGIC WHERE selB.properties.trip.gtfs.trip_headsign LIKE 'Praha%'
# MAGIC AND (selB.properties.trip.gtfs.route_short_name=allB.properties.trip.gtfs.route_short_name
# MAGIC   AND selB.properties.trip.gtfs.trip_headsign <> allB.properties.trip.gtfs.trip_headsign)
# MAGIC group by selB.properties.trip.gtfs.route_short_name, selB.properties.trip.gtfs.trip_headsign, allB.properties.trip.gtfs.route_short_name, allB.properties.trip.gtfs.trip_headsign
# MAGIC order by selB.properties.trip.gtfs.route_short_name;

# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC   buses.properties.trip.gtfs.route_short_name as lineNumber,
# MAGIC   buses.properties.trip.gtfs.trip_headsign as direction,
# MAGIC   avg(buses.properties.last_position.delay.actual) as avgDelay
# MAGIC from buses
# MAGIC   RIGHT JOIN numStops
# MAGIC      ON numStops.lineNumber = buses.properties.trip.gtfs.route_short_name
# MAGIC      AND numStops.direction = buses.properties.trip.gtfs.trip_headsign
# MAGIC      AND numStops.numStops = buses.properties.last_position.last_stop.sequence
# MAGIC    RIGHT JOIN pragueLines
# MAGIC      ON pragueLines.lineNumber = buses.properties.trip.gtfs.route_short_name
# MAGIC group by
# MAGIC   buses.properties.trip.gtfs.route_short_name,
# MAGIC   buses.properties.trip.gtfs.trip_headsign
# MAGIC order by buses.properties.trip.gtfs.route_short_name
# MAGIC limit 50;

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC   buses.properties.trip.gtfs.route_short_name as lineNumber,
# MAGIC   buses.properties.trip.gtfs.trip_headsign as direction,
# MAGIC   buses.properties.trip.gtfs.trip_id as trip_id,
# MAGIC   buses.properties.trip.start_timestamp as startTime,
# MAGIC   buses.properties.last_position.delay.actual as delay
# MAGIC from buses
# MAGIC   RIGHT JOIN numStops
# MAGIC      ON numStops.lineNumber = buses.properties.trip.gtfs.route_short_name
# MAGIC      AND numStops.direction = buses.properties.trip.gtfs.trip_headsign
# MAGIC      AND numStops.numStops = buses.properties.last_position.last_stop.sequence
# MAGIC    RIGHT JOIN pragueLines
# MAGIC      ON pragueLines.lineNumber = buses.properties.trip.gtfs.route_short_name
# MAGIC order by buses.properties.trip.gtfs.route_short_name
# MAGIC limit 50;

# COMMAND ----------

# MAGIC %sql
# MAGIC create table routesToPrague
# MAGIC select
# MAGIC   buses.properties.trip.gtfs.route_short_name as lineNumber,
# MAGIC   buses.properties.trip.gtfs.trip_headsign as direction,
# MAGIC   buses.properties.trip.gtfs.trip_id as trip_id,
# MAGIC   buses.properties.trip.start_timestamp as startTime,
# MAGIC   buses.properties.last_position.delay.actual as delay
# MAGIC from buses
# MAGIC   RIGHT JOIN numStops
# MAGIC      ON numStops.lineNumber = buses.properties.trip.gtfs.route_short_name
# MAGIC      AND numStops.direction = buses.properties.trip.gtfs.trip_headsign
# MAGIC      AND numStops.numStops = buses.properties.last_position.last_stop.sequence
# MAGIC    RIGHT JOIN pragueLines
# MAGIC      ON pragueLines.lineNumber = buses.properties.trip.gtfs.route_short_name
# MAGIC order by buses.properties.trip.gtfs.route_short_name;

# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC   lineNumber, direction, avg(delay)
# MAGIC from routesToPrague
# MAGIC where
# MAGIC   hour(startTime)=17 OR hour(startTime)=18
# MAGIC group by
# MAGIC   lineNumber, direction
# MAGIC order by lineNumber
# MAGIC limit 50;

# COMMAND ----------

# MAGIC %pip install scipy==1.10.0
# MAGIC %pip install osmnx

# COMMAND ----------

# MAGIC %sql
# MAGIC create table coords
# MAGIC SELECT
# MAGIC cast(geometry.coordinates[0] as double) AS x,
# MAGIC cast(geometry.coordinates[1] as double) AS y
# MAGIC FROM buses
# MAGIC limit 10;

# COMMAND ----------

import osmnx as ox
custom_filter='["highway"~"motorway|motorway_link|trunk|trunk_link|primary|primary_link|secondary|secondary_link|road|road_link"]'
G = ox.graph_from_place("Praha, Czechia", custom_filter=custom_filter)
import matplotlib.pyplot as plt
fig, ax = ox.plot_graph(G, show=False, close=False)


df_geo_p = sqlContext.table("coords").toPandas()
x = df_geo_p.loc[1:10,'x']
y = df_geo_p.loc[1:10,'y']
ax.scatter(x, y, c='red')
plt.show()

# COMMAND ----------

# MAGIC %sql
# MAGIC create table tripLastDist
# MAGIC select
# MAGIC   buses.properties.trip.gtfs.trip_id as trip_id,
# MAGIC   max(buses.properties.last_position.shape_dist_traveled) as dist
# MAGIC from buses
# MAGIC group by
# MAGIC   buses.properties.trip.gtfs.trip_id

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from tripLastDist;

# COMMAND ----------

# MAGIC %sql
# MAGIC create table toPragueCoords
# MAGIC select
# MAGIC   buses.properties.trip.gtfs.route_short_name as lineNumber,
# MAGIC   buses.properties.trip.gtfs.trip_headsign as direction,
# MAGIC   buses.properties.trip.gtfs.trip_id as trip_id,
# MAGIC   buses.properties.trip.start_timestamp as startTime,
# MAGIC   buses.properties.last_position.shape_dist_traveled as dist,
# MAGIC   buses.properties.last_position.delay.actual as delay,
# MAGIC   cast(buses.geometry.coordinates[0] as double) AS x,
# MAGIC   cast(buses.geometry.coordinates[1] as double) AS y
# MAGIC from buses
# MAGIC    RIGHT JOIN tripLastDist
# MAGIC      ON tripLastDist.trip_id = buses.properties.trip.gtfs.trip_id
# MAGIC      AND tripLastDist.dist = buses.properties.last_position.shape_dist_traveled
# MAGIC    RIGHT JOIN pragueLines
# MAGIC      ON pragueLines.lineNumber = buses.properties.trip.gtfs.route_short_name
# MAGIC      AND pragueLines.directionToPrague = buses.properties.trip.gtfs.trip_headsign
# MAGIC order by buses.properties.trip.gtfs.route_short_name
# MAGIC limit 100;

# COMMAND ----------

import osmnx as ox
custom_filter='["highway"~"motorway|motorway_link|trunk|trunk_link|primary|primary_link|secondary|secondary_link|road|road_link"]'
G = ox.graph_from_place("Praha, Czechia", custom_filter=custom_filter)
import matplotlib.pyplot as plt
fig, ax = ox.plot_graph(G, show=False, close=False)


df = sqlContext.table("toPragueCoords").toPandas()
x_data = df["x"].to_numpy().tolist()
y_data = df["y"].to_numpy().tolist()
delays = df["delay"].to_numpy().tolist()
for idx in range(len(delays)):
    col = 'green'
    if delays[idx]>120:
        col='orange'
    elif delays[idx]>240:
        col='red'
    plt.scatter(x_data[idx], y_data[idx], c=col)
plt.show()

# COMMAND ----------

properties.trip.gtfs.trip_headsign
lineNumber = properties.trip.gtfs.route_short_name
delay = properties.last_position.delay.actual

direction = properties.trip.gtfs.trip_headsign

properties.last_stop.sequence

select Subject, Count(*) 
from Subject_Selection
group by Subject

line, 


get lines that go to and from Prague - substring Prague in trip_headsign
get lines that go to Prague rn - substring Prague in trip_headsign

get last stops of such buses
get delays of such buses at the last stops



TABLES
- numStops
- pragueLines
