# Databricks notebook source
# MAGIC %md
# MAGIC <h3>Homework #4: Differences by direction of travel - to and from Prague</h3>

# COMMAND ----------

# MAGIC %md
# MAGIC <p>Assignment:
# MAGIC From the data stream, implement a stream processing application that will calculate differences in delay for suburban lines - arrival and departure delays.</p>
# MAGIC <p>Input: stream</p>
# MAGIC <p>Output: Dashboard map with arrivals to Prague and marking the differences in delays during the day</p>

# COMMAND ----------

# MAGIC %run "./pid_schema"

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
select_stream = select_base_buses.writeStream.option("checkpointLocation", "checkpoint/dirBuses").toTable("busTable")

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table pragueLines;
# MAGIC drop table numStops;
# MAGIC drop table routesToPrague;
# MAGIC drop table tripLastDist;
# MAGIC drop table toPragueCoords;
# MAGIC drop table fromPragueCoords;

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists pragueLines
# MAGIC select
# MAGIC   selB.properties.trip.gtfs.route_short_name as lineNumber,
# MAGIC   selB.properties.trip.gtfs.trip_headsign as directionToPrague,
# MAGIC   allB.properties.trip.gtfs.trip_headsign as directionFromPrague
# MAGIC from busTable allB, busTable selB
# MAGIC WHERE selB.properties.trip.gtfs.trip_headsign LIKE 'Praha%'
# MAGIC AND (selB.properties.trip.gtfs.route_short_name=allB.properties.trip.gtfs.route_short_name
# MAGIC   AND selB.properties.trip.gtfs.trip_headsign <> allB.properties.trip.gtfs.trip_headsign)
# MAGIC group by selB.properties.trip.gtfs.route_short_name, selB.properties.trip.gtfs.trip_headsign, allB.properties.trip.gtfs.route_short_name, allB.properties.trip.gtfs.trip_headsign
# MAGIC order by selB.properties.trip.gtfs.route_short_name;

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists numStops
# MAGIC select
# MAGIC   properties.trip.gtfs.route_short_name as lineNumber,
# MAGIC   properties.trip.gtfs.trip_headsign as direction,
# MAGIC   max(properties.last_position.last_stop.sequence) as numStops
# MAGIC from busTable
# MAGIC group by
# MAGIC   properties.trip.gtfs.route_short_name,
# MAGIC   properties.trip.gtfs.trip_headsign
# MAGIC order by lineNumber;

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists routesToPrague
# MAGIC select
# MAGIC   buses.properties.trip.gtfs.route_short_name as lineNumber,
# MAGIC   buses.properties.trip.gtfs.trip_headsign as direction,
# MAGIC   buses.properties.trip.gtfs.trip_id as trip_id,
# MAGIC   buses.properties.trip.start_timestamp as startTime,
# MAGIC   buses.properties.last_position.delay.actual as delay
# MAGIC from busTable buses
# MAGIC   RIGHT JOIN numStops
# MAGIC      ON numStops.lineNumber = buses.properties.trip.gtfs.route_short_name
# MAGIC      AND numStops.direction = buses.properties.trip.gtfs.trip_headsign
# MAGIC      AND numStops.numStops = buses.properties.last_position.last_stop.sequence
# MAGIC    RIGHT JOIN pragueLines
# MAGIC      ON pragueLines.lineNumber = buses.properties.trip.gtfs.route_short_name
# MAGIC order by buses.properties.trip.gtfs.route_short_name;

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists tripLastDist
# MAGIC select
# MAGIC   buses.properties.trip.gtfs.trip_id as trip_id,
# MAGIC   max(buses.properties.last_position.shape_dist_traveled) as dist
# MAGIC from busTable buses
# MAGIC group by
# MAGIC   buses.properties.trip.gtfs.trip_id

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists toPragueCoords
# MAGIC select
# MAGIC   buses.properties.trip.gtfs.route_short_name as lineNumber,
# MAGIC   buses.properties.trip.gtfs.trip_headsign as direction,
# MAGIC   buses.properties.trip.gtfs.trip_id as trip_id,
# MAGIC   buses.properties.trip.start_timestamp as startTime,
# MAGIC   hour(buses.properties.trip.start_timestamp) as hourStartTime,
# MAGIC   buses.properties.last_position.shape_dist_traveled as dist,
# MAGIC   buses.properties.last_position.delay.actual as delay,
# MAGIC   cast(buses.geometry.coordinates[0] as double) AS x,
# MAGIC   cast(buses.geometry.coordinates[1] as double) AS y
# MAGIC from busTable buses
# MAGIC    RIGHT JOIN tripLastDist
# MAGIC      ON tripLastDist.trip_id = buses.properties.trip.gtfs.trip_id
# MAGIC      AND tripLastDist.dist = buses.properties.last_position.shape_dist_traveled
# MAGIC    RIGHT JOIN pragueLines
# MAGIC      ON pragueLines.lineNumber = buses.properties.trip.gtfs.route_short_name
# MAGIC      AND pragueLines.directionToPrague = buses.properties.trip.gtfs.trip_headsign
# MAGIC order by buses.properties.trip.gtfs.route_short_name;

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists fromPragueCoords
# MAGIC select
# MAGIC   buses.properties.trip.gtfs.route_short_name as lineNumber,
# MAGIC   buses.properties.trip.gtfs.trip_headsign as direction,
# MAGIC   buses.properties.trip.gtfs.trip_id as trip_id,
# MAGIC   buses.properties.trip.start_timestamp as startTime,
# MAGIC   hour(buses.properties.trip.start_timestamp) as hourStartTime,
# MAGIC   buses.properties.last_position.shape_dist_traveled as dist,
# MAGIC   buses.properties.last_position.delay.actual as delay,
# MAGIC   cast(buses.geometry.coordinates[0] as double) AS x,
# MAGIC   cast(buses.geometry.coordinates[1] as double) AS y
# MAGIC from busTable buses
# MAGIC    RIGHT JOIN tripLastDist
# MAGIC      ON tripLastDist.trip_id = buses.properties.trip.gtfs.trip_id
# MAGIC      AND tripLastDist.dist = buses.properties.last_position.shape_dist_traveled
# MAGIC    RIGHT JOIN pragueLines
# MAGIC      ON pragueLines.lineNumber = buses.properties.trip.gtfs.route_short_name
# MAGIC      AND pragueLines.directionFromPrague = buses.properties.trip.gtfs.trip_headsign
# MAGIC order by buses.properties.trip.gtfs.route_short_name;

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table toPragueCoordsCurrent;
# MAGIC drop table fromPragueCoordsCurrent;
# MAGIC 
# MAGIC drop table fromPragueCoordsMorning;
# MAGIC drop table toPragueCoordsMorning;
# MAGIC drop table fromPragueCoordsLunch;
# MAGIC drop table toPragueCoordsLunch;
# MAGIC drop table fromPragueCoordsAfternoon;
# MAGIC drop table toPragueCoordsAfternoon;
# MAGIC drop table fromPragueCoordsEvening;
# MAGIC drop table toPragueCoordsEvening;

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists toPragueCoordsCurrent
# MAGIC select x, y, delay
# MAGIC from toPragueCoords
# MAGIC where DATEDIFF(minute, GETDATE(), startTime) < 60;
# MAGIC 
# MAGIC create table if not exists fromPragueCoordsCurrent
# MAGIC select x, y, delay
# MAGIC from fromPragueCoords
# MAGIC where DATEDIFF(minute, GETDATE(), startTime) < 60;
# MAGIC 
# MAGIC create table if not exists fromPragueCoordsMorning
# MAGIC select x, y, delay
# MAGIC from fromPragueCoords
# MAGIC where hourStartTime = 8 or hourStartTime = 9;
# MAGIC 
# MAGIC create table if not exists toPragueCoordsMorning
# MAGIC select x, y, delay
# MAGIC from toPragueCoords
# MAGIC where hourStartTime = 8 or hourStartTime = 9;
# MAGIC 
# MAGIC create table if not exists fromPragueCoordsLunch
# MAGIC select x, y, delay
# MAGIC from fromPragueCoords
# MAGIC where hourStartTime = 12 or hourStartTime = 13;
# MAGIC 
# MAGIC create table if not exists toPragueCoordsLunch
# MAGIC select x, y, delay
# MAGIC from toPragueCoords
# MAGIC where hourStartTime = 12 or hourStartTime = 13;
# MAGIC 
# MAGIC create table if not exists fromPragueCoordsAfternoon
# MAGIC select x, y, delay
# MAGIC from fromPragueCoords
# MAGIC where hourStartTime = 16 or hourStartTime = 17;
# MAGIC 
# MAGIC create table if not exists toPragueCoordsAfternoon
# MAGIC select x, y, delay
# MAGIC from toPragueCoords
# MAGIC where hourStartTime = 16 or hourStartTime = 17;
# MAGIC 
# MAGIC create table if not exists fromPragueCoordsEvening
# MAGIC select x, y, delay
# MAGIC from fromPragueCoords
# MAGIC where hourStartTime = 19 or hourStartTime = 20;
# MAGIC 
# MAGIC create table if not exists toPragueCoordsEvening
# MAGIC select x, y, delay
# MAGIC from toPragueCoords
# MAGIC where hourStartTime = 19 or hourStartTime = 20;

# COMMAND ----------

# MAGIC %sql select * from fromPragueCoordsAfternoon; 

# COMMAND ----------

# MAGIC %md
# MAGIC buses that go to Prague

# COMMAND ----------

# MAGIC %sql drop table avgDelaysForPragueLinesByDirection;

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC create table avgDelaysForPragueLinesByDirection
# MAGIC select
# MAGIC   buses.properties.trip.gtfs.route_short_name as lineNumber,
# MAGIC   buses.properties.trip.gtfs.trip_headsign as direction,
# MAGIC   round(avg(buses.properties.last_position.delay.actual)/60,1) as avg_delay_min
# MAGIC   from busTable as buses
# MAGIC   RIGHT JOIN numStops
# MAGIC      ON numStops.lineNumber = buses.properties.trip.gtfs.route_short_name
# MAGIC      AND numStops.direction = buses.properties.trip.gtfs.trip_headsign
# MAGIC      AND numStops.numStops = buses.properties.last_position.last_stop.sequence
# MAGIC    RIGHT JOIN pragueLines
# MAGIC      ON pragueLines.lineNumber = buses.properties.trip.gtfs.route_short_name
# MAGIC group by
# MAGIC   buses.properties.trip.gtfs.route_short_name,
# MAGIC   buses.properties.trip.gtfs.trip_headsign
# MAGIC order by buses.properties.trip.gtfs.route_short_name;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from avgDelaysForPragueLinesByDirection
# MAGIC order by lineNumber;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     a.lineNumber,
# MAGIC     CASE WHEN a.avg_delay_min > b.avg_delay_min THEN 'TRUE' ELSE 'FALSE' END AS toPragueSlowerThanFromPrague
# MAGIC FROM avgDelaysForPragueLinesByDirection a
# MAGIC INNER JOIN avgDelaysForPragueLinesByDirection b ON a.lineNumber = b.lineNumber
# MAGIC LEFT JOIN pragueLines
# MAGIC   ON pragueLines.lineNumber = a.lineNumber 
# MAGIC WHERE pragueLines.directionToPrague = a.direction
# MAGIC AND pragueLines.directionFromPrague = b.direction
# MAGIC order by lineNumber;

# COMMAND ----------

# MAGIC %pip install scipy==1.10.0
# MAGIC %pip install osmnx

# COMMAND ----------

import osmnx as ox
import matplotlib.pyplot as plt
custom_filter='["highway"~"motorway|motorway_link|trunk|trunk_link|primary|primary_link|secondary|secondary_link|road|road_link"]'
G = ox.graph_from_place("Praha, Czechia", custom_filter=custom_filter)

# COMMAND ----------

# MAGIC %md
# MAGIC <p>Bellow follow maps showing the delays of the regional lines going from/to Prague.</p> 
# MAGIC 
# MAGIC - green colours points: lines with delay less than 2 minutes
# MAGIC - orange colours points: lines with delay of 2-4 minutes
# MAGIC - red colours points: lines with delay more than 4 minutes
# MAGIC 
# MAGIC <p>Maps on the left show lines heading to Prague. Maps on the right show lines going from Prague.</p>
# MAGIC 
# MAGIC <p>Maps feature:</p>
# MAGIC 
# MAGIC - current delays (for trips in the last 60 minutes)
# MAGIC - delays for the regional buses that start at 8 and 9 am
# MAGIC - delays for the regional buses that start at 12 and 13 pm
# MAGIC - delays for the regional buses that start at 16 and 17 am
# MAGIC - delays for the regional buses that start at 19 and 20 am

# COMMAND ----------

fig, ax = ox.plot_graph(G, show=False, close=False)
df = sqlContext.table("toPragueCoordsCurrent").toPandas()
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

fig, ax = ox.plot_graph(G, show=False, close=False)
df = sqlContext.table("fromPragueCoordsCurrent").toPandas()
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

fig, ax = ox.plot_graph(G, show=False, close=False)
df = sqlContext.table("fromPragueCoordsMorning").toPandas()
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

fig, ax = ox.plot_graph(G, show=False, close=False)
df = sqlContext.table("toPragueCoordsMorning").toPandas()
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

fig, ax = ox.plot_graph(G, show=False, close=False)
df = sqlContext.table("fromPragueCoordsLunch").toPandas()
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

fig, ax = ox.plot_graph(G, show=False, close=False)
df = sqlContext.table("toPragueCoordsLunch").toPandas()
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

fig, ax = ox.plot_graph(G, show=False, close=False)
df = sqlContext.table("fromPragueCoordsAfternoon").toPandas()
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

fig, ax = ox.plot_graph(G, show=False, close=False)
df = sqlContext.table("toPragueCoordsAfternoon").toPandas()
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

fig, ax = ox.plot_graph(G, show=False, close=False)
df = sqlContext.table("fromPragueCoordsEvening").toPandas()
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

fig, ax = ox.plot_graph(G, show=False, close=False)
df = sqlContext.table("toPragueCoordsEvening").toPandas()
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


