"""
Description: Receiving the tweets coming fro Kafka Broker. Processing the tweets posted in Ukraine. The tweets will be uploaded in a MariaDB database.

Required Packages: configparser, argparse, findspark, os, SparkContext, SparkSession, split, col, window, concat, lit, TimestampType

Usage: python3 twitter_consumer.py twitter_consumer_config.ini

"""

#To read conf. file
import configparser
#To read arg. from command line
import argparse

#Search Spark Installation. This step is required just because we are working in the course environment.
import findspark
findspark.init()

#Sending to the Spark Cluster the main program. It'll execute the script and during the execution the code will be optimized by Catalist and then, the code will be executed in the executors and coordinated by the driver process.
import os
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages "org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.3" pyspark-shell'

#To initiate our Spark Session
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

from pyspark.sql.functions import split, col, window, concat, lit
from pyspark.sql.types import TimestampType

#This function is providing the schema for each event or batch and connecting it to an external MariaDB database.
def foreach_batch_function(df, epoch_id, config):
    print ("Batch %d received" % epoch_id)
    
    url = config['DEFAULT']['rdbms_url']
    table = config['DEFAULT']['rdbms_table']
    mode = "append"
    props = {"user":config['DEFAULT']['rdbms_user'],
             "password":config['DEFAULT']['rdbms_password']} 
    
    df.select("event_time", "screen_name", "text", "hashtags", 
              "coordinates", "country", "country_code","location") \
      .write \
      .jdbc(url,table,mode,props) 
    #data source

if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument("configuration_file", 
                        help="configuration file with all the information to run the job")
    args = parser.parse_args()
    
    print ("The streaming_consumer_template is about to start...")
    
    # Read the configuration file to setup the Spark Streaming Application (job)
    #
    print ("   - Reading the configuration of my Spark Streaming Application.")
    config = configparser.ConfigParser()
    config.read(args.configuration_file)
    
    # Create a SparkSession to run our Spark Streaming Application (job)
    #
    print ("   - Creating the Spark Session that will allow me to interact with Spark.")
    sc = SparkSession.builder\
        .master('local')\
        .appName('groupassignment')\
        .getOrCreate()

    # Logic of our Spark Streaming Application (job)
    #

    # Get the configurations for our use case
    kafka_input_topic = config['DEFAULT']['kafka_input_topic']
    kafka_group_id = config['DEFAULT']['kafka_group_id']
    
    # Build the DataFrame from the source
    
    #Building the rawEventsDF DataFrame with data coming from Kafka.\
    #Have a look at the schema you get by default when you create a DataFrame on top of a Kafka topic.\
    #The value field is the one containing the data from the ingestion layer, the Twitter producer in our case.\
    #Bear in mind thataAs we're simplifying things, we're not relying on schemas and we're sending sequence of\
    #bytes to Kafka topics.\
    #Later in the notebook, we're going to convert that sequence of bytes in a proper JSON document \
    #representing every tweet as it was received.")
    rawTweetsDF = sc.readStream \
                       .format("kafka") \
                       .option("kafka.bootstrap.servers", "localhost:9092") \
                       .option("subscribe", "tweets") \
                       .option("startingOffsets", "latest") \
                       .load()\
 
    #Map the sequence of bytes to a Dataframe.\
    #We're going to apply the following logic to the events we get from the topic:\
    #1- Define the schema that matches the raw sequence of bytes we get from the topic.\
    #2- Cast the default data type of the field value (byte) to the String data type.\
    #3- Convert the String into a proper JSON document by using the from_json function.\
    #4- Flatten the JSON file and display event time, user name, text and the id.\

             
    ## 1- Define the schema that matches the raw sequence of bytes we get from the topic.
    tweet_schema="""
    created_at string,
    id bigint,
    id_str string,
    text string,
    source string,
    truncated boolean,
    in_reply_to_status_id bigint,
    in_reply_to_status_id_str string,
    in_reply_to_user_id bigint,
    in_reply_to_user_id_str string,
    in_reply_to_screen_name string,
    `user` struct<
                id:bigint,
                id_str:string,
                name:string,
                screen_name:string,
                location:string,
                url:string,
                description:string,
                protected:boolean,
                verified:boolean,
                followers_count:bigint,
                friends_count:bigint,
                listed_count:bigint,
                favourites_count:bigint,
                statuses_count:bigint,
                created_at:string,
                profile_banner_url:string,
                profile_image_url_https:string,
                default_profile:boolean,
                default_profile_image:boolean,
                withheld_in_countries: array<string>,
                withheld_scope:string,
                geo_enabled:boolean
                >,
    coordinates struct <
                coordinates:array<float>,
                type:string
                >,
    place struct<
                country:string,
                country_code:string,
                full_name:string,
                place_type:string,
                url:string
                >,
    quoted_status_id bigint,
    quoted_status_id_str string,
    is_quote_status boolean,
    quote_count bigint,
    reply_count bigint,
    retweet_count bigint,
    favorite_count bigint,
    entities struct<
                user_mentions:array<struct<screen_name:string>>,
                hashtags:array<struct<text:string>>, 
                media:array<struct<expanded_url:string>>, 
                urls:array<struct<expanded_url:string>>, 
                symbols:array<struct<text:string>>
                >,
    favorited boolean,
    retweeted boolean,
    possibly_sensitive boolean,
    filter_level string,
    lang string
    """
           
    from pyspark.sql.types import StructType,StructField, StringType, IntegerType
    from pyspark.sql.functions import from_json, col         
           

 #   print("   - Polishing raw events and building a DataFrame ready to apply the logic.")
 #   tweetsDF = rawTweetsDF.select(split("value",'\,').alias("fields")) \
 #                           .withColumn("created_at",col("fields").getItem(0)) \
 #                           .withColumn("screen_name",col("fields").getItem(15)) \
 #                           .withColumn("text",col("fields").getItem(3)) \
 #                           .withColumn("hashtags",col("fields").getItem(55))\
 #                           .withColumn("coordinates",col("fields").getItem(36))\
 #                           .withColumn("location", col("fields").getItem(18))\
 #                           .withColumn("country", col("fields").getItem(40))\
 #                           .withColumn("country_code", col("fields").getItem(41))\
 #                           .where(col("country_code")=="UA") \
 #                           .withColumn("event_time",col("created_at").cast(TimestampType()))\
 #                           .select("event_time", "screen_name", "text", "hashtags","coordinates", "country" , "country_code", "location")
           
             
   
    # 2. Cast the default data type of the field value (byte) to the String data type.
    # 3. Convert the String into a proper JSON document by using the from_json function.
    # 4. Flatten the JSON file and display event time, user name, text and the id.

    tweetsDF = rawTweetsDF.selectExpr("CAST(value AS STRING)") \
                          .select(from_json(col("value"), tweet_schema).alias("data")) \
                          .select(col("data.created_at").alias("event_time"), 
                                col("data.user.screen_name").alias("screen_name"),
                                col("data.text").alias("text"),
                                col("data.entities.hashtags").alias("hashtags"),
                                col("data.coordinates.coordinates").alias("coordinates"),
                                col("data.place.country").alias("country"),
                                col("data.place.country_code").alias("country_code"),
                                col("data.user.location").alias("location"))\
                          .filter((col("location")=="UA"))

    
    
    # Configure the sink to send the results of the processing and start the streaming query
    print ("   - Configuring the foreach sink to handle batches by ourselves.")
    streamingQuery = tweetsDF.writeStream \
                                .foreachBatch(lambda df,epochId:foreach_batch_function(df, epochId, config))\
                                .start()

    # Await for the termination of the Streaming Query (otherwise the SparkSession will be closed)
    print ("The streaming query is running in the background (Ctrl+C to stop it)")
    streamingQuery.awaitTermination()