package com.paypay.de

import java.nio.charset.StandardCharsets
import java.sql.Timestamp

import org.apache.spark.api.java.StorageLevels
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Dataset, Encoders, SaveMode, SparkSession}


case class LogSchema(create_time:Timestamp, elb:String, client_host_port:String, backend_host_port:String, request_processing_time:Double, backend_processing_time:Double, response_processing_time:Double, elb_status_code:Int, backend_status_code:Int, received_bytes:Int, sent_bytes:Int, request:String, user_agent:String, ssl_cipher:String, ssl_protocol:String)

class LogAnalyse(spark:SparkSession,inputPath:String, numOfPartitions:Int) {

    val encoder = Encoders.product[LogSchema]
    //load log data into spark-dataset
    val inputDataset:Dataset[LogSchema]=spark
      .read
      .option("delimiter"," ")
      .option("quote","\"")
      .option("header",false)
      .option("charset",StandardCharsets.UTF_8.name())
      .schema(encoder.schema)
      .csv(inputPath)
      .as(encoder)
    val inputDs2:Dataset[LogSchema]=inputDataset.repartition(numOfPartitions)

  def run(): Unit = {
    val formattedDatasets=inputDs2
      .withColumn("client_host",split(inputDs2("client_host_port"),":").getItem(0))
        .withColumn("url",split(inputDs2("request")," ").getItem(1))
    //formattedDatasets.show(false)

    val datasetForSessionize=formattedDatasets.select("create_time","client_host","url")
    //datasetForSessionize.show(false)

    //TODO 1. GET total hits by an IP during an interval

    var sessionedData=datasetForSessionize.withColumn("interval", window(datasetForSessionize("create_time"), "15 minutes"))
    //persisting dataset as we will be using this multiple times.
    sessionedData.persist(StorageLevels.MEMORY_AND_DISK_SER_2)
    //sessionedData.show(false)
    val dsForTotalHits=sessionedData.groupBy(sessionedData("interval"),sessionedData("client_host")).count().as("num_hits_ip")
   // dsForTotalHits.show(50,false)
   // dsForTotalHits.write.mode(SaveMode.Overwrite).parquet("data/output/total_hits_per_session")

    //TODO 2. average session time
    //get first hit for a ip per session
    var df3=sessionedData.groupBy("interval","client_host")
      .agg(
        min("create_time").as("first_hit_time"),
        max("create_time").as("last_hit_time")
      )
    //df3.show(false)
    //get duration in seconds
    df3=df3.withColumn("session_duration",unix_timestamp(df3("last_hit_time"))-unix_timestamp(df3("first_hit_time")))
    //df3.show(false)
    //TODO find avg on durations for all data
    val avgSession = df3.groupBy().avg("session_duration")
    //avgSession.show()
    //avgSession.write.mode(SaveMode.Overwrite).parquet("data/output/avg_session_durations")

    //TODO 3. unique url visits per session , per session each url hist
    val dfURL = sessionedData.groupBy("client_host","interval","URL").count().distinct().withColumnRenamed("count", "unique_url_hits")
    //dfURL.write.mode(SaveMode.Overwrite).parquet("data/output/unique_url_visits_per_session")
    //dfURL.show(20)

    //TODO 4. ips with longest sessions/durations
     val longestSession= df3.sort(desc("session_duration")).distinct()
     longestSession.show(2)
    //longestSession.write.mode(SaveMode.Overwrite).parquet("data/output/host_with_longest_session")
  }
}
