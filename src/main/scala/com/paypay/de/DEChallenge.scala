package com.paypay.de

import java.io.File
import java.nio.charset.StandardCharsets
import java.sql.Timestamp

import scala.math.max
import org.apache.spark.sql.{Dataset, Encoders, SparkSession}
import org.apache.spark.sql.functions._


case class LogSchema(create_time:Timestamp, elb:String, client_host_port:String, backend_host_port:String, request_processing_time:Double, backend_processing_time:Double, response_processing_time:Double, elb_status_code:Int, backend_status_code:Int, received_bytes:Int, sent_bytes:Int, request:String, user_agent:String, ssl_cipher:String, ssl_protocol:String)

//timestamp elb client:port backend:port request_processing_time backend_processing_time response_processing_time elb_status_code backend_status_code received_bytes sent_bytes "request" "user_agent" ssl_cipher ssl_protocol

object DEChallenge {
  def main(args: Array[String]): Unit = {

    if(args.length<1){
      println("USAGE: <input file path>")
      sys.exit(-1)
    }
    val inputDataPath=args(0)
    //validate input path
    val inFIle=new File(inputDataPath)
    if(! inFIle.exists()){
      throw new RuntimeException(s"Input File $inputDataPath Not Found")
    }
    val numOfPartitions=max(inFIle.length()/1000000,10).toInt
    //initialize the spark session
    val spark = SparkSession.builder()
      .appName("DE-Challenge")
      .master("local[*]")
      .getOrCreate()
    val encoder = Encoders.product[LogSchema]
    //load log data into spark-dataset
    val inputDs:Dataset[LogSchema]=spark
      .read
      .option("delimiter"," ")
      .option("quote","\"")
      .option("header",false)
      .option("charset",StandardCharsets.UTF_8.name())
      .schema(encoder.schema)
      .csv(inputDataPath)
      .as(encoder)
    val inputDs2:Dataset[LogSchema]=inputDs.repartition(numOfPartitions)
    val formattedDs=inputDs2
      .withColumn("client_host",split(inputDs2("client_host_port"),":").getItem(0))
      .drop("client_host_port")
    val dataDs=formattedDs.select(formattedDs("create_time"),formattedDs("client_host"))
    dataDs.show(false)
  }
}
