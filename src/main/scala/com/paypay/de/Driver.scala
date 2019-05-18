package com.paypay.de

import java.io.File

import org.apache.spark.sql.SparkSession

import scala.math.max

object Driver {

  def main(args: Array[String]): Unit = {
    val args=Array[String]{"data/2015_07_22_mktplace_shop_web_log_sample_100.log"}

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

    val runner=new LogAnalyse(spark,inputDataPath,numOfPartitions)
    runner.run()
  }

}
