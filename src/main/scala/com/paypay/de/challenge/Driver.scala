package com.paypay.de.challenge

import java.io.File

import org.apache.spark.sql.SparkSession

import scala.math.min

object Driver {

  def main(args: Array[String]): Unit = {
    val args=Array[String]{"data/2015_07_22_mktplace_shop_web_log_sample.log.gz"}

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
    //TODO find number of data partitons taking 10MB as block
    val numOfPartitions=min(inFIle.length()/10000000,10).toInt

    //TODO initialize the spark session
    val spark = SparkSession.builder()
      .appName("DE-Challenge")
      .master("local[*]")
      .getOrCreate()

    val runner=new LogAnalyse(spark,inputDataPath,numOfPartitions)
    runner.run()
  }

}
