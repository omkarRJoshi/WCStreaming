package com.revision.sparkStreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import org.apache.log4j.Logger
import org.apache.log4j.Level

object WordCountStreaming {
  def main(args : Array[String]){
    Logger.getLogger("org").setLevel(Level.ERROR)
    // above line is for hiding logs, which will see output clearly without interruption big red lines
    // Use of above line is not compulsory
    
    val conf = new SparkConf().setMaster("local[*]").setAppName("Spark Streaming")
    // here local[*] shows that spark code will run on all possible cores.
    val ssc = new StreamingContext(conf, Seconds(1))
    val lines = ssc.socketTextStream("localhost", 8899)
    // input stream will be taken from port no. 889 of localhost.
    // type nc -lk <port_no> on terminal to give input stream.
    val count = lines.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)
    
    count.print()
    
    ssc.start()            //starts computation
    ssc.awaitTermination() //wait for the computation to terminate
    
  }
}