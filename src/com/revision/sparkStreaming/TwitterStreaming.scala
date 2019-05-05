package com.revision.sparkStreaming

import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.SparkConf
import org.apache.spark.SparkConf

object TwitterStreaming {
  
  def main(args: Array[String]){
     
    Logger.getLogger("org").setLevel(Level.ERROR)
    
   val conf = new SparkConf().setMaster("local[*]").setAppName("Spark Streaming")
    // here local[*] shows that spark code will run on all possible cores.
    val ssc = new StreamingContext(conf, Seconds(10))
     
    val consumerKey = "AYlFUAbUgkLNuJcTK4ZlSVU4U"
    val consumerSecret = "gf4lBLySoYUOTMTtjAuWxV2rGDvnRv0BMxdLa4M80X7VSPBdfR"
    val accessToken = "986971830513696769-U2mYoMCfxOyDojOvn7JPbvypcgl7kmM"
    val accessTokenSecret = "fzrqsLZA9HI7TdAPgMd5mTxXUpvougAdfcb481LdfPGvQ"
    
    System.setProperty("twitter4j.oauth.consumerKey", consumerKey)
    System.setProperty("twitter4j.oauth.consumerSecret", consumerSecret)
    System.setProperty("twitter4j.oauth.accessToken", accessToken)
    System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret)
    
    val tweets = TwitterUtils.createStream(ssc, None)
    val statuses = tweets.map(_.getText())
    statuses.print()
    
    
    ssc.start()
    ssc.awaitTermination()
    
  }
  
 
}