package com.revision.sparkStreaming

import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.SparkConf
import org.apache.spark.SparkConf
import com.revision.twitterStreaming.TwitterSetUp

object TwitterStreaming {
  
  def main(args: Array[String]){
    
    val setUp = new TwitterSetUp(); // TwitterSetUp is a class created in another project at which keys of twitter are set
                                    // For Security purpose
    Logger.getLogger("org").setLevel(Level.ERROR)
    
   val conf = new SparkConf().setMaster("local[*]").setAppName("Spark Streaming")
    // here local[*] shows that spark code will run on all possible cores.
    val ssc = new StreamingContext(conf, Seconds(10))
     
    val consumerKey = setUp.consumerKey
    val consumerSecret = setUp.consumerSecret
    val accessToken = setUp.accessToken
    val accessTokenSecret = setUp.accessTokenSecret
    
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