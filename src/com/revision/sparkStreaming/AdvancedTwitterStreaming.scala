package com.revision.sparkStreaming

import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.SparkConf
import org.apache.spark.SparkConf
import scala.io.Source
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import twitter4j.Status
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream


object AdvancedTwitterStreaming {
  
   val conf = new SparkConf().setMaster("local[*]").setAppName("Spark Streaming")
    // here local[*] shows that spark code will run on all possible cores.
    val ssc = new StreamingContext(conf, Seconds(5))
    
    var consumerKey = ""
    var consumerSecret = ""
    var accessToken = ""
    var accessTokenSecret = ""
    
    val keyFile = "/home/omkar/Desktop/twitterSetup"
    val line = Source.fromFile(keyFile).getLines()
    
    for(line <- Source.fromFile(keyFile).getLines()){
       val keys = line.split(",")
       consumerKey = keys(0).toString()
       consumerSecret = keys(1).toString()
       accessToken = keys(2).toString()
       accessTokenSecret = keys(3).toString()
    }
   
    def tweetConversionFunction(tweets : Status) : (String, String, String, String, String) = {
       
             val name = tweets.getUser.getName
             val location = tweets.getUser.getLocation
             val lang = tweets.getLang 
             val tweet = tweets.getText
             val hashtag = tweet.split(" ").filter(_.startsWith("#")).toString()
             val latitude = tweets.getGeoLocation.getLatitude
             val longitude = tweets.getGeoLocation.getLongitude
             
             (name, location, lang, tweet, hashtag)
       
     }
    
  def main(args: Array[String]){
    
    
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    System.setProperty("twitter4j.oauth.consumerKey", consumerKey)
    System.setProperty("twitter4j.oauth.consumerSecret", consumerSecret)
    System.setProperty("twitter4j.oauth.accessToken", accessToken)
    System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret)
    
    val filters = Array("EngladvWI", "WIvEngland")
    
    val tweets = TwitterUtils.createStream(ssc, None)
    
    val tweetConversion = tweets.map(tweetConversionFunction)
    
    tweetConversion.print()
      
    ssc.start()
    ssc.awaitTermination()
    
  }
  
 
}