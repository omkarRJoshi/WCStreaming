package com.revision.sparkStreaming

import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.SparkConf
import org.apache.spark.SparkConf
import scala.io.Source


object AdvancedTwitterStreaming {
  
  def main(args: Array[String]){
    
    
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    val conf = new SparkConf().setMaster("local[*]").setAppName("Spark Streaming")
    // here local[*] shows that spark code will run on all possible cores.
    val ssc = new StreamingContext(conf, Seconds(5))
    
    var consumerKey = ""
    var consumerSecret = ""
    var accessToken = ""
    var accessTokenSecret = ""
    
    val keyFile = "/home/omkar/Desktop/twitter-setup"
    val line = Source.fromFile(keyFile).getLines()
    
    for(line <- Source.fromFile(keyFile).getLines()){
       val keys = line.split(",")
       consumerKey = keys(0).toString()
       consumerSecret = keys(1).toString()
       accessToken = keys(2).toString()
       accessTokenSecret = keys(3).toString()
    }
     
   
    
    System.setProperty("twitter4j.oauth.consumerKey", consumerKey)
    System.setProperty("twitter4j.oauth.consumerSecret", consumerSecret)
    System.setProperty("twitter4j.oauth.accessToken", accessToken)
    System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret)
    
    val filters = Array("DCvsSRH", "SRHvsDC","vivoipl", "vivoipl2019")
    
    val tweets = TwitterUtils.createStream(ssc, None)
    
    val tweetMap = tweets.map(status => 
      
      "Location : "+status.getUser.getLocation +
      "\nname : "+status.getUser.getName+
      "\nhashtag : "+status.getText.split(" ").filter(_.startsWith("#")).mkString(" ") +
      "\ntweet : "+status.getText+
      "\nlanguage : "+status.getUser.getLang +
      "\ntime : "+status.getCreatedAt.getTime+
      "\nMail ID : "+status.getUser.getEmail+
      "\nURL : "+status.getURLEntities.mkString(" ")+
      "\n"
    )
    
    tweetMap.print()
      
    ssc.start()
    ssc.awaitTermination()
    
  }
  
 
}