package com.tweeter.streaming

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.twitter.TwitterUtils
import org.slf4j.LoggerFactory
import twitter4j.{FilterQuery, GeoLocation, Place, Status}
import java.sql.{Date, Timestamp}
import com.tweeter.streaming.TweetGeoLocation._
import com.tweeter.streaming.TweetDataSaver._
import org.apache.spark.storage.StorageLevel

case class Tweeter(id: Long, tweet_date: Timestamp, message: String, user_id: Long,
                   loc: String, favourite: Boolean, favourite_Count: Int, retweeted: Boolean, retweet_count: Int,
                   sensitive: Boolean, verified: Boolean, hashtags: String//,geo:String,
                   ,country:String,country_code:String,place:String,place_type:String,geo_coordinate:String
                  )

object TwitterSentimentAnalysis {

  val logger = LoggerFactory.getLogger(TwitterSentimentAnalysis.getClass)

  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)

    //Initializing the parameters
    var destinationLoc = ""
    var tracks: Seq[String] = Seq.empty
    if (args.length > 0) {
      destinationLoc = args(0).trim
      tracks = args(1).trim.split(",").toSeq
      println("HostName :: " + destinationLoc + " :: \n" + "HashTags :: " + args(1)+" ::")
    }
    else {
      logger.error("provide valid no of argument")
      System.exit(0)
    }

    //Creating spark session
    val sparkSession = SparkSession.builder().master("yarn").appName("SentimentalAnalysis").getOrCreate()
    import sparkSession.implicits._

    //Setting tweeter authentication properties
    System.setProperty("twitter4j.oauth.consumerKey", "6VDeu4W5Uoy7D9jVUz13Q4UTq")
    System.setProperty("twitter4j.oauth.consumerSecret", "Yv3iVz8AFnxjbBn2qtzeJaoGqzVbhme9isLRDVbKo6A0xZiI5B")
    System.setProperty("twitter4j.oauth.accessToken", "1103351796486062080-53BFBfHJ47mpB4VjNAm2xufPUBFuyh")
    System.setProperty("twitter4j.oauth.accessTokenSecret", "E5NojYxdv7t0Yg3h6SwAVdCdbzQR1ISzzbR4yCUTIPQoF")

    //Creating spark streaming context
    val ssc:StreamingContext = new StreamingContext(sparkSession.sparkContext, Seconds(120))
    logger.info("streaming context has been created successfully")
    //Creating check pointing for spark streaming
    ssc.checkpoint("hdfs:///user/spark/sentimentaanalysis.log")

    //Defining filter query for spark streaming
    val filters = new FilterQuery().track(tracks: _*).language("en")//.locations(locations: _*)
    try {

      val streamData: ReceiverInputDStream[Status] = TwitterUtils.createFilteredStream(ssc, None, Option(filters), StorageLevel.MEMORY_AND_DISK_SER_2)
      streamData.foreachRDD(rdd=>{
      val tweetRDD= rdd.filter(tweet=>tweet.getText != null && tweet.getUser.getLocation != null).filter(_.getPlace.getCountryCode == "GB").
        map(status=>{
        val coordinate:String =estimateTweetGeolocation(status)
        val place:String = Option(status.getPlace).map(_.getName).getOrElse("NA")
        val country:String=Option(status.getPlace).map(_.getCountry).getOrElse("NA")
        val countryCode:String=Option(status.getPlace).map(_.getCountryCode).getOrElse("NA")
        val placeType:String=Option(status.getPlace).map(_.getPlaceType).getOrElse("NA")

        Tweeter(status.getId, new Timestamp(status.getCreatedAt.getTime),
          status.getText.replaceAll("[\\p{Cntrl}&&[^\r\n\t]]", " ")
            //.replace('\t', ' ').replace('|', ' ')
            //.replaceAll("(@[A-Za-z0-9]+)|(https([^\\s]+).*)|(RT.+?(?=\\s)\\s)|([^0-9A-Za-z \\t])|(\\w+:\\/\\/\\S+)|([^\\x00-\\x7F]+)",
            .replaceAll("(@[A-Za-z0-9]+)|(https([^\\s]+).*)|([^0-9A-Za-z \\t])|(\\w+:\\/\\/\\S+)|([^\\x00-\\x7F]+)",
              " "),
          status.getUser.getId, status.getUser.getLocation, status.isFavorited,
          status.getFavoriteCount, status.isRetweeted, status.getRetweetCount, status.isPossiblySensitive,
          status.getUser.isVerified, status.getHashtagEntities.map(hashTag => hashTag.getText).mkString(","),
          country,countryCode,place,placeType,coordinate)
      })
        storeDataToBucket(tweetRDD.toDF().filter(col("country_code") === "GB"), destinationLoc)
      })

    } catch {
      case e: Exception =>
        logger.error("Exception has occurred " + e.getMessage)
    }
    ssc.start()
    ssc.awaitTermination()
  }

}

