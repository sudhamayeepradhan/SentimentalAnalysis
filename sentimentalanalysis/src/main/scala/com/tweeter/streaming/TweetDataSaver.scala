package com.tweeter.streaming

import com.tweeter.streaming.TwitterSentimentAnalysis.logger
import org.apache.spark.sql.DataFrame

object TweetDataSaver {

  /**
   * This function is used to store the dataframe in gs bucket
   * @param resultDF result dataframe
   * @param path path o gs bucket
   */
  def storeDataToBucket(resultDF: DataFrame, path: String): Unit ={
    try {
      resultDF.coalesce(1).write.mode("append").option("header","true").option("sep","|").csv(path)
    } catch {
      case e: Exception =>
        logger.error("Exception has occurred " + e.getMessage)
    }
  }

  /**
   * This function is used to store the tweets to my sql database in google cloud
   * @param resultDF result dataframe
   * @param hostName hostname of the my sql database
   */
  def loadTweetsToDB(resultDF: DataFrame, hostName: String): Unit = {
    val prop = new java.util.Properties
    prop.setProperty("driver", "com.mysql.jdbc.Driver")
    prop.setProperty("user", "tweets")
    prop.setProperty("password", "tweets")
    val connectionURL = s"jdbc:mysql://$hostName:3306/tweetsdb?rewriteBatchedStatements=true"
    val table = "tweets"
    try {
      resultDF.write.mode("append").jdbc(connectionURL, table, prop)
    } catch {
      case e: Exception =>
        logger.error("Exception has occurred " + e.getMessage)
    }
  }
}
