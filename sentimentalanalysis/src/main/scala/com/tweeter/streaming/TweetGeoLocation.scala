package com.tweeter.streaming

import twitter4j.{Place, Status}

object TweetGeoLocation {
  /**
   * This function is used to getgeo location from the twitter status class
   * @param tweet twitter status class
   * @return returns estimated geo location in string format
   */
  def estimateTweetGeolocation(tweet: Status): String = {
    if(Option(tweet.getGeoLocation).isDefined)
      s"[${tweet.getGeoLocation.getLatitude},${tweet.getGeoLocation.getLongitude}]"
    else getApproximatePlaceGeolocation(Option(tweet.getPlace))
  }

  /**
   * This function is used to get approx geolocation
   * @param place place getting from twitter library
   * @return returns geo location in string format
   */
  private def getApproximatePlaceGeolocation(place: Option[Place]): String = {

    val isNotNull:Boolean = place.isDefined
    if(!isNotNull) return "NA"

    val boundingBoxCoordinates = place.get.getBoundingBoxCoordinates
    val latitudes = boundingBoxCoordinates.flatMap(a => a.map(_.getLatitude))
    val longitudes = boundingBoxCoordinates.flatMap(a => a.map(_.getLongitude))

    "["+((latitudes.min + latitudes.max) / 2)+","+ ((longitudes.min + longitudes.max) / 2)+"]"
  }
}
