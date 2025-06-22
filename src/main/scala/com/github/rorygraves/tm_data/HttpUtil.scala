package com.github.rorygraves.tm_data

import org.slf4j.LoggerFactory

object HttpUtil {

  private val logger = LoggerFactory.getLogger("HttpUtil")

  /** Fetches the content of a URL as a String
    *
    * @param url
    *   The URL to fetch
    * @param reject
    *   Function to reject content based on its value
    * @return
    *   The content of the URL
    */
  def get(
      url: String,
      reject: String => Boolean = _ => false
  ): Option[String] = {
    logger.info(s"Fetching from URL: $url")
    val response = requests.get(url, readTimeout = 20000, connectTimeout = 20000)
    if (response.statusCode != 200) {
      throw new Exception(
        s"Failed to fetch URL: $url, status code: ${response.statusCode}"
      )
    }
    val content = response.text()
    
    if (reject(content)) {
      None
    } else {
      Some(content)
    }
  }

}
