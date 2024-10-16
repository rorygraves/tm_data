package app

import java.io.{File, PrintWriter}
import scala.io.Source

object HttpUtil {

  /** Fetches the content of a URL as a String, using a cached version if available, otherwise fetch
    *  file, cache and return
    *
    * @param url The URL to fetch
    * @param cacheFolder The folder to store the cache files
    * @return The content of the URL
    */
  def cachedGet(
      url: String,
      cacheFolder: String,
      reject: String => Boolean = _ => false
  ): Option[String] = {
    val cacheFile = new File(cacheFolder, url.hashCode.toString)

    println("Cache file = " + cacheFile)
    if (cacheFile.exists()) {
      println(s"  Fetching from cache: $cacheFile")
      // Read from cache
      val source = Source.fromFile(cacheFile)
      try {
        val content = source.mkString
        if (reject(content)) {
          None
        } else
          Some(content)
      } finally {
        source.close()
      }
    } else {
      // Fetch from URL
      println(s"  Fetched content from URL: $url")
      val response = requests.get(url)
      if (response.statusCode != 200) {
        throw new Exception(
          s"Failed to fetch URL: $url, status code: ${response.statusCode}"
        )
      }
      val content = response.text

      println(s"  Caching content to: $cacheFile")
      // Cache the result
      val writer = new PrintWriter(cacheFile)
      try {
        writer.write(content)
      } finally {
        writer.close()
      }

      if (reject(content)) {
        None
      } else {
        Some(content)
      }
    }
  }

}
