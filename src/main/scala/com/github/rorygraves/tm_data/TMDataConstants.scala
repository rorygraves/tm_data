package com.github.rorygraves.tm_data

import java.time.LocalDate
import org.slf4j.LoggerFactory

object TMDataConstants {
  val START_YEAR = 2020

  private val logger = LoggerFactory.getLogger("TMDataConstants")

  @volatile private var recentMonthsThreshold: Int = 2

  def setRecentMonthsThreshold(months: Int): Unit = {
    require(months >= 0, "Recent months threshold must be non-negative")
    recentMonthsThreshold = months
    logger.info(s"tm_data recent months threshold set to $months month(s)")
  }

  def getRecentMonthsThreshold: Int = recentMonthsThreshold

  def isRecentMonth(targetMonth: LocalDate): Boolean = {
    val threshold = recentMonthsThreshold
    targetMonth.isAfter(LocalDate.now().minusMonths(threshold))
  }
}
