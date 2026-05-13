package com.github.rorygraves.tm_data.util

object DistrictUtil {

  def cleanDistrict(str: String): String = {
    if (str.startsWith("0")) cleanDistrict(str.drop(1)).trim
    else
      str
  }

  def padDistrictId(str: String): String = {
    val trimmed = str.trim
    if (trimmed.nonEmpty && trimmed.forall(_.isDigit)) f"${trimmed.toInt}%02d"
    else trimmed
  }
}
