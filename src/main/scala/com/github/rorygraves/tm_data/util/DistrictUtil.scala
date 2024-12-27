package com.github.rorygraves.tm_data.util

object DistrictUtil {

  def cleanDistrict(str: String): String = {
    if (str.startsWith("0")) cleanDistrict(str.drop(1)).trim
    else
      str
  }
}
