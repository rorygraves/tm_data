package com.github.rorygraves.tm_data

sealed trait DocumentType {
  def urlSegment: String
  def args: String
}

object DocumentType {
  // https://dashboards.toastmasters.org/2020-2021/?id=91&progYear=2020-2021&hideclub=1
  case object Overview extends DocumentType {
    val urlSegment: String = ""
    val args: String = "hideclub=1"
  }

  // https://dashboards.toastmasters.org/2020-2021/District.aspx?id=15&hideclub=1
  case object DistrictPerformance extends DocumentType {
    val urlSegment: String = "District.aspx"
    val args: String = ""
  }

  // https://dashboards.toastmasters.org/2020-2021/Division.aspx?id=15
  case object Division extends DocumentType {
    val urlSegment: String = "Division.aspx"
    val args: String = ""
  }

  // https://dashboards.toastmasters.org/2020-2021/Club.aspx?id=15
  case object Club extends DocumentType {
    val urlSegment: String = "Club.aspx"
    val args: String = ""
  }

}
