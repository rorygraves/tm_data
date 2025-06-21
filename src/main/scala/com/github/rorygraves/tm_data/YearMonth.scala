package com.github.rorygraves.tm_data

case class YearMonth(year: Int, month: Int) {
  def toYYYYMM: String = f"$year-$month%02d"

}
