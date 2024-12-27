package com.github.rorygraves.tm_data.util

import kantan.csv.{CellCodec, DecodeResult}

import java.time.LocalDate

object CSVCodec {

  private val defaultDate: LocalDate = LocalDate.parse("1970-01-01")

  val flexibleLocalDateCodec: CellCodec[LocalDate] = {
    val jd1 = java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd")
    val jd3 = java.time.format.DateTimeFormatter.ofPattern("[dd][d]/[MM][M]/yyyy")
    CellCodec.from(s =>
      DecodeResult(
        if (s == "") defaultDate
        else if (s.contains("-")) java.time.LocalDate.parse(s, jd1)
        else
          java.time.LocalDate.parse(s, jd3)
      )
    )(d => jd1.format(d))
  }

  implicit val localDateCodec: CellCodec[LocalDate] = flexibleLocalDateCodec

}
