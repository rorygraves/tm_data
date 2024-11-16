package app.util

import java.time.LocalDate

object TMUtil {
  // given a program start year and a program year month compute the month end date for that month

  def currentProgramYear: Int = {
    val now = LocalDate.now
    if (now.getMonthValue < 7) now.getYear - 1 else now.getYear
  }

  def programMonthToSOMDate(programYear: Int, month: Int): LocalDate = {
    LocalDate.of(if (month < 7) programYear + 1 else programYear, month, 1)
  }

  def computeMonthEndDate(programYear: Int, month: Int): LocalDate = {
    programMonthToSOMDate(programYear, month).plusMonths(1).minusDays(1)
  }
}
