package app.util

import java.text.DecimalFormat

object FormatUtil {

  // create a 2dp decimal formatter
  val df2dp: DecimalFormat = new java.text.DecimalFormat("#.##")
  // create a 4dp decimal formatter
  val df4dp: DecimalFormat = new java.text.DecimalFormat("#.####")
  // create a 2dp decimal formatter
  val df5dp: DecimalFormat = new java.text.DecimalFormat("#.#####")
}
