package com.github.rorygraves.tm_data

import com.github.rorygraves.tm_data.util.TMUtil
import org.apache.commons.csv.{CSVFormat, CSVParser}
import org.slf4j.LoggerFactory

import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.util.Locale
import scala.jdk.CollectionConverters.{IteratorHasAsScala, MapHasAsScala}

object TMDocumentDownloader {

  val refreshOverride = false
  private val logger  = LoggerFactory.getLogger(getClass)

  def reportDownloader[T](
      progYear: Int,
      month: Int,
      docType: DocumentType,
      district: Option[String],
      cacheFolder: String,
      rowTransform: (Int, Int, LocalDate, Map[String, String]) => T
  ): List[T] = {

    fetchEOMData(progYear, month, docType, district, cacheFolder) match {
      case None =>
        val districtStr = district.map("District=" + _ + ", ").getOrElse("")
        logger.info(s"$districtStr progYear=$progYear, month=$month - Skipping month - no data")
        List.empty
      case Some((asOfDate, rawFileData)) =>
        val rowData =
          csvToKeyValuePairs(
            rawFileData
          ).distinct // some months have duplicate data.. *sigh*
        rowData.map { row => rowTransform(progYear, month, asOfDate, row) }
    }
  }

  def csvToKeyValuePairs(csvContent: String): List[Map[String, String]] = {
    // Remove the last line
    val lines = csvContent.split("\n").dropRight(1).mkString("\n")

    // Parse the CSV content
    val format = CSVFormat.RFC4180
      .builder()
      .setHeader()
      .setSkipHeaderRecord(true)
      .build()
    val parser  = CSVParser.parse(lines, format)
    val records = parser.getRecords

    // Convert to list of key-value pairs
    records
      .iterator()
      .asScala
      .map { record =>
        record.toMap.asScala.toMap
      }
      .toList
  }

  val asOfDatePattern =
    """<option selected="selected" value=?"?.*?"?>As of (\d{1,2}-\w{3}-\d{4})</option>""".r

  val monthStrMap = Map(
    1  -> "<option selected=\"selected\" value=\"1\">Jan</option>",
    2  -> "<option selected=\"selected\" value=\"2\">Feb</option>",
    3  -> "<option selected=\"selected\" value=\"3\">Mar</option>",
    4  -> "<option selected=\"selected\" value=\"4\">Apr</option>",
    5  -> "<option selected=\"selected\" value=\"5\">May</option>",
    6  -> "<option selected=\"selected\" value=\"6\">Jun</option>",
    7  -> "<option selected=\"selected\" value=\"7\">Jul</option>",
    8  -> "<option selected=\"selected\" value=\"8\">Aug</option>",
    9  -> "<option selected=\"selected\" value=\"9\">Sep</option>",
    10 -> "<option selected=\"selected\" value=\"10\">Oct</option>",
    11 -> "<option selected=\"selected\" value=\"11\">Nov</option>",
    12 -> "<option selected=\"selected\" value=\"12\">Dec</option>"
  )

  /** Fetch the end of month data for a given program year, month, document type (e.g. Club) and district.
    *
    * We do this by fetching the dashboard page,
    * @param programYear
    *   The program year (e.g. 2023 - for 2023-2024)
    * @param month
    *   The month (1-12)
    * @param documentType
    *   The document type (e.g. Club)
    * @param district
    *   The district number
    * @return
    *   The content of the url as a String
    */
  def fetchEOMData(
      programYear: Int,
      month: Int,
      documentType: DocumentType,
      district: Option[String],
      cacheFolder: String
  ): Option[(LocalDate, String)] = {
    val districtIdStr = district.map("District=" + _ + " ").getOrElse("")
    logger.info(
      s"Fetching EOM data for $districtIdStr, programYear=$programYear, month=$month, docType = $documentType"
    )
    // if the program year is current do not include the year in the url
    val isCurrentProgramYear = programYear == TMUtil.currentProgramYear

    val eomDate = TMUtil.computeMonthEndDate(programYear, month)
    val refresh = eomDate.isAfter(LocalDate.now().minusMonths(2))

    val adjustedDistrictId = district.map { d =>
      val intOpt = d.toIntOption
      intOpt
        .map { i =>
          f"$i%02d"
        }
        .getOrElse(d)
    }
    val districtStr = adjustedDistrictId.map("id=" + _ + "&").getOrElse("")
    val programYearString =
      if (isCurrentProgramYear) "" else s"$programYear-${programYear + 1}/"

    val pageUrl =
      s"https://dashboards.toastmasters.org/${programYearString}${documentType.urlSegment}?${districtStr}month=$month"
    //    val pageUrl =
    //      s"https://dashboards.toastmasters.org/${programYearString}?id=21&month=$month"
    // https://dashboards.toastmasters.org/${programYearString}District.aspx?id=91&hideclub=1
    // https://dashboards.toastmasters.org/${programYearString}Division.aspx?id=91&month=1
    // https://dashboards.toastmasters.org/${programYearString}Club.aspx?id=91&month=1
//    println("pageUrl: " + pageUrl)
    // retrieve the page from pageURL and extract the string value dll_onchange value

    val pageTextOpt = HttpUtil.cachedGet(
      pageUrl,
      cacheFolder,
      refresh = refresh || refreshOverride,
      reject = !_.contains(monthStrMap(month)) // "class=\"ddl PastDate\"")
    )

    pageTextOpt match {
      case None =>
        println("PastDate rejection (from page)")
        None
      case Some(pageText) =>
        // extract the dll_onchange value from the page
        // which appears in the page like so:
        // ...<select name="ctl00$cpContent$TopControls2$ddlExport" id="cpContent_TopControls2_ddlExport" class="ddl" onchange="dll_onchange(this.value,'districtsummary~7/31/2023~8/11/2023~2023-2024', this)">
        val searchString = "dll_onchange(this.value,&#39;"
        // onchange="dll_onchange(this.value,'"
        // https://dashboards.toastmasters.org/2023-2024/?id=21&month=3
        // https://dashboards.toastmasters.org/2023-2024/?id=21&month=3
        val searchStart = pageText.indexOf(searchString)
        val tagStart    = searchStart + searchString.length
        val tagEnd      = pageText.indexOf("&#39;", tagStart)

        // look for the option value preceding the tag (which will be the last asOfDate)

        val dateFormatter =
          DateTimeFormatter.ofPattern("d-MMM-yyyy", Locale.ENGLISH)

        val asOfDate = asOfDatePattern
          .findFirstMatchIn(pageText)
          .map { m =>
            LocalDate.parse(m.group(1), dateFormatter)
          }
          .getOrElse({
            println("PAGE: " + pageText.take(1000))
            pageText.lines.forEach(
              println
            ) // filter(_.contains("selected")).forEach(println)
            throw new IllegalStateException("Unable to determine asOfDate")
          })

        val downloadName = pageText.substring(tagStart, tagEnd)

        val downloadURL =
          s"https://dashboards.toastmasters.org/${programYearString}export.aspx?type=CSV&report=" + downloadName

        val content = HttpUtil.cachedGet(downloadURL, cacheFolder, refresh = refresh).get

        Some((asOfDate, content))
    }
  }

}
