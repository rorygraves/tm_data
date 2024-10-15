import org.apache.commons.csv.{CSVFormat, CSVParser, CSVPrinter}

import java.io.{File, PrintWriter, StringWriter}
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.util.Locale
import scala.io.Source
import scala.jdk.CollectionConverters.{
  IterableHasAsJava,
  IteratorHasAsScala,
  MapHasAsScala
}

object Main {

  val cacheFolder = "/Users/rory.graves/Downloads/tm_cache"

  trait ColumnCalculator {
    def name: String
    def calculation: TMClubDataPoint => String
  }

  // create a 2dp decimal formatter
  val decimalFormatter = new java.text.DecimalFormat("#.##")

  case class ColumnCalculation(
      name: String,
      calculation: TMClubDataPoint => String
  ) extends ColumnCalculator

  val clubColumnGenerator: List[ColumnCalculator] = List[ColumnCalculator](
    ColumnCalculation("District", t => t.district),
    ColumnCalculation("Division", t => t.division),
    ColumnCalculation("Area", t => t.area),
    ColumnCalculation("ProgramYear", t => t.programYear.toString),
    ColumnCalculation("Month", t => t.month.toString),
    ColumnCalculation("AsOfDate", t => t.asOfDate.toString),
    ColumnCalculation("ClubNumber", t => t.clubNumber),
    ColumnCalculation("ClubName", t => t.clubName),
    ColumnCalculation("ClubStatus", t => t.clubStatus),
    ColumnCalculation("BaseMembers", t => t.memBase.toString),
    ColumnCalculation("ActiveMembers", t => t.activeMembers.toString),
    ColumnCalculation("GoalsMet", t => t.goalsMet.toString),
    ColumnCalculation("CCs", t => t.dcpData.oldCCs.toString),
    ColumnCalculation("CCsAdd", t => t.dcpData.oldCCsAdd.toString),
    ColumnCalculation("ACs", t => t.dcpData.oldACs.toString),
    ColumnCalculation("ACsAdd", t => t.dcpData.oldACsAdd.toString),
    ColumnCalculation("Ldr", t => t.dcpData.oldLeaders.toString),
    ColumnCalculation("LdrsAdd", t => t.dcpData.oldLeadersAdd.toString),
    ColumnCalculation("Level1s", t => t.dcpData.level1s.toString),
    ColumnCalculation("Level2s", t => t.dcpData.level2s.toString),
    ColumnCalculation("Level2sAdd", t => t.dcpData.level2sAdd.toString),
    ColumnCalculation("Level3s", t => t.dcpData.level3s.toString),
    ColumnCalculation("Level45DTMs", t => t.dcpData.level4s.toString),
    ColumnCalculation("Level45DTMsAdd", t => t.dcpData.level5s.toString),
    ColumnCalculation("NewMembers", t => t.dcpData.newMembers.toString),
    ColumnCalculation("AddNewMembers", t => t.dcpData.addNewMembers.toString),
    ColumnCalculation(
      "OfficersTrainedRd1",
      t => t.dcpData.officersTrainedRd1.toString
    ),
    ColumnCalculation(
      "OfficersTrainedRd2",
      t => t.dcpData.officersTrainedRd2.toString
    ),
    ColumnCalculation("MembersDuesOnTimeOct", t => t.memDuesOnTimeOct.toString),
    ColumnCalculation("MembersDuesOnTimeApr", t => t.memDuesOnTimeApr.toString),
    ColumnCalculation(
      "OfficerListOnTime",
      t => t.dcpData.officerListOnTime.toString
    ),
    ColumnCalculation("COTMet", t => t.cotMet.toString),
    ColumnCalculation("Goal10Met", t => t.goal10Met.toString),
    ColumnCalculation("DistinguishedStatus", t => t.clubDistinctiveStatus),
    ColumnCalculation("MembersGrowth", t => t.membershipGrowth.toString),
    ColumnCalculation(
      "AwardsPerMember",
      t => decimalFormatter.format(t.awardsPerMember)
    ),
    ColumnCalculation("DCPEligibility", t => t.dcpEligability.toString),
    ColumnCalculation("MonthlyGrowth", _ => "XXX"),
    ColumnCalculation("30SeptMembers", _ => "XXX"),
    ColumnCalculation("31MarMembers", _ => "XXX"),
    ColumnCalculation("Region", _ => "XXX"),
    ColumnCalculation("NovADVisit", _ => "XXX"),
    ColumnCalculation("MarADVisit", _ => "XXX")
  )

  def sourceIterator(
      startYear: Int,
      endYear: Int,
      docType: DocumentType,
      district: Int
  )(f: TMClubDataPoint => Unit): Unit = {
    // iterate over each toastmasters year, first fetch months 7-12 then 1-6 to align with the TM year (July to June)
    for (year <- startYear to endYear) {
      for (month <- List(7, 8, 9, 10, 11, 12, 1, 2, 3, 4, 5, 6)) {
        fetchEOMData(year, month, docType, district) match {
          case None =>
            println("Skipping month  " + month + " for program year " + year)
          case Some((asOfDate, rawFileData)) =>
            val rowData = csvToKeyValuePairs(rawFileData)
            rowData.foreach { row =>
              val point = TMClubDataPoint.fromDistrictClubReportCSV(
                year,
                month,
                asOfDate,
                row
              )
              f(point)
            }
        }
      }
    }

  }
  def main(args: Array[String]): Unit = {

//    https://dashboards.toastmasters.org/2010-2011/Club.aspx?id=21&month=11
//    https://dashboards.toastmasters.org/2012-2013//Club.aspx?id=91&month=7

    try {
      val out = new StringWriter()
      val printer = new CSVPrinter(out, CSVFormat.RFC4180)
      try {
        printer.printRecord(clubColumnGenerator.map(_.name).asJava)

        sourceIterator(2012, 2024, DocumentType.Club, 91) { tmclubpoint =>
          // only calculate values for one club right now
//          if (tmclubpoint.clubNumber == "00002390") {
          val rowValues = clubColumnGenerator.map(_.calculation(tmclubpoint))
          printer.printRecord(rowValues.asJava)
//          }
        }

        println(out.toString.take(2000))

        // write out.toString to a data/club_data.csv file
        val writer = new PrintWriter(new File("data/club_data.csv"))
        writer.write(out.toString)
        writer.close()

      } catch {
        case ex: Throwable =>
          ex.printStackTrace()
      } finally if (printer != null) printer.close()
    }

//    println(clubColumnGenerator.map(_.name).mkString(","))
//
//    sourceIterator(2012, 2024, DocumentType.Club, 91) { row =>
//      val rowValues = clubColumnGenerator.map(_.calculation(row))
//      println(rowValues.mkString(","))
//    }

//    // iterate over each toastmasters year, first fetch months 7-12 then 1-6 to align with the TM year (July to June)
//    for (year <- 2012 to 2024) {
//      for (month <- List(7, 8, 9, 10, 11, 12, 1, 2, 3, 4, 5, 6)) {
//        val rawFileData = fetchEOMData(year, month, DocumentType.Club, 91)
////        println("Raw file data: " + rawFileData.take(1000))
//        val rowData = csvToKeyValuePairs(rawFileData)
//        println("Row data: " + rowData.take(1))
//
//      }
//    }
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
    val parser = CSVParser.parse(lines, format)
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

  val asOfDatePattern =
    """<option selected="selected" value=?"?.*?"?>As of (\d{1,2}-\w{3}-\d{4})</option>""".r

  val monthStrMap = Map(
    1 -> "<option selected=\"selected\" value=\"1\">Jan</option>",
    2 -> "<option selected=\"selected\" value=\"2\">Feb</option>",
    3 -> "<option selected=\"selected\" value=\"3\">Mar</option>",
    4 -> "<option selected=\"selected\" value=\"4\">Apr</option>",
    5 -> "<option selected=\"selected\" value=\"5\">May</option>",
    6 -> "<option selected=\"selected\" value=\"6\">Jun</option>",
    7 -> "<option selected=\"selected\" value=\"7\">Jul</option>",
    8 -> "<option selected=\"selected\" value=\"8\">Aug</option>",
    9 -> "<option selected=\"selected\" value=\"9\">Sep</option>",
    10 -> "<option selected=\"selected\" value=\"10\">Oct</option>",
    11 -> "<option selected=\"selected\" value=\"11\">Nov</option>",
    12 -> "<option selected=\"selected\" value=\"12\">Dec</option>"
  )

  /** Fetch the end of month data for a given program year, month, document type (e.g. Club) and district.
    *
    * We do this by fetching the dashboard page,
    * @param programYear The program year (e.g. 2023 - for 2023-2024)
    * @param month The month (1-12)
    * @param documentType The document type (e.g. Club)
    * @param district The district number
    * @return The content of the url as a String
    */
  def fetchEOMData(
      programYear: Int,
      month: Int,
      documentType: DocumentType,
      district: Int
  ): Option[(LocalDate, String)] = {
    // if the program year is current do not include the year in the url
    val currentDate = java.time.LocalDate.now()
    val currentProgramYear =
      (currentDate.getMonth.getValue > 6) && (programYear == currentDate.getYear.toInt) ||
        (currentDate.getMonth.getValue <= 6 && (programYear == currentDate.getYear.toInt - 1))

    val programYearString =
      if (currentProgramYear) "" else s"${programYear}-${programYear + 1}/"

    val pageUrl =
      s"https://dashboards.toastmasters.org/${programYearString}${documentType.urlSegment}?id=$district&month=$month"
//    val pageUrl =
//      s"https://dashboards.toastmasters.org/${programYearString}?id=21&month=$month"
    // https://dashboards.toastmasters.org/${programYearString}District.aspx?id=91&hideclub=1
    // https://dashboards.toastmasters.org/${programYearString}Division.aspx?id=91&month=1
    // https://dashboards.toastmasters.org/${programYearString}Club.aspx?id=91&month=1
    println("pageUrl: " + pageUrl)
    //retrieve the page from pfgfdageURL and extract the string value dll_onchange value
    val pageTextOpt = cachedGet(
      pageUrl,
      cacheFolder,
      reject = !_.contains(monthStrMap(month)) //"class=\"ddl PastDate\"")
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
        val tagStart = searchStart + searchString.length
        val tagEnd = pageText.indexOf("&#39;", tagStart)
        searchStart

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
            ) //filter(_.contains("selected")).forEach(println)
            throw new IllegalStateException("Unable to determine asOfDate")
          })

        val downloadName = pageText.substring(tagStart, tagEnd)
        println("downloadName: " + downloadName)

        val downloadURL =
          s"https://dashboards.toastmasters.org/${programYearString}/export.aspx?type=CSV&report=" + downloadName

        println("downloadURL: " + downloadURL)
        val content = cachedGet(downloadURL, cacheFolder).get

        Some((asOfDate, content))

    }
  }
}
