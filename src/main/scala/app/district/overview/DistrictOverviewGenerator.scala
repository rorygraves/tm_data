package app.district.overview

import app.district.overview.DistrictOverviewOutputFormat.columnGenerator
import app.{DocumentType, TMDocumentDownloader}
import org.apache.commons.csv.{CSVFormat, CSVParser, CSVPrinter}

import scala.jdk.CollectionConverters._
import java.io.{File, PrintWriter, StringReader, StringWriter}

object DistrictOverviewGenerator {

  def generateDistrictOverview(districtId: Int, year: String): Unit = {
    val data = fetchDistrictOverview(districtId, year)
    outputDistrictOverview(data, districtId)
  }

  def outputDistrictOverview(data: Seq[DistrictOverviewDataPoint], districtId: Int): Unit = {
    // output results to CSV
    val out = new StringWriter()
    val printer = new CSVPrinter(out, CSVFormat.RFC4180)
    try {
      // output the headers
      printer.printRecord(columnGenerator.map(_.name).asJava)
      // output the rows
      data.foreach { point =>
        val rowValues = columnGenerator.map(_.calculation(point))
        printer.printRecord(rowValues.asJava)
      }

      // log the output for debug
      println(out.toString.take(1500))

      val writer = new PrintWriter(new File(s"data/district_overview_$districtId.csv"))
      writer.write(out.toString)
      writer.close()

    } catch {
      case ex: Throwable =>
        ex.printStackTrace()
    } finally if (printer != null) printer.close()
  }

  def fetchDistrictOverview(districtId: Int, year: String): Seq[DistrictOverviewDataPoint] = {
    val content = TMDocumentDownloader.downloadDocument(DocumentType.Overview, districtId, year)
    
    // Find the CSV data in the content
    val csvStart = content.indexOf("REGION,DISTRICT,DSP")
    if (csvStart == -1) {
      throw new RuntimeException("Could not find CSV data in content")
    }
    
    val csvEnd = content.indexOf("\n\n", csvStart)
    val csvData = if (csvEnd == -1) content.substring(csvStart) else content.substring(csvStart, csvEnd)
    
    // Parse the CSV data
    val parser = CSVFormat.RFC4180.withFirstRecordAsHeader().parse(new StringReader(csvData))
    
    parser.getRecords.asScala.map { record =>
      val rowMap = record.toMap
      DistrictOverviewTableDef.fromCsvRow(rowMap)
    }.toSeq
  }
}