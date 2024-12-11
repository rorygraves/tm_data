package com.github.rorygraves.tm_data.data.district

trait DistrictImportResult

case class SuccessfulDistrictImportResult(
    districtId: String,
    clubCount: Int,
    clubInfoCount: Int
) extends DistrictImportResult {

  override def toString: String =
    s"DistrictImportResult($districtId, $clubCount, $clubInfoCount)"

}

case class FailedDistrictImportResult(districtId: String, reason: String) extends DistrictImportResult {

  override def toString: String = s"FailedDistrictImportResult($districtId, $reason)"

}
