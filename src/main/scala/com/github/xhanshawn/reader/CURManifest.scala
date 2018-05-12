package com.github.xhanshawn.reader

import java.sql.Timestamp

case class CURManifest(assemblyId: String,
                       reportId: String,
                       account: String,
                       billingPeriod: BillingPeriod,
                       columns: Array[CURColumn],
                       reportName: String,
                       reportKeys: Array[String],
                       formatData: FormatData
                      ) {
  val reportKeyRegex = s"(.*)/$reportName-[0-9]+.${formatData.extension}".r
  def reportPrefix: Option[String] = {
    val prefixes = reportKeys.flatMap(
      _ match {
        case reportKeyRegex(prefix) => Some(prefix)
        case _ => None
      }
    ).distinct
    if (prefixes.length == 1)
      Some(prefixes(0))
    else
      None
  }
  //  val curParts = reportKeys.map(CURPart(path, _))
}

case class CURPart(bucket: String, reportKey: String) {
}

case class CURColumn(category: String, name: String)
case class BillingPeriod(start: Timestamp, end: Timestamp)
case class FormatData(charset: String, compression: String, contentType: String) {
  val extension: String = {
    val format = contentType match {
      case "text/csv" => "csv"
      case _ => throw new IllegalArgumentException(s"Unsupported CUR content type $contentType")
    }
    val zipType = compression match {
      case "GZIP" => "gz"
      case "ZIP" => "zip"
      case _ => throw new IllegalArgumentException(s"Unsupported CUR compression type $compression")
    }
    s"$format.$zipType"
  }
}
