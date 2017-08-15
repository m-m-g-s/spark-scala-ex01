package io.mmgs.ex01

import java.io.{File, PrintWriter}
import java.util
import java.util.LinkedHashMap

import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.functions.{to_date, to_timestamp}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.yaml.snakeyaml.Yaml

import scala.io.Source

class AppConfig(val sparkAppName: String,
                val sparkMaster: String,
                val dataModelFile: String,
                val dataFile: String,
                val dataHeaderExists: String,
                val dataDelimiter: String,
                val dataQuote: String,
                val dataDateFormat: String,
                val dataTimestampFormat: String,
                val reportFile: String)

object AppConfig {
  def fromFile(path: String): AppConfig = {
    val yamlConfig = (new Yaml).load(Source.fromInputStream(getClass.getResourceAsStream(s"""/$path""")).mkString)

    val document = yamlConfig.asInstanceOf[util.LinkedHashMap[String, String]]
    val sparkAppName = document.getOrDefault("sparkAppName", "SimpleReport")
    val sparkMaster = document.getOrDefault("sparkMaster", "local[*]")
    val dataModelFile = document.get("dataModel")
    val dataFile = document.get("dataFile")
    val dataHeaderExists = String.valueOf(document.getOrDefault("dataHeaderExists", "true"))
    val dataDelimiter = document.getOrDefault("dataDelimiter", ",")
    val dataQuote = document.getOrDefault("dataQuote", "'")
    val dataDateFormat = document.getOrDefault("dataDateFormat", "dd-MM-yyyy")
    val dataTimestampFormat = document.getOrDefault("dataTimestampFormat", "dd-MM-yyyy hh:mm:ss")
    val reportFile = document.get("reportFile")

    new AppConfig(sparkAppName,
      sparkMaster,
      dataModelFile,
      dataFile,
      dataHeaderExists,
      dataDelimiter,
      dataQuote,
      dataDateFormat,
      dataTimestampFormat,
      reportFile)
  }
}

/**
  * Created by mmgs on 8/11/17.
  */
object SimpleReport extends App {

  val appConfig = AppConfig.fromFile("config.yaml")

  val sparkSession = SparkSession.builder()
    .appName(appConfig.sparkAppName)
    .master(appConfig.sparkMaster)
    .getOrCreate()

  val dataModel = Schema.fromFile(appConfig.dataModelFile)

  val dataDF: DataFrame = sparkSession
    .read
    .format("com.databricks.spark.csv")
    .option("header", appConfig.dataHeaderExists)
    .option("delimiter", appConfig.dataDelimiter)
    .option("quote", appConfig.dataQuote)
    .option("inferSchema", "false")
    .option("nullValue", "null")
    .option("mode", "PERMISSIVE")
    .load(appConfig.dataFile)

  //  dataDF.printSchema()
  //  dataDF.show()

  private def containsEmpty(row: Row): Boolean = {
    val len = row.length
    var i = 0
    while (i < len) {
      if (row.get(i) != null && row.getString(i).trim == "") {
        return true
      }
      i += 1
    }
    false
  }

  val filteredDF = dataDF.filter(!containsEmpty(_))

  val fromFields = filteredDF.schema.fields

  val selectColumns = for (i <- dataModel.columns.indices;
                           fromName = fromFields(i).name;
                           toName = dataModel.columns(i).name;
                           toType = CatalystSqlParser.parseDataType(dataModel.columns(i).dataType);
                           value = filteredDF(fromName);
                           newValue = (toType match {
                             case DateType => to_date(value, appConfig.dataDateFormat)
                             case TimestampType => to_timestamp(value, "dd-MM-yyyy HH:mm:SS")
                             case _ => value.cast(toType)
                           }).alias(toName)
  )
    yield newValue

  val formattedDF = filteredDF.select(selectColumns: _*)

  //  formattedDF.printSchema()
  //  formattedDF.show()

  val report: Array[ColumnReport] = for (column <- filteredDF.columns;
                                         columnDf = filteredDF.select(column).filter(!_.isNullAt(0));
                                         cnt = columnDf.distinct().count();
                                         byValue = columnDf
                                           .select(column)
                                           .groupBy(column)
                                           .count()
                                           .collect()
                                           .map(row => Value(row.get(0).asInstanceOf[String], row.get(1).asInstanceOf[Long]))

  ) yield ColumnReport(column, cnt, byValue)

  sparkSession.stop()

  val writer = new PrintWriter(new File(appConfig.reportFile))
  report.foreach(item => writer.println(item.toJson()))
  writer.close()

}