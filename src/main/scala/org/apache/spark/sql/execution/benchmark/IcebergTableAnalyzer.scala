package org.apache.spark.sql.execution.benchmark

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory

case class IcebergTableAnalyzer(spark: SparkSession) {
  private val logger = LoggerFactory.getLogger(classOf[IcebergTableAnalyzer])

  def analyze(database: String, tableName: String, verbose: Boolean = false): Unit = {

    generalStatistics(database, tableName, verbose)

    val isPartitioned = isPartitionedTable(database, tableName)

    val unoptimizedFiles = if (isPartitioned) {
      unoptimizedFilesForPartitionedTable(database, tableName)
    } else {
      unoptimizedFilesForNonPartitionedTable(database, tableName)
    }

    if (unoptimizedFiles.count() > 0) {
      logger.warn(s"There are non-optimized files for table (partitioned: $isPartitioned): $tableName")
      unoptimizedFiles.show(50, false)
    }
  }

  private def generalStatistics(database: String, tableName: String, verbose: Boolean = false): Unit = {
    val namespace = getFullyQualifiedDatabaseName(database)
    if (verbose) {
      spark.sql(s"SELECT * FROM $namespace.$tableName.files").show()
    }

    logger.info(s"Summary for table: $tableName")
    spark.sql(
      s"""SELECT COUNT(1) number_of_files,
         |    SUM(record_count) total_records,
         |    round(sum(file_size_in_bytes)/1000000, 2) as size_in_MB,
         |    round(sum(file_size_in_bytes)/1000000000, 2) size_in_GB
         |    FROM $namespace.$tableName.files""".stripMargin).show()
  }

  private def unoptimizedFilesForNonPartitionedTable(database: String, tableName: String): DataFrame = {
    val namespace = getFullyQualifiedDatabaseName(database)

    spark.sql(
      s"""
         |with tbl_summary (
         |    SELECT
         |        count(1) number_of_files,
         |        SUM(record_count) total_records,
         |        round(sum(file_size_in_bytes)/1000000, 2) as size_in_MB,
         |        round(sum(file_size_in_bytes)/1000000000, 2) size_in_GB
         |    FROM $namespace.$tableName.files
         |    WHERE file_size_in_bytes <= 400000000 or file_size_in_bytes > 600000000
         |)
         |SELECT
         |    number_of_files, total_records, size_in_MB, size_in_GB,  ROUND((size_in_MB / number_of_files), 2) as avg_size_per_file
         |from tbl_summary
         |WHERE number_of_files > 1
         |ORDER BY number_of_files desc
         |""".stripMargin)
  }

  private def unoptimizedFilesForPartitionedTable(database: String, tableName: String): DataFrame = {
    val namespace = getFullyQualifiedDatabaseName(database)

    spark.sql(
      s"""
         |with tbl_summary (
         |    SELECT
         |        partition,
         |        count(1) number_of_files,
         |        SUM(record_count) total_records,
         |        round(sum(file_size_in_bytes)/1000000, 2) as size_in_MB,
         |        round(sum(file_size_in_bytes)/1000000000, 2) size_in_GB
         |    FROM $namespace.$tableName.files
         |    WHERE file_size_in_bytes <= 400000000 or file_size_in_bytes > 600000000
         |    GROUP BY partition
         |)
         |SELECT
         |    partition, number_of_files, total_records, size_in_MB, size_in_GB,  ROUND((size_in_MB / number_of_files), 2) as avg_size_per_file
         |FROM tbl_summary
         |WHERE number_of_files > 1
         |ORDER BY number_of_files desc
         |""".stripMargin)

  }

  private def isPartitionedTable(database: String, tableName: String): Boolean = {
    val namespace = getFullyQualifiedDatabaseName(database)

    spark.table(s"$namespace.$tableName.partitions")
      .schema.fieldNames.contains("partition")
  }

  private def getFullyQualifiedDatabaseName(databaseName: String): String = {
    if (databaseName.contains(".")) {
      // If the database name already contains a catalog name, return it as is
      databaseName
    } else {
      // If not, prepend "spark_catalog" as the catalog name
      s"spark_catalog.$databaseName"
    }
  }

}
