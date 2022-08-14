/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.execution.benchmark

import java.io.{File, FilenameFilter}
import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.benchmark.Utils.createDirectory
import org.slf4j.LoggerFactory

class TPCDSDatagenSuite extends SparkFunSuite {

  private val logger = LoggerFactory.getLogger(classOf[TPCDSDatagenSuite])

  test("datagen") {
//    val outputTempDir = Utils.createTempDir()

    val lakehouseDir = TestUtil.regenerateDirectory("lakehouse")

    val spark = TestUtil.hadoopSparkSession("TPCDSDatagenSuite", lakehouseDir.getAbsolutePath)

    logger.info("outputTempDir: " + lakehouseDir.getAbsolutePath)

    val tpcdsTables = new Tables(spark.sqlContext, 1)
    tpcdsTables.genData(
      partitionTables = true,
      useDoubleForDecimal = false,
      useStringForChar = false,
      tableFilter = Set("inventory"),
      numPartitions = 4)

//    analyze(spark, "call_center")
    analyzeNonPartitionedTable(spark, "inventory")
  }

  val catalog = "spark_catalog"
  val database = "default"

  def analyze(spark: SparkSession, tableName: String): Unit = {
    spark.sql(s"SELECT * FROM $catalog.$database.$tableName.files").show()

    logger.info(s"Summary for table: $tableName")
    spark.sql(
      s"""SELECT COUNT(1) number_of_files,
         |    SUM(record_count) total_records,
         |    round(sum(file_size_in_bytes)/1000000, 2) as size_in_MB,
         |    round(sum(file_size_in_bytes)/1000000000, 2) size_in_GB
         |    FROM $catalog.$database.$tableName.files""".stripMargin).show()


    val df = spark.sql(
      s"""
         |with tbl_summary (
         |    SELECT
         |        count(1) number_of_files,
         |        SUM(record_count) total_records,
         |        round(sum(file_size_in_bytes)/1000000, 2) as size_in_MB,
         |        round(sum(file_size_in_bytes)/1000000000, 2) size_in_GB
         |    FROM $catalog.$database.$tableName.files
         |    WHERE file_size_in_bytes <= 400000000 or file_size_in_bytes > 600000000
         |)
         |SELECT
         |    number_of_files, total_records, size_in_MB, size_in_GB,  ROUND((size_in_MB / number_of_files), 2) as avg_size_per_file
         |from tbl_summary
         |WHERE number_of_files > 1
         |ORDER BY number_of_files desc
         |""".stripMargin)

    if (df.count() > 0) {
      logger.warn(s"There are non-optimized files for table: $tableName")
      df.show(50, false)
    }
  }

  def analyzeNonPartitionedTable(spark: SparkSession, tableName: String): Unit = {
    spark.sql(s"SELECT * FROM $catalog.$database.$tableName.files").show()

    logger.info(s"Summary for table: $tableName")
    spark.sql(
      s"""SELECT COUNT(1) number_of_files,
         |    SUM(record_count) total_records,
         |    round(sum(file_size_in_bytes)/1000000, 2) as size_in_MB,
         |    round(sum(file_size_in_bytes)/1000000000, 2) size_in_GB
         |    FROM $catalog.$database.$tableName.files""".stripMargin).show()


    val df = spark.sql(
      s"""
         |with tbl_summary (
         |    SELECT
         |        partition,
         |        count(1) number_of_files,
         |        SUM(record_count) total_records,
         |        round(sum(file_size_in_bytes)/1000000, 2) as size_in_MB,
         |        round(sum(file_size_in_bytes)/1000000000, 2) size_in_GB
         |    FROM $catalog.$database.$tableName.files
         |    WHERE file_size_in_bytes <= 400000000 or file_size_in_bytes > 600000000
         |    GROUP BY partition
         |)
         |SELECT
         |    partition, number_of_files, total_records, size_in_MB, size_in_GB,  ROUND((size_in_MB / number_of_files), 2) as avg_size_per_file
         |FROM tbl_summary
         |WHERE number_of_files > 1
         |ORDER BY number_of_files desc
         |""".stripMargin)

    if (df.count() > 0) {
      logger.warn(s"There are non-optimized files for table: $tableName")
      df.show(50, false)
    }
  }
}
