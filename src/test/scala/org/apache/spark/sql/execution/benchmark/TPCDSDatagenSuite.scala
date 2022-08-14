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
import org.slf4j.LoggerFactory

class TPCDSDatagenSuite extends SparkFunSuite {

  private val logger = LoggerFactory.getLogger(classOf[TPCDSDatagenSuite])

  test("datagen") {
    val outputTempDir = Utils.createTempDir()

    val spark = SparkSession
      .builder()
      .appName("IntegrationTest")
      .master("local")
      .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
      .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")
      .config("spark.sql.catalog.spark_catalog.type", "hadoop")
      .config("spark.sql.catalog.spark_catalog.warehouse", outputTempDir.getAbsolutePath)
      .config("spark.sql.catalog.default_iceberg", "org.apache.iceberg.spark.SparkCatalog")
      .config("spark.sql.catalog.default_iceberg.type", "hadoop")
      .config("spark.sql.catalog.default_iceberg.warehouse", outputTempDir.getAbsolutePath)
      .config("spark.sql.legacy.createHiveTableByDefault", "false")
      .config("spark.sql.sources.default", "iceberg")
      .getOrCreate()

    logger.info("outputTempDir: " + outputTempDir.getAbsolutePath)

    val tpcdsTables = new Tables(spark.sqlContext, 1)
    tpcdsTables.genData(
      location = outputTempDir.getAbsolutePath,
      format = "iceberg",
      overwrite = false,
      partitionTables = true,
      useDoubleForDecimal = false,
      useStringForChar = false,
      clusterByPartitionColumns = false,
      filterOutNullPartitionValues = false,
      tableFilter = Set("call_center", "catalog_page"),
      numPartitions = 4)

    val tpcdsExpectedTables = Set(
      "call_center", "catalog_page")

    assert(outputTempDir.list.toSet === tpcdsExpectedTables)
  }
}
