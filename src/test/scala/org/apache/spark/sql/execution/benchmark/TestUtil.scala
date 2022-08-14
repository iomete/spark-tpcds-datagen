package org.apache.spark.sql.execution.benchmark

import org.apache.spark.sql.SparkSession

object TestUtil {

  def hadoopSparkSession(appName: String, warehouseLocation: String): SparkSession = {
    SparkSession
      .builder()
      .appName(appName)
      .master("local")
      .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
      .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")
      .config("spark.sql.catalog.spark_catalog.type", "hadoop")
      .config("spark.sql.catalog.spark_catalog.warehouse", warehouseLocation)
      .config("spark.sql.catalog.default_iceberg", "org.apache.iceberg.spark.SparkCatalog")
      .config("spark.sql.catalog.default_iceberg.type", "hadoop")
      .config("spark.sql.catalog.default_iceberg.warehouse", warehouseLocation)
      .config("spark.sql.legacy.createHiveTableByDefault", "false")
      .config("spark.sql.sources.default", "iceberg")
      .getOrCreate()
  }

}
