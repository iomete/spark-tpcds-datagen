package org.apache.spark.sql.execution.benchmark

import org.apache.spark.sql.SparkSession

import java.io.File

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
      .config("spark.sql.legacy.createHiveTableByDefault", "false")
      .config("spark.sql.sources.default", "iceberg")
      .getOrCreate()
  }


  def regenerateDirectory(dir: String): File = {
    deleteDirectories(dir)

    val file = new File(dir)
    if (file.exists()) {
      file.delete()
    }
    file.mkdirs()
    file
  }

  private def deleteDirectories(parentDir: String): Unit = {
    val dir = new java.io.File(parentDir)
    if (dir.exists() && dir.isDirectory) {
      val children = dir.listFiles()
      if (children != null) {
        for (child <- children) {
          if (child.isDirectory) {
            deleteDirectories(child.getAbsolutePath)
          } else {
            child.delete()
          }
        }
      }
    }
    dir.delete()
  }

}
