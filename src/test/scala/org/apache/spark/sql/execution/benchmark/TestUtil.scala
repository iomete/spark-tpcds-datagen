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
      .config("spark.sql.catalog.default_iceberg", "org.apache.iceberg.spark.SparkCatalog")
      .config("spark.sql.catalog.default_iceberg.type", "hadoop")
      .config("spark.sql.catalog.default_iceberg.warehouse", warehouseLocation)
      .config("spark.sql.legacy.createHiveTableByDefault", "false")
      .config("spark.sql.sources.default", "iceberg")
      .getOrCreate()
  }

  def remoteSparkSession(appName: String): SparkSession = {
    SparkSession
      .builder()
      .appName(appName)
      .master("local")
      .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")

      .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")
      .config("spark.sql.catalog.spark_catalog.warehouse", "s3a://iom-lakehouse-000000000000/managed")
      .config("spark.sql.catalog.spark_catalog.catalog-impl", "org.apache.iceberg.jdbc.JdbcCatalog")
      .config("spark.sql.catalog.spark_catalog.uri", "jdbc:mysql://iceberg_000000000000_user:iceberg_000000000000_pass@localhost:33063/iceberg_000000000000_db")
      .config("spark.sql.catalog.spark_catalog.jdbc.verifyServerCertificate", "true")
      .config("spark.sql.catalog.spark_catalog.jdbc.useSSL", "true")

      .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")

      .config("spark.sql.warehouse.dir", "s3a://iom-lakehouse-000000000000/managed")
      .config("spark.hive.metastore.uris", "thrift://localhost:9083")

      .config("spark.sql.legacy.createHiveTableByDefault", "false")
      .config("spark.sql.sources.default", "iceberg")
      .enableHiveSupport()
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
