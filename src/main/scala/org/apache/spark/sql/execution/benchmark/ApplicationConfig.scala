package org.apache.spark.sql.execution.benchmark

case class ApplicationConfig(
                              scaleFactor: Int = 1,
                              partitionTables: Boolean = true,
                              useDoubleForDecimal: Boolean = false,
                              useStringForChar: Boolean = true,
                              tableFilter: Set[String] = Set.empty,
                              numPartitions: Int = 100,
                              database: String = "default"
                            )
