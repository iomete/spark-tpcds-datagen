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

import org.apache.spark.SparkFunSuite
import org.slf4j.LoggerFactory

class TPCDSDatagenRemoteSuite extends SparkFunSuite {

  private val logger = LoggerFactory.getLogger(classOf[TPCDSDatagenRemoteSuite])

  test("datagen_remote") {
    val spark = TestUtil.remoteSparkSession("TPCDSDatagenSuite")

//    spark.sql("show databases").show()

    val tpcdsTables = new Tables(spark, 1)
    tpcdsTables.genData(
      partitionTables = true,
      useDoubleForDecimal = false,
      useStringForChar = true,
      tableFilter = Set("call_center"),
      numPartitions = 4)
  }
}
