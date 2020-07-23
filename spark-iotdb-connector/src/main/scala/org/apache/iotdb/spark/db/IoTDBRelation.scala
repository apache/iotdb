/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.spark.db

import org.apache.spark.Partition
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources.{BaseRelation, Filter, PrunedFilteredScan}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Row, SQLContext, SparkSession}
import org.slf4j.LoggerFactory

import scala.collection.mutable.ArrayBuffer

private case class IoTDBPartitioningInfo(
                                          start: Long,
                                          end: Long,
                                          numPartitions: Int)

private object IoTDBRelation {

  private final val logger = LoggerFactory.getLogger(classOf[IoTDBRelation])

  def getPartitions(partitionInfo: IoTDBPartitioningInfo): Array[Partition] = {
    if (partitionInfo == null || partitionInfo.numPartitions <= 1 ||
      partitionInfo.start == partitionInfo.end) {
      return Array[Partition](IoTDBPartition(null, 0, 0L, 0L))
    }
    val start = partitionInfo.start
    val end = partitionInfo.end

    //if start <= end , can not partition
    require(start <= end,
      "Operation not allowed: the start time is larger than end time " +
        s"time start: $start; end: $end")

    //numPartitions needs to be less and equal than (end - start)
    val numPartitions =
    if ((end - start) >= partitionInfo.numPartitions) {
      partitionInfo.numPartitions
    } else {
      logger.warn("The number of partitions is reduced because the specified number of " +
        "partitions is less than the difference between upper bound and lower bound. " +
        s"Updated number of partitions: ${end - start}; Input number of " +
        s"partitions: ${partitionInfo.numPartitions}; Lower bound: $start; " +
        s"Upper bound: $end.")
      end - start
    }

    var partitions = new ArrayBuffer[Partition]()

    val length: Long = (end - start) / numPartitions + 1
    var i: Int = 0
    var currentValue: Long = start
    while (i < numPartitions) {
      var where = s""
      if (i == 0) {

        where = s"${SQLConstant.RESERVED_TIME} >= $currentValue and ${SQLConstant.RESERVED_TIME} <= ${currentValue + length}"
        partitions += IoTDBPartition(where, i, currentValue, currentValue + length)
      }
      else {
        where = s"${SQLConstant.RESERVED_TIME} > $currentValue and ${SQLConstant.RESERVED_TIME} <= ${currentValue + length}"
        partitions += IoTDBPartition(where, i, currentValue + 1, currentValue + length)
      }

      i = i + 1
      currentValue += length
    }
    partitions.toArray
  }
}

class IoTDBRelation protected[iotdb](val options: IoTDBOptions)(@transient val sparkSession: SparkSession)
  extends BaseRelation with PrunedFilteredScan {

  override def sqlContext: SQLContext = sparkSession.sqlContext

  private final val logger = LoggerFactory.getLogger(classOf[IoTDBRelation])

  override def schema: StructType = {
    Converter.toSparkSchema(options)
  }

  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {
    val start: Long = options.lowerBound.toLong
    val end: Long = options.upperBound.toLong
    val numPartition = options.numPartition.toInt

    val partitionInfo = IoTDBPartitioningInfo(start, end, numPartition)

    val parts = IoTDBRelation.getPartitions(partitionInfo)

    new IoTDBRDD(sparkSession.sparkContext,
      options,
      schema,
      requiredColumns,
      filters,
      parts).asInstanceOf[RDD[Row]]
  }
}
