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

import org.apache.iotdb.session.Session
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType.{BOOLEAN, DOUBLE, FLOAT, INT32, INT64, TEXT}
import org.apache.spark.sql.{DataFrame, Row}

import java.util
import java.lang

object DataFrameTools {
  def insertDataFrame(options: IoTDBOptions, dataframe: DataFrame): Unit = {
    val filteredColumns = Array[String]("Time", "Device")
    val sensorTypes = dataframe.dtypes.filter(x => !filteredColumns.contains(x._1))

    dataframe
      .repartition(options.numPartition.toInt)
      .sortWithinPartitions(dataframe.col("Device"))
      .foreachPartition { (partition: Iterator[Row]) =>
        val hostPort = options.url.split("//")(1).replace("/", "").split(":")
        val session = new Session(
          hostPort(0),
          hostPort(1).toInt,
          options.user,
          options.password
        )
        session.open()

        var device: lang.String = ""
        val times = new util.ArrayList[lang.Long]()
        val measurementsList = new util.ArrayList[util.List[lang.String]]()
        val typesList = new util.ArrayList[util.List[TSDataType]]()
        val valuesList = new util.ArrayList[util.List[Object]]()

        val batchSize = 1000
        var currentSize = 0

        partition.foreach { record =>
          if ("".equals(device)) device = record.get(1).toString
          else if (!device.equals(record.get(1).toString)) {
            insertAndEmptyDataSet(session, device, times, measurementsList, typesList, valuesList)
            device = record.get(1).toString
            currentSize = 0
          }
          val measurements = new util.ArrayList[lang.String]()
          val types = new util.ArrayList[TSDataType]()
          val values = new util.ArrayList[Object]()
          for (i <- 2 until record.length if !(record.get(i) == null)) {
            val value = typeTrans(record.get(i).toString, getType(sensorTypes(i - 2)._2))

            values.add(value)
            measurements.add(sensorTypes(i - 2)._1)
            types.add(getType(sensorTypes(i - 2)._2))
          }
          if (!values.isEmpty) {
            times.add(record.get(0).asInstanceOf[Long])
            measurementsList.add(measurements)
            typesList.add(types)
            valuesList.add(values)
            currentSize += 1
          }
          if (currentSize >= batchSize) {
            insertAndEmptyDataSet(session, device, times, measurementsList, typesList, valuesList)
            currentSize = 0
          }
        }

        insertAndEmptyDataSet(session, device, times, measurementsList, typesList, valuesList)
        session.close()
      }

  }

  def typeTrans(value: lang.String, dataType: TSDataType): Object = {
    dataType match {
      case TSDataType.TEXT => value
      case TSDataType.BOOLEAN => lang.Boolean.valueOf(value)
      case TSDataType.INT32 => lang.Integer.valueOf(value)
      case TSDataType.INT64 => lang.Long.valueOf(value)
      case TSDataType.FLOAT => lang.Float.valueOf(value)
      case TSDataType.DOUBLE => lang.Double.valueOf(value)
      case _ => null
    }
  }

  def getType(typeStr: lang.String): TSDataType = {
    typeStr match {
      case "StringType" => TEXT
      case "BooleanType" => BOOLEAN
      case "IntegerType" => INT32
      case "LongType" => INT64
      case "FloatType" => FLOAT
      case "DoubleType" => DOUBLE
      case _ => null
    }
  }

  def insertAndEmptyDataSet(session: Session,
                            device: lang.String,
                            times: util.ArrayList[lang.Long],
                            measurementsList: util.ArrayList[util.List[lang.String]],
                            typesList: util.ArrayList[util.List[TSDataType]],
                            valuesList: util.ArrayList[util.List[Object]]): Unit = {
    if (!valuesList.isEmpty) {
      session.insertRecordsOfOneDevice(device, times, measurementsList, typesList, valuesList)
      times.clear()
      measurementsList.clear()
      typesList.clear()
      valuesList.clear()
    }
  }
}