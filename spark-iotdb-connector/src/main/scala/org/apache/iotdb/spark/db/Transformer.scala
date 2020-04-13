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

import org.apache.spark.sql.{SparkSession, _}
import org.apache.spark.sql.types._


object Transformer {
  /**
    * transfer old form to new form
    *
    * @param spark your SparkSession
    * @param df    dataFrame need to be transfer
    *              +---------+-------------+-------------+-------------+-------------+-------------+-------------+
    *              |timestamp|root.ln.d1.m1|root.ln.d1.m2|root.ln.d1.m3|root.ln.d2.m1|root.ln.d2.m2|root.ln.d2.m3|
    *              +---------+-------------+-------------+-------------+-------------+-------------+-------------+
    *              |        1|           11|           12|         null|           21|           22|           23|
    *              +---------+-------------+-------------+-------------+-------------+-------------+-------------+
    * @return tansferred data frame
    *         +---------+-----------+---+---+----+
    *         |timestamp|device_name| m1| m2|  m3|
    *         +---------+-----------+---+---+----+
    *         |        1| root.ln.d2| 21| 22|  23|
    *         |        1| root.ln.d1| 11| 12|null|
    *         +---------+-----------+---+---+----+
    */
  def toNarrowForm(spark: SparkSession, df: DataFrame): DataFrame = {
    df.createOrReplaceTempView("tsfle_wide_form")
    // use to record device and their measurement
    var map = new scala.collection.mutable.HashMap[String, List[String]]()
    // use to record all the measurement, prepare for the union
    var mMap = scala.collection.mutable.HashMap[String, DataType]()

    // this step is to record device_name and measurement_name
    df.schema.foreach(f => {
      if (!SQLConstant.TIMESTAMP_STR.equals(f.name)) {
        val pos = f.name.lastIndexOf('.')
        val diviceName = f.name.substring(0, pos)
        val measurementName = f.name.substring(pos + 1)
        if (map.contains(diviceName)) {
          map(diviceName) = map(diviceName) :+ measurementName
        }
        else {
          var l: List[String] = List()
          l = l :+ measurementName
          map += (diviceName -> l)
        }
        mMap += (measurementName -> f.dataType)
      }
    })

    // we first get each device's measurement data and then union them to get what we want, means:
    // +---------+-----------+---+---+----+
    // |timestamp|device_name| m1| m2|  m3|
    // +---------+-----------+---+---+----+
    // |        1| root.ln.d2| 21| 22|  23|
    // |        1| root.ln.d1| 11| 12|null|
    // +---------+-----------+---+---+----+
    var res: org.apache.spark.sql.DataFrame = null
    map.keys.foreach { deviceName =>
      // build query
      var query = "select " + SQLConstant.TIMESTAMP_STR + ", \"" + deviceName + "\" as device_name"
      val measurementName = map(deviceName)
      mMap.keySet.foreach { m =>
        val pos = measurementName.indexOf(m)
        if (pos >= 0) {
          // select normal column
          query += ", `" + deviceName + "." + m + "` as " + m
        }
        else {
          // fill null column
          query += ", NULL as " + m
        }
      }

      query += " from tsfle_wide_form"
      val curDF = spark.sql(query)

      if (res == null) {
        res = curDF
      }
      else {
        res = res.union(curDF)
      }
    }

    res
  }

  /**
    * transfer new form to old form
    *
    * @param spark your SparkSession
    * @param df    dataFrame need to be tansfer
    *              +---------+-----------+---+---+----+
    *              |timestamp|device_name| m1| m2|  m3|
    *              +---------+-----------+---+---+----+
    *              |        1| root.ln.d2| 21| 22|  23|
    *              |        1| root.ln.d1| 11| 12|null|
    *              +---------+-----------+---+---+----+
    * @return tansferred data frame
    *         +---------+-------------+-------------+-------------+-------------+-------------+-------------+
    *         |timestamp|root.ln.d1.m1|root.ln.d1.m2|root.ln.d1.m3|root.ln.d2.m1|root.ln.d2.m2|root.ln.d2.m3|
    *         +---------+-------------+-------------+-------------+-------------+-------------+-------------+
    *         |        1|           11|           12|         null|           21|           22|           23|
    *         +---------+-------------+-------------+-------------+-------------+-------------+-------------+
    *
    */
  def toWideForm(spark: SparkSession, df: DataFrame): DataFrame = {
    df.createOrReplaceTempView("tsfle_narrow_form")
    // get all device_name
    val deviceNames = spark.sql("select distinct device_name from tsfle_narrow_form").collect()
    val tableDF = spark.sql("select * from tsfle_narrow_form")

    import scala.collection.mutable.ListBuffer
    // get all measurement_name
    val measurementNames = new ListBuffer[String]()

    tableDF.schema.foreach(f => {
      if (!SQLConstant.TIMESTAMP_STR.equals(f.name) && !"device_name".equals(f.name)) {
        measurementNames += f.name
      }
    })

    var res: org.apache.spark.sql.DataFrame = null
    // we first get each device's data and then join them with timestamp to build tsRecord form which means:
    // +---------+-------------+-------------+-------------+-------------+-------------+-------------+
    // |timestamp|root.ln.d1.m1|root.ln.d1.m2|root.ln.d1.m3|root.ln.d2.m1|root.ln.d2.m2|root.ln.d2.m3|
    // +---------+-------------+-------------+-------------+-------------+-------------+-------------+
    // |        1|           11|           12|         null|           21|           22|           23|
    // +---------+-------------+-------------+-------------+-------------+-------------+-------------+

    deviceNames.foreach(deviceName => {
      var query = "select " + SQLConstant.TIMESTAMP_STR

      measurementNames.foreach(measurementName => {
        query = query + ", " + measurementName + " as `" + deviceName(0) + "." + measurementName + "`"
      })

      query = query + " from tsfle_narrow_form where device_name = \"" + deviceName(0) + "\""
      val curDF = spark.sql(query)

      if (res == null) {
        res = curDF
      }
      else {
        res = res.join(curDF, List(SQLConstant.TIMESTAMP_STR), "outer")
      }
    })

    res
  }
}

