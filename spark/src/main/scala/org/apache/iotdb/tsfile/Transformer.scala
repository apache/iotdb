/**
  * Licensed to the Apache Software Foundation (ASF) under one
  * or more contributor license agreements.  See the NOTICE file
  * distributed with this work for additional information
  * regarding copyright ownership.  The ASF licenses this file
  * to you under the Apache License, Version 2.0 (the
  * "License"); you may not use this file except in compliance
  * with the License.  You may obtain a copy of the License at
  *
  * http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing,
  * software distributed under the License is distributed on an
  * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  * KIND, either express or implied.  See the License for the
  * specific language governing permissions and limitations
  * under the License.
  */
package org.apache.iotdb.tsfile

import org.apache.spark.sql.types._
import org.apache.spark.sql._
import org.apache.iotdb.tsfile.common.constant.QueryConstant
import org.apache.spark.sql.SparkSession


object Transformer {
  /**
    * transfer old form to new form
    *
    * @param spark your SparkSession
    * @param df    dataFrame need to be tansfer
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
  def toNewForm(spark: SparkSession,
                df: DataFrame): DataFrame = {
    df.createOrReplaceTempView("iotdb_old_form")
    // use to record device and their measurement
    var map = new scala.collection.mutable.HashMap[String, List[String]]()
    // use to record all the measurement, prepare for the union
    var m_map = scala.collection.mutable.HashMap[String, DataType]()

    // this step is to record device_name and measurement_name
    df.schema.foreach(f => {
      if (!QueryConstant.RESERVED_TIME.equals(f.name)) {
        val pos = f.name.lastIndexOf('.')
        val divice_name = f.name.substring(0, pos)
        val measurement_name = f.name.substring(pos + 1)
        if (map.contains(divice_name)) {
          map(divice_name) = map(divice_name) :+ measurement_name
        }
        else {
          var l: List[String] = List()
          l = l :+ (measurement_name)
          map += (divice_name -> l)
        }
        m_map += (measurement_name -> f.dataType)
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
    map.keys.foreach { device_name =>
      // build query
      var query = "select " + QueryConstant.RESERVED_TIME + ", \"" + device_name + "\" as device_name"
      val measurement_name = map(device_name)
      m_map.keySet.foreach { m =>
        val pos = measurement_name.indexOf(m)
        if (pos >= 0) {
          // select normal column
          query += ", `" + device_name + "." + m + "` as " + m
        }
        else {
          // fill null column
          query += ", NULL as " + m
        }
      }

      query += " from iotdb_old_form"
      var cur_df = spark.sql(query)

      if (res == null) {
        res = cur_df
      }
      else {
        res = res.union(cur_df)
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
  def toOldForm(spark: SparkSession,
                df: DataFrame): DataFrame = {
    df.createOrReplaceTempView("iotdb_new_form")
    // get all device_name
    val device_names = spark.sql("select distinct device_name from iotdb_new_form").collect()
    val table_df = spark.sql("select * from iotdb_new_form")

    import scala.collection.mutable.ListBuffer
    // get all measurement_name
    val measurement_names = new ListBuffer[String]()

    table_df.schema.foreach(f => {
      if (!QueryConstant.RESERVED_TIME.equals(f.name) && !"device_name".equals(f.name)) {
        measurement_names += f.name
      }
    })

    var res: org.apache.spark.sql.DataFrame = null
    // we first get each device's data and then join them with timestamp to build tsRecord form which means:
    // +---------+-------------+-------------+-------------+-------------+-------------+-------------+
    // |timestamp|root.ln.d1.m1|root.ln.d1.m2|root.ln.d1.m3|root.ln.d2.m1|root.ln.d2.m2|root.ln.d2.m3|
    // +---------+-------------+-------------+-------------+-------------+-------------+-------------+
    // |        1|           11|           12|         null|           21|           22|           23|
    // +---------+-------------+-------------+-------------+-------------+-------------+-------------+

    device_names.foreach(device_name => {
      var query = "select " + QueryConstant.RESERVED_TIME

      measurement_names.foreach(measurement_name => {
        query = query + ", " + measurement_name + " as `" + device_name(0) + "." + measurement_name + "`"
      })

      query = query + " from iotdb_new_form where device_name = \"" + device_name(0) + "\""
      val cur_df = spark.sql(query)

      if (res == null) {
        res = cur_df
      }
      else {
        res = res.join(cur_df, List(QueryConstant.RESERVED_TIME), "outer")
      }
    })

    res
  }
}

