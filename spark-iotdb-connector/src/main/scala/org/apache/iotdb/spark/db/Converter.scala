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

import java.sql.{Statement, _}

import org.apache.iotdb.jdbc.IoTDBJDBCResultSet
import org.apache.spark.sql.types._
import org.slf4j.LoggerFactory

import scala.collection.mutable.ListBuffer

class Converter

object Converter {
  private final val logger = LoggerFactory.getLogger(classOf[Converter])

  def toSqlData(field: StructField, value: String): Any = {
    if (value == null || value.equals(SQLConstant.NULL_STR)) return null

    val r = field.dataType match {
      case BooleanType => java.lang.Boolean.valueOf(value)
      case IntegerType => value.toInt
      case LongType => value.toLong
      case FloatType => value.toFloat
      case DoubleType => value.toDouble
      case StringType => value
      case other => throw new UnsupportedOperationException(s"Unsupported type $other")
    }
    r
  }

  def toSparkSchema(options: IoTDBOptions): StructType = {

    Class.forName("org.apache.iotdb.jdbc.IoTDBDriver")
    val sqlConn: Connection = DriverManager.getConnection(options.url, options.user, options.password)
    val sqlStatement: Statement = sqlConn.createStatement()
    val hasResultSet: Boolean = sqlStatement.execute(options.sql)

    val fields = new ListBuffer[StructField]()
    if (hasResultSet) {
      val resultSet: ResultSet = sqlStatement.getResultSet
      val resultSetMetaData: ResultSetMetaData = resultSet.getMetaData

      val printTimestamp = !resultSet.asInstanceOf[IoTDBJDBCResultSet].isIgnoreTimeStamp
      if (printTimestamp) {
        fields += StructField(SQLConstant.TIMESTAMP_STR, LongType, nullable = false)
      }

      val colCount = resultSetMetaData.getColumnCount
      for (i <- 2 to colCount) {
        fields += StructField(resultSetMetaData.getColumnLabel(i), resultSetMetaData.getColumnType(i) match {
          case Types.BOOLEAN => BooleanType
          case Types.INTEGER => IntegerType
          case Types.BIGINT => LongType
          case Types.FLOAT => FloatType
          case Types.DOUBLE => DoubleType
          case Types.VARCHAR => StringType
          case other => throw new UnsupportedOperationException(s"Unsupported type $other")
        }, nullable = true)
      }
      StructType(fields.toList)
    }
    else {
      StructType(fields)
    }
  }
}
