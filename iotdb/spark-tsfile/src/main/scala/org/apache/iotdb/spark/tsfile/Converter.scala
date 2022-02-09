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

package org.apache.iotdb.spark.tsfile

import java.util

import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType
import org.apache.iotdb.tsfile.read.common.Field
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._

import scala.collection.mutable.ListBuffer

/**
  * A series has a name and a type.
  *
  * @param nameVar name
  * @param typeVar type
  */
class Series(nameVar: String, typeVar: TSDataType) {
  var seriesName: String = nameVar
  var seriesType: TSDataType = typeVar

  def getName = seriesName

  def getType = seriesType

  override def toString = s"[" + seriesName + "," + seriesType + "]"
}

/**
  * Created by SilverNarcissus on 2019/9/3.
  */
abstract class Converter {
  /**
    * Convert TSFile data to SparkSQL data.
    *
    * @param field one data point in TsFile
    * @return SparkSQL data
    */
  def toSqlValue(field: Field): Any = {
    if (field == null)
      return null
    if (field.getDataType == null)
      null
    else field.getDataType match {
      case TSDataType.BOOLEAN => field.getBoolV
      case TSDataType.INT32 => field.getIntV
      case TSDataType.INT64 => field.getLongV
      case TSDataType.FLOAT => field.getFloatV
      case TSDataType.DOUBLE => field.getDoubleV
      case TSDataType.TEXT => field.getStringValue
      case other => throw new UnsupportedOperationException(s"Unsupported type $other")
    }
  }

  /**
    * Construct fields with the TSFile data type converted to the SparkSQL data type.
    *
    * @param tsfileSchema tsfileSchema
    * @param addTimeField true to add a time field; false to not
    * @return the converted list of fields
    */
  def toSqlField(tsfileSchema: util.ArrayList[Series], addTimeField: Boolean): ListBuffer[StructField]

  /**
    * Convert TSFile columns to SparkSQL schema.
    *
    * @param tsfileSchema all time series information in TSFile
    * @return SparkSQL table schema with the time field added
    */
  def toSqlSchema(tsfileSchema: util.ArrayList[Series]): Option[StructType] = {
    val fields = toSqlField(tsfileSchema, true) // true to add the time field

    SchemaType(StructType(fields.toList), nullable = false).dataType match {
      case t: StructType => Some(t)
      case _ => throw new RuntimeException(
        s"""TSFile schema cannot be converted to a Spark SQL StructType:
           |${tsfileSchema.toString}
           |""".stripMargin)
    }
  }

  /**
    * Return the TsFile data type of given SparkSQL data type.
    *
    * @param dataType SparkSQL data type
    * @return TsFile data type
    */
  def getTsDataType(dataType: DataType): TSDataType = {
    dataType match {
      case BooleanType => TSDataType.BOOLEAN
      case IntegerType => TSDataType.INT32
      case LongType => TSDataType.INT64
      case FloatType => TSDataType.FLOAT
      case DoubleType => TSDataType.DOUBLE
      case StringType => TSDataType.TEXT
      case other => throw new UnsupportedOperationException(s"Unsupported type $other")
    }
  }


  protected def isValidFilter(filter: Filter): Boolean = {
    filter match {
      case f: EqualTo => true
      case f: GreaterThan => true
      case f: GreaterThanOrEqual => true
      case f: LessThan => true
      case f: LessThanOrEqual => true
      case f: Or => isValidFilter(f.left) && isValidFilter(f.right)
      case f: And => isValidFilter(f.left) && isValidFilter(f.right)
      case f: Not => isValidFilter(f.child)
      case _ => false
    }
  }

  class SparkSqlFilterException(message: String, cause: Throwable)
    extends Exception(message, cause) {
    def this(message: String) = this(message, null)
  }

  case class SchemaType(dataType: DataType, nullable: Boolean)

}

object FilterTypes extends Enumeration {
  val Eq, Gt, GtEq, Lt, LtEq = Value
}
