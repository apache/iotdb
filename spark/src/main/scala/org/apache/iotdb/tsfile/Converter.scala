/**
  * Copyright © 2019 Apache IoTDB(incubating) (dev@iotdb.apache.org)
  *
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
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  */

package org.apache.iotdb.tsfile

import java.util

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileStatus
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor
import org.apache.iotdb.tsfile.common.constant.QueryConstant
import org.apache.iotdb.tsfile.file.metadata.TsFileMetaData
import org.apache.iotdb.tsfile.file.metadata.enums.{TSDataType, TSEncoding}
import org.apache.iotdb.tsfile.io.HDFSInput
import org.apache.iotdb.tsfile.read.TsFileSequenceReader
import org.apache.iotdb.tsfile.read.common.{Field, Path}
import org.apache.iotdb.tsfile.read.expression.impl.{BinaryExpression, GlobalTimeExpression, SingleSeriesExpression}
import org.apache.iotdb.tsfile.read.expression.{IExpression, QueryExpression}
import org.apache.iotdb.tsfile.read.filter.{TimeFilter, ValueFilter}
import org.apache.iotdb.tsfile.write.record.TSRecord
import org.apache.iotdb.tsfile.write.record.datapoint.DataPoint
import org.apache.iotdb.tsfile.write.schema.{FileSchema, MeasurementSchema, SchemaBuilder}
import org.apache.spark.sql.Row
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
  * This object contains methods that are used to convert schema and data between sparkSQL and TSFile.
  *
  */
object Converter {

  /**
    * Get union series in all tsfiles
    * e.g. (tsfile1:s1,s2) & (tsfile2:s2,s3) = s1,s2,s3
    *
    * @param files tsfiles
    * @param conf  hadoop configuration
    * @return union series
    */
  def getUnionSeries(files: Seq[FileStatus], conf: Configuration): util.ArrayList[MeasurementSchema] = {
    val unionSeries = new util.ArrayList[MeasurementSchema]()
    var seriesSet: mutable.Set[String] = mutable.Set()

    files.foreach(f => {
      val in = new HDFSInput(f.getPath, conf)
      val reader = new TsFileSequenceReader(in)
      val tsFileMetaData = reader.readFileMetadata
      val devices = tsFileMetaData.getDeviceMap.keySet()
      val measurements = tsFileMetaData.getMeasurementSchema

      devices.foreach(d => {
        measurements.foreach(m => {
          val fullPath = d + "." + m._1
          if (!seriesSet.contains(fullPath)) {
            seriesSet += fullPath
            unionSeries.add(new MeasurementSchema(fullPath, m._2.getType, m._2.getEncodingType)
            )
          }
        })
      })
    })

    unionSeries
  }

  /**
    * Get series from the given tsFileMetaData
    *
    * @param tsFileMetaData TsFileMetaData
    * @return union series
    */
  def getSeries(tsFileMetaData: TsFileMetaData): util.ArrayList[MeasurementSchema] = {
    val series = new util.ArrayList[MeasurementSchema]()

    val devices = tsFileMetaData.getDeviceMap.keySet()
    val measurements = tsFileMetaData.getMeasurementSchema

    devices.foreach(d => {
      measurements.foreach(m => {
        val fullPath = d + "." + m._1
        series.add(new MeasurementSchema(fullPath, m._2.getType, m._2.getEncodingType)
        )
      })
    })

    series
  }


  /**
    * Convert TSFile data to sparkSQL data.
    *
    * @param field one data point in TsFile
    * @return sparkSQL data
    */
  def toSqlValue(field: Field): Any = {
    if (field == null)
      return null
    if (field.isNull)
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
    * convert tsfile data type to sparkSQL data type
    *
    * @param tsfileSchema given tsfileSchema
    * @param isTimeField  true to add a time field at the beginning
    * @return the converted list of fields
    */
  def convert2SqlType(tsfileSchema: util.ArrayList[MeasurementSchema], isTimeField: Boolean): ListBuffer[StructField] = {
    val fields = new ListBuffer[StructField]()

    if (isTimeField) {
      fields += StructField(QueryConstant.RESERVED_TIME, LongType, nullable = false)
    }

    tsfileSchema.foreach((series: MeasurementSchema) => {
      fields += StructField(series.getMeasurementId, series.getType match {
        case TSDataType.BOOLEAN => BooleanType
        case TSDataType.INT32 => IntegerType
        case TSDataType.INT64 => LongType
        case TSDataType.FLOAT => FloatType
        case TSDataType.DOUBLE => DoubleType
        case TSDataType.TEXT => StringType
        case other => throw new UnsupportedOperationException(s"Unsupported type $other")
      }, nullable = true)
    })

    fields
  }

  /**
    * Convert TSFile columns to sparkSQL schema.
    *
    * @param tsfileSchema all time series information in TSFile
    * @return sparkSQL table schema including the additional time field
    */
  def toSqlSchema(tsfileSchema: util.ArrayList[MeasurementSchema]): Option[StructType] = {
    val fields = convert2SqlType(tsfileSchema, true) // add time field

    SchemaType(StructType(fields.toList), nullable = false).dataType match {
      case t: StructType => Some(t)
      case _ => throw new RuntimeException(
        s"""TSFile schema cannot be converted to a Spark SQL StructType:
           |${tsfileSchema.toString}
           |""".stripMargin)
    }
  }

  /*
    preprocess requiredSchema to get queriedSchema
   */
  def prep4requiredSchema(requiredSchema: StructType, tsFileMetaData: TsFileMetaData): StructType = {
    var queriedSchema: StructType = new StructType()

    if (requiredSchema.isEmpty
      || (requiredSchema.size == 1 && requiredSchema.iterator.next().name == QueryConstant.RESERVED_TIME)) {
      // for example, (i) select count(*) from table; (ii) select time from table

      val fileSchema = Converter.getSeries(tsFileMetaData)
      queriedSchema = StructType(convert2SqlType(fileSchema, false).toList)

    } else { // Remove nonexistent schema according to the current file's metadata.
      // This may happen when queried TsFiles in the same folder do not have the same schema.

      val devices = tsFileMetaData.getDeviceMap.keySet()
      val measurementIds = tsFileMetaData.getMeasurementSchema.keySet()
      requiredSchema.foreach(f => {
        if (!QueryConstant.RESERVED_TIME.equals(f.name)) {
          val path = new org.apache.iotdb.tsfile.read.common.Path(f.name)
          if (devices.contains(path.getDevice) && measurementIds.contains(path.getMeasurement)) {
            queriedSchema = queriedSchema.add(f)
          }
        }
      })

    }

    queriedSchema
  }


  /**
    * return the TsFile data type of given spark sql data type
    *
    * @param dataType spark sql data type
    * @return TsFile data type
    */
  def getTsDataType(dataType: DataType): TSDataType = {
    dataType match {
      case IntegerType => TSDataType.INT32
      case LongType => TSDataType.INT64
      case BooleanType => TSDataType.BOOLEAN
      case FloatType => TSDataType.FLOAT
      case DoubleType => TSDataType.DOUBLE
      case StringType => TSDataType.TEXT
      case other => throw new UnsupportedOperationException(s"Unsupported type $other")
    }
  }

  /**
    * construct series schema from name and data type
    *
    * @param field   series name
    * @param options series data type
    * @return series schema
    */
  def getSeriesSchema(field: StructField, options: Map[String, String]): MeasurementSchema = {
    val conf = TSFileDescriptor.getInstance.getConfig
    val dataType = getTsDataType(field.dataType)
    val encodingStr = dataType match {
      case TSDataType.INT32 => options.getOrElse(QueryConstant.INT32, TSEncoding.RLE.toString)
      case TSDataType.INT64 => options.getOrElse(QueryConstant.INT64, TSEncoding.RLE.toString)
      case TSDataType.FLOAT => options.getOrElse(QueryConstant.FLOAT, TSEncoding.RLE.toString)
      case TSDataType.DOUBLE => options.getOrElse(QueryConstant.DOUBLE, TSEncoding.RLE.toString)
      case TSDataType.TEXT => options.getOrElse(QueryConstant.BYTE_ARRAY, TSEncoding.PLAIN.toString)
      case other => throw new UnsupportedOperationException(s"Unsupported type $other")
    }
    val encoding = TSEncoding.valueOf(encodingStr)
    val fullPath = new Path(field.name)
    val measurement = fullPath.getMeasurement
    new MeasurementSchema(measurement, dataType, encoding)
  }

  /**
    * given a spark sql struct type, generate the TsFile schema
    *
    * Note: It is impossible to have two sensors with the same name in the same TsFile.
    *
    * @param structType given sql schema
    * @return TsFile schema
    */
  def toTsFileSchema(structType: StructType, options: Map[String, String]): FileSchema = {
    val schemaBuilder = new SchemaBuilder()
    structType.fields.filter(f => {
      !QueryConstant.RESERVED_TIME.equals(f.name)
    }).foreach(f => {
      val seriesSchema = getSeriesSchema(f, options)
      schemaBuilder.addSeries(seriesSchema)
    })
    schemaBuilder.build()
  }

  /**
    * convert a row in spark table to a list of TSRecord
    *
    * @param row given spark sql row
    * @return TSRecord
    */
  def toTsRecord(row: Row): List[TSRecord] = {
    val schema = row.schema
    val time = row.getAs[Long](QueryConstant.RESERVED_TIME)
    val deviceToRecord = scala.collection.mutable.Map[String, TSRecord]()

    schema.fields.filter(f => {
      !QueryConstant.RESERVED_TIME.equals(f.name)
    }).foreach(f => {
      val name = f.name
      val fullPath = new Path(name)
      val device = fullPath.getDevice
      val measurement = fullPath.getMeasurement
      if (!deviceToRecord.contains(device)) {
        deviceToRecord.put(device, new TSRecord(time, device))
      }
      val tsRecord: TSRecord = deviceToRecord.getOrElse(device, new TSRecord(time, device))

      val dataType = getTsDataType(f.dataType)
      val index = row.fieldIndex(name)
      if (!row.isNullAt(index)) {
        val value = f.dataType match {
          case IntegerType => row.getAs[Int](name)
          case LongType => row.getAs[Long](name)
          case FloatType => row.getAs[Float](name)
          case DoubleType => row.getAs[Double](name)
          case StringType => row.getAs[String](name)
          case other => throw new UnsupportedOperationException(s"Unsupported type $other")
        }
        val dataPoint = DataPoint.getDataPoint(dataType, measurement, value.toString)
        tsRecord.addTuple(dataPoint)
      }
    })
    deviceToRecord.values.toList
  }


  /**
    * construct queryExpression based on queriedSchema and filters
    *
    * @param schema  selected columns.
    * @param filters filters.
    * @return query expression
    */
  def toQueryExpression(schema: StructType, filters: Seq[Filter]): QueryExpression = {
    //get paths from schema
    val paths = new util.ArrayList[org.apache.iotdb.tsfile.read.common.Path]
    schema.foreach(f => {
      if (!QueryConstant.RESERVED_TIME.equals(f.name)) { // the time field in is excluded
        paths.add(new org.apache.iotdb.tsfile.read.common.Path(f.name))
      }
    })

    //remove invalid filters
    val validFilters = new ListBuffer[Filter]()
    filters.foreach { f => {
      if (isValidFilter(f))
        validFilters.add(f)
    }
    }
    if (validFilters.isEmpty) {
      val queryExpression = QueryExpression.create(paths, null)
      queryExpression
    } else {
      //construct filters to a binary tree
      var filterTree = validFilters.get(0)
      for (i <- 1 until validFilters.length) {
        filterTree = And(filterTree, validFilters.get(i))
      }

      //convert filterTree to FilterOperator
      val finalFilter = transformFilter(schema, filterTree)

      // create query expression
      val queryExpression = QueryExpression.create(paths, finalFilter)

      queryExpression
    }
  }

  private def isValidFilter(filter: Filter): Boolean = {
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

  /**
    * Transform sparkSQL's filter binary tree to TsFile's filter expression.
    *
    * @param schema to get relative columns' dataType information
    * @param node   filter tree's node
    * @return TSFile filter expression
    */
  private def transformFilter(schema: StructType, node: Filter): IExpression = {
    var filter: IExpression = null
    node match {
      case node: Not =>
        throw new Exception("NOT filter is not supported now")

      case node: And =>
        filter = BinaryExpression.and(transformFilter(schema, node.left), transformFilter(schema, node.right))
        filter

      case node: Or =>
        filter = BinaryExpression.or(transformFilter(schema, node.left), transformFilter(schema, node.right))
        filter

      case node: EqualTo =>
        if (QueryConstant.RESERVED_TIME.equals(node.attribute.toLowerCase())) {
          filter = new GlobalTimeExpression(TimeFilter.eq(node.value.asInstanceOf[java.lang.Long]))
        } else {
          filter = constructEqFilter(schema, node.attribute, node.value)
        }
        filter

      case node: LessThan =>
        if (QueryConstant.RESERVED_TIME.equals(node.attribute.toLowerCase())) {
          filter = new GlobalTimeExpression(TimeFilter.lt(node.value.asInstanceOf[java.lang.Long]))
        } else {
          filter = constructLtFilter(schema, node.attribute, node.value)
        }
        filter

      case node: LessThanOrEqual =>
        if (QueryConstant.RESERVED_TIME.equals(node.attribute.toLowerCase())) {
          filter = new GlobalTimeExpression(TimeFilter.ltEq(node.value.asInstanceOf[java.lang.Long]))
        } else {
          filter = constructLtEqFilter(schema, node.attribute, node.value)
        }
        filter

      case node: GreaterThan =>
        if (QueryConstant.RESERVED_TIME.equals(node.attribute.toLowerCase())) {
          filter = new GlobalTimeExpression(TimeFilter.gt(node.value.asInstanceOf[java.lang.Long]))
        } else {
          filter = constructGtFilter(schema, node.attribute, node.value)
        }
        filter

      case node: GreaterThanOrEqual =>
        if (QueryConstant.RESERVED_TIME.equals(node.attribute.toLowerCase())) {
          filter = new GlobalTimeExpression(TimeFilter.gtEq(node.value.asInstanceOf[java.lang.Long]))
        } else {
          filter = constructGtEqFilter(schema, node.attribute, node.value)
        }
        filter

      case _ =>
        throw new Exception("unsupported filter:" + node.toString)
    }
  }

  def constructEqFilter(schema: StructType, nodeName: String, nodeValue: Any): IExpression = {
    val fieldNames = schema.fieldNames
    val index = fieldNames.indexOf(nodeName)
    if (index == -1) {
      // TODO placeholder for an invalid filter in the current TsFile
      val filter = new SingleSeriesExpression(new Path(nodeName), null)
      filter
    } else {
      val dataType = schema.get(index).dataType

      dataType match {
        case IntegerType =>
          val filter = new SingleSeriesExpression(new Path(nodeName),
            ValueFilter.eq(nodeValue.asInstanceOf[java.lang.Integer]))
          filter
        case LongType =>
          val filter = new SingleSeriesExpression(new Path(nodeName),
            ValueFilter.eq(nodeValue.asInstanceOf[java.lang.Long]))
          filter
        case FloatType =>
          val filter = new SingleSeriesExpression(new Path(nodeName),
            ValueFilter.eq(nodeValue.asInstanceOf[java.lang.Float]))
          filter
        case DoubleType =>
          val filter = new SingleSeriesExpression(new Path(nodeName),
            ValueFilter.eq(nodeValue.asInstanceOf[java.lang.Double]))
          filter
        case StringType =>
          val filter = new SingleSeriesExpression(new Path(nodeName),
            ValueFilter.eq(nodeValue.asInstanceOf[java.lang.String]))
          filter
        case other => throw new UnsupportedOperationException(s"Unsupported type $other")
      }
    }
  }

  def constructGtFilter(requiredSchema: StructType, nodeName: String, nodeValue: Any): IExpression = {
    val fieldNames = requiredSchema.fieldNames
    val index = fieldNames.indexOf(nodeName)
    if (index == -1) {
      // TODO placeholder for an invalid filter in the current TsFile
      val filter = new SingleSeriesExpression(new Path(nodeName), null)
      filter
    } else {
      val dataType = requiredSchema.get(index).dataType

      dataType match {
        case IntegerType =>
          val filter = new SingleSeriesExpression(new Path(nodeName),
            ValueFilter.gt(nodeValue.asInstanceOf[java.lang.Integer]))
          filter
        case LongType =>
          val filter = new SingleSeriesExpression(new Path(nodeName),
            ValueFilter.gt(nodeValue.asInstanceOf[java.lang.Long]))
          filter
        case FloatType =>
          val filter = new SingleSeriesExpression(new Path(nodeName),
            ValueFilter.gt(nodeValue.asInstanceOf[java.lang.Float]))
          filter
        case DoubleType =>
          val filter = new SingleSeriesExpression(new Path(nodeName),
            ValueFilter.gt(nodeValue.asInstanceOf[java.lang.Double]))
          filter
        case StringType =>
          val filter = new SingleSeriesExpression(new Path(nodeName),
            ValueFilter.gt(nodeValue.asInstanceOf[java.lang.String]))
          filter
        case other => throw new UnsupportedOperationException(s"Unsupported type $other")
      }
    }
  }

  def constructGtEqFilter(requiredSchema: StructType, nodeName: String, nodeValue: Any): IExpression = {
    val fieldNames = requiredSchema.fieldNames
    val index = fieldNames.indexOf(nodeName)
    if (index == -1) {
      // TODO placeholder for an invalid filter in the current TsFile
      val filter = new SingleSeriesExpression(new Path(nodeName), null)
      filter
    } else {
      val dataType = requiredSchema.get(index).dataType

      dataType match {
        case IntegerType =>
          val filter = new SingleSeriesExpression(new Path(nodeName),
            ValueFilter.gtEq(nodeValue.asInstanceOf[java.lang.Integer]))
          filter
        case LongType =>
          val filter = new SingleSeriesExpression(new Path(nodeName),
            ValueFilter.gtEq(nodeValue.asInstanceOf[java.lang.Long]))
          filter
        case FloatType =>
          val filter = new SingleSeriesExpression(new Path(nodeName),
            ValueFilter.gtEq(nodeValue.asInstanceOf[java.lang.Float]))
          filter
        case DoubleType =>
          val filter = new SingleSeriesExpression(new Path(nodeName),
            ValueFilter.gtEq(nodeValue.asInstanceOf[java.lang.Double]))
          filter
        case StringType =>
          val filter = new SingleSeriesExpression(new Path(nodeName),
            ValueFilter.gtEq(nodeValue.asInstanceOf[java.lang.String]))
          filter
        case other => throw new UnsupportedOperationException(s"Unsupported type $other")
      }
    }
  }

  def constructLtFilter(requiredSchema: StructType, nodeName: String, nodeValue: Any): IExpression = {
    val fieldNames = requiredSchema.fieldNames
    val index = fieldNames.indexOf(nodeName)
    if (index == -1) {
      // TODO placeholder for an invalid filter in the current TsFile
      val filter = new SingleSeriesExpression(new Path(nodeName), null)
      filter
    } else {
      val dataType = requiredSchema.get(index).dataType

      dataType match {
        case IntegerType =>
          val filter = new SingleSeriesExpression(new Path(nodeName),
            ValueFilter.lt(nodeValue.asInstanceOf[java.lang.Integer]))
          filter
        case LongType =>
          val filter = new SingleSeriesExpression(new Path(nodeName),
            ValueFilter.lt(nodeValue.asInstanceOf[java.lang.Long]))
          filter
        case FloatType =>
          val filter = new SingleSeriesExpression(new Path(nodeName),
            ValueFilter.lt(nodeValue.asInstanceOf[java.lang.Float]))
          filter
        case DoubleType =>
          val filter = new SingleSeriesExpression(new Path(nodeName),
            ValueFilter.lt(nodeValue.asInstanceOf[java.lang.Double]))
          filter
        case StringType =>
          val filter = new SingleSeriesExpression(new Path(nodeName),
            ValueFilter.lt(nodeValue.asInstanceOf[java.lang.String]))
          filter
        case other => throw new UnsupportedOperationException(s"Unsupported type $other")
      }
    }
  }

  def constructLtEqFilter(requiredSchema: StructType, nodeName: String, nodeValue: Any): IExpression = {
    val fieldNames = requiredSchema.fieldNames
    val index = fieldNames.indexOf(nodeName)
    if (index == -1) {
      // TODO placeholder for an invalid filter in the current TsFile
      val filter = new SingleSeriesExpression(new Path(nodeName), null)
      filter
    } else {
      val dataType = requiredSchema.get(index).dataType

      dataType match {
        case IntegerType =>
          val filter = new SingleSeriesExpression(new Path(nodeName),
            ValueFilter.ltEq(nodeValue.asInstanceOf[java.lang.Integer]))
          filter
        case LongType =>
          val filter = new SingleSeriesExpression(new Path(nodeName),
            ValueFilter.ltEq(nodeValue.asInstanceOf[java.lang.Long]))
          filter
        case FloatType =>
          val filter = new SingleSeriesExpression(new Path(nodeName),
            ValueFilter.ltEq(nodeValue.asInstanceOf[java.lang.Float]))
          filter
        case DoubleType =>
          val filter = new SingleSeriesExpression(new Path(nodeName),
            ValueFilter.ltEq(nodeValue.asInstanceOf[java.lang.Double]))
          filter
        case StringType =>
          val filter = new SingleSeriesExpression(new Path(nodeName),
            ValueFilter.ltEq(nodeValue.asInstanceOf[java.lang.String]))
          filter
        case other => throw new UnsupportedOperationException(s"Unsupported type $other")
      }
    }
  }

  class SparkSqlFilterException(message: String, cause: Throwable)
    extends Exception(message, cause) {
    def this(message: String) = this(message, null)
  }

  case class SchemaType(dataType: DataType, nullable: Boolean)


}