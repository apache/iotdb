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

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileStatus
import org.apache.iotdb.hadoop.fileSystem.HDFSInput
import org.apache.iotdb.spark.tsfile.qp.QueryProcessor
import org.apache.iotdb.spark.tsfile.qp.common.{BasicOperator, FilterOperator, SQLConstant, TSQueryPlan}
import org.apache.iotdb.tsfile.common.constant.QueryConstant
import org.apache.iotdb.tsfile.file.metadata.TsFileMetadata
import org.apache.iotdb.tsfile.file.metadata.enums.{TSDataType, TSEncoding}
import org.apache.iotdb.tsfile.read.TsFileSequenceReader
import org.apache.iotdb.tsfile.read.common.Path
import org.apache.iotdb.tsfile.read.expression.impl.{BinaryExpression, GlobalTimeExpression, SingleSeriesExpression}
import org.apache.iotdb.tsfile.read.expression.{IExpression, QueryExpression}
import org.apache.iotdb.tsfile.read.filter.{TimeFilter, ValueFilter}
import org.apache.iotdb.tsfile.write.record.TSRecord
import org.apache.iotdb.tsfile.write.record.datapoint.DataPoint
import org.apache.iotdb.tsfile.write.schema.{MeasurementSchema, Schema}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._

import java.util
import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
 * This object contains methods that are used to convert schema and data between SparkSQL
 * and TSFile.
 *
 */
object NarrowConverter extends Converter {

  val TEMPLATE_NAME = "spark_template"
  val DEVICE_NAME = "device_name"

  /**
   * Get union series in all tsfiles.
   * e.g. (tsfile1:s1,s2) & (tsfile2:s2,s3) = s1,s2,s3
   *
   * @param files tsfiles
   * @param conf  hadoop configuration
   * @return union series
   */
  def getUnionSeries(files: Seq[FileStatus], conf: Configuration): util.ArrayList[Series] = {
    val unionSeries = new util.ArrayList[Series]()
    var seriesSet: mutable.Set[String] = mutable.Set()

    files.foreach(f => {
      val in = new HDFSInput(f.getPath, conf)
      val reader = new TsFileSequenceReader(in)
      val measurements = reader.getAllMeasurements

      measurements.foreach(m => {
        if (!seriesSet.contains(m._1)) {
          seriesSet += m._1
          unionSeries.add(new Series(m._1, m._2)
          )
        }
      })

      reader.close()
    })

    unionSeries
  }


  /**
   * Construct fields with the TSFile data type converted to the SparkSQL data type.
   *
   * @param tsfileSchema tsfileSchema
   * @param addTimeField true to add a time field; false to not
   * @return the converted list of fields
   */
  override def toSqlField(tsfileSchema: util.ArrayList[Series], addTimeField: Boolean):
  ListBuffer[StructField] = {
    val fields = new ListBuffer[StructField]()

    if (addTimeField) {
      fields += StructField(QueryConstant.RESERVED_TIME, LongType, nullable = false)
    }
    fields += StructField(DEVICE_NAME, StringType, nullable = false)

    tsfileSchema.foreach((series: Series) => {
      fields += StructField(series.getName, series.getType match {
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
   * Prepare queriedSchema from requiredSchema.
   *
   * @param requiredSchema requiredSchema
   * @param tsFileMetaData tsFileMetaData
   * @return
   */
  def prepSchema(requiredSchema: StructType, tsFileMetaData: TsFileMetadata,
                 reader: TsFileSequenceReader): StructType = {
    var queriedSchema: StructType = new StructType()

    if (requiredSchema.isEmpty
      || (requiredSchema.size == 1 && requiredSchema.iterator.next().name ==
      QueryConstant.RESERVED_TIME)) {
      // for example, (i) select count(*) from table; (ii) select time from table

      val fileSchema = WideConverter.getSeries(tsFileMetaData, reader)
      queriedSchema = StructType(toSqlField(fileSchema, false).toList)

    } else {
      // Remove nonexistent schema according to the current file's metadata.
      // This may happen when queried TsFiles in the same folder do not have the same schema.

      val measurementIds = reader.getAllMeasurements.keySet()
      requiredSchema.foreach(f => {
        if (!QueryConstant.RESERVED_TIME.equals(f.name) && !DEVICE_NAME.equals(f.name)) {
          if (measurementIds.contains(f.name)) {
            queriedSchema = queriedSchema.add(f)
          }
        }
      })

    }

    queriedSchema
  }


  /**
   * Construct queryExpression based on queriedSchema and filters.
   *
   * @param schema           schema
   * @param device_name      device_names
   * @param measurement_name measurement_names
   * @return query expression
   */
  def toQueryExpression(schema: StructType,
                        device_name: util.List[String],
                        measurement_name: util.Set[String],
                        filters: Seq[Filter],
                        in: TsFileSequenceReader,
                        start: java.lang.Long,
                        end: java.lang.Long): util.ArrayList[QueryExpression] = {
    // build filter
    var finalFilter: FilterOperator = null
    //remove invalid filters
    val validFilters = new ListBuffer[Filter]()
    //query processor
    val queryProcessor = new QueryProcessor()
    filters.foreach(f => {
      if (isValidFilter(f))
        validFilters.add(f)
    }
    )
    if (validFilters.nonEmpty) {
      //construct filters to a binary tree
      var filterTree = validFilters.get(0)
      for (i <- 1 until validFilters.length) {
        filterTree = And(filterTree, validFilters.get(i))
      }

      //convert filterTree to FilterOperator
      finalFilter = transformFilter(filterTree)
    }

    //get paths from device name and measurement name
    val res = new util.ArrayList[QueryExpression]
    val paths = new util.ArrayList[String](measurement_name)

    val columnNames = new util.ArrayList[String]()
    columnNames += DEVICE_NAME
    val queryPlans = queryProcessor.generatePlans(finalFilter, paths, columnNames, in, start, end)

    queryPlans.foreach(plan => {
      res.add(queryToExpression(schema, plan))
    })

    res
  }

  /**
   * Used in toQueryConfigs() to convert one query plan to one QueryConfig.
   *
   * @param queryPlan TsFile logical query plan
   * @return TsFile physical query plan
   */
  private def queryToExpression(schema: StructType, queryPlan: TSQueryPlan): QueryExpression = {
    val selectedColumns = queryPlan.getPaths
    val timeFilter = queryPlan.getTimeFilterOperator
    val valueFilter = queryPlan.getValueFilterOperator

    val paths = new util.ArrayList[Path]()
    selectedColumns.foreach(path => {
      paths.add(new Path(path, true))
    })

    val deviceName = paths.get(0).getDevice
    var finalFilter: IExpression = null
    if (timeFilter != null) {
      finalFilter = transformFilterToExpression(schema, timeFilter, deviceName)
    }
    if (valueFilter != null) {
      if (finalFilter != null) {
        finalFilter = BinaryExpression.and(finalFilter, transformFilterToExpression(schema,
          valueFilter, deviceName))
      }
      else {
        finalFilter = transformFilterToExpression(schema, valueFilter, deviceName)
      }
    }

    QueryExpression.create(paths, finalFilter)
  }

  /**
   * Transform sparkSQL's filter binary tree to filterOperator binary tree.
   *
   * @param node filter tree's node
   * @return TSFile filterOperator binary tree
   */
  private def transformFilter(node: Filter): FilterOperator = {
    var operator: FilterOperator = null
    node match {
      case node: Not =>
        operator = new FilterOperator(SQLConstant.KW_NOT)
        operator.addChildOPerator(transformFilter(node.child))
        operator

      case node: And =>
        operator = new FilterOperator(SQLConstant.KW_AND)
        operator.addChildOPerator(transformFilter(node.left))
        operator.addChildOPerator(transformFilter(node.right))
        operator

      case node: Or =>
        operator = new FilterOperator(SQLConstant.KW_OR)
        operator.addChildOPerator(transformFilter(node.left))
        operator.addChildOPerator(transformFilter(node.right))
        operator

      case node: EqualTo =>
        operator = new BasicOperator(SQLConstant.EQUAL, node.attribute, node.value.toString)
        operator

      case node: LessThan =>
        operator = new BasicOperator(SQLConstant.LESSTHAN, node.attribute, node.value.toString)
        operator

      case node: LessThanOrEqual =>
        operator = new BasicOperator(SQLConstant.LESSTHANOREQUALTO, node.attribute,
          node.value.toString)
        operator

      case node: GreaterThan =>
        operator = new BasicOperator(SQLConstant.GREATERTHAN, node.attribute, node.value.toString)
        operator

      case node: GreaterThanOrEqual =>
        operator = new BasicOperator(SQLConstant.GREATERTHANOREQUALTO, node.attribute,
          node.value.toString)
        operator

      case _ =>
        throw new Exception("unsupported filter:" + node.toString)
    }
  }

  /**
   * Transform SparkSQL's filter binary tree to TsFile's filter expression.
   *
   * @param schema to get relative columns' dataType information
   * @param node   filter tree's node
   * @return TSFile filter expression
   */
  private def transformFilterToExpression(schema: StructType, node: FilterOperator,
                                          device_name: String): IExpression = {
    var filter: IExpression = null
    node.getTokenIntType match {
      case SQLConstant.KW_NOT =>
        throw new Exception("NOT filter is not supported now")

      case SQLConstant.KW_AND =>
        node.childOperators.foreach((child: FilterOperator) => {
          if (filter == null) {
            filter = transformFilterToExpression(schema, child, device_name)
          }
          else {
            filter = BinaryExpression.and(filter, transformFilterToExpression(schema, child,
              device_name))
          }
        })
        filter

      case SQLConstant.KW_OR =>
        node.childOperators.foreach((child: FilterOperator) => {
          if (filter == null) {
            filter = transformFilterToExpression(schema, child, device_name)
          }
          else {
            filter = BinaryExpression.or(filter, transformFilterToExpression(schema, child,
              device_name))
          }
        })
        filter


      case SQLConstant.EQUAL =>
        val basicOperator = node.asInstanceOf[BasicOperator]
        if (QueryConstant.RESERVED_TIME.equals(basicOperator.getSeriesPath.toLowerCase())) {
          filter = new GlobalTimeExpression(TimeFilter.eq(java.lang.Long.parseLong(
            basicOperator.getSeriesValue)))
        } else {
          filter = constructExpression(schema, basicOperator.getSeriesPath,
            basicOperator.getSeriesValue, FilterTypes.Eq, device_name)
        }
        filter

      case SQLConstant.LESSTHAN =>
        val basicOperator = node.asInstanceOf[BasicOperator]
        if (QueryConstant.RESERVED_TIME.equals(basicOperator.getSeriesPath.toLowerCase())) {
          filter = new GlobalTimeExpression(TimeFilter.lt(java.lang.Long.parseLong(
            basicOperator.getSeriesValue)))
        } else {
          filter = constructExpression(schema, basicOperator.getSeriesPath,
            basicOperator.getSeriesValue, FilterTypes.Lt, device_name)
        }
        filter

      case SQLConstant.LESSTHANOREQUALTO =>
        val basicOperator = node.asInstanceOf[BasicOperator]
        if (QueryConstant.RESERVED_TIME.equals(basicOperator.getSeriesPath.toLowerCase())) {
          filter = new GlobalTimeExpression(TimeFilter.ltEq(java.lang.Long.parseLong(
            basicOperator.getSeriesValue)))
        } else {
          filter = constructExpression(schema, basicOperator.getSeriesPath,
            basicOperator.getSeriesValue, FilterTypes.LtEq, device_name)
        }
        filter

      case SQLConstant.GREATERTHAN =>
        val basicOperator = node.asInstanceOf[BasicOperator]
        if (QueryConstant.RESERVED_TIME.equals(basicOperator.getSeriesPath.toLowerCase())) {
          filter = new GlobalTimeExpression(TimeFilter.gt(java.lang.Long.parseLong(
            basicOperator.getSeriesValue)))
        } else {
          filter = constructExpression(schema, basicOperator.getSeriesPath,
            basicOperator.getSeriesValue, FilterTypes.Gt, device_name)
        }
        filter

      case SQLConstant.GREATERTHANOREQUALTO =>
        val basicOperator = node.asInstanceOf[BasicOperator]
        if (QueryConstant.RESERVED_TIME.equals(basicOperator.getSeriesPath.toLowerCase())) {
          filter = new GlobalTimeExpression(TimeFilter.gtEq(java.lang.Long.parseLong(
            basicOperator.getSeriesValue)))
        } else {
          filter = constructExpression(schema, basicOperator.getSeriesPath,
            basicOperator.getSeriesValue, FilterTypes.GtEq, device_name)
        }
        filter

      case other =>
        throw new Exception(s"Unsupported filter $other")
    }
  }


  def constructExpression(schema: StructType, nodeName: String, nodeValue: String,
                          filterType: FilterTypes.Value, device_name: String): IExpression = {
    val fieldNames = schema.fieldNames
    val index = fieldNames.indexOf(nodeName)
    if (index == -1) {
      // placeholder for an invalid filter in the current TsFile
      val filter = new SingleSeriesExpression(new Path(device_name, nodeName, true), null)
      filter
    } else {
      val dataType = schema.get(index).dataType

      filterType match {
        case FilterTypes.Eq =>
          dataType match {
            case BooleanType =>
              val filter = new SingleSeriesExpression(new Path(device_name, nodeName, true),
                ValueFilter.eq(new java.lang.Boolean(nodeValue)))
              filter
            case IntegerType =>
              val filter = new SingleSeriesExpression(new Path(device_name, nodeName, true),
                ValueFilter.eq(new java.lang.Integer(nodeValue)))
              filter
            case LongType =>
              val filter = new SingleSeriesExpression(new Path(device_name, nodeName, true),
                ValueFilter.eq(new java.lang.Long(nodeValue)))
              filter
            case FloatType =>
              val filter = new SingleSeriesExpression(new Path(device_name, nodeName, true),
                ValueFilter.eq(new java.lang.Float(nodeValue)))
              filter
            case DoubleType =>
              val filter = new SingleSeriesExpression(new Path(device_name, nodeName, true),
                ValueFilter.eq(new java.lang.Double(nodeValue)))
              filter
            case StringType =>
              val filter = new SingleSeriesExpression(new Path(device_name, nodeName, true),
                ValueFilter.eq(nodeValue))
              filter
            case other => throw new UnsupportedOperationException(s"Unsupported type $other")
          }
        case FilterTypes.Gt =>
          dataType match {
            case IntegerType =>
              val filter = new SingleSeriesExpression(new Path(device_name, nodeName, true),
                ValueFilter.gt(new java.lang.Integer(nodeValue)))
              filter
            case LongType =>
              val filter = new SingleSeriesExpression(new Path(device_name, nodeName, true),
                ValueFilter.gt(new java.lang.Long(nodeValue)))
              filter
            case FloatType =>
              val filter = new SingleSeriesExpression(new Path(device_name, nodeName, true),
                ValueFilter.gt(new java.lang.Float(nodeValue)))
              filter
            case DoubleType =>
              val filter = new SingleSeriesExpression(new Path(device_name, nodeName, true),
                ValueFilter.gt(new java.lang.Double(nodeValue)))
              filter
            case other => throw new UnsupportedOperationException(s"Unsupported type $other")
          }
        case FilterTypes.GtEq =>
          dataType match {
            case IntegerType =>
              val filter = new SingleSeriesExpression(new Path(device_name, nodeName, true),
                ValueFilter.gtEq(new java.lang.Integer(nodeValue)))
              filter
            case LongType =>
              val filter = new SingleSeriesExpression(new Path(device_name, nodeName, true),
                ValueFilter.gtEq(new java.lang.Long(nodeValue)))
              filter
            case FloatType =>
              val filter = new SingleSeriesExpression(new Path(device_name, nodeName, true),
                ValueFilter.gtEq(new java.lang.Float(nodeValue)))
              filter
            case DoubleType =>
              val filter = new SingleSeriesExpression(new Path(device_name, nodeName, true),
                ValueFilter.gtEq(new java.lang.Double(nodeValue)))
              filter
            case other => throw new UnsupportedOperationException(s"Unsupported type $other")
          }
        case FilterTypes.Lt =>
          dataType match {
            case IntegerType =>
              val filter = new SingleSeriesExpression(new Path(device_name, nodeName, true),
                ValueFilter.lt(new java.lang.Integer(nodeValue)))
              filter
            case LongType =>
              val filter = new SingleSeriesExpression(new Path(device_name, nodeName, true),
                ValueFilter.lt(new java.lang.Long(nodeValue)))
              filter
            case FloatType =>
              val filter = new SingleSeriesExpression(new Path(device_name, nodeName, true),
                ValueFilter.lt(new java.lang.Float(nodeValue)))
              filter
            case DoubleType =>
              val filter = new SingleSeriesExpression(new Path(device_name, nodeName, true),
                ValueFilter.lt(new java.lang.Double(nodeValue)))
              filter
            case other => throw new UnsupportedOperationException(s"Unsupported type $other")
          }
        case FilterTypes.LtEq =>
          dataType match {
            case IntegerType =>
              val filter = new SingleSeriesExpression(new Path(device_name, nodeName, true),
                ValueFilter.ltEq(new java.lang.Integer(nodeValue)))
              filter
            case LongType =>
              val filter = new SingleSeriesExpression(new Path(device_name, nodeName, true),
                ValueFilter.ltEq(new java.lang.Long(nodeValue)))
              filter
            case FloatType =>
              val filter = new SingleSeriesExpression(new Path(device_name, nodeName, true),
                ValueFilter.ltEq(new java.lang.Float(nodeValue)))
              filter
            case DoubleType =>
              val filter = new SingleSeriesExpression(new Path(device_name, nodeName, true),
                ValueFilter.ltEq(new java.lang.Double(nodeValue)))
              filter
            case other => throw new UnsupportedOperationException(s"Unsupported type $other")
          }
      }
    }
  }

  /**
   * Construct MeasurementSchema from the given field.
   *
   * @param field   field
   * @param options encoding options
   * @return MeasurementSchema
   */
  def getSeriesSchema(field: StructField, options: Map[String, String]): MeasurementSchema = {
    val dataType = getTsDataType(field.dataType)
    val encodingStr = dataType match {
      case TSDataType.BOOLEAN => options.getOrElse(QueryConstant.BOOLEAN, TSEncoding.PLAIN.toString)
      case TSDataType.INT32 => options.getOrElse(QueryConstant.INT32, TSEncoding.RLE.toString)
      case TSDataType.INT64 => options.getOrElse(QueryConstant.INT64, TSEncoding.RLE.toString)
      case TSDataType.FLOAT => options.getOrElse(QueryConstant.FLOAT, TSEncoding.RLE.toString)
      case TSDataType.DOUBLE => options.getOrElse(QueryConstant.DOUBLE, TSEncoding.RLE.toString)
      case TSDataType.TEXT => options.getOrElse(QueryConstant.BYTE_ARRAY, TSEncoding.PLAIN.toString)
      case other => throw new UnsupportedOperationException(s"Unsupported type $other")
    }
    val encoding = TSEncoding.valueOf(encodingStr)
    new MeasurementSchema(field.name, dataType, encoding)
  }

  /**
   * Given a SparkSQL struct type, generate the TsFile schema.
   * Note: Measurements of the same name should have the same schema.
   *
   * @param structType given sql schema
   * @return TsFile schema
   */
  def toTsFileSchema(structType: StructType, options: Map[String, String]): Schema = {
    val schema = new Schema()
    structType.fields.filter(f => {
      (!QueryConstant.RESERVED_TIME.equals(f.name)).&&(!DEVICE_NAME.equals(f.name))
    }).foreach(f => {
      val seriesSchema = getSeriesSchema(f, options)
      schema.extendTemplate(TEMPLATE_NAME, seriesSchema)
    })
    schema
  }

  /**
   * Convert a row in the spark table to a list of TSRecord.
   *
   * @param row given spark sql row
   * @return TSRecord
   */
  def toTsRecord(row: InternalRow, dataSchema: StructType): TSRecord = {
    val time = row.getLong(0)
    val res = new TSRecord(time, row.getString(1))
    var i = 2

    dataSchema.fields.filter(f => {
      (!QueryConstant.RESERVED_TIME.equals(f.name)).&&(!DEVICE_NAME.equals(f.name))
    }).foreach(f => {
      val dataType = getTsDataType(f.dataType)
      if (!row.isNullAt(i)) {
        val value = f.dataType match {
          case BooleanType => row.getBoolean(i)
          case IntegerType => row.getInt(i)
          case LongType => row.getLong(i)
          case FloatType => row.getFloat(i)
          case DoubleType => row.getDouble(i)
          case StringType => row.getString(i)
          case other => throw new UnsupportedOperationException(s"Unsupported type $other")
        }
        val dataPoint = DataPoint.getDataPoint(dataType, f.name, value.toString)
        res.addTuple(dataPoint)
      }
      i += 1
    })

    res
  }
}
