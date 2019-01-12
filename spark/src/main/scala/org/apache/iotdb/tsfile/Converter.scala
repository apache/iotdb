package org.apache.iotdb.tsfile

import java.util

import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor
import org.apache.iotdb.tsfile.common.utils.ITsRandomAccessFileReader
import org.apache.iotdb.tsfile.file.metadata.enums.{TSDataType, TSEncoding}
import org.apache.iotdb.tsfile.timeseries.read.management.SeriesSchema
import org.apache.iotdb.tsfile.timeseries.read.query.{QueryConfig, QueryEngine}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileStatus
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._
import org.apache.iotdb.tsfile.io.HDFSInputStream
import org.apache.iotdb.tsfile.qp.QueryProcessor
import org.apache.iotdb.tsfile.qp.common.{BasicOperator, FilterOperator, SQLConstant, TSQueryPlan}
import org.apache.iotdb.tsfile.timeseries.read.support.Field
import org.apache.iotdb.tsfile.timeseries.write.desc.MeasurementDescriptor
import org.apache.iotdb.tsfile.timeseries.write.record.{DataPoint, TSRecord}
import org.apache.iotdb.tsfile.timeseries.write.schema.{FileSchema, SchemaBuilder}
import org.apache.spark.sql.Row

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}

/**
  * This object contains methods that are used to convert schema and data between sparkSQL and TSFile.
  *
  */
object Converter {

  class SparkSqlFilterException (message: String, cause: Throwable)
    extends Exception(message, cause){
    def this(message: String) = this(message, null)
  }

  case class SchemaType(dataType: DataType, nullable: Boolean)

  /**
    * Get union series in all tsfiles
    * e.g. (tsfile1:s1,s2) & (tsfile2:s2,s3) = s1,s2,s3
    *
    * @param files tsfiles
    * @param conf hadoop configuration
    * @return union series
    */
  def getUnionSeries(files: Seq[FileStatus], conf: Configuration): util.ArrayList[SeriesSchema] = {
    val unionSeries = new util.ArrayList[SeriesSchema]()
    var seriesSet: mutable.Set[String] = mutable.Set()

    files.foreach(f => {
      val in = new HDFSInputStream(f.getPath, conf)
      val queryEngine = new QueryEngine(in)
      val series = queryEngine.getAllSeriesSchema
      series.foreach(s => {
        if(!seriesSet.contains(s.name)) {
          seriesSet += s.name
          unionSeries.add(s)
        }
      })
    })

    unionSeries
  }

  /**
    * Convert TSFile columns to sparkSQL schema.
    *
    * @param tsfileSchema all time series information in TSFile
    * @param columns e.g. {device:1, board:2} or {delta_object:0}
    * @return sparkSQL table schema
    */
  def toSqlSchema(tsfileSchema: util.ArrayList[SeriesSchema], columns: ArrayBuffer[String]): Option[StructType] = {
    val fields = new ListBuffer[StructField]()
    fields += StructField(SQLConstant.RESERVED_TIME, LongType, nullable = false)

    columns.filter(f => !f.equals("root"))foreach(f => {
      fields += StructField(f, StringType, nullable = false)
    })

    tsfileSchema.foreach((series: SeriesSchema) => {
      fields += StructField(series.name, series.dataType match {
        case TSDataType.BOOLEAN => BooleanType
        case TSDataType.INT32 => IntegerType
        case TSDataType.INT64 => LongType
        case TSDataType.FLOAT => FloatType
        case TSDataType.DOUBLE => DoubleType
        case TSDataType.ENUMS => StringType
        case TSDataType.TEXT => StringType
        case TSDataType.FIXED_LEN_BYTE_ARRAY => BinaryType
        case other => throw new UnsupportedOperationException(s"Unsupported type $other")
      }, nullable = true)
    })

    SchemaType(StructType(fields.toList), nullable = false).dataType match {
      case t: StructType => Some(t)
      case _ =>throw new RuntimeException(
        s"""TSFile schema cannot be converted to a Spark SQL StructType:
           |${tsfileSchema.toString}
           |""".stripMargin)
    }
  }


  /**
    * given a spark sql struct type, generate TsFile schema
    * @param structType given sql schema
    * @return TsFile schema
    */
  def toTsFileSchema(columnNames: ArrayBuffer[String], structType: StructType, options: Map[String, String]): FileSchema = {
    val schemaBuilder = new SchemaBuilder()
    structType.fields.filter(f => {
      !SQLConstant.isReservedPath(f.name) && !columnNames.contains(f.name)
    }).foreach(f => {
      val seriesSchema = getSeriesSchema(f, options)
      schemaBuilder.addSeries(seriesSchema)
    })
    schemaBuilder.build()
  }


  /**
    * construct series schema from name and data type
    * @param field series name
    * @param options series data type
    * @return series schema
    */
  def getSeriesSchema(field: StructField, options: Map[String, String]): MeasurementDescriptor = {
    val conf = TSFileDescriptor.getInstance.getConfig
    val dataType = getTsDataType(field.dataType)
    val encodingStr = dataType match {
      case TSDataType.INT32 => options.getOrElse(SQLConstant.INT32, TSEncoding.RLE.toString)
      case TSDataType.INT64 => options.getOrElse(SQLConstant.INT64, TSEncoding.RLE.toString)
      case TSDataType.FLOAT => options.getOrElse(SQLConstant.FLOAT, TSEncoding.RLE.toString)
      case TSDataType.DOUBLE => options.getOrElse(SQLConstant.DOUBLE, TSEncoding.RLE.toString)
      case TSDataType.TEXT => options.getOrElse(SQLConstant.BYTE_ARRAY, TSEncoding.PLAIN.toString)
      case other => throw new UnsupportedOperationException(s"Unsupported type $other")
    }
    val encoding = TSEncoding.valueOf(encodingStr)
    new MeasurementDescriptor(field.name, dataType, encoding, null)
  }


  /**
    * return the TsFile data type of given spark sql data type
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
    * Use information given by sparkSQL to construct TSFile QueryConfigs for querying data.
    *
    * @param in file input stream
    * @param requiredSchema The schema of the data that should be output for each row.
    * @param filters A set of filters than can optionally be used to reduce the number of rows output
    * @param columnNames e.g. {device:1, board:2}
    * @param start the start offset in file partition
    * @param end the end offset in file partition
    * @return TSFile physical query plans
    */
  def toQueryConfigs(
                      in: ITsRandomAccessFileReader,
                      requiredSchema: StructType,
                      filters: Seq[Filter],
                      columnNames: ArrayBuffer[String],
                      start : java.lang.Long,
                      end : java.lang.Long): Array[QueryConfig] = {

    val queryConfigs = new ArrayBuffer[QueryConfig]()

    val paths = new ListBuffer[String]()
    requiredSchema.foreach(f => {
      paths.add(f.name)
    })

    //remove invalid filters
    val validFilters = new ListBuffer[Filter]()
    filters.foreach {f => {
      if(isValidFilter(f))
        validFilters.add(f)}
    }

    if(validFilters.isEmpty) {

      //generatePlans operatorTree to TSQueryPlan list
      val queryProcessor = new QueryProcessor()
      val queryPlans = queryProcessor.generatePlans(null, paths, columnNames, in, start, end).toArray

      //construct TSQueryPlan list to QueryConfig list
      queryPlans.foreach(f => {
        queryConfigs.append(queryToConfig(f.asInstanceOf[TSQueryPlan]))
      })
    } else {
      //construct filters to a binary tree
      var filterTree = validFilters.get(0)
      for(i <- 1 until validFilters.length) {
        filterTree = And(filterTree, validFilters.get(i))
      }

      //convert filterTree to FilterOperator
      val operator = transformFilter(filterTree)

      //generatePlans operatorTree to TSQueryPlan list
      val queryProcessor = new QueryProcessor()
      val queryPlans = queryProcessor.generatePlans(operator, paths, columnNames, in, start, end).toArray

      //construct TSQueryPlan list to QueryConfig list
      queryPlans.foreach(f => {
        queryConfigs.append(queryToConfig(f.asInstanceOf[TSQueryPlan]))
      })
    }
    queryConfigs.toArray
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
    * Used in toQueryConfigs() to convert one query plan to one QueryConfig.
    *
    * @param queryPlan TsFile logical query plan
    * @return TsFile physical query plan
    */
  private def queryToConfig(queryPlan: TSQueryPlan): QueryConfig = {
    val selectedColumns = queryPlan.getPaths.toArray
    val timeFilter = queryPlan.getTimeFilterOperator
    val valueFilter = queryPlan.getValueFilterOperator

    var select = ""
    var colNum = 0
    selectedColumns.foreach(f => {
      if(colNum == 0) {
        select += f.asInstanceOf[String]
      }
      else {
        select += "|" + f.asInstanceOf[String]
      }
      colNum += 1
    })

    var single = false
    if(colNum == 1 && valueFilter != null){
      if(select.equals(valueFilter.getSinglePath)){
        single = true
      }
    }
    val timeFilterStr = timeFilterToString(timeFilter)
    val valueFilterStr = valueFilterToString(valueFilter, single)
    new QueryConfig(select, timeFilterStr, "null", valueFilterStr)
  }


  /**
    * Convert a time filter to QueryConfig's timeFilter parameter.
    *
    * @param operator time filter
    * @return QueryConfig's timeFilter parameter
    */
  private def timeFilterToString(operator: FilterOperator): String = {
    if(operator == null)
      return "null"

    "0," + timeFilterToPartString(operator)
  }


  /**
    * Used in timeFilterToString to construct specified string format.
    *
    * @param operator time filter
    * @return QueryConfig's partial timeFilter parameter
    */
  private def timeFilterToPartString(operator: FilterOperator): String = {
    val token = operator.getTokenIntType
    token match {
      case SQLConstant.KW_AND =>
        "(" + timeFilterToPartString(operator.getChildren()(0)) + ")&(" +
          timeFilterToPartString(operator.getChildren()(1)) + ")"
      case SQLConstant.KW_OR =>
        "(" + timeFilterToPartString(operator.getChildren()(0)) + ")|(" +
          timeFilterToPartString(operator.getChildren()(1)) + ")"
      case _ =>
        val basicOperator = operator.asInstanceOf[BasicOperator]
        basicOperator.getTokenSymbol + basicOperator.getSeriesValue
    }
  }


  /**
    * Convert a value filter to QueryConfig's valueFilter parameter. Each query is a cross query.
    *
    * @param operator value filter
    * @param single single series query
    * @return QueryConfig's valueFilter parameter
    */
  private def valueFilterToString(operator: FilterOperator, single : Boolean): String = {
    if(operator == null)
      return "null"

    val token = operator.getTokenIntType
    token match {
      case SQLConstant.KW_AND =>
        "[" + valueFilterToString(operator.getChildren()(0), single = true) + "]&[" +
          valueFilterToString(operator.getChildren()(1), single = true) + "]"
      case SQLConstant.KW_OR =>
        "[" + valueFilterToString(operator.getChildren()(0), single = true) + "]|[" +
          valueFilterToString(operator.getChildren()(1), single = true) + "]"
      case _ =>
        val basicOperator = operator.asInstanceOf[BasicOperator]
        val path = basicOperator.getSinglePath
        val res = new StringBuilder
        if(single){
          res.append("2," + path + "," +
            basicOperator.getTokenSymbol + basicOperator.getSeriesValue)
        }
        else{
          res.append("[2," + path + "," +
            basicOperator.getTokenSymbol + basicOperator.getSeriesValue + "]")
        }
        res.toString()
    }
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
        operator = new BasicOperator(SQLConstant.LESSTHANOREQUALTO, node.attribute, node.value.toString)
        operator

      case node: GreaterThan =>
        operator = new BasicOperator(SQLConstant.GREATERTHAN, node.attribute, node.value.toString)
        operator

      case node: GreaterThanOrEqual =>
        operator = new BasicOperator(SQLConstant.GREATERTHANOREQUALTO, node.attribute, node.value.toString)
        operator

      case _ =>
        throw new Exception("unsupported filter:" + node.toString)
    }
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
    else field.dataType match {
      case TSDataType.BOOLEAN => field.getBoolV
      case TSDataType.INT32 => field.getIntV
      case TSDataType.INT64 => field.getLongV
      case TSDataType.FLOAT => field.getFloatV
      case TSDataType.DOUBLE => field.getDoubleV
      case TSDataType.FIXED_LEN_BYTE_ARRAY => field.getStringValue
      case TSDataType.TEXT => field.getStringValue
      case TSDataType.ENUMS => field.getStringValue
      case other => throw new UnsupportedOperationException(s"Unsupported type $other")
    }
  }


  /**
    * convert row to TSRecord
    * @param columnNames delta_object column names
    * @param row given spark sql row
    * @return TSRecord
    */
  def toTsRecord(columnNames: ArrayBuffer[String], row: Row): TSRecord = {
    val schema = row.schema
    val time = row.getAs[Long](SQLConstant.RESERVED_TIME)
    val delta_object = {
      if ( columnNames.contains(SQLConstant.RESERVED_DELTA_OBJECT)) {
        row.getAs[String](SQLConstant.RESERVED_DELTA_OBJECT)
      } else {
        var delta_str = "root"
        columnNames.filter(f => !f.equals("root")) foreach (f => {
          delta_str += SQLConstant.PATH_SEPARATOR + row.getAs[String](f)
        })
        delta_str
      }
    }
    val tsRecord = new TSRecord(time, delta_object)
    schema.fields.filter(f => {
      !SQLConstant.isReservedPath(f.name) && !columnNames.contains(f.name)
    }).foreach(f => {
      val name = f.name
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
        val dataPoint = DataPoint.getDataPoint(dataType, name, value.toString)
        tsRecord.addTuple(dataPoint)
      }
    })
    tsRecord
  }


}