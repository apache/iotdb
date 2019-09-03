package org.apache.iotdb.tsfile

import java.util

import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor
import org.apache.iotdb.tsfile.common.constant.QueryConstant
import org.apache.iotdb.tsfile.file.metadata.enums.{TSDataType, TSEncoding}
import org.apache.iotdb.tsfile.read.common.{Field, Path}
import org.apache.iotdb.tsfile.write.record.TSRecord
import org.apache.iotdb.tsfile.write.record.datapoint.DataPoint
import org.apache.iotdb.tsfile.write.schema.{MeasurementSchema, Schema, SchemaBuilder}
import org.apache.spark.sql.catalyst.InternalRow
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

  /**
    * Construct MeasurementSchema from the given field.
    *
    * @param field   field
    * @param options encoding options
    * @return MeasurementSchema
    */
  def getSeriesSchema(field: StructField, options: Map[String, String]): MeasurementSchema = {
    val conf = TSFileDescriptor.getInstance.getConfig
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
    val fullPath = new Path(field.name)
    val measurement = fullPath.getMeasurement
    new MeasurementSchema(measurement, dataType, encoding)
  }

  /**
    * Given a SparkSQL struct type, generate the TsFile schema.
    * Note: Measurements of the same name should have the same schema.
    *
    * @param structType given sql schema
    * @return TsFile schema
    */
  def toTsFileSchema(structType: StructType, options: Map[String, String]): Schema = {
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
    * Convert a row in the spark table to a list of TSRecord.
    *
    * @param row given spark sql row
    * @return TSRecord
    */
  def toTsRecord(row: InternalRow, dataSchema: StructType): List[TSRecord] = {
    val time = row.getLong(0)
    val deviceToRecord = scala.collection.mutable.Map[String, TSRecord]()
    var index = 1

    dataSchema.fields.filter(f => {
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
      if (!row.isNullAt(index)) {
        val value = f.dataType match {
          case BooleanType => row.getBoolean(index)
          case IntegerType => row.getInt(index)
          case LongType => row.getLong(index)
          case FloatType => row.getFloat(index)
          case DoubleType => row.getDouble(index)
          case StringType => row.getString(index)
          case other => throw new UnsupportedOperationException(s"Unsupported type $other")
        }
        val dataPoint = DataPoint.getDataPoint(dataType, measurement, value.toString)
        tsRecord.addTuple(dataPoint)
      }
      index += 1
    })
    deviceToRecord.values.toList
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

  class SparkSqlFilterException(message: String, cause: Throwable)
    extends Exception(message, cause) {
    def this(message: String) = this(message, null)
  }

  case class SchemaType(dataType: DataType, nullable: Boolean)

}

object FilterTypes extends Enumeration {
  val Eq, Gt, GtEq, Lt, LtEq = Value
}
