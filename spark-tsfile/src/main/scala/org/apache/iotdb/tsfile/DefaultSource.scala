/**
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

package org.apache.iotdb.tsfile

import java.io.{ObjectInputStream, ObjectOutputStream, _}
import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.hadoop.mapreduce.Job
import org.apache.iotdb.tsfile.DefaultSource.SerializableConfiguration
import org.apache.iotdb.tsfile.common.constant.QueryConstant
import org.apache.iotdb.tsfile.io.HDFSInput
import org.apache.iotdb.tsfile.qp.Executor
import org.apache.iotdb.tsfile.read.common.Field
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet
import org.apache.iotdb.tsfile.read.{ReadOnlyTsFile, TsFileSequenceReader}
import org.apache.spark.TaskContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.apache.spark.sql.execution.datasources.{FileFormat, OutputWriterFactory, PartitionedFile}
import org.apache.spark.sql.sources.{DataSourceRegister, Filter}
import org.apache.spark.sql.types._
import org.slf4j.LoggerFactory

private[tsfile] class DefaultSource extends FileFormat with DataSourceRegister {

  override def equals(other: Any): Boolean = other match {
    case _: DefaultSource => true
    case _ => false
  }

  override def inferSchema(
                            spark: SparkSession,
                            options: Map[String, String],
                            files: Seq[FileStatus]): Option[StructType] = {
    val conf = spark.sparkContext.hadoopConfiguration

    //check if the path is given
    options.getOrElse(DefaultSource.path, throw new TSFileDataSourceException(s"${DefaultSource.path} must be specified for org.apache.iotdb.tsfile DataSource"))

    if(options.getOrElse(DefaultSource.isNewForm, "").equals("new_form")){
      val tsfileSchema = NewConverter.getUnionSeries(files, conf)

      NewConverter.toSqlSchema(tsfileSchema)
    }
    else{
      //get union series in TsFile
      val tsfileSchema = Converter.getUnionSeries(files, conf)

      Converter.toSqlSchema(tsfileSchema)
    }

  }

  override def isSplitable(
                            sparkSession: SparkSession,
                            options: Map[String, String],
                            path: org.apache.hadoop.fs.Path): Boolean = {
    true
  }

  override def buildReader(
                            sparkSession: SparkSession,
                            dataSchema: StructType,
                            partitionSchema: StructType,
                            requiredSchema: StructType,
                            filters: Seq[Filter],
                            options: Map[String, String],
                            hadoopConf: Configuration): (PartitionedFile) => Iterator[InternalRow] = {
    val broadcastedConf =
      sparkSession.sparkContext.broadcast(new SerializableConfiguration(hadoopConf))

    (file: PartitionedFile) => {
      val log = LoggerFactory.getLogger(classOf[DefaultSource])
      log.info(file.toString())

      val conf = broadcastedConf.value.value
      val in = new HDFSInput(new Path(new URI(file.filePath)), conf)

      val reader: TsFileSequenceReader = new TsFileSequenceReader(in)

      Option(TaskContext.get()).foreach { taskContext => {
        taskContext.addTaskCompletionListener { _ => in.close() }
        log.info("task Id: " + taskContext.taskAttemptId() + " partition Id: " + taskContext.partitionId())
      }
      }

      val tsFileMetaData = reader.readFileMetadata

      // get queriedSchema from requiredSchema
      var queriedSchema = Converter.prepSchema(requiredSchema, tsFileMetaData)
      val readTsFile: ReadOnlyTsFile = new ReadOnlyTsFile(reader)

      if (options.getOrElse(DefaultSource.isNewForm, "").equals("new_form")) {
        val device_names = tsFileMetaData.getDeviceMap.keySet()
        val measurement_names = tsFileMetaData.getMeasurementSchema.keySet()
        // construct queryExpression based on queriedSchema and filters
        val queryExpressions = NewConverter.toQueryExpression(dataSchema, device_names, measurement_names, filters, reader, file.start.asInstanceOf[java.lang.Long], (file.start + file.length).asInstanceOf[java.lang.Long])

        val queryDataSets = Executor.query(readTsFile, queryExpressions, file.start.asInstanceOf[java.lang.Long], (file.start + file.length).asInstanceOf[java.lang.Long])
        var queryDataSet : QueryDataSet = null
        var device_name:String = null

        def queryNext(): Boolean = {
          if(queryDataSet != null && queryDataSet.hasNext){
            return true
          }

          if(queryDataSets.isEmpty){
            return false
          }

          queryDataSet = queryDataSets.remove(queryDataSets.size() - 1)
          while(!queryDataSet.hasNext){
            if(queryDataSets.isEmpty){
              return false
            }
            queryDataSet = queryDataSets.remove(queryDataSets.size() - 1)
          }
          device_name = queryDataSet.getPaths.get(0).getDevice
          true
        }

        new Iterator[InternalRow] {
          private val rowBuffer = Array.fill[Any](requiredSchema.length)(null)

          private val safeDataRow = new GenericRow(rowBuffer)

          // Used to convert `Row`s containing data columns into `InternalRow`s.
          private val encoderForDataColumns = RowEncoder(requiredSchema)

          override def hasNext: Boolean = {
            queryNext()
          }

          override def next(): InternalRow = {
            queryNext()

            val curRecord = queryDataSet.next()
            val fields = curRecord.getFields
            val paths = queryDataSet.getPaths

            //index in one required row
            var index = 0
            requiredSchema.foreach((field: StructField) => {
              if (field.name == QueryConstant.RESERVED_TIME) {
                rowBuffer(index) = curRecord.getTimestamp
              }
              else if(field.name == NewConverter.DEVICE_NAME){
                rowBuffer(index) = device_name
              }
              else {
                val pos = paths.indexOf(new org.apache.iotdb.tsfile.read.common.Path(device_name + "." + field.name))
                var curField: Field = null
                if (pos != -1) {
                  curField = fields.get(pos)
                }
                rowBuffer(index) = NewConverter.toSqlValue(curField)
              }
              index += 1
            })

            encoderForDataColumns.toRow(safeDataRow)
          }
        }
      }
      else {
        // construct queryExpression based on queriedSchema and filters
        val queryExpression = Converter.toQueryExpression(queriedSchema, filters)


        val queryDataSet = readTsFile.query(queryExpression, file.start.asInstanceOf[java.lang.Long],
          (file.start + file.length).asInstanceOf[java.lang.Long])

        new Iterator[InternalRow] {
          private val rowBuffer = Array.fill[Any](requiredSchema.length)(null)

          private val safeDataRow = new GenericRow(rowBuffer)

          // Used to convert `Row`s containing data columns into `InternalRow`s.
          private val encoderForDataColumns = RowEncoder(requiredSchema)

          override def hasNext: Boolean = {
            val hasNext = queryDataSet.hasNext
            hasNext
          }

          override def next(): InternalRow = {

            val curRecord = queryDataSet.next()
            val fields = curRecord.getFields
            val paths = queryDataSet.getPaths

            //index in one required row
            var index = 0
            requiredSchema.foreach((field: StructField) => {
              if (field.name == QueryConstant.RESERVED_TIME) {
                rowBuffer(index) = curRecord.getTimestamp
              } else {
                val pos = paths.indexOf(new org.apache.iotdb.tsfile.read.common.Path(field.name))
                var curField: Field = null
                if (pos != -1) {
                  curField = fields.get(pos)
                }
                rowBuffer(index) = Converter.toSqlValue(curField)
              }
              index += 1
            })

            encoderForDataColumns.toRow(safeDataRow)
          }
        }
      }
    }
  }

  override def shortName(): String = "tsfile"

  override def prepareWrite(sparkSession: SparkSession,
                            job: Job,
                            options: Map[String, String],
                            dataSchema: StructType): OutputWriterFactory = {

    new TsFileWriterFactory(options)
  }

  class TSFileDataSourceException(message: String, cause: Throwable)
    extends Exception(message, cause) {
    def this(message: String) = this(message, null)
  }

}


private[tsfile] object DefaultSource {
  val path = "path"
  val isNewForm = "form"

  class SerializableConfiguration(@transient var value: Configuration) extends Serializable {
    private def writeObject(out: ObjectOutputStream): Unit = {
      out.defaultWriteObject()
      value.write(out)
    }

    private def readObject(in: ObjectInputStream): Unit = {
      value = new Configuration(false)
      value.readFields(in)
    }
  }

}
