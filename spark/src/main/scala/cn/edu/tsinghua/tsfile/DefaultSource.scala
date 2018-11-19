package cn.edu.tsinghua.tsfile

import java.io._
import java.net.URI
import java.util

import cn.edu.tsinghua.tsfile.common.constant.QueryConstant
import cn.edu.tsinghua.tsfile.DefaultSource.SerializableConfiguration
import cn.edu.tsinghua.tsfile.timeseries.read.query.QueryDataSet
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.TaskContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.apache.spark.sql.execution.datasources.{FileFormat, OutputWriterFactory, PartitionedFile}
import org.apache.spark.sql.sources.{DataSourceRegister, Filter}
import org.apache.spark.sql.types.{StructField, StructType}
import org.slf4j.LoggerFactory
import java.io.{ObjectInputStream, ObjectOutputStream}
import cn.edu.tsinghua.tsfile.timeseries.read.support.{Field, RowRecord}
import cn.edu.tsinghua.tsfile.io.HDFSInputStream
import cn.edu.tsinghua.tsfile.qp.Executor
import cn.edu.tsinghua.tsfile.qp.common.SQLConstant

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer


private[tsfile] class DefaultSource extends FileFormat with DataSourceRegister {

  class TSFileDataSourceException(message: String, cause: Throwable)
    extends Exception(message, cause) {
    def this(message: String) = this(message, null)
  }

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
    options.getOrElse(DefaultSource.path, throw new TSFileDataSourceException(s"${DefaultSource.path} must be specified for cn.edu.thu.tsfile DataSource"))

    //get union series in TsFile
    val tsfileSchema = Converter.getUnionSeries(files, conf)

    DefaultSource.columnNames.clear()

    //unfold delta_object
    if (options.contains(SQLConstant.DELTA_OBJECT_NAME)) {
      val columns = options(SQLConstant.DELTA_OBJECT_NAME).split(SQLConstant.REGEX_PATH_SEPARATOR)
      columns.foreach( f => {
        DefaultSource.columnNames += f
      })
    } else {
      //using delta_object
      DefaultSource.columnNames += SQLConstant.RESERVED_DELTA_OBJECT
    }

    Converter.toSqlSchema(tsfileSchema, DefaultSource.columnNames)
  }

  override def isSplitable(
                            sparkSession: SparkSession,
                            options: Map[String, String],
                            path: Path): Boolean = {
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
      val in = new HDFSInputStream(new Path(new URI(file.filePath)), conf)

      Option(TaskContext.get()).foreach { taskContext => {
        taskContext.addTaskCompletionListener { _ => in.close() }
        log.info("task Id: " + taskContext.taskAttemptId() + " partition Id: " + taskContext.partitionId())
      }
      }

      val parameters = new util.HashMap[java.lang.String, java.lang.Long]()
      parameters.put(QueryConstant.PARTITION_START_OFFSET, file.start.asInstanceOf[java.lang.Long])
      parameters.put(QueryConstant.PARTITION_END_OFFSET, (file.start + file.length).asInstanceOf[java.lang.Long])

      //convert tsfile query to QueryConfigs
      val queryConfigs = Converter.toQueryConfigs(in, requiredSchema, filters, DefaultSource.columnNames,
        file.start.asInstanceOf[java.lang.Long], (file.start + file.length).asInstanceOf[java.lang.Long])

      //use QueryConfigs to query data in tsfile
      val dataSets = Executor.query(in, queryConfigs.toList, parameters)

      case class Record(record: RowRecord, index: Int)

      implicit object RowRecordOrdering extends Ordering[Record] {
        override def compare(r1: Record, r2: Record): Int = {
          if (r1.record.timestamp == r2.record.timestamp) {
            r1.record.getFields.get(0).deltaObjectId.compareTo(r2.record.getFields.get(0).deltaObjectId)
          } else if (r1.record.timestamp < r2.record.timestamp) {
            1
          } else {
            -1
          }
        }
      }

      val priorityQueue = new mutable.PriorityQueue[Record]()

      //init priorityQueue with first record of each dataSet
      var queryDataSet: QueryDataSet = null
      for (i <- 0 until dataSets.size()) {
        queryDataSet = dataSets(i)
        if (queryDataSet.hasNextRecord) {
          val rowRecord = queryDataSet.getNextRecord
          priorityQueue.enqueue(Record(rowRecord, i))
        }
      }

      var curRecord: Record = null

      new Iterator[InternalRow] {
        private val rowBuffer = Array.fill[Any](requiredSchema.length)(null)

        private val safeDataRow = new GenericRow(rowBuffer)

        // Used to convert `Row`s containing data columns into `InternalRow`s.
        private val encoderForDataColumns = RowEncoder(requiredSchema)

        private var deltaObjectId = "null"

        override def hasNext: Boolean = {
          var hasNext = false

          while (priorityQueue.nonEmpty && !hasNext) {
            //get a record from priorityQueue
            val tmpRecord = priorityQueue.dequeue()
            //insert a record to priorityQueue
            if (dataSets(tmpRecord.index).hasNextRecord) {
              priorityQueue.enqueue(Record(dataSets(tmpRecord.index).getNextRecord, tmpRecord.index))
            }

            if (curRecord == null || tmpRecord.record.timestamp != curRecord.record.timestamp ||
              !tmpRecord.record.getFields.get(0).deltaObjectId.equals(curRecord.record.getFields.get(0).deltaObjectId)) {
              curRecord = tmpRecord
              deltaObjectId = curRecord.record.getFields.get(0).deltaObjectId
              hasNext = true
            }
          }

          hasNext
        }

        override def next(): InternalRow = {

          val fields = new scala.collection.mutable.HashMap[String, Field]()
          for (i <- 0 until curRecord.record.fields.size()) {
            val field = curRecord.record.fields.get(i)
            fields.put(field.measurementId, field)
          }

          //index in one required row
          var index = 0
          requiredSchema.foreach((field: StructField) => {
            if (field.name == SQLConstant.RESERVED_TIME) {
              rowBuffer(index) = curRecord.record.timestamp
            } else if (field.name == SQLConstant.RESERVED_DELTA_OBJECT) {
              rowBuffer(index) = deltaObjectId
            } else if (DefaultSource.columnNames.contains(field.name)) {
              val columnIndex = DefaultSource.columnNames.indexOf(field.name)
              val columns = deltaObjectId.split(SQLConstant.REGEX_PATH_SEPARATOR)
              rowBuffer(index) = columns(columnIndex)
            } else {
              rowBuffer(index) = Converter.toSqlValue(fields.getOrElse(field.name, null))
            }
            index += 1
          })

          encoderForDataColumns.toRow(safeDataRow)
        }
      }
    }
  }

  override def shortName(): String = "tsfile"

  override def prepareWrite(sparkSession: SparkSession,
                            job: Job,
                            options: Map[String, String],
                            dataSchema: StructType): OutputWriterFactory = {
    DefaultSource.columnNames.clear()

    //unfold delta_object
    if (options.contains(SQLConstant.DELTA_OBJECT_NAME)) {
      val columns = options(SQLConstant.DELTA_OBJECT_NAME).split(SQLConstant.REGEX_PATH_SEPARATOR)
      columns.foreach( f => {
        DefaultSource.columnNames += f
      })
    } else {
      //using delta_object
      DefaultSource.columnNames += SQLConstant.RESERVED_DELTA_OBJECT
    }

    new TsFileWriterFactory(options, DefaultSource.columnNames)
  }

}


private[tsfile] object DefaultSource {
  val path = "path"
  val columnNames = new ArrayBuffer[String]()

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
