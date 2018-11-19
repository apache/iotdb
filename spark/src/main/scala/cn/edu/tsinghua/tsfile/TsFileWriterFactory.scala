package cn.edu.tsinghua.tsfile

import org.apache.hadoop.mapreduce.TaskAttemptContext
import org.apache.spark.sql.execution.datasources.{OutputWriter, OutputWriterFactory}
import org.apache.spark.sql.types.StructType
import scala.collection.mutable.ArrayBuffer

private[tsfile] class TsFileWriterFactory(options: Map[String, String], columnNames: ArrayBuffer[String]) extends OutputWriterFactory{

  override def newInstance(
                            path: String,
                            bucketId: Option[Int],
                            dataSchema: StructType,
                            context: TaskAttemptContext): OutputWriter = {
    new TsFileOutputWriter(path, columnNames, dataSchema, options, context)
  }
}
