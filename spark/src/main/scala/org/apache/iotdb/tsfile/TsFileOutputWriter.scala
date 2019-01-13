/**
 * Copyright © 2019 Apache IoTDB(incubating) (dev@iotdb.apache.org)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.iotdb.tsfile

import org.apache.iotdb.tsfile.timeseries.write.record.TSRecord
import org.apache.iotdb.tsfile.io.TsFileOutputFormat
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.mapreduce.{RecordWriter, TaskAttemptContext}
import org.apache.spark.sql.Row
import org.apache.spark.sql.execution.datasources.OutputWriter
import org.apache.spark.sql.types._

import scala.collection.mutable.ArrayBuffer

private[tsfile] class TsFileOutputWriter(
                                          pathStr: String,
                                          columnNames: ArrayBuffer[String],
                                          dataSchema: StructType,
                                          options: Map[String, String],
                                          context: TaskAttemptContext) extends OutputWriter{

  private val recordWriter: RecordWriter[NullWritable, TSRecord] = {
    val fileSchema = Converter.toTsFileSchema(columnNames, dataSchema, options)
    new TsFileOutputFormat(fileSchema).getRecordWriter(context)
  }

  override def write(row: Row): Unit = {
    if( row != null) {
      val tsRecord = Converter.toTsRecord(columnNames, row)
      recordWriter.write(NullWritable.get(), tsRecord)
    }
  }

  override def close(): Unit = {
    recordWriter.close(context)
  }
}
