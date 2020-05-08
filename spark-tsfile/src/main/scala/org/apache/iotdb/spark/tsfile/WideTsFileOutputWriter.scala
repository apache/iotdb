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

import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.mapreduce.{RecordWriter, TaskAttemptContext}
import org.apache.iotdb.spark.tsfile.io.TsFileOutputFormat
import org.apache.iotdb.tsfile.write.record.TSRecord
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.OutputWriter
import org.apache.spark.sql.types._

private[tsfile] class WideTsFileOutputWriter(
                                              pathStr: String,
                                              dataSchema: StructType,
                                              options: Map[String, String],
                                              context: TaskAttemptContext) extends OutputWriter {

  private val recordWriter: RecordWriter[NullWritable, TSRecord] = {
    val fileSchema = WideConverter.toTsFileSchema(dataSchema, options)
    new TsFileOutputFormat(fileSchema).getRecordWriter(context)
  }

  override def write(row: InternalRow): Unit = {
    if (row != null) {
      val tsRecord = WideConverter.toTsRecord(row, dataSchema)
      tsRecord.foreach(r => {
        recordWriter.write(NullWritable.get(), r)
      })
    }
  }

  override def close(): Unit = {
    recordWriter.close(context)
  }
}
