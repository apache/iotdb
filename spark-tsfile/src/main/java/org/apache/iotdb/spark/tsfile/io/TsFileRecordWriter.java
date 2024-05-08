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
package org.apache.iotdb.spark.tsfile.io;

import org.apache.iotdb.hadoop.fileSystem.HDFSOutput;
import org.apache.iotdb.tsfile.exception.write.WriteProcessException;
import org.apache.iotdb.tsfile.write.TsFileWriter;
import org.apache.iotdb.tsfile.write.record.TSRecord;
import org.apache.iotdb.tsfile.write.schema.Schema;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

public class TsFileRecordWriter extends RecordWriter<NullWritable, TSRecord> {

  private TsFileWriter tsFileWriter = null;

  public TsFileRecordWriter(TaskAttemptContext job, Path file, Schema schema) throws IOException {
    HDFSOutput hdfsOutput =
        new HDFSOutput(file.toString(), job.getConfiguration(), false); // NOTE overwrite false here

    tsFileWriter = new TsFileWriter(hdfsOutput, schema);
  }

  @Override
  public void close(TaskAttemptContext context) throws IOException {
    tsFileWriter.close();
  }

  @Override
  public synchronized void write(NullWritable arg0, TSRecord tsRecord) throws IOException {
    try {
      tsFileWriter.write(tsRecord);
    } catch (WriteProcessException e) {
      e.printStackTrace();
    }
  }
}
