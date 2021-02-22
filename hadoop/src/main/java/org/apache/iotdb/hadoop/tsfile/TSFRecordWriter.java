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
package org.apache.iotdb.hadoop.tsfile;

import org.apache.iotdb.hadoop.fileSystem.HDFSOutput;
import org.apache.iotdb.hadoop.tsfile.record.HDFSTSRecord;
import org.apache.iotdb.tsfile.exception.write.WriteProcessException;
import org.apache.iotdb.tsfile.write.TsFileWriter;
import org.apache.iotdb.tsfile.write.schema.Schema;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class TSFRecordWriter extends RecordWriter<NullWritable, HDFSTSRecord> {

  private static final Logger logger = LoggerFactory.getLogger(TSFRecordWriter.class);

  private TsFileWriter writer;

  public TSFRecordWriter(TaskAttemptContext job, Path path, Schema schema) throws IOException {
    HDFSOutput hdfsOutput = new HDFSOutput(path.toString(), job.getConfiguration(), false);
    writer = new TsFileWriter(hdfsOutput, schema);
  }

  @Override
  public synchronized void write(NullWritable key, HDFSTSRecord value)
      throws IOException, InterruptedException {
    try {
      writer.write(value.convertToTSRecord());
    } catch (WriteProcessException e) {
      throw new InterruptedException(String.format("Write tsfile record error %s", e));
    }
  }

  @Override
  public void close(TaskAttemptContext context) throws IOException {
    logger.info("Close the record writer, the task attempt id is {}", context.getTaskAttemptID());
    writer.close();
  }
}
