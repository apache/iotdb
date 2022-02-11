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

import org.apache.iotdb.hadoop.tsfile.record.HDFSTSRecord;
import org.apache.iotdb.tsfile.write.schema.Schema;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Objects;

public class TSFOutputFormat extends FileOutputFormat<NullWritable, HDFSTSRecord> {

  private static final Logger logger = LoggerFactory.getLogger(TSFOutputFormat.class);
  private static final String extension = "";
  private static Schema schema;

  public static Schema getSchema() {
    return schema;
  }

  public static void setSchema(Schema schema) {
    TSFOutputFormat.schema = schema;
  }

  @Override
  public RecordWriter<NullWritable, HDFSTSRecord> getRecordWriter(TaskAttemptContext job)
      throws IOException {

    Path outputPath = getDefaultWorkFile(job, extension);
    logger.info(
        "The task attempt id is {}, the output path is {}", job.getTaskAttemptID(), outputPath);
    return new TSFRecordWriter(job, outputPath, Objects.requireNonNull(schema));
  }
}
