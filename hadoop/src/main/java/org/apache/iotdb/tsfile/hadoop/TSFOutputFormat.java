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
package org.apache.iotdb.tsfile.hadoop;

import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TSFOutputFormat extends FileOutputFormat<NullWritable, TSRow> {

  public static final String FILE_SCHEMA = "tsfile.schema";
  private static final Logger LOGGER = LoggerFactory.getLogger(TSFOutputFormat.class);
  private static final String extension = "tsfile";

  public static void setWriterSchema(Job job, JSONObject schema) {

    LOGGER.info("Set the write schema - {}", schema.toString());

    job.getConfiguration().set(FILE_SCHEMA, schema.toString());
  }

  public static void setWriterSchema(Job job, String schema) {

    LOGGER.info("Set the write schema - {}", schema);

    job.getConfiguration().set(FILE_SCHEMA, schema);
  }

  public static JSONObject getWriterSchema(JobContext jobContext) throws InterruptedException {

    String schema = jobContext.getConfiguration().get(FILE_SCHEMA);
    if (schema == null || schema == "") {
      throw new InterruptedException("The tsfile schema is null or empty");
    }
    JSONObject jsonSchema = new JSONObject(schema);

    return jsonSchema;
  }

  @Override
  public RecordWriter<NullWritable, TSRow> getRecordWriter(TaskAttemptContext job)
      throws IOException, InterruptedException {

    Path outputPath = getDefaultWorkFile(job, extension);
    LOGGER.info("The task attempt id is {}, the output path is {}", job.getTaskAttemptID(),
        outputPath);
    JSONObject schema = getWriterSchema(job);
    return new TSFRecordWriter(outputPath, schema);
  }
}
