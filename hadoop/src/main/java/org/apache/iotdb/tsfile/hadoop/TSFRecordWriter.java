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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.iotdb.tsfile.hadoop.io.HDFSOutputStream;
import org.apache.iotdb.tsfile.timeseries.basis.TsFile;
import org.apache.iotdb.tsfile.write.exception.InvalidJsonSchemaException;
import org.apache.iotdb.tsfile.write.exception.WriteProcessException;
import org.apache.iotdb.tsfile.write.schema.FileSchema;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TSFRecordWriter extends RecordWriter<NullWritable, TSRow> {

  private static final Logger LOGGER = LoggerFactory.getLogger(TSFRecordWriter.class);

  private TsFile write = null;

  public TSFRecordWriter(Path path, JSONObject schema) throws InterruptedException, IOException {
    // construct the internalrecordwriter
    FileSchema fileSchema = null;
    try {
      fileSchema = new FileSchema(schema);
    } catch (InvalidJsonSchemaException e) {
      e.printStackTrace();
      LOGGER.error("Construct the tsfile schema failed, the reason is {}", e.getMessage());
      throw new InterruptedException(e.getMessage());
    }

    HDFSOutputStream hdfsOutputStream = new HDFSOutputStream(path, new Configuration(), false);
    try {
      write = new TsFile(hdfsOutputStream, fileSchema);
    } catch (WriteProcessException e) {
      e.printStackTrace();
      throw new IOException(e.getMessage());
    }
  }

  @Override
  public void write(NullWritable key, TSRow value) throws IOException, InterruptedException {

    try {
      write.writeRecord(value.getRow());
    } catch (WriteProcessException e) {
      e.printStackTrace();
      LOGGER.error("Write tsfile record error, the error message is {}", e.getMessage());
      throw new InterruptedException(e.getMessage());
    }
  }

  @Override
  public void close(TaskAttemptContext context) throws IOException, InterruptedException {

    LOGGER.info("Close the recordwriter, the task attempt id is {}", context.getTaskAttemptID());
    write.close();
  }

}
