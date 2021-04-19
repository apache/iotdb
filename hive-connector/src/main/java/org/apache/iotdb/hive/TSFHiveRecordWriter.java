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
package org.apache.iotdb.hive;

import org.apache.iotdb.hadoop.fileSystem.HDFSOutput;
import org.apache.iotdb.hadoop.tsfile.TSFRecordWriter;
import org.apache.iotdb.hadoop.tsfile.record.HDFSTSRecord;
import org.apache.iotdb.tsfile.exception.write.WriteProcessException;
import org.apache.iotdb.tsfile.write.TsFileWriter;
import org.apache.iotdb.tsfile.write.schema.Schema;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * The function implement is same as {@link org.apache.iotdb.hadoop.tsfile.TSFRecordWriter} and is
 * customized for Hive
 */
public class TSFHiveRecordWriter implements FileSinkOperator.RecordWriter {

  private static final Logger logger = LoggerFactory.getLogger(TSFRecordWriter.class);

  private TsFileWriter writer;

  public TSFHiveRecordWriter(JobConf job, Path path, Schema schema) throws IOException {

    HDFSOutput hdfsOutput = new HDFSOutput(path.toString(), job, false);
    writer = new TsFileWriter(hdfsOutput, schema);
  }

  @Override
  public void write(Writable writable) throws IOException {
    if (!(writable instanceof HDFSTSRecord))
      throw new IOException(
          "Expecting instance of HDFSTSRecord, but received"
              + writable.getClass().getCanonicalName());
    try {
      writer.write(((HDFSTSRecord) writable).convertToTSRecord());
    } catch (WriteProcessException e) {
      throw new IOException(String.format("Write tsfile record error %s", e));
    }
  }

  @Override
  public void close(boolean b) throws IOException {
    logger.info("Close the record writer");
    writer.close();
  }
}
