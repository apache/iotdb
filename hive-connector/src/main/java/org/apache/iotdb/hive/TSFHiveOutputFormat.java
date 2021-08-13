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

import org.apache.iotdb.hadoop.tsfile.TSFOutputFormat;
import org.apache.iotdb.hadoop.tsfile.record.HDFSTSRecord;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.io.HiveOutputFormat;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Progressable;

import java.io.IOException;
import java.util.Properties;

/**
 * The function implement is same as {@link org.apache.iotdb.hadoop.tsfile.TSFOutputFormat} and is
 * customized for Hive
 */
public class TSFHiveOutputFormat extends TSFOutputFormat
    implements HiveOutputFormat<NullWritable, HDFSTSRecord> {

  @Override
  public FileSinkOperator.RecordWriter getHiveRecordWriter(
      JobConf jobConf,
      Path path,
      Class<? extends Writable> aClass,
      boolean b,
      Properties properties,
      Progressable progressable)
      throws IOException {
    return new TSFHiveRecordWriter(jobConf, path, null);
  }

  // no records will be emitted from Hive
  @Override
  public RecordWriter<NullWritable, HDFSTSRecord> getRecordWriter(
      FileSystem ignored, JobConf job, String name, Progressable progress) {
    return new RecordWriter<NullWritable, HDFSTSRecord>() {
      @Override
      public void write(NullWritable key, HDFSTSRecord value) {
        throw new RuntimeException("Should not be called");
      }

      @Override
      public void close(Reporter reporter) {}
    };
  }

  // Not doing any check
  @Override
  public void checkOutputSpecs(FileSystem ignored, JobConf job) {}
}
