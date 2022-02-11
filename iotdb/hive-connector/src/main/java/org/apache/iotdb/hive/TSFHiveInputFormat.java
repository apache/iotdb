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

import org.apache.iotdb.hadoop.tsfile.TSFInputFormat;

import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;

/**
 * The class implement is same as {@link org.apache.iotdb.hadoop.tsfile.TSFInputFormat} and is
 * customized for Hive to implements JobConfigurable interface.
 */
public class TSFHiveInputFormat extends FileInputFormat<NullWritable, MapWritable> {

  private static final Logger logger = LoggerFactory.getLogger(TSFHiveInputFormat.class);

  @Override
  public RecordReader<NullWritable, MapWritable> getRecordReader(
      InputSplit split, JobConf job, Reporter reporter) throws IOException {
    return new TSFHiveRecordReader(split, job);
  }

  @Override
  public InputSplit[] getSplits(JobConf job, int numSplits) throws IOException {
    job.setBoolean(INPUT_DIR_RECURSIVE, true);
    return TSFInputFormat.getTSFInputSplit(job, Arrays.asList(super.listStatus(job)), logger)
        .toArray(new InputSplit[0]);
  }
}
