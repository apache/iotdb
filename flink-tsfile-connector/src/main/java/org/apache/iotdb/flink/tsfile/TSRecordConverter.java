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

package org.apache.iotdb.flink.tsfile;

import org.apache.iotdb.tsfile.write.record.TSRecord;
import org.apache.iotdb.tsfile.write.schema.Schema;

import org.apache.flink.util.Collector;

import java.io.IOException;
import java.io.Serializable;

/**
 * The converter describes how to turn a data object into multiple TSRecord objects, which is
 * required by the {@link TSRecordOutputFormat}.
 *
 * @param <T> The type of the upstream data.
 */
public interface TSRecordConverter<T> extends Serializable {

  /**
   * Opens current converter.
   *
   * @param schema The schema of the TSRecord.
   */
  void open(Schema schema) throws IOException;

  /**
   * Converts the input data into one or multiple TSRecords. The collector in param list is used to
   * collect the output.
   *
   * <p>When this method is called, the converter is guaranteed to be opened.
   *
   * @param input The input data.
   * @param collector The collector used to collect the output.
   */
  void convert(T input, Collector<TSRecord> collector) throws IOException;

  /**
   * Method that marks the end of the life-cycle of this converter.
   *
   * <p>When this method is called, the converter is guaranteed to be opened.
   */
  void close() throws IOException;
}
