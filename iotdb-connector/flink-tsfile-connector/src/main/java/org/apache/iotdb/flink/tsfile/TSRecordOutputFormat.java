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

import org.apache.iotdb.tsfile.common.conf.TSFileConfig;
import org.apache.iotdb.tsfile.exception.write.WriteProcessException;
import org.apache.iotdb.tsfile.write.record.TSRecord;
import org.apache.iotdb.tsfile.write.schema.Schema;

import org.apache.flink.util.Collector;
import org.apache.flink.util.FlinkRuntimeException;

import java.io.IOException;

/**
 * Output format that writes TsFiles by {@link TSRecord}. Users need to provide a {@link
 * TSRecordConverter} used to convert the upstream data to {@link TSRecord}.
 *
 * @param <T> The input type of this output format.
 */
public class TSRecordOutputFormat<T> extends TsFileOutputFormat<T> {

  private final TSRecordConverter<T> converter;

  private transient TSRecordCollector tsRecordCollector = null;

  public TSRecordOutputFormat(String path, Schema schema, TSRecordConverter<T> converter) {
    this(path, schema, converter, null);
  }

  public TSRecordOutputFormat(Schema schema, TSRecordConverter<T> converter) {
    super(null, schema, null);
    this.converter = converter;
  }

  public TSRecordOutputFormat(
      String path, Schema schema, TSRecordConverter<T> converter, TSFileConfig config) {
    super(path, schema, config);
    this.converter = converter;
  }

  @Override
  public void open(int taskNumber, int numTasks) throws IOException {
    super.open(taskNumber, numTasks);
    converter.open(schema);
    tsRecordCollector = new TSRecordCollector();
  }

  @Override
  public void close() throws IOException {
    converter.close();
    super.close();
  }

  @Override
  public void writeRecord(T t) throws IOException {
    try {
      converter.convert(t, tsRecordCollector);
    } catch (FlinkRuntimeException e) {
      throw new IOException(e.getCause());
    }
  }

  private class TSRecordCollector implements Collector<TSRecord> {

    @Override
    public void collect(TSRecord tsRecord) {
      try {
        writer.write(tsRecord);
      } catch (IOException | WriteProcessException e) {
        throw new FlinkRuntimeException(e);
      }
    }

    @Override
    public void close() {}
  }

  public TSRecordConverter<T> getConverter() {
    return converter;
  }
}
