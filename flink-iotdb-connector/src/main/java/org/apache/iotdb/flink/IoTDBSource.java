/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.iotdb.flink;

import org.apache.iotdb.flink.options.IoTDBSourceOptions;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.session.SessionDataSet;
import org.apache.iotdb.tsfile.read.common.RowRecord;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class IoTDBSource<T> extends RichSourceFunction<T> {

  private static final Logger LOG = LoggerFactory.getLogger(IoTDBSource.class);
  private static final long serialVersionUID = 1L;
  private IoTDBSourceOptions sourceOptions;

  private transient Session session;
  private transient SessionDataSet dataSet;

  protected IoTDBSource(IoTDBSourceOptions ioTDBSourceOptions) {
    this.sourceOptions = ioTDBSourceOptions;
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    initSession();
  }

  /**
   * Convert raw data (in form of RowRecord) extracted from IoTDB to user-defined data type
   *
   * @param rowRecord row record from IoTDB
   * @return object in user-defined form
   */
  public abstract T convert(RowRecord rowRecord);

  @Override
  public void run(SourceContext<T> sourceContext) throws Exception {
    dataSet = session.executeQueryStatement(sourceOptions.getSql());
    dataSet.setFetchSize(sourceOptions.getFetchSize());
    while (dataSet.hasNext()) {
      sourceContext.collect(convert(dataSet.next()));
    }
    dataSet.closeOperationHandle();
  }

  @Override
  public void cancel() {
    try {
      dataSet.closeOperationHandle();
    } catch (StatementExecutionException | IoTDBConnectionException e) {
      LOG.error(e.getMessage());
    }
  }

  @Override
  public void close() throws Exception {
    super.close();
    try {
      dataSet.closeOperationHandle();
    } catch (StatementExecutionException | IoTDBConnectionException e) {
      throw e;
    } finally {
      session.close();
    }
  }

  void initSession() throws IoTDBConnectionException {
    session =
        new Session(
            sourceOptions.getHost(),
            sourceOptions.getPort(),
            sourceOptions.getUser(),
            sourceOptions.getPassword());
    session.open();
  }
}
