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

package org.apache.iotdb.extpipe;

import org.apache.iotdb.pipe.external.api.ExternalPipeSinkWriterStatus;
import org.apache.iotdb.pipe.external.api.IExternalPipeSinkWriter;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class ExtPipeSinkWriterImpl implements IExternalPipeSinkWriter {
  private Map<String, String> sinkParams;
  // private ExtSession extSession;   //maintain the connect to ext DB system
  private long startTime;

  public ExtPipeSinkWriterImpl(Map<String, String> sinkParams) {
    this.sinkParams = sinkParams;
  }

  /** IoTDB call this method to initialize 1 IExternalPipeSinkWriter instance. */
  @Override
  public void open() {
    // == Use the parameters in sinkParams to start 1 session to ext DB system
    // extSession = new ExtSession(sinkParams);

    // == Record the start-time of current external session.
    startTime = System.currentTimeMillis();
  }

  @Override
  public void insertBoolean(String sgName, String[] path, long timestamp, boolean value)
      throws IOException {
    // == Here, handle inserted Boolean type data from IoTDB.
    // extSession.insertBoolean(...);
    // ...
  }

  @Override
  public void insertInt32(String sgName, String[] path, long timestamp, int value)
      throws IOException {
    // == Here, handle inserted Int32 type data from IoTDB.
    // extSession.insertInt32(...);
    // ...
  }

  @Override
  public void insertInt64(String sgName, String[] path, long time, long value) throws IOException {
    // == Here, handle inserted Int64 type data from IoTDB.
    // ...
  }

  @Override
  public void insertFloat(String sgName, String[] path, long time, float value) throws IOException {
    // == Here, handle inserted float type data from IoTDB.
    // extSession.insertFloat(...);
    // ...
  }

  @Override
  public void insertDouble(String sgName, String[] path, long time, double value)
      throws IOException {
    // == Here, handle inserted double type data from IoTDB.
    // extSession.insertDouble(...);
    // ...
  }

  @Override
  public void insertText(String sgName, String[] path, long time, String value) throws IOException {
    // == Here, handle inserted Text type data from IoTDB.
    // extSession.insertText(...);
    // ..
  }

  @Override
  public void delete(String sgName, String delPath, long startTime, long endTime)
      throws IOException {

    // == Here, handle delete operation.
    // extSession.delete(...);
    // ...
  }

  //  @Override
  //  public void createTimeSeries(String[] path, DataType dataType) {
  //    //== Here, handle create TimeSeries operation.
  //    //extSession.createTable(...);
  //    //...
  //  }
  //
  //  @Override
  //  public void deleteTimeSeries(String[] path) {
  //    //== Here, handle delete TimeSeries operation.
  //    //extSession.deleteTable(...);
  //    //...
  //  }

  /**
   * IoTDB call this method to flush data in plugin buf to external DB system, if data buf exist.
   *
   * @throws IOException
   */
  @Override
  public void flush() throws IOException {
    // extSession.flush(...);
    // ...
  }

  /**
   * When run CMD "stop pipe ..." or "drop pipe ..." , IoTDB will call this method to close
   * connection to external DB system.
   *
   * @throws IOException
   */
  @Override
  public void close() throws IOException {
    flush();

    // == Close connection to external DB system.
    // extSession.close(...);
  }

  /**
   * /** IoTDB use this method to collect statistic info of 1 ExternalPipeSinkWriter. When run CMD
   * "show pipes", the statistic information will be show.
   *
   * @return
   */
  @Override
  public ExternalPipeSinkWriterStatus getStatus() {
    ExternalPipeSinkWriterStatus status = new ExternalPipeSinkWriterStatus();

    // == set basic statistic info
    status.setStartTime(startTime); // ExternalPipeSinkWriter's beginning time
    // status.setNumOfRecordsTransmitted(extSession.getNumOffRecords());
    // status.setNumOfBytesTransmitted(extSession.getNumOfBytes());

    // == Here, customer may define & add other information.
    Map<String, String> extendedFields = new HashMap<>();
    // extendedFields.put("AverageSpeed", Long.toString(extSession.getAvgSpeed());
    // extendedFields.put("Speed", Long.toString(extSession.getSpeed()));
    // extendedFields.put("SessionId", extSession == null ? "N/A" : extSession.getId());
    status.setExtendedFields(extendedFields);

    return status;
  }
}
