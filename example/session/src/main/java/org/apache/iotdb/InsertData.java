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

package org.apache.iotdb;

import org.apache.iotdb.isession.template.Template;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.session.template.MeasurementNode;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;

import java.io.IOException;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

public class InsertData {
  private static Session session;
  private static final String DEVICE = "root.sg";

  private static final long TIME_PARTITION_INTERVAL = 10000 * 200;

  private static final long FILE_NUM = 200;

  private static final long RECORD_NUM = 10000;

  public static void main(String[] args)
      throws IoTDBConnectionException, StatementExecutionException, IOException {
    session = new Session("127.0.0.1", 6667, "root", "root");
    session.open(false);

    session.setFetchSize(10000);

    writeTargetData();
    writeTestData();
  }

  public static void writeTargetData()
      throws IoTDBConnectionException, StatementExecutionException {
    String target = "s0";
    for (int fileNum = 0; fileNum < FILE_NUM; fileNum++) {
      for (int recordNum = 0; recordNum < RECORD_NUM; recordNum++) {
        long time = RECORD_NUM * fileNum + recordNum;
        session.insertRecord(
            DEVICE,
            time,
            Collections.singletonList(target),
            Collections.singletonList(TSDataType.INT64),
            time);
      }
      session.executeNonQueryStatement("flush");
    }
  }

  public static void writeTestData() throws IoTDBConnectionException, StatementExecutionException {
    String[] measurements =
        new String[] {"s1", "s2", "s3", "s4", "s5", "s6", "s7", "s8", "s9", "s10"};
    for (int i = 0; i < measurements.length; i++) {
      String measurement = measurements[i];
      long startTime = (i + 1) * FILE_NUM * RECORD_NUM;
      for (int fileNum = 0; fileNum < 200; fileNum++) {
        for (int recordNum = 0; recordNum < 10000; recordNum++) {
          long time = startTime + RECORD_NUM * fileNum + recordNum;
          session.insertRecord(
              DEVICE,
              time,
              Collections.singletonList(measurement),
              Collections.singletonList(TSDataType.INT64),
              time);
        }
        session.executeNonQueryStatement("flush");
      }
    }
  }

  private static void createTemplate()
      throws IoTDBConnectionException, IOException, StatementExecutionException {
    String[] names = new String[] {"`27`"};
    boolean isAligned = true;
    Map<String, Object[]> structureInfo = new LinkedHashMap<>(6);
    structureInfo.put(
        "s_boolean", new Object[] {TSDataType.BOOLEAN, TSEncoding.RLE, CompressionType.SNAPPY});
    structureInfo.put(
        "s_int", new Object[] {TSDataType.INT32, TSEncoding.RLE, CompressionType.SNAPPY});
    structureInfo.put(
        "s_long", new Object[] {TSDataType.INT64, TSEncoding.RLE, CompressionType.SNAPPY});
    structureInfo.put(
        "s_float", new Object[] {TSDataType.FLOAT, TSEncoding.GORILLA, CompressionType.SNAPPY});
    structureInfo.put(
        "s_double", new Object[] {TSDataType.DOUBLE, TSEncoding.GORILLA, CompressionType.SNAPPY});
    structureInfo.put(
        "s_text", new Object[] {TSDataType.TEXT, TSEncoding.DICTIONARY, CompressionType.SNAPPY});
    for (String templateName : names) {
      String loadNode = "root.template.aligned." + templateName;
      Template template = new Template(templateName, isAligned);

      structureInfo.forEach(
          (key, value) -> {
            MeasurementNode mNode =
                new MeasurementNode(
                    key, (TSDataType) value[0], (TSEncoding) value[1], (CompressionType) value[2]);
            try {
              template.addToTemplate(mNode);
            } catch (StatementExecutionException e) {
              throw new RuntimeException(e);
            }
          });

      session.createSchemaTemplate(template);
      //  IOTDB-5437 StatementExecutionException: 300: COUNT_MEASUREMENTShas not been supported.
      //        assert 6 == session.countMeasurementsInTemplate(templateName) : "查看模版中sensor数目";
      session.setSchemaTemplate(templateName, loadNode);
    }
  }
}
