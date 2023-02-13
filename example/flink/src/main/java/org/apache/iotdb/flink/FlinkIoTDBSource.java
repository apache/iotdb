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
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.common.RowRecord;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.List;

public class FlinkIoTDBSource {

  static final String LOCAL_HOST = "127.0.0.1";
  static final String ROOT_SG1_D1_S1 = "root.sg1.d1.s1";
  static final String ROOT_SG1_D1 = "root.sg1.d1";

  public static void main(String[] args) throws Exception {
    prepareData();

    // run the flink job on local mini cluster
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    IoTDBSourceOptions ioTDBSourceOptions =
        new IoTDBSourceOptions(
            LOCAL_HOST, 6667, "root", "root", "select s1 from " + ROOT_SG1_D1 + " align by device");

    IoTDBSource<RowRecord> source =
        new IoTDBSource<RowRecord>(ioTDBSourceOptions) {
          @Override
          public RowRecord convert(RowRecord rowRecord) {
            return rowRecord;
          }
        };
    env.addSource(source).name("sensor-source").print().setParallelism(2);
    env.execute();
  }

  private static void prepareData() throws IoTDBConnectionException, StatementExecutionException {
    try (Session session = new Session(LOCAL_HOST, 6667, "root", "root")) {
      session.open(false);
      session.setStorageGroup("root.sg1");
      if (!session.checkTimeseriesExists(ROOT_SG1_D1_S1)) {
        session.createTimeseries(
            ROOT_SG1_D1_S1, TSDataType.INT64, TSEncoding.RLE, CompressionType.SNAPPY);
        List<String> measurements = new ArrayList<>();
        measurements.add("s1");
        measurements.add("s2");
        measurements.add("s3");
        List<TSDataType> types = new ArrayList<>();
        types.add(TSDataType.INT64);
        types.add(TSDataType.INT64);
        types.add(TSDataType.INT64);

        for (long time = 0; time < 100; time++) {
          List<Object> values = new ArrayList<>();
          values.add(1L);
          values.add(2L);
          values.add(3L);
          session.insertRecord(ROOT_SG1_D1, time, measurements, types, values);
        }
      }
    } catch (StatementExecutionException e) {
      if (e.getStatusCode() != TSStatusCode.PATH_ALREADY_EXIST.getStatusCode()) {
        throw e;
      }
    }
  }
}
