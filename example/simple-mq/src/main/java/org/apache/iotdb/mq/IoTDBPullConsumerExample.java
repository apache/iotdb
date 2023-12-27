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

package org.apache.iotdb.mq;

import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.session.mq.IoTDBPollConsumer;
import org.apache.iotdb.tsfile.write.record.Tablet;

import java.net.URISyntaxException;

public class IoTDBPullConsumerExample {

  //  private void printTablet(Tablet tablet) {
  //    List<MeasurementSchema> schemas = tablet.getSchemas();
  //    int rowSize = tablet.rowSize;
  //    HashMap<String, Pair<BitMap, List<Object>>> values = new HashMap<>();
  //    List<String> timeseriesList = new ArrayList<>();
  //    timeseriesList.add("Time");
  //    for (MeasurementSchema schema : schemas) {
  //      String timeseries = String.format("%s.%s", tablet.deviceId, schema.getMeasurementId());
  //      timeseriesList.add(timeseries);
  //      values.put(
  //          timeseries,
  //          new Pair<>(
  //              tablet.bitMaps[schemas.indexOf(schema)],
  //              object2List(tablet.values[schemas.indexOf(schema)])));
  //    }
  //    System.out.println(StringUtils.joinWith("\t", timeseriesList));
  //    for (int i = 0; i < rowSize; i++) {
  //      ArrayList<Object> row = new ArrayList<>();
  //      row.add(tablet.timestamps[i]);
  //      for (String timeseries : timeseriesList) {
  //        if ("Time".equals(timeseries)) {
  //          continue;
  //        }
  //        if (values.containsKey(timeseries)
  //            && (values.get(timeseries).getLeft() == null
  //                || !values.get(timeseries).getLeft().isMarked(i))) {
  //          row.add(values.get(timeseries).getRight().get(i));
  //        } else {
  //          row.add(null);
  //        }
  //      }
  //      System.out.println(StringUtils.joinWith("\t", row));
  //    }
  //  }
  //
  //  public static List<Object> object2List(Object obj) {
  //    ArrayList<Object> objects = new ArrayList<>();
  //    int length = Array.getLength(obj);
  //    for (int i = 0; i < length; i++) {
  //      objects.add(Array.get(obj, i));
  //    }
  //    return objects;
  //  }

  public static void main(String[] args)
      throws IoTDBConnectionException, URISyntaxException, InterruptedException,
          StatementExecutionException {
    IoTDBPollConsumer pollConsumer =
        new IoTDBPollConsumer.Builder()
            .host("127.0.0.1")
            .port(6667)
            .brokerPort(18080)
            .username("root")
            .password("root")
            .tabletBufferSize(10)
            .pattern("root")
            .id("1")
            .build();
    pollConsumer.open();

    Session session = new Session.Builder().port(6668).build();
    session.open(false);

    int counter = 0;
    while (true) {
      try {
        Tablet tablet = pollConsumer.poll(1);
        if (tablet != null) {
          session.insertTablet(tablet);
          System.out.println(String.format("Sent tablet: %d", ++counter));
        } else {
          System.out.println(String.format("No data"));
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      } catch (IoTDBConnectionException | StatementExecutionException e) {
        throw new RuntimeException(e);
      }
    }
  }
}
