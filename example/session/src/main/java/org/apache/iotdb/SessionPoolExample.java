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

import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.pool.SessionPool;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.write.record.Tablet;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

@SuppressWarnings({"squid:S106", "squid:S1144"})
public class SessionPoolExample {
  private static final Logger LOGGER = LoggerFactory.getLogger(SessionPoolExample.class);

  private static SessionPool sessionPool;
  private static ExecutorService service;

  private static int loop = 10;

  /** Build a redirect-able SessionPool for this example */
  private static void constructRedirectSessionPool() {
    List<String> nodeUrls = new ArrayList<>();
    nodeUrls.add("127.0.0.1:6667");
    sessionPool =
        new SessionPool.Builder()
            .nodeUrls(nodeUrls)
            .user("root")
            .password("root")
            .maxSize(10)
            .build();
  }

  public static void main(String[] args) {
    // Choose the SessionPool you going to use
    constructRedirectSessionPool();

    try {
      loop = Integer.parseInt(args[0]);
      System.out.println("loop is " + loop);
      service = Executors.newFixedThreadPool(Integer.parseInt(args[1]));
      long time = 0;
      insertTablets(time, 1000, 1);
      time += 1000L * loop;
      Thread.sleep(10000);

      insertTablets(time, 1000, 10);
      time += 1000L * loop;
      Thread.sleep(10000);

      insertTablets(time, 1000, 100);
      time += 1000L * loop;
      Thread.sleep(10000);

      insertTablets(time, 1000, 1000);
    } catch (Throwable e) {
      System.out.println("写入失败" + e.getMessage());
    } finally {
      sessionPool.close();
      service.shutdown();
    }
  }

  private static void insertTablets(long timestamp, int batchSize, int deviceInBatch) {
    // The schema of measurements of one device
    // only measurementId and data type in MeasurementSchema take effects in Tablet
    List<MeasurementSchema> schemaList = new ArrayList<>();
    for (int measurement = 0; measurement < 200; measurement++) {
      schemaList.add(new MeasurementSchema("s" + measurement, TSDataType.INT64));
    }
    List<String> allDevices = new ArrayList<>();
    for (int device = 0; device < 1000; device++) {
      allDevices.add("root.sg1.d" + device);
    }
    List<Future<?>> futures = new ArrayList<>();
    long startTime = System.currentTimeMillis();
    for (int no = 0; no < loop; no++) {
      final int finalNo = no;
      final long finalTimestamp = timestamp;
      Future<?> future =
          service.submit(
              () -> {
                try {
                  long myTime = finalTimestamp + (long) finalNo * batchSize;
                  Collections.shuffle(allDevices);
                  Map<String, Tablet> tablets = new HashMap<>();
                  List<String> loopDevice = allDevices.subList(0, deviceInBatch);
                  for (String deviceId : loopDevice) {
                    Tablet tablet = new Tablet(deviceId, schemaList, batchSize / deviceInBatch);
                    tablets.put(deviceId, tablet);
                  }
                  for (int batch = 0; batch < batchSize; batch++) {
                    Tablet tablet = tablets.get(loopDevice.get(batch % deviceInBatch));
                    int row = tablet.rowSize++;
                    for (long measurement = 0; measurement < 200; measurement++) {
                      tablet.addTimestamp(row, myTime + batch);
                      tablet.addValue("s" + measurement, row, measurement);
                    }
                  }
                  //                          for(Map.Entry<String,Tablet>
                  // entry:tablets.entrySet()){
                  //                            sessionPool.insertTablet(entry.getValue());
                  //                          }
                  sessionPool.insertTablets(tablets);
                } catch (IoTDBConnectionException | StatementExecutionException e) {
                  System.out.println("Failed to insert tablets because " + e.getMessage());
                }
              });
      futures.add(future);
    }
    for (Future<?> future : futures) {
      try {
        future.get();
      } catch (Exception e) {
        System.out.println("Failed to execute future " + e.getMessage());
        Thread.currentThread().interrupt();
      }
    }
    System.out.println("耗时" + (System.currentTimeMillis() - startTime) + "ms");
  }
}
