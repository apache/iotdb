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
package org.apache.iotdb.db.mpp.plan.planner.plan.node;

import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.pool.SessionPool;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.filter.operator.In;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Random;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import static java.lang.System.out;

public class HJQTest {

  private static final long CLIENT_NUMBER = 100;
  private static final long DEVICE_NUMBER = 40000000;
  private static final int NUM_PICKS = 35000;
  private static final int MIN_NUMBER = 1;
  private static final int MAX_NUMBER = 39999;

  private static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

  private static final AtomicInteger doneDevice = new AtomicInteger(0);

  private static SessionPool sessionPool;

  public static void main(String[] args) throws Exception {
    Timer timer = new Timer();
    try {
      sessionPool =
          new SessionPool.Builder()
              .host("11.101.17.15")
              .port(6667)
              .user("root")
              .password("root")
              .maxSize(800)
              .build();
      insert();

//      prepareSchema();
      Thread.sleep(600_000);
      // 写十分钟
    } finally {
      timer.cancel();
      sessionPool.close();
    }
  }

  private static void insert() {
    List<String> templete = Arrays.asList("guid", "updateTime", "status", "source", "content");

    ExecutorService executorService = Executors.newFixedThreadPool(500);
    IntStream.range(0,499).forEach(
            internal->      executorService.submit(()->{
              while (true){
                try {
                  int internalNode = internal;
                  boolean b = new Random().nextBoolean();
                  if(b){
                    internalNode += 500;
                  }
                  List<Long> times = new ArrayList<>();
                  List<List<String>> values = new ArrayList<>();
                  List<List<String>> measurements = new ArrayList<>();
                  for (int i = 0; i < 1000; i++) {
                    measurements.add(templete);
                    times.add(System.currentTimeMillis());
                    values.add(
                            Arrays.asList(
                                    "12345679012345678",
                                    String.valueOf(System.currentTimeMillis()),
                                    "",
                                    "rhtx-securityDevice",
                                    "xxxxx监测到声音"));
                  }
                  sessionPool.insertAlignedRecords(
                          pickRandomNumbers(internalNode, 1000, MIN_NUMBER, MAX_NUMBER),
                          times,
                          measurements,
                          values);
                } catch (Exception e) {
                  e.printStackTrace();
                }
              }
            })
    );
  }

    private static List<String> pickRandomNumbers(int internal, int numPicks, int minNumber, int maxNumber) {
      List<String> res = new ArrayList<>();
      Random random = new Random();
      for (int i = 0; i < numPicks; i++) {

        String phoneNum = String.format("131%05d", random.nextInt(maxNumber - minNumber + 1) + minNumber);
        res.add(String.format("root.hjq_push.i_%s.u_%s", internal ,phoneNum));
      }
      return res;
    }

  private static void prepareSchema() throws Exception {
    // create database
    sessionPool.createDatabase("root.hjq_push");
    sessionPool.createSchemaTemplate(
        "hjq_push_template",
        Arrays.asList("guid", "updateTime", "status", "source", "content"),
        Arrays.asList(
            TSDataType.INT64, TSDataType.INT64, TSDataType.INT32, TSDataType.TEXT, TSDataType.TEXT),
        Arrays.asList(
            TSEncoding.RLE, TSEncoding.RLE, TSEncoding.RLE, TSEncoding.PLAIN, TSEncoding.PLAIN),
        Arrays.asList(
            CompressionType.SNAPPY,
            CompressionType.SNAPPY,
            CompressionType.SNAPPY,
            CompressionType.SNAPPY,
            CompressionType.SNAPPY),
        true);
    sessionPool.setSchemaTemplate("hjq_push_template", "root.hjq_push");
    out.println("successfully create database and template");

    long startTime = System.currentTimeMillis();
    ExecutorService pool = Executors.newFixedThreadPool((int) CLIENT_NUMBER);
    for (int j = 0; j < CLIENT_NUMBER; j++) {
      pool.execute(
          new SessionClientRunnable(
              (int) (DEVICE_NUMBER / CLIENT_NUMBER), (int) (j * DEVICE_NUMBER / CLIENT_NUMBER)));
    }
    pool.shutdown();
    while (true) { // 等待所有任务都执行结束
      if (pool.isTerminated()) { // 所有的子线程都结束了
        System.out.printf("创建TS成功, 共耗时: %fs", (System.currentTimeMillis() - startTime) / 1000.0);
        break;
      }
    }
  }

  static class SessionClientRunnable implements Runnable {
    private int deviceNum;
    private int startIndex;

    public SessionClientRunnable(int deviceNum, int startIndex) {
      this.startIndex = startIndex;
      this.deviceNum = deviceNum;
    }

    @Override
    public void run() {
      int BATCH = 1000;
      try {
        for (int i = 0; i < deviceNum; i+=BATCH) {
          List<String> device = new ArrayList<>();
          for (int j = 0; j < BATCH; j++) {
            String phoneNum = String.format("131%08d", startIndex + i + j);
            device.add(String.format("root.hjq_push.i_%s.u_%s",  phoneNum.substring(8),phoneNum.substring(0,8)));
          }
          sessionPool.createTimeseriesUsingSchemaTemplate(device);
          doneDevice.addAndGet(BATCH);
        }
      } catch (Exception e) {
        out.println(
            " deviceNum: " + deviceNum + ", startIndex: " + startIndex + " " + e.getMessage());
      }
    }
  }
}
