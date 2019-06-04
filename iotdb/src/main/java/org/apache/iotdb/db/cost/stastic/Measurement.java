/**
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
package org.apache.iotdb.db.cost.stastic;

import java.util.Date;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.iotdb.db.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.db.concurrent.ThreadName;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Measurement {

  /**
   * queue for store time latrncies async.
   */
  private Queue<Long>[] operationLatenciesQueue;

  /**
   * latencies sum of each operation.
   */
  private long[] operationLatencies;

  /**
   * count of each operation.
   */
  private long[] operationCnt;

  /**
   * display thread.
   */
  private ScheduledExecutorService service;

  public static final Measurement INSTANCE = AsyncMeasurementHolder.MEASUREMENT;

  private boolean isEnableStat;
  private long displayIntervalInMs;
  private static final Logger LOGGER = LoggerFactory.getLogger(Measurement.class);



  private Measurement(){
    isEnableStat = IoTDBDescriptor.getInstance().getConfig().isEnableWritePerformanceStat();
    if (isEnableStat) {
      displayIntervalInMs = IoTDBDescriptor.getInstance().getConfig().getPerformanceStatDisplayInterval();
      operationLatenciesQueue = new ConcurrentLinkedQueue[Operation.values().length];
      operationLatencies = new long[Operation.values().length];
      operationCnt = new long[Operation.values().length];
      for (Operation op : Operation.values()){
        operationLatenciesQueue[op.ordinal()] = new ConcurrentLinkedQueue<>();
        operationCnt[op.ordinal()] = 0;
        operationLatencies[op.ordinal()] = 0;
      }

      for (int i =0; i <operationLatenciesQueue.length; i++) {
        new Thread(new NumThread(i)).start();
      }

      service = IoTDBThreadPoolFactory.newScheduledThreadPool(1,
          ThreadName.TIME_COST_STATSTIC.getName());
      service.scheduleWithFixedDelay(
          new Measurement.DisplayRunnable(), 10, displayIntervalInMs, TimeUnit.MILLISECONDS);
      System.out.println("init finish!");
    }
  }
  class NumThread implements  Runnable{
    int i;
    public NumThread(int i) {
      this.i = i;
    }

    @Override
    public void run() {
      Queue<Long> queue = operationLatenciesQueue[i];
      while (true) {
        Long time = queue.poll();
        if (time != null) {
          operationLatencies[i] += time;
          operationCnt[i]++;
        }else {
          try {
            Thread.sleep(10);
          } catch (InterruptedException e) {
          }
        }
      }
    }
  }

  public void addOperationLatency(Operation op, long startTime) {
    if (isEnableStat) {
      operationLatenciesQueue[op.ordinal()].add((System.nanoTime() - startTime));
    }
  }

  private static class  AsyncMeasurementHolder{
    private static final Measurement MEASUREMENT = new Measurement();
    private AsyncMeasurementHolder(){}
  }

  public void showMeasurements() {
    Date date = new Date();
    LOGGER.info("====================================={} Measurement (ms)======================================", date.toString());
    String head = String.format("%-45s%-30s%-30s%-30s","OPERATION", "COUNT", "TOTAL_TIME", "AVG_TIME");
    LOGGER.info(head);
    for(Operation operation : Operation.values()){
      long cnt = operationCnt[operation.ordinal()];
      long totalInMs = 0;
      totalInMs = operationLatencies[operation.ordinal()] / 1000000;
      String avg = String.format("%.4f", (totalInMs/(cnt+1e-9)));
      String item = String.format("%-45s%-30s%-30s%-30s", operation.name, cnt+"", totalInMs+"", avg);
      LOGGER.info(item);
    }
    LOGGER.info(
        "=================================================================================================================");
  }

  class DisplayRunnable implements Runnable{
    @Override
    public void run() {
      showMeasurements();
    }
  }
}
