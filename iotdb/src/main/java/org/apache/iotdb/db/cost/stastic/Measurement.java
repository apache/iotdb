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
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.iotdb.db.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.db.concurrent.ThreadName;

public class Measurement {

  private Map<Operation, AtomicLong> operationLatencies;
  private Map<Operation, AtomicLong> operationCnt;
  private ScheduledExecutorService service;

  public static final Measurement INSTANCE = MeasurementHolder.MEASUREMENT;

  private Measurement() {
    operationLatencies = new ConcurrentHashMap<>();
    for (Operation operation : Operation.values()) {
      operationLatencies.put(operation, new AtomicLong(0));
    }

    operationCnt = new ConcurrentHashMap<>();
    for (Operation operation : Operation.values()) {
      operationCnt.put(operation, new AtomicLong(0));
    }

    service = IoTDBThreadPoolFactory.newScheduledThreadPool(1,
        ThreadName.TIME_COST_STATSTIC.getName());
    service.scheduleWithFixedDelay(
        new DisplayRunnable(), 30, 60, TimeUnit.SECONDS);
    System.out.println("AFTER SERVICE:"+Operation.values());
  }

  public void addOperationLatency(Operation op, long latency) {
    operationLatencies.get(op).getAndAdd(latency);
    operationCnt.get(op).incrementAndGet();
  }

  public void showMeasurements() {
    Date date = new Date();
    System.out.println("--------------------------------"+String.format("%s Measurement (ms)", date.toString())+"-----------------------------------");
    String head = String.format("%-45s%-30s%-30s%-30s","OPERATION", "COUNT", "TOTAL_TIME", "AVG_TIME");
    System.out.println(head);
    for(Operation operation : Operation.values()){
      long cnt = operationCnt.get(operation).get();
      long totalInMs = operationLatencies.get(operation).get() / 1000000;
      String avg = String.format("%.4f", (totalInMs/(cnt+1e-9)));
      String item = String.format("%-45s%-30s%-30s%-30s", operation.name, cnt+"", totalInMs+"", avg);
      System.out.println(item);
    }
    System.out.println(
        "-----------------------------------------------------------------------------------------------------------------");
  }

  class DisplayRunnable implements Runnable{
    @Override
    public void run() {
      showMeasurements();
    }
  }

  private static class MeasurementHolder{
    private static final Measurement MEASUREMENT = new Measurement();
    private MeasurementHolder(){}
  }

  public static void main(String[] args){
    Measurement.INSTANCE.addOperationLatency(Operation.CHECK_AUTHORIZATION, 90L);
    System.out.println("hhhhhh");
  }
}

