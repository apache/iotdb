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
package org.apache.iotdb.db.engine.storagegroup;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.iotdb.db.engine.StorageEngine;
import org.apache.iotdb.db.exception.ProcessorException;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.MetadataErrorException;
import org.apache.iotdb.db.exception.PathErrorException;
import org.apache.iotdb.db.exception.StorageGroupException;
import org.apache.iotdb.db.metadata.MManager;
import org.apache.iotdb.db.qp.physical.crud.InsertPlan;
import org.apache.iotdb.db.utils.RandomNum;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.write.record.TSRecord;
import org.apache.iotdb.tsfile.write.record.datapoint.LongDataPoint;

/**
 * Bench The storage group manager with mul-thread and get its performance.
 */
public class FileNodeManagerBenchmark {

  private static int numOfWorker = 10;
  private static int numOfDevice = 10;
  private static int numOfMeasurement = 10;
  private static long numOfTotalLine = 10000000;
  private static CountDownLatch latch = new CountDownLatch(numOfWorker);
  private static AtomicLong atomicLong = new AtomicLong();

  private static String[] devices = new String[numOfDevice];
  private static String prefix = "root.bench";
  private static String[] measurements = new String[numOfMeasurement];

  static {
    for (int i = 0; i < numOfDevice; i++) {
      devices[i] = prefix + "." + "device_" + i;
    }
  }

  static {
    for (int i = 0; i < numOfMeasurement; i++) {
      measurements[i] = "measurement_" + i;
    }
  }

  private static void prepare()
      throws MetadataErrorException, PathErrorException, IOException, StorageGroupException {
    MManager manager = MManager.getInstance();
    manager.setStorageGroupToMTree(prefix);
    for (String device : devices) {
      for (String measurement : measurements) {
        manager.addPathToMTree(device + "." + measurement, TSDataType.INT64.toString(),
            TSEncoding.PLAIN.toString());
      }
    }
  }

  private static void tearDown() throws IOException, StorageEngineException {
    EnvironmentUtils.cleanEnv();
  }

  public static void main(String[] args)
      throws InterruptedException, IOException, MetadataErrorException,
      PathErrorException, StorageEngineException, StorageGroupException {
    tearDown();
    prepare();
    long startTime = System.currentTimeMillis();
    for (int i = 0; i < numOfWorker; i++) {
      Worker worker = new Worker();
      worker.start();
    }
    latch.await();
    long endTime = System.currentTimeMillis();
    System.out.println("Elapsed time: " + (endTime - startTime) + "ms");
    tearDown();
  }

  private static TSRecord getRecord(String deltaObjectId, long timestamp) {
    TSRecord tsRecord = new TSRecord(timestamp, deltaObjectId);
    for (String measurement : measurements) {
      tsRecord.addTuple(new LongDataPoint(measurement, timestamp));
    }
    return tsRecord;
  }

  private static class Worker extends Thread {

    @Override
    public void run() {
      try {
        while (true) {
          long seed = atomicLong.addAndGet(1);
          if (seed > numOfTotalLine) {
            break;
          }
          long time = RandomNum.getRandomLong(1, seed);
          String deltaObject = devices[(int) (time % numOfDevice)];
          TSRecord tsRecord = getRecord(deltaObject, time);
          StorageEngine.getInstance().insert(new InsertPlan(tsRecord));
        }
      } catch (ProcessorException e) {
        e.printStackTrace();
      } finally {
        latch.countDown();
      }
    }
  }
}
