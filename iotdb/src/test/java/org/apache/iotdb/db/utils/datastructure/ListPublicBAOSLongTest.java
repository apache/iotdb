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
package org.apache.iotdb.db.utils.datastructure;

import java.io.*;
import java.lang.Thread;
import java.util.Random;

import static org.apache.iotdb.db.utils.datastructure.ListPublicBAOSPool.ARRAY_SIZE;

public class ListPublicBAOSLongTest {
  private static final int consumerNum = 1;

  public static void main(String[] args) throws InterruptedException, FileNotFoundException {
    ListPublicBAOS initPublicBAOSPool = new ListPublicBAOS(1024 * 512);
    initPublicBAOSPool.reset();

    System.out.println("start testing");

    Consumer[] consumers = new Consumer[consumerNum];
    for (int i = 0; i < consumerNum; i++) {
      consumers[i] = new Consumer(i);
      consumers[i].start();
    }

    for (int i = 0; i < consumerNum; i++) {
      try {
        consumers[i].join();
      } catch (InterruptedException e) {
        System.out.println("Thread " + i + " interrupted.");
      }
    }
  }
}

class Consumer extends Thread {

  private static final int maxRandomWriteTimes = 512 * 1024 + 1;

  private static final int statisticSpace = 100 * 1024;

  private Random random = new Random();

  private final byte[] block = new byte[512 * 1024];

  private double[] chunkTotalWriteTime = new double[maxRandomWriteTimes / statisticSpace + 1];

  private double[] chunkWriteCnt = new double[maxRandomWriteTimes / statisticSpace + 1];

  private int consumerId;

  private int count = 0;

  private double chunkUnusedRoom = 0;

  Consumer(int id) {
    consumerId = id;
  }

  public void run() {
    for (int i = 0; i < chunkTotalWriteTime.length; i++) {
      chunkTotalWriteTime[i] = 0;
      chunkWriteCnt[i] = 0;
    }
    int printInterval = 0;
    while (true) {
      chunkWrite();

      printInterval++;
      if (printInterval == 500) {
        generateStatisticData(chunkTotalWriteTime, chunkWriteCnt);
        printInterval = 0;
      }
    }
  }

  private void chunkWrite() {
    ListPublicBAOS chunk = new ListPublicBAOS();
    int writeNum = random.nextInt(maxRandomWriteTimes);

    //get write time
    long start, end;
    start = System.nanoTime();
    chunk.write(block, 0, writeNum);
    end = System.nanoTime();

    int index = writeNum / statisticSpace;
    chunkTotalWriteTime[index] += (end - start);
    chunkWriteCnt[index] += writeNum;

    count++;
    chunkUnusedRoom += ARRAY_SIZE - (chunk.size() % ARRAY_SIZE);

    chunk.reset();
  }

  private void generateStatisticData(double[] writeTime, double[] writeCnt) {
    for (int i = 0; i < writeTime.length; i++) {
      double totalTime = writeTime[i] / 1000000.0;//ms
      double totalWrite = writeCnt[i] / 1024 / 1024;//MB
      System.out.println(totalTime / totalWrite);// ms/MB
    }
    System.out.println(chunkUnusedRoom / count);
    System.out.println("-----------------------------------------------");
    System.out.println();
  }
}

