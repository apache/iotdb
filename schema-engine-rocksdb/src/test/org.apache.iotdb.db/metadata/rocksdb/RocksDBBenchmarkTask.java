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
package org.apache.iotdb.db.metadata.schemaregion.rocksdb;

import org.apache.commons.lang3.time.StopWatch;

import java.util.Collection;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

public class RocksDBBenchmarkTask<T> {
  private Collection<T> dataSet;
  private int workCount;
  private int timeoutInMin;

  RocksDBBenchmarkTask(Collection<T> dataSet, int workCount, int timeoutInMin) {
    this.dataSet = dataSet;
    this.workCount = workCount;
    this.timeoutInMin = timeoutInMin;
  }

  public BenchmarkResult runBatchWork(Function<T, TaskResult> work, String name) {
    ExecutorService executor = Executors.newFixedThreadPool(workCount);
    AtomicInteger sucCounter = new AtomicInteger(0);
    AtomicInteger failCounter = new AtomicInteger(0);
    StopWatch stopWatch = new StopWatch();
    stopWatch.start();
    for (T input : dataSet) {
      executor.submit(
          () -> {
            TaskResult result = work.apply(input);
            sucCounter.addAndGet(result.success);
            failCounter.addAndGet(result.failure);
          });
    }
    try {
      executor.shutdown();
      executor.awaitTermination(timeoutInMin, TimeUnit.MINUTES);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    stopWatch.stop();
    return new BenchmarkResult(name, sucCounter.get(), failCounter.get(), stopWatch.getTime());
  }

  public BenchmarkResult runWork(Function<T, Boolean> work, String name) {
    ExecutorService executor = Executors.newFixedThreadPool(workCount);
    AtomicInteger sucCounter = new AtomicInteger(0);
    AtomicInteger failCounter = new AtomicInteger(0);
    StopWatch stopWatch = new StopWatch();
    stopWatch.start();
    for (T input : dataSet) {
      executor.submit(
          () -> {
            if (work.apply(input)) {
              sucCounter.incrementAndGet();
            } else {
              failCounter.incrementAndGet();
            }
          });
    }
    try {
      executor.shutdown();
      executor.awaitTermination(timeoutInMin, TimeUnit.MINUTES);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    stopWatch.stop();
    return new BenchmarkResult(name, sucCounter.get(), failCounter.get(), stopWatch.getTime());
  }

  public static class TaskResult {
    public int success = 0;
    public int failure = 0;
  }

  public static class BenchmarkResult {
    public String name;
    public long successCount;
    public long failCount;
    public long costInMs;

    BenchmarkResult(String name, long successCount, long failCount, long cost) {
      this.name = name;
      this.successCount = successCount;
      this.failCount = failCount;
      this.costInMs = cost;
    }
  }
}
