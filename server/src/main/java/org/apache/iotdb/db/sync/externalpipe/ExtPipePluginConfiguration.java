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

package org.apache.iotdb.db.sync.externalpipe;

import org.apache.iotdb.db.sync.datasource.PipeStorageGroupInfo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/** Configuration class for external pipes. */
public class ExtPipePluginConfiguration {
  private static final Logger logger = LoggerFactory.getLogger(ExtPipePluginConfiguration.class);

  public static final int DEFAULT_NUM_OF_THREADS = 1;
  public static final int DEFAULT_ATTEMPT_TIMES = 3;
  public static final int DEFAULT_BACKOFF_INTERVAL = 1_000;
  public static final int DEFAULT_OPERATION_BATCH_SIZE = 100_000;

  private int numOfThreads = DEFAULT_NUM_OF_THREADS;
  // Number of attempt time for each operation.
  private int attemptTimes = DEFAULT_ATTEMPT_TIMES;
  // Backoff interval between two attempts in milliseconds.
  private int backOffInterval = DEFAULT_BACKOFF_INTERVAL;
  // Number of operations to get from pipe source each time.
  private int operationBatchSize = DEFAULT_OPERATION_BATCH_SIZE;

  private String pipeName;

  /** Array: BucketNum => SG => PipeStorageGroupInfo */
  private Map<String, PipeStorageGroupInfo>[] sgInfoBuckets;

  private ExtPipePluginConfiguration(String pipeName) {
    this.pipeName = pipeName;
  }

  public int getNumOfThreads() {
    return numOfThreads;
  }

  public int getAttemptTimes() {
    return attemptTimes;
  }

  public int getBackOffInterval() {
    return backOffInterval;
  }

  public int getOperationBatchSize() {
    return operationBatchSize;
  }

  public Map<String, PipeStorageGroupInfo> getBucketSgInfoMap(int threadIndex) {
    if (threadIndex >= sgInfoBuckets.length) {
      logger.error(
          "getBucketSgInfoMap(), error. pipeName = {}, threadIndex = {}.", pipeName, threadIndex);
      return null;
    }

    return sgInfoBuckets[threadIndex];
  }

  public static class Builder {
    private final ExtPipePluginConfiguration configuration;

    public Builder(String pipeName) {
      configuration = new ExtPipePluginConfiguration(pipeName);
    }

    /** Set the degree of parallelism of the data transmission. */
    public Builder numOfThreads(int numOfThreads) {
      if (numOfThreads < 1) {
        throw new IllegalArgumentException("Invalid number of threads. Should be larger than 0.");
      }
      configuration.numOfThreads = numOfThreads;
      initSgInfoBuckets(numOfThreads);

      return this;
    }

    private void initSgInfoBuckets(int bucketNum) {
      configuration.sgInfoBuckets = new Map[bucketNum];

      for (int i = 0; i < bucketNum; i++) {
        configuration.sgInfoBuckets[i] = new ConcurrentHashMap<>();
      }
    }

    /** Set the maximum attempt times of handling an operation. */
    public Builder attemptTimes(int attemptTimes) {
      if (attemptTimes < 1) {
        throw new IllegalArgumentException("Invalid attempt times. Should be larger than 0.");
      }
      configuration.attemptTimes = attemptTimes;
      return this;
    }

    /** Set the backoff interval on the failure of handling an operation. */
    public Builder backOffInterval(int backOffInterval) {
      if (backOffInterval < 1) {
        throw new IllegalArgumentException("Invalid backoff interval. Should be larger than 0.");
      }
      configuration.backOffInterval = backOffInterval;
      return this;
    }

    /** Set the maximum number of operations to get from the pipe source. */
    public Builder operationBatchSize(int operationBatchSize) {
      if (operationBatchSize < 1) {
        throw new IllegalArgumentException(
            "Invalid operation batch size. Should be larger than 0.");
      }
      configuration.operationBatchSize = operationBatchSize;
      return this;
    }

    public ExtPipePluginConfiguration build() {
      return configuration;
    }
  }
}
