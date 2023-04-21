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

package org.apache.iotdb.db.quotas;

import org.apache.iotdb.common.rpc.thrift.TTimedQuota;
import org.apache.iotdb.common.rpc.thrift.ThrottleType;
import org.apache.iotdb.commons.exception.RpcThrottlingException;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.rpc.TSStatusCode;

import java.util.Map;

public class QuotaLimiter {

  public enum RateLimiterType {
    FixedIntervalRateLimiter,
    AverageIntervalRateLimiter;
  }

  private IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
  private RateLimiter reqsLimiter = null;
  private RateLimiter reqSizeLimiter = null;
  private RateLimiter writeReqsLimiter = null;
  private RateLimiter writeSizeLimiter = null;
  private RateLimiter readReqsLimiter = null;
  private RateLimiter readSizeLimiter = null;

  public QuotaLimiter() {
    if (RateLimiterType.FixedIntervalRateLimiter.name().equals(config.getRateLimiterType())) {
      reqsLimiter = new FixedIntervalRateLimiter();
      reqSizeLimiter = new FixedIntervalRateLimiter();
      writeReqsLimiter = new FixedIntervalRateLimiter();
      writeSizeLimiter = new FixedIntervalRateLimiter();
      readReqsLimiter = new FixedIntervalRateLimiter();
      readSizeLimiter = new FixedIntervalRateLimiter();
    } else {
      reqsLimiter = new AverageIntervalRateLimiter();
      reqSizeLimiter = new AverageIntervalRateLimiter();
      writeReqsLimiter = new AverageIntervalRateLimiter();
      writeSizeLimiter = new AverageIntervalRateLimiter();
      readReqsLimiter = new AverageIntervalRateLimiter();
      readSizeLimiter = new AverageIntervalRateLimiter();
    }
  }

  public static QuotaLimiter fromThrottle(
      Map<ThrottleType, TTimedQuota> throttleLimit, QuotaLimiter limiter) {
    TTimedQuota timedQuota;
    if (throttleLimit.containsKey(ThrottleType.REQUEST_NUMBER)) {
      timedQuota = throttleLimit.get(ThrottleType.REQUEST_NUMBER);
      limiter.reqsLimiter.set(timedQuota.getSoftLimit(), timedQuota.getTimeUnit());
    }

    if (throttleLimit.containsKey(ThrottleType.REQUEST_SIZE)) {
      timedQuota = throttleLimit.get(ThrottleType.REQUEST_SIZE);
      limiter.reqSizeLimiter.set(timedQuota.getSoftLimit(), timedQuota.getTimeUnit());
    }

    if (throttleLimit.containsKey(ThrottleType.WRITE_NUMBER)) {
      timedQuota = throttleLimit.get(ThrottleType.WRITE_NUMBER);
      limiter.writeReqsLimiter.set(timedQuota.getSoftLimit(), timedQuota.getTimeUnit());
    }

    if (throttleLimit.containsKey(ThrottleType.WRITE_SIZE)) {
      timedQuota = throttleLimit.get(ThrottleType.WRITE_SIZE);
      limiter.writeSizeLimiter.set(timedQuota.getSoftLimit(), timedQuota.getTimeUnit());
    }

    if (throttleLimit.containsKey(ThrottleType.READ_NUMBER)) {
      timedQuota = throttleLimit.get(ThrottleType.READ_NUMBER);
      limiter.readReqsLimiter.set(timedQuota.getSoftLimit(), timedQuota.getTimeUnit());
    }

    if (throttleLimit.containsKey(ThrottleType.READ_SIZE)) {
      timedQuota = throttleLimit.get(ThrottleType.READ_SIZE);
      limiter.readSizeLimiter.set(timedQuota.getSoftLimit(), timedQuota.getTimeUnit());
    }
    return limiter;
  }

  /**
   * Checks if it is possible to execute the specified operation.
   *
   * @param writeReqs the write requests that will be checked against the available quota
   * @param estimateWriteSize the write size that will be checked against the available quota
   * @param readReqs the read requests that will be checked against the available quota
   * @param estimateReadSize the read size that will be checked against the available quota
   * @throws RpcThrottlingException thrown if not enough available resources to perform operation.
   */
  public void checkQuota(
      long writeReqs, long estimateWriteSize, long readReqs, long estimateReadSize)
      throws RpcThrottlingException {
    if (!reqsLimiter.canExecute(writeReqs + readReqs)) {
      throw new RpcThrottlingException(
          "number of requests exceeded - wait " + reqsLimiter.waitInterval() + "ms",
          TSStatusCode.NUM_REQUESTS_EXCEEDED.getStatusCode());
    }

    if (!reqSizeLimiter.canExecute(estimateWriteSize + estimateReadSize)) {
      throw new RpcThrottlingException(
          "request size limit exceeded - wait " + reqSizeLimiter.waitInterval() + "ms",
          TSStatusCode.REQUEST_SIZE_EXCEEDED.getStatusCode());
    }

    if (estimateWriteSize > 0) {
      if (!writeReqsLimiter.canExecute(writeReqs)) {
        throw new RpcThrottlingException(
            "number of write requests exceeded - wait " + writeReqsLimiter.waitInterval() + "ms",
            TSStatusCode.NUM_WRITE_REQUESTS_EXCEEDED.getStatusCode());
      }

      if (!writeSizeLimiter.canExecute(estimateWriteSize)) {
        throw new RpcThrottlingException(
            "write size limit exceeded - wait " + writeSizeLimiter.waitInterval() + "ms",
            TSStatusCode.WRITE_SIZE_EXCEEDED.getStatusCode());
      }
    }

    if (estimateReadSize > 0) {
      if (!readReqsLimiter.canExecute(readReqs)) {
        throw new RpcThrottlingException(
            "number of read requests exceeded - wait " + readReqsLimiter.waitInterval() + "ms",
            TSStatusCode.NUM_READ_REQUESTS_EXCEEDED.getStatusCode());
      }

      if (!readSizeLimiter.canExecute(estimateReadSize)) {
        throw new RpcThrottlingException(
            "read size limit exceeded - wait " + readSizeLimiter.waitInterval() + "ms",
            TSStatusCode.READ_SIZE_EXCEEDED.getStatusCode());
      }
    }
  }

  /**
   * Removes the specified write and read amount from the quota. At this point the write and read
   * amount will be an estimate, that will be later adjusted with a consumeWrite()/consumeRead()
   * call.
   *
   * @param writeReqs the write requests that will be removed from the current quota
   * @param writeSize the write size that will be removed from the current quota
   * @param readReqs the read requests that will be removed from the current quota
   * @param readSize the read size that will be removed from the current quota
   */
  public void grabQuota(long writeReqs, long writeSize, long readReqs, long readSize) {
    reqsLimiter.consume(writeReqs + readReqs);
    reqSizeLimiter.consume(writeSize + readSize);

    if (writeSize > 0) {
      writeReqsLimiter.consume(writeReqs);
      writeSizeLimiter.consume(writeSize);
    }
    if (readSize > 0) {
      readReqsLimiter.consume(readReqs);
      readSizeLimiter.consume(readSize);
    }
  }

  /** Returns the number of bytes available to read to avoid exceeding the quota */
  public long getReadAvailable() {
    return readSizeLimiter.getAvailable();
  }

  /**
   * Removes or add back some write amount to the quota. (called at the end of an operation in case
   * the estimate quota was off)
   */
  public void consumeWrite(final long size) {
    reqSizeLimiter.consume(size);
    writeSizeLimiter.consume(size);
  }

  /**
   * Removes or add back some read amount to the quota. (called at the end of an operation in case
   * the estimate quota was off)
   */
  public void consumeRead(final long size) {
    reqSizeLimiter.consume(size);
    readSizeLimiter.consume(size);
  }
}
