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

package org.apache.iotdb.cluster.server.handlers.caller;

import java.nio.ByteBuffer;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import javax.activation.UnsupportedDataTypeException;
import org.apache.iotdb.cluster.utils.SerializeUtils;
import org.apache.iotdb.tsfile.read.TimeValuePair;
import org.apache.thrift.async.AsyncMethodCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PreviousFillHandler implements AsyncMethodCallback<ByteBuffer> {

  private static final Logger logger = LoggerFactory.getLogger(PreviousFillHandler.class);
  private static final long MAX_WAIT_MIN = 3;
  private CountDownLatch latch;
  private TimeValuePair result = new TimeValuePair(Long.MIN_VALUE, null);

  public PreviousFillHandler(CountDownLatch latch) {
    this.latch = latch;
  }

  @Override
  public synchronized void onComplete(ByteBuffer response) {
    if (response == null || (response.limit() - response.position()) == 0) {
      latch.countDown();
    } else {
      try {
        TimeValuePair timeValuePair = SerializeUtils.deserializeTVPair(response);
        if (timeValuePair.getTimestamp() > result.getTimestamp()) {
          result = timeValuePair;
        }
        latch.countDown();
      } catch (UnsupportedDataTypeException e) {
        logger.error("Cannot deserialize TVPair", e);
      }
    }
  }

  public synchronized void onComplete(TimeValuePair timeValuePair) {
    if (timeValuePair.getTimestamp() > result.getTimestamp()) {
      result = timeValuePair;
    }
    latch.countDown();
  }

  @Override
  public synchronized void onError(Exception exception) {
    logger.error("Cannot get previous fill result", exception);
    latch.countDown();
  }

  public CountDownLatch getLatch() {
    return latch;
  }

  public TimeValuePair getResult() {
    try {
      latch.await(MAX_WAIT_MIN, TimeUnit.MINUTES);
    } catch (InterruptedException e) {
      logger.error("Unexpected interruption when waiting for the result of previous fill");
    }
    return result;
  }
}
