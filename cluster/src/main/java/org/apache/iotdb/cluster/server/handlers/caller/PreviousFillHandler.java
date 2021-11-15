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

import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.utils.SerializeUtils;
import org.apache.iotdb.tsfile.read.TimeValuePair;

import org.apache.thrift.async.AsyncMethodCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class PreviousFillHandler implements AsyncMethodCallback<ByteBuffer> {

  private static final Logger logger = LoggerFactory.getLogger(PreviousFillHandler.class);
  private static final long MAX_WAIT_MIN = 3;
  private CountDownLatch latch;
  private TimeValuePair result = new TimeValuePair(Long.MIN_VALUE, null);
  private List<Exception> exceptions;

  public PreviousFillHandler(CountDownLatch latch) {
    this.latch = latch;
    this.exceptions = new ArrayList<>();
  }

  @Override
  public synchronized void onComplete(ByteBuffer response) {
    if (response != null && (response.limit() - response.position()) != 0) {
      TimeValuePair timeValuePair = SerializeUtils.deserializeTVPair(response);
      if (timeValuePair != null && timeValuePair.getTimestamp() > result.getTimestamp()) {
        result = timeValuePair;
      }
    }
    latch.countDown();
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
    this.exceptions.add(exception);
    latch.countDown();
  }

  public TimeValuePair getResult() throws QueryProcessException {
    if (!exceptions.isEmpty()) {
      QueryProcessException e =
          new QueryProcessException(
              "Exception happened when performing previous fill. "
                  + "See the suppressed exceptions for causes.");
      for (Exception exception : exceptions) {
        e.addSuppressed(exception);
      }
      throw e;
    }

    try {
      if (!latch.await(MAX_WAIT_MIN, TimeUnit.MINUTES)) {
        logger.warn(
            "Not all nodes returned previous fill result when timed out, remaining {}",
            latch.getCount());
        throw new QueryProcessException(
            "Failed to get the previous fill result since "
                + latch.getCount()
                + " nodes didn't respond");
      }
    } catch (InterruptedException e) {
      throw new QueryProcessException(e.getMessage());
    }
    return result;
  }
}
