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

import org.apache.iotdb.cluster.rpc.thrift.Node;

import org.apache.thrift.TException;
import org.apache.thrift.async.AsyncMethodCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.ConnectException;
import java.util.concurrent.atomic.AtomicReference;

/**
 * GenericHandler simply put the response into an AtomicReference and wake up the caller. Notice:
 * the caller should wait on "result" if it wants to get the result. Please refer to the current
 * usages before using this class.
 *
 * @param <T>
 */
public class GenericHandler<T> implements AsyncMethodCallback<T> {

  private static final Logger logger = LoggerFactory.getLogger(GenericHandler.class);

  private Node source;
  private AtomicReference<T> result;
  private Exception e;

  public GenericHandler(Node source, AtomicReference<T> result) {
    this.source = source;
    this.result = result;
  }

  @Override
  public void onComplete(T response) {
    if (result != null) {
      synchronized (result) {
        result.set(response);
        result.notifyAll();
      }
    }
  }

  @Override
  public void onError(Exception exception) {
    if (!(exception instanceof ConnectException)) {
      logger.error("Cannot receive result from {}", source, exception);
    } else {
      logger.warn("Cannot receive result from {} : {}", source, exception.getMessage());
    }

    if (result != null) {
      synchronized (result) {
        result.notifyAll();
        e = exception;
      }
    }
  }

  public Exception getException() {
    return e;
  }

  @SuppressWarnings("java:S2274") // enable timeout
  public T getResult(long timeout) throws InterruptedException, TException {
    synchronized (result) {
      if (result.get() == null && getException() == null) {
        result.wait(timeout);
      }
    }
    if (getException() != null) {
      throw new TException(getException());
    }
    return result.get();
  }
}
