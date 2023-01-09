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
package org.apache.iotdb.db.wal.utils.listener;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** This abstract class aims to listen to the result of some operation. */
public abstract class AbstractResultListener {
  private static final Logger logger = LoggerFactory.getLogger(AbstractResultListener.class);

  /** true means waiting until getting the result */
  protected final boolean wait;

  protected volatile Status status;
  protected volatile Exception cause;

  protected AbstractResultListener(boolean wait) {
    this.wait = wait;
    this.status = Status.RUNNING;
    this.cause = null;
  }

  /** Set status to success and notify all threads waiting for the result. */
  public synchronized AbstractResultListener succeed() {
    status = Status.SUCCESS;
    if (wait) {
      this.notifyAll();
    }
    return this;
  }

  /** Set status to failure and notify all threads waiting for the result. */
  public synchronized AbstractResultListener fail(Exception e) {
    status = Status.FAILURE;
    cause = e;
    if (wait) {
      this.notifyAll();
    }
    return this;
  }

  /**
   * Wait until getting the result. <br>
   * Notice: wait only when wait status is ture.
   */
  public synchronized Status waitForResult() {
    if (wait) {
      while (status == Status.RUNNING) {
        try {
          this.wait();
        } catch (InterruptedException e) {
          logger.warn("Interrupted when waiting for result.", e);
          Thread.currentThread().interrupt();
        }
      }
    }
    return status;
  }

  /** Get the cause exception to failure. */
  public Exception getCause() {
    return cause;
  }

  public enum Status {
    SUCCESS,
    FAILURE,
    RUNNING,
  }
}
