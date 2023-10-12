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

package org.apache.iotdb.commons.concurrent;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

/** This class supports passing exception message when using CountDownLatch. */
public class ExceptionalCountDownLatch {
  private final CountDownLatch latch;
  private final AtomicReference<String> exceptionMessage = new AtomicReference<>();

  public ExceptionalCountDownLatch(int count) {
    this.latch = new CountDownLatch(count);
  }

  public void countDown() {
    latch.countDown();
  }

  public void countDownWithException(String message) {
    exceptionMessage.set(message);
    countDown();
  }

  public void await() throws InterruptedException {
    latch.await();
  }

  public boolean hasException() {
    return exceptionMessage.get() != null;
  }

  public String getExceptionMessage() {
    return exceptionMessage.get();
  }
}
