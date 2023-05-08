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
package org.apache.iotdb.db.utils.concurrent;

import java.util.concurrent.Semaphore;

/**
 * FiniteSemaphore defines a special Semaphore that the upper limit of permit is capacity. If
 * permits exceed the capacity, the release request will be ignored.
 */
public class FiniteSemaphore {
  private final int capacity;
  private final Semaphore semaphore;
  private int permit;

  public FiniteSemaphore(int capacity, int permit) {
    if (capacity < permit) {
      throw new IllegalArgumentException("Capacity should be larger than initial permits.");
    }
    this.capacity = capacity;
    this.semaphore = new Semaphore(permit);
    this.permit = permit;
  }

  public void release() {
    synchronized (this) {
      if (permit < capacity) {
        permit++;
        semaphore.release();
      }
    }
  }

  public void acquire() throws InterruptedException {
    semaphore.acquire();
    synchronized (this) {
      permit--;
    }
  }
}
