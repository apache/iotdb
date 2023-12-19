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
package org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.pbtree.memory.cache;

import java.util.concurrent.atomic.AtomicInteger;

public class CacheEntry {

  private volatile boolean isVolatile = false;

  private final AtomicInteger pinSemaphore = new AtomicInteger(0);

  private final AtomicInteger volatileDescendantSemaphore = new AtomicInteger(0);

  public boolean isVolatile() {
    return isVolatile;
  }

  public void setVolatile(boolean aVolatile) {
    isVolatile = aVolatile;
  }

  public void pin() {
    pinSemaphore.getAndIncrement();
  }

  public void unPin() {
    pinSemaphore.getAndDecrement();
  }

  public boolean isPinned() {
    return pinSemaphore.get() > 0;
  }

  public void incVolatileDescendant() {
    volatileDescendantSemaphore.getAndIncrement();
  }

  public void decVolatileDescendant() {
    volatileDescendantSemaphore.getAndDecrement();
  }

  public boolean hasVolatileDescendant() {
    return volatileDescendantSemaphore.get() > 0;
  }
}
