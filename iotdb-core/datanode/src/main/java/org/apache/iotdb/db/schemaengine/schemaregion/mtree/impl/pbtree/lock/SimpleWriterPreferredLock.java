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

package org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.pbtree.lock;

public class SimpleWriterPreferredLock {

  private volatile int readCount = 0;
  private volatile int readWait = 0;
  private volatile int writeCount = 0;
  private volatile int writeWait = 0;

  private final Object readLock = new Object();

  private final Object writeLock = new Object();

  public synchronized void readLock() {
    readWait++;
    while (writeCount > 0 || writeWait > 0) {
      try {
        readLock.wait();
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }
    readWait--;
    readCount++;
  }

  public synchronized void readUnlock() {
    readCount--;
    if (readCount == 0 && writeWait > 0) {
      writeLock.notify();
    }
  }

  public synchronized void writeLock() {
    writeWait++;
    while (readCount > 0) {
      try {
        writeLock.wait();
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }
    writeWait--;
    writeCount++;
  }

  public synchronized void writeUnlock() {
    writeCount--;
    if (writeWait > 0) {
      writeLock.notify();
    } else if (readWait > 0) {
      readLock.notifyAll();
    }
  }
}
