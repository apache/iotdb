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

package org.apache.iotdb.cluster.utils;

import java.nio.ByteBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DoublyBuffer {

  private static final Logger logger = LoggerFactory.getLogger(DoublyBuffer.class);
  private ByteBuffer workingBuffer;
  private ByteBuffer idlingBuffer;
  private ByteBuffer flushingBuffer = null;
  private final Object switchBufferCondition = new Object();

  public DoublyBuffer(int size) {
    this.workingBuffer = ByteBuffer.allocate(size);
    this.idlingBuffer = ByteBuffer.allocate(size);
  }

  public void switchWorkingBufferToFlushing() throws InterruptedException {
    logger
        .debug("{} has start to do switchWorkingBufferToFlushing",
            Thread.currentThread().getName());
    synchronized (switchBufferCondition) {

      while (flushingBuffer != null) {
        switchBufferCondition.wait();
      }
      flushingBuffer = workingBuffer;
      workingBuffer = null;
      switchBufferCondition.notifyAll();
      logger
          .debug("{} has done in switchWorkingBufferToFlushing", Thread.currentThread().getName());
    }

  }

  public void switchFlushingBufferToIdling() throws InterruptedException {
    logger
        .debug("{} has start to do switchFlushingBufferToIdling, flushingBuffer={}",
            Thread.currentThread().getName(), flushingBuffer);
    synchronized (switchBufferCondition) {
      while (idlingBuffer != null) {
        switchBufferCondition.wait();
      }
      idlingBuffer = flushingBuffer;
      idlingBuffer.clear();
      flushingBuffer = null;
      switchBufferCondition.notifyAll();
      logger
          .debug("{} has done in switchFlushingBufferToIdling, flushingBuffer={}",
              Thread.currentThread().getName(), flushingBuffer);

    }
  }

  public void switchIdlingBufferToWorking() throws InterruptedException {
    logger
        .debug("{} has start to do switchIdlingBufferToWorking", Thread.currentThread().getName());
    synchronized (switchBufferCondition) {
      while (idlingBuffer == null) {
        switchBufferCondition.wait();
      }
      workingBuffer = idlingBuffer;
      idlingBuffer = null;
      switchBufferCondition.notifyAll();
      logger.debug("{} has done in switchIdlingBufferToWorking", Thread.currentThread().getName());

    }
  }

  public ByteBuffer getWorkingBuffer() {
    return workingBuffer;
  }

  public ByteBuffer getFlushingBuffer() {
    return flushingBuffer;
  }

  @Override
  public String toString() {
    return "DoublyBuffer{"
        + " workingBuffer=" + workingBuffer
        + ", idlingBuffer=" + idlingBuffer
        + ", flushingBuffer=" + flushingBuffer
        + ", switchBufferCondition=" + switchBufferCondition
        + "}";
  }
}
