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
package org.apache.iotdb.db.doublewrite;

import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.fileSystem.SystemFileFactory;
import org.apache.iotdb.db.writelog.io.LogWriter;
import org.apache.iotdb.session.pool.SessionPool;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Random;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * DoubleWriteProducer using BlockingQueue to cache PhysicalPlan. And persist some PhysicalPlan when
 * they are too many to transmit
 */
public class DoubleWriteProducer {
  private static final Logger LOGGER = LoggerFactory.getLogger(DoubleWriteProducer.class);

  private final BlockingQueue<ByteBuffer> doubleWriteQueue;
  private final int doubleWriteCacheSize;
  private final DoubleWriteProtectorService service;

  public DoubleWriteProducer(
    BlockingQueue<ByteBuffer> doubleWriteQueue, DoubleWriteProtectorService service) {
    this.doubleWriteQueue = doubleWriteQueue;
    this.service = service;
    doubleWriteCacheSize = IoTDBDescriptor.getInstance().getConfig().getDoubleWriteProducerCacheSize();
  }

  public void put(ByteBuffer planBuffer) {
    // Persist when there are too many PhysicalPlan to transmit
    if (doubleWriteQueue.size() == doubleWriteCacheSize) {
      LogWriter doubleWriteLogWriter = service.acquireLogWriter();
      try {
        doubleWriteLogWriter.write(planBuffer);
      } catch (IOException e) {
        LOGGER.error("DoubleWriteProducer can't serialize physicalPlan", e);
      }
      service.releaseLogWriter();
      return;
    }

    try {
      planBuffer.position(0);
      doubleWriteQueue.put(planBuffer);
    } catch (InterruptedException e) {
      LOGGER.error("double write cache failed.", e);
    }
  }
}
