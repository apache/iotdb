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

package org.apache.iotdb.confignode.manager.pipe.task;

import org.apache.iotdb.pipe.api.exception.PipeException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.BlockingDeque;
import java.util.concurrent.LinkedBlockingDeque;

/**
 * PipeTaskCoordinatorLock is a cross thread lock for pipe task coordinator. It is used to ensure
 * that only one thread can execute the pipe task coordinator at the same time.
 */
public class PipeTaskCoordinatorLock {

  private static final Logger LOGGER = LoggerFactory.getLogger(PipeTaskCoordinatorLock.class);

  private final BlockingDeque<Object> deque = new LinkedBlockingDeque<>(1);
  private final Object object = new Object();

  void lock() {
    try {
      deque.put(object);
      LOGGER.info("Lock acquired by thread {}", Thread.currentThread().getName());
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new PipeException(String.format("Interrupted while waiting for lock: %s", e));
    }
  }

  void unlock() {
    deque.poll();
    LOGGER.info("Lock released by thread {}", Thread.currentThread().getName());
  }
}
