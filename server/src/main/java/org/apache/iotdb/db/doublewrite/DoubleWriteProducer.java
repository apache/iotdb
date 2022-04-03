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

import org.apache.iotdb.tsfile.utils.Pair;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.concurrent.BlockingQueue;

/**
 * DoubleWriteProducer using BlockingQueue to cache PhysicalPlan. And persist some PhysicalPlan when
 * they are too many to transmit
 */
public class DoubleWriteProducer {

  private static final Logger LOGGER = LoggerFactory.getLogger(DoubleWriteProducer.class);

  private final BlockingQueue<Pair<ByteBuffer, DoubleWritePlanTypeUtils.DoubleWritePlanType>>
      doubleWriteQueue;

  public DoubleWriteProducer(
      BlockingQueue<Pair<ByteBuffer, DoubleWritePlanTypeUtils.DoubleWritePlanType>>
          doubleWriteQueue) {
    this.doubleWriteQueue = doubleWriteQueue;
  }

  public void put(Pair<ByteBuffer, DoubleWritePlanTypeUtils.DoubleWritePlanType> planPair) {
    try {
      planPair.left.position(0);
      doubleWriteQueue.put(planPair);
    } catch (InterruptedException e) {
      LOGGER.error("double write cache failed.", e);
    }
  }
}
