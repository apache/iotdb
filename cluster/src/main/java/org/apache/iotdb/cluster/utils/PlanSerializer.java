/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at      http://www.apache.org/licenses/LICENSE-2.0  Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the License for the specific language governing permissions and limitations under the License.
 */

package org.apache.iotdb.cluster.utils;

import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.utils.CommonUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.LinkedBlockingDeque;

public class PlanSerializer {

  private static final Logger logger = LoggerFactory.getLogger(PlanSerializer.class);
  private static final int DEFAULT_BAOS_SIZE = CommonUtils.getCpuCores() * 4;
  private BlockingDeque<ByteArrayOutputStream> baosBlockingDeque = new LinkedBlockingDeque<>();

  private static final PlanSerializer instance = new PlanSerializer();

  private PlanSerializer() {
    for (int i = 0; i < DEFAULT_BAOS_SIZE; i++) {
      baosBlockingDeque.push(new ByteArrayOutputStream(4096));
    }
  }

  public static PlanSerializer getInstance() {
    return instance;
  }

  public byte[] serialize(PhysicalPlan plan) throws IOException {
    ByteArrayOutputStream poll;
    try {
      poll = baosBlockingDeque.take();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IOException("take byte array output stream interrupted", e);
    }
    poll.reset();

    try (DataOutputStream dataOutputStream = new DataOutputStream(poll)) {
      plan.serialize(dataOutputStream);
      return poll.toByteArray();
    } finally {
      try {
        baosBlockingDeque.put(poll);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        logger.error("Putting byte array output stream back interrupted");
      }
    }
  }
}
