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
package org.apache.iotdb.db.doublelive;

import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.tsfile.utils.Pair;

import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

public class OperationSyncDMLProtector extends OperationSyncProtector {

  private final OperationSyncDDLProtector ddlProtector;
  private final OperationSyncProducer producer;

  public OperationSyncDMLProtector(
      OperationSyncDDLProtector ddlProtector, OperationSyncProducer producer) {
    super();
    this.ddlProtector = ddlProtector;
    this.producer = producer;
  }

  @Override
  protected void preCheck() {
    while (ddlProtector.isAtWork()) {
      try {
        TimeUnit.SECONDS.sleep(5);
      } catch (InterruptedException ignore) {
        // ignore and retry
      }
    }
  }

  @Override
  protected void transmitPhysicalPlan(ByteBuffer planBuffer, PhysicalPlan physicalPlan) {
    producer.put(
        new Pair<>(planBuffer, OperationSyncPlanTypeUtils.getOperationSyncPlanType(physicalPlan)));
  }
}
