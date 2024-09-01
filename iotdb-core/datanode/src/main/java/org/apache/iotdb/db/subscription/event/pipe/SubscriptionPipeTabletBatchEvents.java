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

package org.apache.iotdb.db.subscription.event.pipe;

import org.apache.iotdb.db.subscription.event.batch.SubscriptionPipeTabletEventBatch;

import java.io.File;

public class SubscriptionPipeTabletBatchEvents implements SubscriptionPipeEvents {

  private final SubscriptionPipeTabletEventBatch batch;

  public SubscriptionPipeTabletBatchEvents(final SubscriptionPipeTabletEventBatch batch) {
    this.batch = batch;
  }

  @Override
  public File getTsFile() {
    return null;
  }

  @Override
  public void ack() {
    batch.ack();
  }

  @Override
  public void cleanUp() {
    batch.cleanUp();
  }

  @Override
  public String toString() {
    return "SubscriptionPipeTabletBatchEvents{batch=" + batch + "}";
  }
}
