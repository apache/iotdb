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

import org.apache.iotdb.db.pipe.event.common.tsfile.PipeTsFileInsertionEvent;

public class SubscriptionPipeTsFilePlainEvent implements SubscriptionPipeEvents {

  private final PipeTsFileInsertionEvent tsFileInsertionEvent;

  public SubscriptionPipeTsFilePlainEvent(final PipeTsFileInsertionEvent tsFileInsertionEvent) {
    this.tsFileInsertionEvent = tsFileInsertionEvent;
  }

  @Override
  public void ack() {
    tsFileInsertionEvent.decreaseReferenceCount(this.getClass().getName(), true);
  }

  @Override
  public void cleanUp() {
    // clear the reference count of event
    tsFileInsertionEvent.clearReferenceCount(this.getClass().getName());
  }

  /////////////////////////////// stringify ///////////////////////////////

  @Override
  public String toString() {
    return "SubscriptionPipeTsFilePlainEvent{tsFileInsertionEvent="
        + tsFileInsertionEvent.coreReportMessage()
        + "}";
  }

  //////////////////////////// APIs provided for metric framework ////////////////////////////

  @Override
  public int getPipeEventCount() {
    return 1;
  }
}
