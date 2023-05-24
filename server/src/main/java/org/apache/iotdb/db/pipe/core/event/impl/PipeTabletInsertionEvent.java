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

package org.apache.iotdb.db.pipe.core.event.impl;

import org.apache.iotdb.db.mpp.plan.planner.plan.node.write.InsertNode;
import org.apache.iotdb.db.pipe.core.event.EnrichedEvent;
import org.apache.iotdb.pipe.api.access.Row;
import org.apache.iotdb.pipe.api.collector.RowCollector;
import org.apache.iotdb.pipe.api.event.dml.insertion.TabletInsertionEvent;
import org.apache.iotdb.tsfile.write.record.Tablet;

import java.util.Iterator;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;

public class PipeTabletInsertionEvent implements TabletInsertionEvent, EnrichedEvent {

  private final InsertNode insertNode;

  private final AtomicInteger referenceCount;

  public PipeTabletInsertionEvent(InsertNode insertNode) {
    this.insertNode = insertNode;
    this.referenceCount = new AtomicInteger(0);
  }

  public InsertNode getInsertNode() {
    return insertNode;
  }

  @Override
  public TabletInsertionEvent processRowByRow(BiConsumer<Row, RowCollector> consumer) {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public TabletInsertionEvent processByIterator(BiConsumer<Iterator<Row>, RowCollector> consumer) {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public TabletInsertionEvent processTablet(BiConsumer<Tablet, RowCollector> consumer) {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public boolean increaseReferenceCount(String holderMessage) {
    // TODO: use WALPipeHandler pinMemtable
    referenceCount.incrementAndGet();
    return true;
  }

  @Override
  public boolean decreaseReferenceCount(String holderMessage) {
    // TODO: use WALPipeHandler unpinMemetable
    referenceCount.decrementAndGet();
    return true;
  }

  @Override
  public int getReferenceCount() {
    // TODO: use WALPipeHandler unpinMemetable
    return referenceCount.get();
  }

  @Override
  public String toString() {
    return "PipeTabletInsertionEvent{" + "insertNode=" + insertNode + '}';
  }
}
