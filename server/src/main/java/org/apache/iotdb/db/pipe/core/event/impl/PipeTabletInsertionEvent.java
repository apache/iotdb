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
import org.apache.iotdb.db.pipe.resource.PipeResourceManager;
import org.apache.iotdb.db.wal.exception.WALPipeException;
import org.apache.iotdb.db.wal.utils.WALPipeHandler;
import org.apache.iotdb.pipe.api.access.Row;
import org.apache.iotdb.pipe.api.collector.RowCollector;
import org.apache.iotdb.pipe.api.event.dml.insertion.TabletInsertionEvent;
import org.apache.iotdb.tsfile.write.record.Tablet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.function.BiConsumer;

public class PipeTabletInsertionEvent implements TabletInsertionEvent, EnrichedEvent {

  private static final Logger LOGGER = LoggerFactory.getLogger(PipeTabletInsertionEvent.class);

  private final WALPipeHandler walPipeHandler;

  public PipeTabletInsertionEvent(WALPipeHandler walPipeHandler) {
    this.walPipeHandler = walPipeHandler;
  }

  public InsertNode getInsertNode() throws WALPipeException {
    return walPipeHandler.getValue();
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
    try {
      PipeResourceManager.wal().pin(walPipeHandler.getMemTableId(), walPipeHandler);
      return true;
    } catch (Exception e) {
      LOGGER.warn(
          String.format(
              "Increase reference count for memtable %d error. Holder Message: %s",
              walPipeHandler.getMemTableId(), holderMessage),
          e);
      return false;
    }
  }

  @Override
  public boolean decreaseReferenceCount(String holderMessage) {
    try {
      PipeResourceManager.wal().unpin(walPipeHandler.getMemTableId());
      return true;
    } catch (Exception e) {
      LOGGER.warn(
          String.format(
              "Decrease reference count for memtable %d error. Holder Message: %s",
              walPipeHandler.getMemTableId(), holderMessage),
          e);
      return false;
    }
  }

  @Override
  public int getReferenceCount() {
    return PipeResourceManager.wal().getReferenceCount(walPipeHandler.getMemTableId());
  }

  @Override
  public String toString() {
    return "PipeTabletInsertionEvent{" + "walPipeHandler=" + walPipeHandler + '}';
  }
}
