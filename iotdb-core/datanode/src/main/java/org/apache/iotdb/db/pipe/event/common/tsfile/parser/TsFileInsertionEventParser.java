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

package org.apache.iotdb.db.pipe.event.common.tsfile.parser;

import org.apache.iotdb.commons.pipe.agent.task.meta.PipeTaskMeta;
import org.apache.iotdb.commons.pipe.datastructure.pattern.TablePattern;
import org.apache.iotdb.commons.pipe.datastructure.pattern.TreePattern;
import org.apache.iotdb.db.pipe.event.common.PipeInsertionEvent;
import org.apache.iotdb.db.pipe.resource.PipeDataNodeResourceManager;
import org.apache.iotdb.db.pipe.resource.memory.PipeMemoryBlock;
import org.apache.iotdb.pipe.api.event.dml.insertion.TabletInsertionEvent;

import org.apache.tsfile.read.TsFileSequenceReader;
import org.apache.tsfile.read.expression.impl.GlobalTimeExpression;
import org.apache.tsfile.read.filter.factory.TimeFilterApi;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public abstract class TsFileInsertionEventParser implements AutoCloseable {

  private static final Logger LOGGER = LoggerFactory.getLogger(TsFileInsertionEventParser.class);

  protected final TreePattern treePattern; // used to filter data
  protected final TablePattern tablePattern; // used to filter data
  protected final GlobalTimeExpression timeFilterExpression; // used to filter data
  protected final long startTime; // used to filter data
  protected final long endTime; // used to filter data

  protected final PipeTaskMeta pipeTaskMeta; // used to report progress
  protected final PipeInsertionEvent sourceEvent; // used to report progress

  protected final PipeMemoryBlock allocatedMemoryBlockForTablet;

  protected TsFileSequenceReader tsFileSequenceReader;

  protected TsFileInsertionEventParser(
      final TreePattern treePattern,
      final TablePattern tablePattern,
      final long startTime,
      final long endTime,
      final PipeTaskMeta pipeTaskMeta,
      final PipeInsertionEvent sourceEvent) {
    this.treePattern = treePattern;
    this.tablePattern = tablePattern;
    timeFilterExpression =
        (startTime == Long.MIN_VALUE && endTime == Long.MAX_VALUE)
            ? null
            : new GlobalTimeExpression(TimeFilterApi.between(startTime, endTime));
    this.startTime = startTime;
    this.endTime = endTime;

    this.pipeTaskMeta = pipeTaskMeta;
    this.sourceEvent = sourceEvent;

    // Allocate empty memory block, will be resized later.
    this.allocatedMemoryBlockForTablet =
        PipeDataNodeResourceManager.memory().forceAllocateForTabletWithRetry(0);
  }

  /**
   * @return {@link TabletInsertionEvent} in a streaming way
   */
  public abstract Iterable<TabletInsertionEvent> toTabletInsertionEvents();

  @Override
  public void close() {
    try {
      if (tsFileSequenceReader != null) {
        tsFileSequenceReader.close();
      }
    } catch (final IOException e) {
      LOGGER.warn("Failed to close TsFileSequenceReader", e);
    }

    if (allocatedMemoryBlockForTablet != null) {
      allocatedMemoryBlockForTablet.close();
    }
  }
}
