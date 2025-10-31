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

import org.apache.iotdb.commons.audit.IAuditEntity;
import org.apache.iotdb.commons.path.PatternTreeMap;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeTaskMeta;
import org.apache.iotdb.commons.pipe.config.PipeConfig;
import org.apache.iotdb.commons.pipe.datastructure.pattern.TablePattern;
import org.apache.iotdb.commons.pipe.datastructure.pattern.TreePattern;
import org.apache.iotdb.db.pipe.event.common.PipeInsertionEvent;
import org.apache.iotdb.db.pipe.metric.overview.PipeTsFileToTabletsMetrics;
import org.apache.iotdb.db.pipe.resource.PipeDataNodeResourceManager;
import org.apache.iotdb.db.pipe.resource.memory.PipeMemoryBlock;
import org.apache.iotdb.db.pipe.resource.memory.PipeMemoryWeightUtil;
import org.apache.iotdb.db.storageengine.dataregion.modification.ModEntry;
import org.apache.iotdb.db.utils.datastructure.PatternTreeMapFactory;
import org.apache.iotdb.pipe.api.event.dml.insertion.TabletInsertionEvent;

import org.apache.tsfile.read.TsFileSequenceReader;
import org.apache.tsfile.read.expression.impl.GlobalTimeExpression;
import org.apache.tsfile.read.filter.factory.TimeFilterApi;
import org.apache.tsfile.write.record.Tablet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public abstract class TsFileInsertionEventParser implements AutoCloseable {

  private static final Logger LOGGER = LoggerFactory.getLogger(TsFileInsertionEventParser.class);

  protected final String pipeName;
  protected final long creationTime;
  protected IAuditEntity entity;
  protected boolean skipIfNoPrivileges;

  protected final TreePattern treePattern; // used to filter data
  protected final TablePattern tablePattern; // used to filter data
  protected final GlobalTimeExpression timeFilterExpression; // used to filter data
  protected final long startTime; // used to filter data
  protected final long endTime; // used to filter data

  protected final PipeTaskMeta pipeTaskMeta; // used to report progress
  protected final PipeInsertionEvent sourceEvent; // used to report progress

  // mods entry
  protected PipeMemoryBlock allocatedMemoryBlockForModifications;
  protected PatternTreeMap<ModEntry, PatternTreeMapFactory.ModsSerializer> currentModifications;

  protected long parseStartTimeNano = -1;
  protected boolean parseStartTimeRecorded = false;
  protected boolean parseEndTimeRecorded = false;

  protected final PipeMemoryBlock allocatedMemoryBlockForTablet;

  protected TsFileSequenceReader tsFileSequenceReader;

  protected Iterable<TabletInsertionEvent> tabletInsertionIterable;

  protected TsFileInsertionEventParser(
      final String pipeName,
      final long creationTime,
      final TreePattern treePattern,
      final TablePattern tablePattern,
      final long startTime,
      final long endTime,
      final PipeTaskMeta pipeTaskMeta,
      final IAuditEntity entity,
      final boolean skipIfNoPrivileges,
      final PipeInsertionEvent sourceEvent) {
    this.pipeName = pipeName;
    this.creationTime = creationTime;
    this.entity = entity;
    this.skipIfNoPrivileges = skipIfNoPrivileges;

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

    this.allocatedMemoryBlockForTablet =
        PipeDataNodeResourceManager.memory()
            .forceAllocateForTabletWithRetry(
                PipeConfig.getInstance().getPipeDataStructureTabletSizeInBytes());
  }

  /**
   * @return {@link TabletInsertionEvent} in a streaming way
   */
  public abstract Iterable<TabletInsertionEvent> toTabletInsertionEvents();

  /**
   * Record parse start time when hasNext() is called for the first time and returns true. Should be
   * called in Iterator.hasNext() when it's the first call.
   */
  protected void recordParseStartTime() {
    if (pipeName == null || parseStartTimeRecorded) {
      return;
    }
    parseStartTimeNano = System.nanoTime();
    parseStartTimeRecorded = true;
  }

  /**
   * Record parse end time when hasNext() is called and returns false (last call). Should be called
   * in Iterator.hasNext() when it returns false.
   */
  protected void recordParseEndTime() {
    if (pipeName == null || !parseStartTimeRecorded || parseEndTimeRecorded) {
      return;
    }
    try {
      final long parseEndTimeNano = System.nanoTime();
      final long totalTimeNanos = parseEndTimeNano - parseStartTimeNano;
      final String taskID = pipeName + "_" + creationTime;
      PipeTsFileToTabletsMetrics.getInstance().recordTsFileToTabletTime(taskID, totalTimeNanos);
      parseEndTimeRecorded = true;
    } catch (final Exception e) {
      LOGGER.warn("Failed to record parse end time for pipe {}", pipeName, e);
    }
  }

  /**
   * Record metrics when a tablet is generated. Should be called by subclasses when generating
   * tablets.
   *
   * @param tablet the generated tablet
   */
  protected void recordTabletMetrics(final Tablet tablet) {
    if (pipeName == null || tablet == null) {
      return;
    }
    try {
      final String taskID = pipeName + "_" + creationTime;
      final long tabletMemorySize = PipeMemoryWeightUtil.calculateTabletSizeInBytes(tablet);
      PipeTsFileToTabletsMetrics.getInstance().recordTabletGenerated(taskID, tabletMemorySize);
    } catch (final Exception e) {
      LOGGER.warn("Failed to record tablet metrics for pipe {}", pipeName, e);
    }
  }

  @Override
  public void close() {
    tabletInsertionIterable = null;

    // Time recording is now handled in Iterator.hasNext(), no need to record here

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

    if (currentModifications != null) {
      // help GC
      currentModifications = null;
    }

    if (allocatedMemoryBlockForModifications != null) {
      allocatedMemoryBlockForModifications.close();
    }
  }
}
