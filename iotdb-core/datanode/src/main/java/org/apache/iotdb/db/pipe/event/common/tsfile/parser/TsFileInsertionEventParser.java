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
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.utils.datastructure.PatternTreeMapFactory;
import org.apache.iotdb.pipe.api.event.dml.insertion.TabletInsertionEvent;
import org.apache.iotdb.pipe.api.exception.PipeException;

import org.apache.tsfile.read.TsFileSequenceReader;
import org.apache.tsfile.read.expression.impl.GlobalTimeExpression;
import org.apache.tsfile.read.filter.factory.TimeFilterApi;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.BitMap;
import org.apache.tsfile.write.record.Tablet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;

public abstract class TsFileInsertionEventParser implements AutoCloseable {

  private static final Logger LOGGER = LoggerFactory.getLogger(TsFileInsertionEventParser.class);

  protected final String pipeName;
  protected final long creationTime;

  protected final TreePattern treePattern; // used to filter data
  protected final TablePattern tablePattern; // used to filter data
  protected final GlobalTimeExpression timeFilterExpression; // used to filter data
  protected final long startTime; // used to filter data
  protected final long endTime; // used to filter data

  protected final PipeTaskMeta pipeTaskMeta; // used to report progress
  protected final PipeInsertionEvent sourceEvent; // used to report progress

  // TsFileResource, used for Object file management
  protected final TsFileResource tsFileResource;
  // Flag indicating whether Tablet has Object data, defaults to true to ensure scanning will be
  // performed
  protected boolean hasObjectData = true;

  // mods entry
  protected PipeMemoryBlock allocatedMemoryBlockForModifications;
  protected PatternTreeMap<ModEntry, PatternTreeMapFactory.ModsSerializer> currentModifications;

  protected long parseStartTimeNano = -1;
  protected boolean parseStartTimeRecorded = false;
  protected boolean parseEndTimeRecorded = false;

  protected final PipeMemoryBlock allocatedMemoryBlockForTablet;

  protected TsFileSequenceReader tsFileSequenceReader;

  protected Iterable<TabletInsertionEvent> tabletInsertionIterable;

  protected final boolean notOnlyNeedObject;

  protected TsFileInsertionEventParser(
      final String pipeName,
      final long creationTime,
      final TreePattern treePattern,
      final TablePattern tablePattern,
      final long startTime,
      final long endTime,
      final PipeTaskMeta pipeTaskMeta,
      final PipeInsertionEvent sourceEvent,
      final TsFileResource tsFileResource,
      final boolean notOnlyNeedObject) {
    this.pipeName = pipeName;
    this.creationTime = creationTime;

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

    // Get TsFileResource and hasObjectData from sourceEvent
    this.tsFileResource =
        tsFileResource != null
            ? tsFileResource
            : (sourceEvent != null ? (TsFileResource) sourceEvent.getTsFileResource() : null);
    this.hasObjectData = sourceEvent != null && sourceEvent.hasObjectData();

    this.allocatedMemoryBlockForTablet =
        PipeDataNodeResourceManager.memory()
            .forceAllocateForTabletWithRetry(
                PipeConfig.getInstance().getPipeDataStructureTabletSizeInBytes());

    this.notOnlyNeedObject = notOnlyNeedObject;
  }

  /**
   * @return {@link TabletInsertionEvent} in a streaming way
   */
  public abstract Iterable<TabletInsertionEvent> toTabletInsertionEvents();

  public abstract Iterable<Binary> getObjectTypeData();

  /**
   * Template method for creating ObjectTypeData iterator. Subclasses should implement
   * createObjectTypeDataIteratorState() to provide data source specific state management.
   */
  protected Iterable<Binary> createObjectTypeDataIterator() {
    return () -> {
      final ObjectTypeDataIteratorState state = createObjectTypeDataIteratorState();
      return new Iterator<Binary>() {
        private Tablet tablet = null;
        private int rowIndex = 0;
        private int columnIndex = 0;

        @Override
        public boolean hasNext() {
          while (true) {
            // Process current Tablet
            if (tablet != null) {
              final Object[] values = tablet.getValues();
              if (values == null || columnIndex >= values.length) {
                resetTablet();
                continue;
              }

              // Check if current column is Binary[] type
              if (!(values[columnIndex] instanceof Binary[])) {
                columnIndex++;
                continue;
              }

              final int rowSize = tablet.getRowSize();

              while (rowIndex < rowSize) {
                if (!isRowNull(tablet, columnIndex, rowIndex)) {
                  return true;
                }
                rowIndex++;
              }

              columnIndex++;
              rowIndex = 0;
              continue;
            }

            // Check if there are more data sources
            if (!state.hasMoreDataSources()) {
              return false;
            }

            // Get next Tablet from data source
            try {
              tablet = state.getNextTablet();
            } catch (final Exception e) {
              close();
              throw new PipeException("failed to read next Tablet", e);
            }

            rowIndex = 0;
            columnIndex = 0;

            // If the fetched Tablet has no data, continue to fetch the next one
            if (tablet == null || tablet.getRowSize() == 0) {
              tablet = null;
              continue;
            }

            final Object[] values = tablet.getValues();
            if (values == null || values.length == 0) {
              tablet = null;
              continue;
            }
          }
        }

        private void resetTablet() {
          tablet = null;
          columnIndex = 0;
          rowIndex = 0;
        }

        private boolean isRowNull(Tablet tablet, int colIndex, int rowIdx) {
          final BitMap[] bitMaps = tablet.getBitMaps();
          return bitMaps != null
              && colIndex < bitMaps.length
              && bitMaps[colIndex] != null
              && bitMaps[colIndex].isMarked(rowIdx);
        }

        @Override
        public Binary next() {
          final Binary[] column = (Binary[]) tablet.getValues()[columnIndex];
          return column[rowIndex++];
        }
      };
    };
  }

  /**
   * Create state object for ObjectTypeData iterator. Should be implemented by subclasses to provide
   * data source specific state management.
   *
   * @return the state object for managing data source iteration
   */
  protected abstract ObjectTypeDataIteratorState createObjectTypeDataIteratorState();

  /**
   * State interface for ObjectTypeData iterator. Subclasses should implement this to manage their
   * specific data source iteration state.
   */
  protected interface ObjectTypeDataIteratorState {
    /**
     * Check if there are more data sources to read from.
     *
     * @return true if there are more data sources, false otherwise
     */
    boolean hasMoreDataSources();

    /**
     * Get the next Tablet from the data source.
     *
     * @return the next Tablet, or null if no more data
     * @throws Exception if an error occurs while reading
     */
    Tablet getNextTablet() throws Exception;
  }

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
