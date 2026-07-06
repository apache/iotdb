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

package org.apache.iotdb.db.pipe.event.common.tsfile.parser.table;

import org.apache.iotdb.commons.audit.IAuditEntity;
import org.apache.iotdb.commons.exception.auth.AccessDeniedException;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeTaskMeta;
import org.apache.iotdb.commons.pipe.config.PipeConfig;
import org.apache.iotdb.commons.pipe.datastructure.pattern.TablePattern;
import org.apache.iotdb.commons.queryengine.plan.relational.metadata.QualifiedObjectName;
import org.apache.iotdb.db.auth.AuthorityChecker;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.i18n.DataNodePipeMessages;
import org.apache.iotdb.db.pipe.event.common.PipeInsertionEvent;
import org.apache.iotdb.db.pipe.event.common.tablet.PipeRawTabletInsertionEvent;
import org.apache.iotdb.db.pipe.event.common.tsfile.parser.TsFileInsertionEventParser;
import org.apache.iotdb.db.pipe.event.common.tsfile.parser.util.ModsOperationUtil;
import org.apache.iotdb.db.pipe.resource.PipeDataNodeResourceManager;
import org.apache.iotdb.db.pipe.resource.memory.PipeMemoryBlock;
import org.apache.iotdb.db.utils.datastructure.PatternTreeMapFactory;
import org.apache.iotdb.pipe.api.event.dml.insertion.TabletInsertionEvent;
import org.apache.iotdb.pipe.api.exception.PipeException;

import org.apache.tsfile.read.TsFileSequenceReader;
import org.apache.tsfile.write.record.Tablet;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Objects;

public class TsFileInsertionEventTableParser extends TsFileInsertionEventParser {

  private final long startTime;
  private final long endTime;
  private final TablePattern tablePattern;
  private final boolean isWithMod;

  private final PipeMemoryBlock allocatedMemoryBlockForBatchData;
  private final PipeMemoryBlock allocatedMemoryBlockForChunk;
  private final PipeMemoryBlock allocatedMemoryBlockForChunkMeta;
  private final PipeMemoryBlock allocatedMemoryBlockForTableSchemas;

  public TsFileInsertionEventTableParser(
      final String pipeName,
      final long creationTime,
      final File tsFile,
      final TablePattern pattern,
      final long startTime,
      final long endTime,
      final PipeTaskMeta pipeTaskMeta,
      final IAuditEntity entity,
      final PipeInsertionEvent sourceEvent,
      final boolean isWithMod)
      throws IOException {
    super(
        tsFile,
        pipeName,
        creationTime,
        null,
        pattern,
        startTime,
        endTime,
        pipeTaskMeta,
        entity,
        true,
        sourceEvent,
        isWithMod);

    this.isWithMod = isWithMod;
    try {
      currentModifications =
          isWithMod
              ? ModsOperationUtil.loadModificationsFromTsFile(tsFile)
              : PatternTreeMapFactory.getModsPatternTreeMap();
      allocatedMemoryBlockForModifications =
          PipeDataNodeResourceManager.memory()
              .forceAllocateForTabletWithRetry(currentModifications.ramBytesUsed());
      long tableSize =
          Math.min(
              IoTDBDescriptor.getInstance().getConfig().getPipeDataStructureTabletSizeInBytes(),
              IoTDBDescriptor.getInstance().getConfig().getTargetChunkSize());

      this.allocatedMemoryBlockForChunk =
          PipeDataNodeResourceManager.memory()
              .forceAllocateForTabletWithRetry(
                  PipeConfig.getInstance().getPipeMaxReaderChunkSize());
      this.allocatedMemoryBlockForBatchData =
          PipeDataNodeResourceManager.memory().forceAllocateForTabletWithRetry(tableSize);
      this.allocatedMemoryBlockForChunkMeta =
          PipeDataNodeResourceManager.memory().forceAllocateForTabletWithRetry(tableSize);
      this.allocatedMemoryBlockForTableSchemas =
          PipeDataNodeResourceManager.memory()
              .forceAllocateForTabletWithRetry(
                  IoTDBDescriptor.getInstance()
                      .getConfig()
                      .getPipeDataStructureTabletSizeInBytes());

      this.startTime = startTime;
      this.endTime = endTime;
      this.tablePattern = pattern;

      this.entity = entity;
      tsFileSequenceReader = new TsFileSequenceReader(tsFile.getPath(), true, true);
    } catch (final Exception e) {
      close();
      throw e;
    }
  }

  public TsFileInsertionEventTableParser(
      final File tsFile,
      final TablePattern pattern,
      final long startTime,
      final long endTime,
      final PipeTaskMeta pipeTaskMeta,
      final IAuditEntity entity,
      final PipeInsertionEvent sourceEvent,
      final boolean isWithMod)
      throws IOException {
    this(
        null, 0, tsFile, pattern, startTime, endTime, pipeTaskMeta, entity, sourceEvent, isWithMod);
  }

  @Override
  public Iterable<TabletInsertionEvent> toTabletInsertionEvents() {
    if (tabletInsertionIterable == null) {
      tabletInsertionIterable =
          () ->
              new Iterator<TabletInsertionEvent>() {

                private TsFileInsertionEventTableParserTabletIterator tabletIterator;
                private PipeRawTabletInsertionEvent nextEvent;
                private Tablet bufferedTablet;
                private boolean iterationClosed = false;

                @Override
                public boolean hasNext() {
                  try {
                    if (nextEvent != null) {
                      return true;
                    }

                    final Tablet tablet = pollNextNonEmptyTablet();
                    if (tablet == null) {
                      return false;
                    }

                    nextEvent = buildTabletInsertionEvent(tablet, !prepareNextNonEmptyTablet());
                    return true;
                  } catch (Exception e) {
                    close();
                    throw new PipeException(
                        DataNodePipeMessages.ERROR_WHILE_PARSING_TSFILE_INSERTION_EVENT, e);
                  }
                }

                private boolean hasTablePrivilege(final String tableName) {
                  if (Objects.isNull(entity)
                      || Objects.isNull(sourceEvent)
                      || Objects.isNull(sourceEvent.getTableModelDatabaseName())
                      || AuthorityChecker.getAccessControl()
                          .checkCanSelectFromTable4Pipe(
                              entity.getUsername(),
                              new QualifiedObjectName(
                                  sourceEvent.getTableModelDatabaseName(), tableName),
                              entity)) {
                    return true;
                  }
                  if (!skipIfNoPrivileges) {
                    throw new AccessDeniedException(
                        String.format(
                            DataNodePipeMessages
                                .PIPE_EXCEPTION_NO_PRIVILEGE_FOR_SELECT_FOR_USER_S_AT_TABLE_S_S_84B0C299,
                            entity.getUsername(),
                            sourceEvent.getTableModelDatabaseName(),
                            tableName));
                  }
                  return false;
                }

                private Tablet pollNextNonEmptyTablet() throws Exception {
                  if (!prepareNextNonEmptyTablet()) {
                    return null;
                  }

                  final Tablet tablet = bufferedTablet;
                  bufferedTablet = null;
                  return tablet;
                }

                private boolean prepareNextNonEmptyTablet() throws Exception {
                  if (bufferedTablet != null) {
                    return true;
                  }
                  if (iterationClosed) {
                    return false;
                  }

                  if (tabletIterator == null) {
                    tabletIterator =
                        new TsFileInsertionEventTableParserTabletIterator(
                            tsFileSequenceReader,
                            entry ->
                                (Objects.isNull(tablePattern)
                                        || tablePattern.matchesTable(entry.getKey()))
                                    && hasTablePrivilege(entry.getKey()),
                            allocatedMemoryBlockForTablet,
                            allocatedMemoryBlockForBatchData,
                            allocatedMemoryBlockForChunk,
                            allocatedMemoryBlockForChunkMeta,
                            allocatedMemoryBlockForTableSchemas,
                            currentModifications,
                            startTime,
                            endTime);
                  }

                  while (tabletIterator.hasNext()) {
                    if (!parseStartTimeRecorded) {
                      recordParseStartTime();
                    }

                    final Tablet tablet = tabletIterator.next();
                    recordTabletMetrics(tablet);
                    if (!PipeRawTabletInsertionEvent.isTabletEmpty(tablet)) {
                      bufferedTablet = tablet;
                      return true;
                    }
                  }

                  closeIteration();
                  return false;
                }

                private void closeIteration() {
                  if (iterationClosed) {
                    return;
                  }

                  if (parseStartTimeRecorded && !parseEndTimeRecorded) {
                    recordParseEndTime();
                  }
                  close();
                  iterationClosed = true;
                }

                private PipeRawTabletInsertionEvent buildTabletInsertionEvent(
                    final Tablet tablet, final boolean needToReport) {
                  return sourceEvent == null
                      ? new PipeRawTabletInsertionEvent(
                          Boolean.TRUE,
                          null,
                          null,
                          null,
                          tablet,
                          true,
                          null,
                          0,
                          pipeTaskMeta,
                          sourceEvent,
                          needToReport)
                      : new PipeRawTabletInsertionEvent(
                          Boolean.TRUE,
                          sourceEvent.getSourceDatabaseNameFromDataRegion(),
                          sourceEvent.getRawTableModelDataBase(),
                          sourceEvent.getRawTreeModelDataBase(),
                          tablet,
                          true,
                          sourceEvent.getPipeName(),
                          sourceEvent.getCreationTime(),
                          pipeTaskMeta,
                          sourceEvent,
                          needToReport);
                }

                @Override
                public TabletInsertionEvent next() {
                  if (!hasNext()) {
                    throw new NoSuchElementException();
                  }

                  final TabletInsertionEvent next = nextEvent;
                  nextEvent = null;
                  return next;
                }
              };
    }

    return tabletInsertionIterable;
  }

  @Override
  public void close() {
    super.close();

    if (allocatedMemoryBlockForBatchData != null) {
      allocatedMemoryBlockForBatchData.close();
    }

    if (allocatedMemoryBlockForChunk != null) {
      allocatedMemoryBlockForChunk.close();
    }

    if (allocatedMemoryBlockForChunkMeta != null) {
      allocatedMemoryBlockForChunkMeta.close();
    }

    if (allocatedMemoryBlockForTableSchemas != null) {
      allocatedMemoryBlockForTableSchemas.close();
    }
  }
}
