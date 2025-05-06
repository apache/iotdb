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

import org.apache.iotdb.commons.pipe.agent.task.meta.PipeTaskMeta;
import org.apache.iotdb.commons.pipe.datastructure.pattern.TablePattern;
import org.apache.iotdb.db.pipe.event.common.PipeInsertionEvent;
import org.apache.iotdb.db.pipe.event.common.tablet.PipeRawTabletInsertionEvent;
import org.apache.iotdb.db.pipe.event.common.tsfile.parser.TsFileInsertionEventParser;
import org.apache.iotdb.db.pipe.resource.PipeDataNodeResourceManager;
import org.apache.iotdb.db.pipe.resource.memory.PipeMemoryBlock;
import org.apache.iotdb.db.queryengine.plan.Coordinator;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.QualifiedObjectName;
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
  private final String userName;

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
      final String userName,
      final PipeInsertionEvent sourceEvent)
      throws IOException {
    super(pipeName, creationTime, null, pattern, startTime, endTime, pipeTaskMeta, sourceEvent);

    try {
      this.allocatedMemoryBlockForChunk =
          PipeDataNodeResourceManager.memory().forceAllocateForTabletWithRetry(0);
      this.allocatedMemoryBlockForBatchData =
          PipeDataNodeResourceManager.memory().forceAllocateForTabletWithRetry(0);
      this.allocatedMemoryBlockForChunkMeta =
          PipeDataNodeResourceManager.memory().forceAllocateForTabletWithRetry(0);
      this.allocatedMemoryBlockForTableSchemas =
          PipeDataNodeResourceManager.memory().forceAllocateForTabletWithRetry(0);

      this.startTime = startTime;
      this.endTime = endTime;
      this.tablePattern = pattern;

      this.userName = userName;
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
      final String userName,
      final PipeInsertionEvent sourceEvent)
      throws IOException {
    this(null, 0, tsFile, pattern, startTime, endTime, pipeTaskMeta, userName, sourceEvent);
  }

  @Override
  public Iterable<TabletInsertionEvent> toTabletInsertionEvents() {
    return () ->
        new Iterator<TabletInsertionEvent>() {

          private TsFileInsertionEventTableParserTabletIterator tabletIterator;

          @Override
          public boolean hasNext() {
            try {
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
                        startTime,
                        endTime);
              }
              if (!tabletIterator.hasNext()) {
                close();
                return false;
              }
              return true;
            } catch (Exception e) {
              close();
              throw new PipeException("Error while parsing tsfile insertion event", e);
            }
          }

          private boolean hasTablePrivilege(final String tableName) {
            return Objects.isNull(userName)
                || Objects.isNull(sourceEvent)
                || Objects.isNull(sourceEvent.getTableModelDatabaseName())
                || Coordinator.getInstance()
                    .getAccessControl()
                    .checkCanSelectFromTable4Pipe(
                        userName,
                        new QualifiedObjectName(
                            sourceEvent.getTableModelDatabaseName(), tableName));
          }

          @Override
          public TabletInsertionEvent next() {
            if (!hasNext()) {
              close();
              throw new NoSuchElementException();
            }

            final Tablet tablet = tabletIterator.next();

            final TabletInsertionEvent next;
            if (!hasNext()) {
              next =
                  sourceEvent == null
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
                          true)
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
                          true);
              close();
            } else {
              next =
                  sourceEvent == null
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
                          false)
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
                          false);
            }
            return next;
          }
        };
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
