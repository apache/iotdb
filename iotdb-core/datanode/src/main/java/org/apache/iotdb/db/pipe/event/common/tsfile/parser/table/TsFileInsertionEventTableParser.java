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
import org.apache.iotdb.db.storageengine.dataregion.modification.ModEntry;
import org.apache.iotdb.db.storageengine.dataregion.modification.TableDeletionEntry;
import org.apache.iotdb.db.utils.ModificationUtils;
import org.apache.iotdb.db.utils.datastructure.PatternTreeMapFactory;
import org.apache.iotdb.pipe.api.event.dml.insertion.TabletInsertionEvent;
import org.apache.iotdb.pipe.api.exception.PipeException;

import org.apache.tsfile.read.TsFileSequenceReader;
import org.apache.tsfile.write.record.Tablet;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Set;

public class TsFileInsertionEventTableParser extends TsFileInsertionEventParser {

  private final boolean collectObjectColumnModEntries;

  private final List<ModEntry> originalModEntries;

  private final Map<String, Set<String>> tableObjectMeasurements = new HashMap<>();

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
      final boolean isWithMod,
      final boolean objectPathsOnly,
      final boolean collectObjectColumnModEntries)
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
        null,
        objectPathsOnly,
        isWithMod);

    this.collectObjectColumnModEntries = collectObjectColumnModEntries;
    this.isWithMod = isWithMod;
    try {
      final boolean loadModificationsFromTsFile =
          this.isWithMod || this.collectObjectColumnModEntries;
      if (this.collectObjectColumnModEntries) {
        originalModEntries = ModsOperationUtil.readAllModificationsFromTsFile(tsFile);
        currentModifications =
            ModsOperationUtil.buildModificationsPatternTreeMap(originalModEntries);
      } else {
        originalModEntries = Collections.emptyList();
        currentModifications =
            loadModificationsFromTsFile
                ? ModsOperationUtil.loadModificationsFromTsFile(tsFile)
                : PatternTreeMapFactory.getModsPatternTreeMap();
      }
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
      final boolean isWithMod,
      final boolean objectPathsOnly)
      throws IOException {
    this(
        null,
        0,
        tsFile,
        pattern,
        startTime,
        endTime,
        pipeTaskMeta,
        entity,
        sourceEvent,
        isWithMod,
        objectPathsOnly,
        false);
  }

  private void recordTableObjectMeasurements(
      final String tableName, final List<String> objectMeasurements) {
    if (tableName == null
        || tableName.isEmpty()
        || objectMeasurements == null
        || objectMeasurements.isEmpty()) {
      return;
    }
    tableObjectMeasurements
        .computeIfAbsent(tableName, ignored -> new LinkedHashSet<>())
        .addAll(objectMeasurements);
  }

  private List<ModEntry> generateObjectColumnModEntries(List<ModEntry> generatedEntries) {
    if (!collectObjectColumnModEntries
        || originalModEntries.isEmpty()
        || tableObjectMeasurements.isEmpty()) {
      return Collections.emptyList();
    }

    for (final ModEntry modEntry : originalModEntries) {
      if (!(modEntry instanceof TableDeletionEntry)) {
        continue;
      }

      final TableDeletionEntry tableDeletionEntry = (TableDeletionEntry) modEntry;
      final Set<String> objectMeasurements =
          tableObjectMeasurements.get(tableDeletionEntry.getTableName());
      if (objectMeasurements == null || objectMeasurements.isEmpty()) {
        continue;
      }

      ModEntry entry =
          ModsOperationUtil.buildObjectColumnDeletionEntries(
              tableDeletionEntry, objectMeasurements);
      if (entry != null) {
        generatedEntries.add(entry);
      }
    }

    return generatedEntries.isEmpty()
        ? Collections.emptyList()
        : ModificationUtils.sortAndMerge(generatedEntries);
  }

  @Override
  public void drainGeneratedObjectColumnModEntriesTo(final List<ModEntry> modEntries) {
    if (modEntries != null) {
      modEntries.addAll(generateObjectColumnModEntries(new ArrayList<>()));
    }
  }

  @Override
  public Iterable<TabletInsertionEvent> toTabletInsertionEvents() {
    if (tabletInsertionIterable == null) {
      tabletInsertionIterable =
          () ->
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
                              currentModifications,
                              startTime,
                              endTime,
                              objectPathsOnly,
                              collectObjectColumnModEntries,
                              collectObjectColumnModEntries && objectPathsOnly
                                  ? TsFileInsertionEventTableParser.this
                                      ::recordTableObjectMeasurements
                                  : null);
                    }
                    final boolean hasNext = tabletIterator.hasNext();
                    if (hasNext && !parseStartTimeRecorded) {
                      // Record start time on first hasNext() that returns true
                      recordParseStartTime();
                    } else if (!hasNext && parseStartTimeRecorded && !parseEndTimeRecorded) {
                      // Record end time on last hasNext() that returns false
                      recordParseEndTime();
                      close();
                    } else if (!hasNext) {
                      close();
                    }
                    return hasNext;
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
                            "No privilege for SELECT for user %s at table %s.%s",
                            entity.getUsername(),
                            sourceEvent.getTableModelDatabaseName(),
                            tableName));
                  }
                  return false;
                }

                @Override
                public TabletInsertionEvent next() {
                  if (!hasNext()) {
                    close();
                    throw new NoSuchElementException();
                  }

                  final Tablet tablet = tabletIterator.next();
                  // Record tablet metrics
                  recordTabletMetrics(tablet);

                  final TabletInsertionEvent next;
                  if (!hasNext()) {
                    final PipeRawTabletInsertionEvent event =
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

                    // Set tsFileResource and hasObjectData
                    event.setTsFileResource(tsFileResource);
                    event.setHasObject(hasObjectData);
                    next = event;
                    close();
                  } else {
                    final PipeRawTabletInsertionEvent event =
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

                    // Set tsFileResource and hasObjectData
                    event.setTsFileResource(tsFileResource);
                    event.setHasObject(hasObjectData);
                    next = event;
                  }
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
