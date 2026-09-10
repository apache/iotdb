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

package org.apache.iotdb.db.pipe.sink.util.builder;

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.commons.schema.table.column.TsTableColumnCategory;
import org.apache.iotdb.db.i18n.DataNodePipeMessages;
import org.apache.iotdb.db.pipe.event.common.tablet.PipeTabletUtils;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.RelationalInsertTabletNode;
import org.apache.iotdb.db.storageengine.dataregion.flush.MemTableFlushTask;
import org.apache.iotdb.db.storageengine.dataregion.memtable.IMemTable;
import org.apache.iotdb.db.storageengine.dataregion.memtable.PrimitiveMemTable;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;

import org.apache.tsfile.enums.ColumnCategory;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.exception.write.WriteProcessException;
import org.apache.tsfile.file.metadata.TableSchema;
import org.apache.tsfile.utils.BitMap;
import org.apache.tsfile.utils.DateUtils;
import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.apache.tsfile.write.writer.RestorableTsFileIOWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class PipeTableModelTsFileBuilderV2 extends PipeTsFileBuilder {

  private static final Logger LOGGER = LoggerFactory.getLogger(PipeTableModelTsFileBuilderV2.class);

  /** Prefix of per-database temp dir for table model Object files (suffix: database_BatchID). */
  private static final String TABLE_MODEL_OBJECT_CACHE_DIR_PREFIX =
      "TableModelTSFileBuilderObjectCache_";

  private static final PlanNodeId PLACEHOLDER_PLAN_NODE_ID =
      new PlanNodeId("PipeTableModelTsFileBuilderV2");

  private final Map<String, List<Tablet>> dataBase2TabletList = new HashMap<>();

  /** Temp dir per database for Object files; renamed to TSFile-named object dir on seal. */
  private final Map<String, File> dataBase2ObjectTempDir = new HashMap<>();

  private final PipeTableModelTsFileBuilder fallbackBuilder;

  public PipeTableModelTsFileBuilderV2(
      final AtomicLong currentBatchId, final AtomicLong tsFileIdGenerator) {
    super(currentBatchId, tsFileIdGenerator);
    fallbackBuilder = new PipeTableModelTsFileBuilder(currentBatchId, tsFileIdGenerator);
  }

  @Override
  public void bufferTableModelTablet(String dataBase, Tablet tablet) {
    dataBase2TabletList.computeIfAbsent(dataBase, db -> new ArrayList<>()).add(tablet);
    fallbackBuilder.bufferTableModelTablet(dataBase, tablet);
  }

  /**
   * Links Object paths from the given iterator into the database's temp dir. Call this when the
   * event has object paths (e.g. after buffering the tablet). At seal time the temp dir is renamed
   * to the TSFile's object dir name.
   */
  public void linkObjectPathsForDatabase(
      final String dataBase,
      final Iterator<String> pathIterator,
      final TsFileResource resource,
      String pipeName) {
    if (pathIterator == null || resource == null || pipeName == null) {
      return;
    }

    File tempDir =
        dataBase2ObjectTempDir.computeIfAbsent(
            dataBase,
            db -> {
              final String dirName =
                  TABLE_MODEL_OBJECT_CACHE_DIR_PREFIX + dataBase + "_" + currentBatchId.get();
              File dir = new File(getBatchFileBaseDir(), dirName);
              if (!dir.exists() && !dir.mkdirs()) {
                LOGGER.warn("Failed to create object temp dir for database: {}", dataBase);
                return null;
              }
              return dir;
            });
    if (tempDir != null) {
      linkObjectEntriesToDir(tempDir, resource, pathIterator, pipeName);
    }
  }

  @Override
  public void bufferTreeModelTablet(Tablet tablet, Boolean isAligned) {
    throw new UnsupportedOperationException(
        DataNodePipeMessages.PIPETABLEMODETSFILEBUILDERV2_DOES_NOT_SUPPORT_TREE_MODEL_TABLET);
  }

  @Override
  @SuppressWarnings("java:S100")
  public List<Pair<String, Pair<File, File>>> convertTabletToTsFileWithDBInfo()
      throws IOException, WriteProcessException {
    if (dataBase2TabletList.isEmpty()) {
      return new ArrayList<>(0);
    }
    final List<Pair<String, Pair<File, File>>> pairList = new ArrayList<>();
    try {
      for (final String dataBase : dataBase2TabletList.keySet()) {
        pairList.addAll(writeTabletsToTsFiles(dataBase));
      }
      return pairList;
    } catch (final Exception e) {
      pairList.forEach(
          pair -> org.apache.iotdb.commons.utils.FileUtils.deleteFileIfExist(pair.right.left));
      final boolean hasObjectDirectories = !dataBase2ObjectTempDir.isEmpty();
      dataBase2ObjectTempDir
          .values()
          .forEach(
              directory ->
                  org.apache.iotdb.commons.utils.FileUtils.deleteFileOrDirectory(directory, true));
      dataBase2ObjectTempDir.clear();
      LOGGER.warn(
          DataNodePipeMessages
              .EXCEPTION_OCCURRED_WHEN_PIPETABLEMODELTSFILEBUILDERV2_WRITING_TABLETS_TO,
          e.getMessage(),
          e);
      if (hasObjectDirectories) {
        // The classic fallback may split one database into multiple TsFiles and cannot safely
        // associate one Object directory with all generated files.
        throw e instanceof WriteProcessException
            ? (WriteProcessException) e
            : new WriteProcessException(e);
      }
      return fallbackBuilder.convertTabletToTsFileWithDBInfo();
    }
  }

  @Override
  public boolean isEmpty() {
    return dataBase2TabletList.isEmpty();
  }

  @Override
  public Object createCheckpoint() {
    final Map<String, Integer> tabletListSizes = new HashMap<>();
    dataBase2TabletList.forEach(
        (database, tablets) -> tabletListSizes.put(database, tablets.size()));
    return new BatchState(tabletListSizes, fallbackBuilder.createCheckpoint());
  }

  @Override
  public void rollbackToCheckpoint(final Object checkpoint) {
    if (!(checkpoint instanceof BatchState)) {
      return;
    }
    final BatchState batchState = (BatchState) checkpoint;
    dataBase2TabletList
        .entrySet()
        .removeIf(
            entry -> {
              final Integer size = batchState.tabletListSizes.get(entry.getKey());
              if (size == null) {
                return true;
              }
              truncate(entry.getValue(), size);
              return false;
            });
    fallbackBuilder.rollbackToCheckpoint(batchState.fallbackCheckpoint);
  }

  @Override
  public synchronized void onSuccess() {
    super.onSuccess();
    dataBase2TabletList.clear();
    dataBase2ObjectTempDir.clear();
    fallbackBuilder.onSuccess();
  }

  @Override
  public synchronized void close() {
    super.close();
    dataBase2TabletList.clear();
    dataBase2ObjectTempDir.clear();
    fallbackBuilder.close();
  }

  private static <T> void truncate(final List<T> list, final int size) {
    if (list.size() > size) {
      list.subList(size, list.size()).clear();
    }
  }

  private static final class BatchState {
    private final Map<String, Integer> tabletListSizes;
    private final Object fallbackCheckpoint;

    private BatchState(
        final Map<String, Integer> tabletListSizes, final Object fallbackCheckpoint) {
      this.tabletListSizes = tabletListSizes;
      this.fallbackCheckpoint = fallbackCheckpoint;
    }
  }

  private List<Pair<String, Pair<File, File>>> writeTabletsToTsFiles(final String dataBase)
      throws WriteProcessException {
    final IMemTable memTable = new PrimitiveMemTable(null, null);
    final List<Pair<String, Pair<File, File>>> sealedFiles = new ArrayList<>();
    File file = null;
    try {
      file = createFile();
      try (final RestorableTsFileIOWriter writer = new RestorableTsFileIOWriter(file)) {
        writeTabletsIntoOneFile(dataBase, memTable, writer);
        final File tempDir = dataBase2ObjectTempDir.get(dataBase);
        sealedFiles.add(new Pair<>(dataBase, new Pair<>(writer.getFile(), tempDir)));
      }
    } catch (final Exception e) {
      if (file != null) {
        org.apache.iotdb.commons.utils.FileUtils.deleteFileIfExist(file);
      }
      LOGGER.warn(
          DataNodePipeMessages.BATCH_ID_FAILED_TO_WRITE_TABLETS_INTO,
          currentBatchId.get(),
          e.getMessage(),
          e);
      throw new WriteProcessException(e);
    } finally {
      memTable.release();
    }

    return sealedFiles;
  }

  private void writeTabletsIntoOneFile(
      final String dataBase, final IMemTable memTable, final RestorableTsFileIOWriter writer)
      throws Exception {
    final List<Tablet> tabletList = Objects.requireNonNull(dataBase2TabletList.get(dataBase));

    final Map<String, List<Tablet>> tableName2TabletList = new HashMap<>();
    for (final Tablet tablet : tabletList) {
      tableName2TabletList
          .computeIfAbsent(tablet.getTableName(), k -> new ArrayList<>())
          .add(tablet);
    }

    for (Map.Entry<String, List<Tablet>> entry : tableName2TabletList.entrySet()) {
      final String tableName = entry.getKey();
      final List<Tablet> tablets = entry.getValue();

      List<IMeasurementSchema> aggregatedSchemas =
          tablets.stream()
              .flatMap(tablet -> tablet.getSchemas().stream())
              .collect(Collectors.toList());
      List<ColumnCategory> aggregatedColumnCategories =
          tablets.stream()
              .flatMap(tablet -> tablet.getColumnTypes().stream())
              .collect(Collectors.toList());

      final Set<IMeasurementSchema> seen = new HashSet<>();
      final List<Integer> distinctIndices =
          IntStream.range(0, aggregatedSchemas.size())
              .filter(i -> Objects.nonNull(aggregatedSchemas.get(i)))
              .filter(
                  i -> seen.add(aggregatedSchemas.get(i))) // Only keep the first occurrence index
              .boxed()
              .collect(Collectors.toList());

      writer
          .getSchema()
          .getTableSchemaMap()
          .put(
              tableName,
              new TableSchema(
                  tableName,
                  distinctIndices.stream().map(aggregatedSchemas::get).collect(Collectors.toList()),
                  distinctIndices.stream()
                      .map(aggregatedColumnCategories::get)
                      .collect(Collectors.toList())));
    }

    for (int i = 0, size = tabletList.size(); i < size; ++i) {
      final Tablet tablet = tabletList.get(i);
      MeasurementSchema[] measurementSchemas =
          tablet.getSchemas().stream()
              .map(schema -> (MeasurementSchema) schema)
              .toArray(MeasurementSchema[]::new);
      Object[] values = Arrays.copyOf(tablet.getValues(), tablet.getValues().length);
      BitMap[] bitMaps = PipeTabletUtils.copyBitMapsOrCreateEmpty(tablet);
      ColumnCategory[] columnCategory = tablet.getColumnTypes().toArray(new ColumnCategory[0]);

      // convert date value to int refer to
      // org.apache.iotdb.db.storageengine.dataregion.memtable.WritableMemChunk.writeNonAlignedTablet
      int validatedIndex = 0;
      for (int j = 0; j < tablet.getSchemas().size(); ++j) {
        final MeasurementSchema schema = measurementSchemas[j];
        if (Objects.isNull(schema) || Objects.isNull(columnCategory[j])) {
          continue;
        }
        if (Objects.equals(TSDataType.DATE, schema.getType()) && values[j] instanceof LocalDate[]) {
          final LocalDate[] dates = ((LocalDate[]) values[j]);
          final int[] dateValues = new int[dates.length];
          for (int k = 0; k < Math.min(dates.length, tablet.getRowSize()); k++) {
            if (Objects.nonNull(dates[k])) {
              dateValues[k] = DateUtils.parseDateExpressionToInt(dates[k]);
            }
          }
          values[j] = dateValues;
        }
        measurementSchemas[validatedIndex] = schema;
        values[validatedIndex] = values[j];
        bitMaps[validatedIndex] = bitMaps[j];
        columnCategory[validatedIndex] = columnCategory[j];
        validatedIndex++;
      }

      if (validatedIndex != measurementSchemas.length) {
        values = Arrays.copyOf(values, validatedIndex);
        measurementSchemas = Arrays.copyOf(measurementSchemas, validatedIndex);
        bitMaps = Arrays.copyOf(bitMaps, validatedIndex);
        columnCategory = Arrays.copyOf(columnCategory, validatedIndex);
      }

      final RelationalInsertTabletNode insertTabletNode =
          new RelationalInsertTabletNode(
              PLACEHOLDER_PLAN_NODE_ID,
              new PartialPath(tablet.getTableName()),
              // the data of the table model is aligned
              true,
              Arrays.stream(measurementSchemas)
                  .map(MeasurementSchema::getMeasurementName)
                  .toArray(String[]::new),
              Arrays.stream(measurementSchemas)
                  .map(MeasurementSchema::getType)
                  .toArray(TSDataType[]::new),
              // TODO: cast
              measurementSchemas,
              tablet.getTimestamps(),
              bitMaps,
              values,
              tablet.getRowSize(),
              Arrays.stream(columnCategory)
                  .map(TsTableColumnCategory::fromTsFileColumnCategory)
                  .toArray(TsTableColumnCategory[]::new));

      final int start = 0;
      final int end = insertTabletNode.getRowCount();

      try {
        if (insertTabletNode.isAligned()) {
          memTable.insertAlignedTablet(insertTabletNode, start, end, null);
        } else {
          memTable.insertTablet(insertTabletNode, start, end);
        }
      } catch (final org.apache.iotdb.db.exception.WriteProcessException e) {
        throw new WriteProcessException(e);
      }
    }

    final MemTableFlushTask memTableFlushTask = new MemTableFlushTask(memTable, writer, null, null);
    memTableFlushTask.syncFlushMemTable();

    writer.endFile();
  }
}
