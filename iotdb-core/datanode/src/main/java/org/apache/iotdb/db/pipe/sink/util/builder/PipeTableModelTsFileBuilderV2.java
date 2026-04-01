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
import org.apache.iotdb.commons.schema.table.column.TsTableColumnCategory;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.RelationalInsertTabletNode;
import org.apache.iotdb.db.storageengine.dataregion.flush.MemTableFlushTask;
import org.apache.iotdb.db.storageengine.dataregion.memtable.IMemTable;
import org.apache.iotdb.db.storageengine.dataregion.memtable.PrimitiveMemTable;

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
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class PipeTableModelTsFileBuilderV2 extends PipeTsFileBuilder {

  private static final Logger LOGGER = LoggerFactory.getLogger(PipeTableModelTsFileBuilderV2.class);

  private static final PlanNodeId PLACEHOLDER_PLAN_NODE_ID =
      new PlanNodeId("PipeTableModelTsFileBuilderV2");

  private final Map<String, List<Tablet>> dataBase2TabletList = new HashMap<>();

  // TODO: remove me later if stable
  private final PipeTableModelTsFileBuilder fallbackBuilder;

  public PipeTableModelTsFileBuilderV2(
      final AtomicLong currentBatchId, final AtomicLong tsFileIdGenerator) {
    super(currentBatchId, tsFileIdGenerator);
    fallbackBuilder = new PipeTableModelTsFileBuilder(currentBatchId, tsFileIdGenerator);
  }

  @Override
  public void bufferTableModelTablet(String dataBase, Tablet tablet) {
    dataBase2TabletList.computeIfAbsent(dataBase, db -> new ArrayList<>()).add(tablet);
  }

  @Override
  public void bufferTreeModelTablet(Tablet tablet, Boolean isAligned) {
    throw new UnsupportedOperationException(
        "PipeTableModeTsFileBuilderV2 does not support tree model tablet to build TSFile");
  }

  @Override
  public List<Pair<String, File>> convertTabletToTsFileWithDBInfo() throws IOException {
    if (dataBase2TabletList.isEmpty()) {
      return new ArrayList<>(0);
    }
    try {
      final List<Pair<String, File>> pairList = new ArrayList<>();
      for (final String dataBase : dataBase2TabletList.keySet()) {
        pairList.addAll(writeTabletsToTsFiles(dataBase));
      }
      return pairList;
    } catch (final Exception e) {
      LOGGER.warn(
          "Exception occurred when PipeTableModelTsFileBuilderV2 writing tablets to tsfile, use fallback tsfile builder: {}",
          e.getMessage(),
          e);
      return fallbackBuilder.convertTabletToTsFileWithDBInfo();
    }
  }

  @Override
  public boolean isEmpty() {
    return dataBase2TabletList.isEmpty();
  }

  @Override
  public synchronized void onSuccess() {
    super.onSuccess();
    dataBase2TabletList.clear();
    fallbackBuilder.onSuccess();
  }

  @Override
  public synchronized void close() {
    super.close();
    dataBase2TabletList.clear();
    fallbackBuilder.close();
  }

  private List<Pair<String, File>> writeTabletsToTsFiles(final String dataBase)
      throws WriteProcessException {
    final IMemTable memTable = new PrimitiveMemTable(null, null);
    final List<Pair<String, File>> sealedFiles = new ArrayList<>();
    try (final RestorableTsFileIOWriter writer = new RestorableTsFileIOWriter(createFile())) {
      writeTabletsIntoOneFile(dataBase, memTable, writer);
      sealedFiles.add(new Pair<>(dataBase, writer.getFile()));
    } catch (final Exception e) {
      LOGGER.warn(
          "Batch id = {}: Failed to write tablets into tsfile, because {}",
          currentBatchId.get(),
          e.getMessage(),
          e);
      // TODO: handle ex
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
      BitMap[] bitMaps = Arrays.copyOf(tablet.getBitMaps(), tablet.getBitMaps().length);
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
