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
import org.apache.iotdb.commons.utils.FileUtils;
import org.apache.iotdb.db.i18n.DataNodePipeMessages;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertTabletNode;
import org.apache.iotdb.db.storageengine.dataregion.flush.MemTableFlushTask;
import org.apache.iotdb.db.storageengine.dataregion.memtable.IMemTable;
import org.apache.iotdb.db.storageengine.dataregion.memtable.PrimitiveMemTable;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.exception.write.WriteProcessException;
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
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

public class PipeTreeModelTsFileBuilderV2 extends PipeTsFileBuilder {

  private static final Logger LOGGER = LoggerFactory.getLogger(PipeTreeModelTsFileBuilderV2.class);

  /** Prefix of the single temp dir for tree model Object files (suffix: batch ID). */
  private static final String TREE_MODEL_OBJECT_CACHE_DIR_PREFIX =
      "TreeModelTSFileBuilderObjectCache_";

  private static final PlanNodeId PLACEHOLDER_PLAN_NODE_ID =
      new PlanNodeId("PipeTreeModelTsFileBuilderV2");

  private final List<Tablet> tabletList = new ArrayList<>();
  private final List<Boolean> isTabletAlignedList = new ArrayList<>();

  /** Single temp dir for tree model Object files; renamed to TSFile object dir on seal. */
  private File treeModelObjectTempDir;

  public PipeTreeModelTsFileBuilderV2(
      final AtomicLong currentBatchId, final AtomicLong tsFileIdGenerator) {
    super(currentBatchId, tsFileIdGenerator);
  }

  @Override
  public void bufferTableModelTablet(final String dataBase, final Tablet tablet) {
    throw new UnsupportedOperationException(
        DataNodePipeMessages.PIPETREEMODELTSFILEBUILDERV2_DOES_NOT_SUPPORT_TABLE_MODEL_TABLET);
  }

  @Override
  public void bufferTreeModelTablet(final Tablet tablet, final Boolean isAligned) {
    tabletList.add(tablet);
    isTabletAlignedList.add(isAligned);
  }

  /**
   * Links Object paths into the tree model's single temp dir. Call when the event has object paths.
   * At seal time the dir is renamed to the TSFile's object dir name.
   */
  public void linkObjectPathsForTreeModel(
      final Iterator<String> pathIterator, final TsFileResource resource, final String pipeName) {
    if (pathIterator == null || resource == null || pipeName == null) {
      return;
    }
    if (treeModelObjectTempDir == null) {
      final String dirName = TREE_MODEL_OBJECT_CACHE_DIR_PREFIX + currentBatchId.get();
      treeModelObjectTempDir = new File(getBatchFileBaseDir(), dirName);
      if (!treeModelObjectTempDir.exists() && !treeModelObjectTempDir.mkdirs()) {
        LOGGER.warn("Failed to create tree model object temp dir");
        return;
      }
    }
    linkObjectEntriesToDir(treeModelObjectTempDir, resource, pathIterator, pipeName);
  }

  @Override
  @SuppressWarnings("java:S100")
  public List<Pair<String, Pair<File, File>>> convertTabletToTsFileWithDBInfo()
      throws IOException, WriteProcessException {
    try {
      return writeTabletsToTsFiles();
    } catch (final WriteProcessException e) {
      LOGGER.warn(
          DataNodePipeMessages
              .EXCEPTION_OCCURRED_WHEN_PIPETREEMODELTSFILEBUILDERV2_WRITING_TABLETS_TO,
          e.getMessage(),
          e);
      throw e;
    }
  }

  @Override
  public boolean isEmpty() {
    return tabletList.isEmpty();
  }

  @Override
  public void onSuccess() {
    super.onSuccess();
    tabletList.clear();
    isTabletAlignedList.clear();
    treeModelObjectTempDir = null;
  }

  @Override
  public synchronized void close() {
    super.close();
    if (treeModelObjectTempDir != null) {
      FileUtils.deleteFileOrDirectory(treeModelObjectTempDir, true);
      treeModelObjectTempDir = null;
    }
    tabletList.clear();
    isTabletAlignedList.clear();
  }

  private List<Pair<String, Pair<File, File>>> writeTabletsToTsFiles()
      throws WriteProcessException {
    final IMemTable memTable = new PrimitiveMemTable(null, null);
    final List<Pair<String, Pair<File, File>>> sealedFiles = new ArrayList<>();
    try (final RestorableTsFileIOWriter writer = new RestorableTsFileIOWriter(createFile())) {
      writeTabletsIntoOneFile(memTable, writer);
      final File tsFile = writer.getFile();
      sealedFiles.add(new Pair<>(null, new Pair<>(tsFile, treeModelObjectTempDir)));
    } catch (final Exception e) {
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
      final IMemTable memTable, final RestorableTsFileIOWriter writer) throws Exception {
    for (int i = 0, size = tabletList.size(); i < size; ++i) {
      final Tablet tablet = tabletList.get(i);
      MeasurementSchema[] measurementSchemas =
          tablet.getSchemas().stream()
              .map(schema -> (MeasurementSchema) schema)
              .toArray(MeasurementSchema[]::new);
      Object[] values = Arrays.copyOf(tablet.getValues(), tablet.getValues().length);
      BitMap[] bitMaps = Arrays.copyOf(tablet.getBitMaps(), tablet.getBitMaps().length);

      // convert date value to int refer to
      // org.apache.iotdb.db.storageengine.dataregion.memtable.WritableMemChunk.writeNonAlignedTablet
      int validatedIndex = 0;
      for (int j = 0; j < tablet.getSchemas().size(); ++j) {
        final IMeasurementSchema schema = measurementSchemas[j];
        if (Objects.isNull(schema)) {
          break;
        }

        if (Objects.equals(TSDataType.DATE, schema.getType()) && values[j] instanceof LocalDate[]) {
          final LocalDate[] dates = ((LocalDate[]) values[j]);
          final int[] dateValues = new int[dates.length];
          for (int k = 0; k < Math.min(dates.length, tablet.getRowSize()); k++) {
            dateValues[k] = DateUtils.parseDateExpressionToInt(dates[k]);
          }
          values[j] = dateValues;
        }
        measurementSchemas[validatedIndex] = measurementSchemas[j];
        values[validatedIndex] = values[j];
        bitMaps[validatedIndex] = bitMaps[j];
        validatedIndex++;
      }

      if (validatedIndex != measurementSchemas.length) {
        values = Arrays.copyOf(values, validatedIndex);
        measurementSchemas = Arrays.copyOf(measurementSchemas, validatedIndex);
        bitMaps = Arrays.copyOf(bitMaps, validatedIndex);
      }

      final InsertTabletNode insertTabletNode =
          new InsertTabletNode(
              PLACEHOLDER_PLAN_NODE_ID,
              new PartialPath(tablet.getDeviceId()),
              isTabletAlignedList.get(i),
              Arrays.stream(measurementSchemas)
                  .map(IMeasurementSchema::getMeasurementName)
                  .toArray(String[]::new),
              Arrays.stream(measurementSchemas)
                  .map(IMeasurementSchema::getType)
                  .toArray(TSDataType[]::new),
              // TODO: cast
              measurementSchemas,
              tablet.getTimestamps(),
              bitMaps,
              values,
              tablet.getRowSize());

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
