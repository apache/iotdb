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

package org.apache.iotdb.db.pipe.connector.util.builder;

import org.apache.iotdb.pipe.api.exception.PipeException;

import org.apache.commons.io.FileUtils;
import org.apache.tsfile.exception.write.WriteProcessException;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.TableSchema;
import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.utils.WriteUtils;
import org.apache.tsfile.write.TsFileWriter;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

public class PipeTableModeTsFileBuilder extends PipeTsFileBuilder {

  private static final Logger LOGGER = LoggerFactory.getLogger(PipeTableModeTsFileBuilder.class);

  private final Map<String, List<Tablet>> dataBase2TabletList = new HashMap<>();

  public PipeTableModeTsFileBuilder(AtomicLong currentBatchId, AtomicLong tsFileIdGenerator) {
    super(currentBatchId, tsFileIdGenerator);
  }

  @Override
  public void bufferTableModelTablet(String dataBase, Tablet tablet) {
    dataBase2TabletList.computeIfAbsent(dataBase, db -> new ArrayList<>()).add(tablet);
  }

  @Override
  public void bufferTreeModelTablet(Tablet tablet, Boolean isAligned) {
    throw new UnsupportedOperationException(
        "PipeTableModeTsFileBuilder does not support tree model tablet to build TSFile");
  }

  @Override
  public List<Pair<String, File>> convertTabletToTsFileWithDBInfo() throws IOException {
    if (dataBase2TabletList.isEmpty()) {
      return new ArrayList<>(0);
    }
    final List<Pair<String, File>> pairList = new ArrayList<>();
    for (Map.Entry<String, List<Tablet>> entry : dataBase2TabletList.entrySet()) {
      final LinkedHashSet<LinkedList<Pair<Tablet, List<Pair<IDeviceID, Integer>>>>> linkedHashSet =
          new LinkedHashSet<>();
      pairList.addAll(
          writeTableModelTabletsToTsFiles(entry.getValue(), entry.getKey(), linkedHashSet));
    }
    return pairList;
  }

  @Override
  public boolean isEmpty() {
    return dataBase2TabletList.isEmpty();
  }

  @Override
  public synchronized void onSuccess() {
    super.onSuccess();
    dataBase2TabletList.clear();
  }

  @Override
  public synchronized void close() {
    super.close();
    dataBase2TabletList.clear();
  }

  private <T extends Pair<Tablet, List<Pair<IDeviceID, Integer>>>>
      List<Pair<String, File>> writeTableModelTabletsToTsFiles(
          final List<Tablet> tabletList,
          final String dataBase,
          LinkedHashSet<LinkedList<T>> linkedHashSet)
          throws IOException {

    final Map<String, List<T>> tableName2Tablets = new HashMap<>();

    // Sort the tablets by dataBaseName
    for (final Tablet tablet : tabletList) {
      tableName2Tablets
          .computeIfAbsent(tablet.getTableName(), k -> new ArrayList<>())
          .add((T) new Pair<>(tablet, WriteUtils.splitTabletByDevice(tablet)));
    }

    // Sort the tablets by start time in first device
    for (final List<T> tablets : tableName2Tablets.values()) {
      tablets.sort(
          (o1, o2) -> {
            final IDeviceID deviceID = o1.right.get(0).left;
            final int result;
            if ((result = deviceID.compareTo(o2.right.get(0).left)) == 0) {
              return Long.compare(o1.left.getTimestamp(0), o2.left.getTimestamp(0));
            }
            return result;
          });
    }

    // Sort the tables by table name
    tableName2Tablets.entrySet().stream()
        .sorted(Map.Entry.comparingByKey(Comparator.naturalOrder()))
        .forEach(entry -> linkedHashSet.add(new LinkedList<>(entry.getValue())));

    // Help GC
    tableName2Tablets.clear();

    final List<Pair<String, File>> sealedFiles = new ArrayList<>();

    // Try making the tsfile size as large as possible
    while (!linkedHashSet.isEmpty()) {
      if (Objects.isNull(fileWriter)) {
        fileWriter = new TsFileWriter(createFile());
      }

      try {
        tryBestToWriteTabletsIntoOneFile(linkedHashSet);
      } catch (final Exception e) {
        LOGGER.warn(
            "Batch id = {}: Failed to write tablets into tsfile, because {}",
            currentBatchId.get(),
            e.getMessage(),
            e);

        try {
          fileWriter.close();
        } catch (final Exception closeException) {
          LOGGER.warn(
              "Batch id = {}: Failed to close the tsfile {} after failed to write tablets into, because {}",
              currentBatchId.get(),
              fileWriter.getIOWriter().getFile().getPath(),
              closeException.getMessage(),
              closeException);
        } finally {
          // Add current writing file to the list and delete the file
          sealedFiles.add(new Pair<>(dataBase, fileWriter.getIOWriter().getFile()));
        }

        for (final Pair<String, File> sealedFile : sealedFiles) {
          final boolean deleteSuccess = FileUtils.deleteQuietly(sealedFile.right);
          LOGGER.warn(
              "Batch id = {}: {} delete the tsfile {} after failed to write tablets into {}. {}",
              currentBatchId.get(),
              deleteSuccess ? "Successfully" : "Failed to",
              sealedFile.right.getPath(),
              fileWriter.getIOWriter().getFile().getPath(),
              deleteSuccess ? "" : "Maybe the tsfile needs to be deleted manually.");
        }
        sealedFiles.clear();

        fileWriter = null;

        throw e;
      }

      fileWriter.close();
      final File sealedFile = fileWriter.getIOWriter().getFile();
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug(
            "Batch id = {}: Seal tsfile {} successfully.",
            currentBatchId.get(),
            sealedFile.getPath());
      }
      sealedFiles.add(new Pair<>(dataBase, sealedFile));
      fileWriter = null;
    }

    return sealedFiles;
  }

  private <T extends Pair<Tablet, List<Pair<IDeviceID, Integer>>>>
      void tryBestToWriteTabletsIntoOneFile(
          final LinkedHashSet<LinkedList<T>> device2TabletsLinkedList) throws IOException {
    final Iterator<LinkedList<T>> iterator = device2TabletsLinkedList.iterator();

    while (iterator.hasNext()) {
      final LinkedList<T> tablets = iterator.next();

      final List<T> tabletsToWrite = new ArrayList<>();
      final Map<IDeviceID, Long> deviceLastTimestampMap = new HashMap<>();
      String tableName = null;

      final List<IMeasurementSchema> columnSchemas = new ArrayList<>();
      final List<Tablet.ColumnCategory> columnCategories = new ArrayList<>();
      final Set<String> columnNames = new HashSet<>();

      while (!tablets.isEmpty()) {
        final T pair = tablets.peekFirst();
        if (timestampsAreNonOverlapping(
            (Pair<Tablet, List<Pair<IDeviceID, Integer>>>) pair, deviceLastTimestampMap)) {
          final Tablet tablet = pair.left;
          if (tableName == null) {
            tableName = tablet.getTableName();
          }

          for (int i = 0, size = tablet.getSchemas().size(); i < size; i++) {
            final IMeasurementSchema schema = tablet.getSchemas().get(i);
            if (schema == null || columnNames.contains(schema.getMeasurementName())) {
              continue;
            }
            columnNames.add(schema.getMeasurementName());
            columnSchemas.add(schema);
            columnCategories.add(tablet.getColumnTypes().get(i));
          }

          tabletsToWrite.add(pair);
          tablets.pollFirst();
          continue;
        }
        break;
      }

      if (tablets.isEmpty()) {
        iterator.remove();
      }

      if (tableName != null) {
        fileWriter.registerTableSchema(new TableSchema(tableName, columnSchemas, columnCategories));
      }

      for (final Pair<Tablet, List<Pair<IDeviceID, Integer>>> pair : tabletsToWrite) {
        final Tablet tablet = pair.left;
        try {
          fileWriter.writeTable(tablet, pair.right);
        } catch (WriteProcessException e) {
          LOGGER.warn(
              "Batch id = {}: Failed to build the table model TSFile. Please check whether the written Tablet has time overlap and whether the Table Schema is correct.",
              currentBatchId.get(),
              e);
          throw new PipeException(
              "The written Tablet time may overlap or the Schema may be incorrect");
        }
      }
    }
  }

  /**
   * A Map is used to record the maximum time each {@link IDeviceID} is written. {@link Pair}
   * records the Index+1 of the maximum timestamp of IDevice in each {@link Tablet}.
   *
   * @return If false, the tablet overlaps with the previous tablet; if true, there is no time
   *     overlap.
   */
  private <T extends Pair<Tablet, List<Pair<IDeviceID, Integer>>>>
      boolean timestampsAreNonOverlapping(
          final T tabletPair, final Map<IDeviceID, Long> deviceLastTimestampMap) {
    int currentTimestampIndex = 0;
    for (Pair<IDeviceID, Integer> deviceTimestampIndexPair : tabletPair.right) {
      final Long lastDeviceTimestamp = deviceLastTimestampMap.get(deviceTimestampIndexPair.left);
      if (lastDeviceTimestamp != null
          && lastDeviceTimestamp >= tabletPair.left.getTimestamp(currentTimestampIndex)) {
        return false;
      }
      currentTimestampIndex = deviceTimestampIndexPair.right;
      deviceLastTimestampMap.put(
          deviceTimestampIndexPair.left, tabletPair.left.getTimestamp(currentTimestampIndex - 1));
    }

    return true;
  }
}
