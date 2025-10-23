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

import org.apache.tsfile.exception.write.WriteProcessException;
import org.apache.tsfile.external.commons.io.FileUtils;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.read.common.Path;
import org.apache.tsfile.utils.BitMap;
import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.write.TsFileWriter;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class PipeTreeModelTsFileBuilder extends PipeTsFileBuilder {

  private static final Logger LOGGER = LoggerFactory.getLogger(PipeTreeModelTsFileBuilder.class);

  private final List<Tablet> tabletList = new ArrayList<>();
  private final List<Boolean> isTabletAlignedList = new ArrayList<>();

  public PipeTreeModelTsFileBuilder(
      final AtomicLong currentBatchId, final AtomicLong tsFileIdGenerator) {
    super(currentBatchId, tsFileIdGenerator);
  }

  @Override
  public void bufferTableModelTablet(final String dataBase, final Tablet tablet) {
    throw new UnsupportedOperationException(
        "PipeTreeModelTsFileBuilder does not support table model tablet to build TSFile");
  }

  @Override
  public void bufferTreeModelTablet(final Tablet tablet, final Boolean isAligned) {
    tabletList.add(tablet);
    isTabletAlignedList.add(isAligned);
  }

  @Override
  public List<Pair<String, File>> convertTabletToTsFileWithDBInfo()
      throws IOException, WriteProcessException {
    return writeTabletsToTsFiles();
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
  }

  @Override
  public synchronized void close() {
    super.close();
    tabletList.clear();
    isTabletAlignedList.clear();
  }

  private List<Pair<String, File>> writeTabletsToTsFiles()
      throws IOException, WriteProcessException {
    final Map<String, List<Tablet>> device2Tablets = new HashMap<>();
    final Map<String, Boolean> device2Aligned = new HashMap<>();

    // Sort the tablets by device id
    for (int i = 0, size = tabletList.size(); i < size; ++i) {
      final Tablet tablet = tabletList.get(i);
      final String deviceId = tablet.getDeviceId();
      device2Tablets.computeIfAbsent(deviceId, k -> new ArrayList<>()).add(tablet);
      device2Aligned.put(deviceId, isTabletAlignedList.get(i));
    }

    // Sort the tablets by start time in each device
    for (final List<Tablet> tablets : device2Tablets.values()) {
      tablets.sort(
          // Each tablet has at least one timestamp
          Comparator.comparingLong(tablet -> tablet.getTimestamp(0)));
    }

    // Sort the devices by device id
    final List<String> devices = new ArrayList<>(device2Tablets.keySet());
    devices.sort(Comparator.naturalOrder());

    // Replace ArrayList with LinkedList to improve performance
    final LinkedHashMap<String, LinkedList<Tablet>> device2TabletsLinkedList =
        new LinkedHashMap<>();
    for (final String device : devices) {
      device2TabletsLinkedList.put(device, new LinkedList<>(device2Tablets.get(device)));
    }

    // Help GC
    devices.clear();
    device2Tablets.clear();

    // Write the tablets to the tsfile device by device, and the tablets
    // in the same device are written in order of start time. Tablets in
    // the same device should not be written if their time ranges overlap.
    // If overlapped, we try to write the tablets whose device id is not
    // the same as the previous one. For the tablets not written in the
    // previous round, we write them in a new tsfile.
    final List<Pair<String, File>> sealedFiles = new ArrayList<>();

    // Try making the tsfile size as large as possible
    while (!device2TabletsLinkedList.isEmpty()) {
      if (Objects.isNull(fileWriter)) {
        fileWriter = new TsFileWriter(createFile());
      }
      try {
        tryBestToWriteTabletsIntoOneFile(device2TabletsLinkedList, device2Aligned);
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
          sealedFiles.add(new Pair<>(null, fileWriter.getIOWriter().getFile()));
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
      sealedFiles.add(new Pair<>(null, sealedFile));
      fileWriter = null;
    }

    return sealedFiles;
  }

  private Tablet tryBestToAggregateTablets(
      final String deviceId, final LinkedList<Tablet> tablets) {
    if (tablets.isEmpty()) {
      return null;
    }

    // Retrieve the first tablet to serve as the basis for the aggregation
    final Tablet firstTablet = tablets.peekFirst();
    final long[] aggregationTimestamps = firstTablet.getTimestamps();
    final int aggregationRow = firstTablet.getRowSize();
    final int aggregationMaxRow = firstTablet.getMaxRowNumber();

    // Prepare lists to accumulate schemas, values, and bitMaps
    final List<IMeasurementSchema> aggregatedSchemas = new ArrayList<>();
    final List<Object> aggregatedValues = new ArrayList<>();
    final List<BitMap> aggregatedBitMaps = new ArrayList<>();

    // Iterate and poll tablets from the head that satisfy the aggregation criteria
    while (!tablets.isEmpty()) {
      final Tablet tablet = tablets.peekFirst();
      if (Arrays.equals(tablet.getTimestamps(), aggregationTimestamps)
          && tablet.getRowSize() == aggregationRow
          && tablet.getMaxRowNumber() == aggregationMaxRow) {
        // Aggregate the current tablet's data
        aggregatedSchemas.addAll(tablet.getSchemas());
        aggregatedValues.addAll(Arrays.asList(tablet.getValues()));
        aggregatedBitMaps.addAll(Arrays.asList(tablet.getBitMaps()));
        // Remove the aggregated tablet
        tablets.pollFirst();
      } else {
        // Stop aggregating once a tablet does not meet the criteria
        break;
      }
    }

    // Remove duplicates from aggregatedSchemas, record the index of the first occurrence, and
    // filter out the corresponding values in aggregatedValues and aggregatedBitMaps based on that
    // index
    final Set<IMeasurementSchema> seen = new HashSet<>();
    final List<Integer> distinctIndices =
        IntStream.range(0, aggregatedSchemas.size())
            .filter(i -> Objects.nonNull(aggregatedSchemas.get(i)))
            .filter(i -> seen.add(aggregatedSchemas.get(i))) // Only keep the first occurrence index
            .boxed()
            .collect(Collectors.toList());

    // Construct a new aggregated Tablet using the deduplicated data
    return new Tablet(
        deviceId,
        distinctIndices.stream().map(aggregatedSchemas::get).collect(Collectors.toList()),
        aggregationTimestamps,
        distinctIndices.stream().map(aggregatedValues::get).toArray(),
        distinctIndices.stream().map(aggregatedBitMaps::get).toArray(BitMap[]::new),
        aggregationRow);
  }

  private void tryBestToWriteTabletsIntoOneFile(
      final LinkedHashMap<String, LinkedList<Tablet>> device2TabletsLinkedList,
      final Map<String, Boolean> device2Aligned)
      throws IOException, WriteProcessException {
    final Iterator<Map.Entry<String, LinkedList<Tablet>>> iterator =
        device2TabletsLinkedList.entrySet().iterator();

    while (iterator.hasNext()) {
      final Map.Entry<String, LinkedList<Tablet>> entry = iterator.next();
      final String deviceId = entry.getKey();
      final LinkedList<Tablet> tablets = entry.getValue();

      final List<Tablet> tabletsToWrite = new ArrayList<>();

      Tablet lastTablet = null;
      while (!tablets.isEmpty()) {
        final Tablet tablet = tryBestToAggregateTablets(deviceId, tablets);
        if (Objects.isNull(lastTablet)
            // lastTablet.rowSize is not 0
            || lastTablet.getTimestamp(lastTablet.getRowSize() - 1) < tablet.getTimestamp(0)) {
          tabletsToWrite.add(tablet);
          lastTablet = tablet;
        } else {
          tablets.addFirst(tablet);
          break;
        }
      }

      if (tablets.isEmpty()) {
        iterator.remove();
      }

      final boolean isAligned = device2Aligned.get(deviceId);
      if (isAligned) {
        final Map<String, List<IMeasurementSchema>> deviceId2MeasurementSchemas = new HashMap<>();
        tabletsToWrite.forEach(
            tablet ->
                deviceId2MeasurementSchemas.compute(
                    tablet.getDeviceId(),
                    (k, v) -> {
                      if (Objects.isNull(v)) {
                        return new ArrayList<>(tablet.getSchemas());
                      }
                      v.addAll(tablet.getSchemas());
                      return v;
                    }));
        for (final Map.Entry<String, List<IMeasurementSchema>> deviceIdWithMeasurementSchemas :
            deviceId2MeasurementSchemas.entrySet()) {
          fileWriter.registerAlignedTimeseries(
              new Path(deviceIdWithMeasurementSchemas.getKey()),
              deviceIdWithMeasurementSchemas.getValue());
        }
        for (final Tablet tablet : tabletsToWrite) {
          fileWriter.writeAligned(tablet);
        }
      } else {
        for (final Tablet tablet : tabletsToWrite) {
          for (final IMeasurementSchema schema : tablet.getSchemas()) {
            try {
              fileWriter.registerTimeseries(
                  IDeviceID.Factory.DEFAULT_FACTORY.create(tablet.getDeviceId()), schema);
            } catch (final WriteProcessException ignore) {
              // Do nothing if the timeSeries has been registered
            }
          }

          fileWriter.writeTree(tablet);
        }
      }
    }
  }
}
