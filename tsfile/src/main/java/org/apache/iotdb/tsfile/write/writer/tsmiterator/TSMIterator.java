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
package org.apache.iotdb.tsfile.write.writer.tsmiterator;

import org.apache.iotdb.tsfile.file.metadata.ChunkGroupMetadata;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.IChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.TimeseriesMetadata;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.utils.PublicBAOS;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * TSMIterator returns full path of series and its TimeseriesMetadata iteratively. It accepts data
 * source from memory or disk. Static method getTSMIteratorInMemory returns a TSMIterator that reads
 * from memory, and static method getTSMIteratorInDisk returns a TSMIterator that reads from disk.
 */
public class TSMIterator {
  private static final Logger LOG = LoggerFactory.getLogger(TSMIterator.class);
  protected List<Pair<Path, List<IChunkMetadata>>> sortedChunkMetadataList;
  protected Iterator<Pair<Path, List<IChunkMetadata>>> iterator;

  protected TSMIterator(List<ChunkGroupMetadata> chunkGroupMetadataList, boolean sorted) {
    this.sortedChunkMetadataList =
        sorted
            ? splitChunkMetadataWithoutSorting(chunkGroupMetadataList, null, null)
            : splitChunkMetadataWithSorting(chunkGroupMetadataList, null, null);
    this.iterator = sortedChunkMetadataList.iterator();
  }

  public static TSMIterator getTSMIteratorInMemory(
      List<ChunkGroupMetadata> chunkGroupMetadataList, boolean sorted) {
    return new TSMIterator(chunkGroupMetadataList, sorted);
  }

  public static TSMIterator getTSMIteratorInDisk(
      File cmtFile, List<ChunkGroupMetadata> chunkGroupMetadataList, LinkedList<Long> serializePos)
      throws IOException {
    return new DiskTSMIterator(cmtFile, chunkGroupMetadataList, serializePos);
  }

  public boolean hasNext() {
    return iterator.hasNext();
  }

  public Pair<String, TimeseriesMetadata> next() throws IOException {
    Pair<Path, List<IChunkMetadata>> nextPair = iterator.next();
    return new Pair<>(
        nextPair.left.getFullPath(),
        constructOneTimeseriesMetadata(nextPair.left.getMeasurement(), nextPair.right));
  }

  public static TimeseriesMetadata constructOneTimeseriesMetadata(
      String measurementId, List<IChunkMetadata> chunkMetadataList) throws IOException {
    // create TimeseriesMetaData
    PublicBAOS publicBAOS = new PublicBAOS();
    TSDataType dataType = chunkMetadataList.get(chunkMetadataList.size() - 1).getDataType();
    Statistics seriesStatistics = Statistics.getStatsByType(dataType);

    int chunkMetadataListLength = 0;
    boolean serializeStatistic = (chunkMetadataList.size() > 1);
    // flush chunkMetadataList one by one
    for (IChunkMetadata chunkMetadata : chunkMetadataList) {
      if (!chunkMetadata.getDataType().equals(dataType)) {
        continue;
      }
      chunkMetadataListLength += chunkMetadata.serializeTo(publicBAOS, serializeStatistic);
      seriesStatistics.mergeStatistics(chunkMetadata.getStatistics());
    }

    TimeseriesMetadata timeseriesMetadata =
        new TimeseriesMetadata(
            (byte)
                ((serializeStatistic ? (byte) 1 : (byte) 0) | chunkMetadataList.get(0).getMask()),
            chunkMetadataListLength,
            measurementId,
            dataType,
            seriesStatistics,
            publicBAOS);
    return timeseriesMetadata;
  }

  public static List<Pair<Path, List<IChunkMetadata>>> splitChunkMetadataWithSorting(
      List<ChunkGroupMetadata> chunkGroupMetadataList,
      String currentDevice,
      List<ChunkMetadata> chunkMetadataList) {
    Map<String, Map<Path, List<IChunkMetadata>>> chunkMetadataMap = new TreeMap<>();
    List<Pair<Path, List<IChunkMetadata>>> sortedChunkMetadataList = new LinkedList<>();
    for (ChunkGroupMetadata chunkGroupMetadata : chunkGroupMetadataList) {
      chunkMetadataMap.computeIfAbsent(chunkGroupMetadata.getDevice(), x -> new TreeMap<>());
      for (IChunkMetadata chunkMetadata : chunkGroupMetadata.getChunkMetadataList()) {
        chunkMetadataMap
            .get(chunkGroupMetadata.getDevice())
            .computeIfAbsent(
                new Path(chunkGroupMetadata.getDevice(), chunkMetadata.getMeasurementUid()),
                x -> new ArrayList<>())
            .add(chunkMetadata);
      }
    }
    if (currentDevice != null) {
      for (IChunkMetadata chunkMetadata : chunkMetadataList) {
        chunkMetadataMap
            .computeIfAbsent(currentDevice, x -> new TreeMap<>())
            .computeIfAbsent(
                new Path(currentDevice, chunkMetadata.getMeasurementUid()), x -> new ArrayList<>())
            .add(chunkMetadata);
      }
    }

    for (Map.Entry<String, Map<Path, List<IChunkMetadata>>> entry : chunkMetadataMap.entrySet()) {
      Map<Path, List<IChunkMetadata>> seriesChunkMetadataMap = entry.getValue();
      for (Map.Entry<Path, List<IChunkMetadata>> seriesChunkMetadataEntry :
          seriesChunkMetadataMap.entrySet()) {
        sortedChunkMetadataList.add(
            new Pair<>(seriesChunkMetadataEntry.getKey(), seriesChunkMetadataEntry.getValue()));
      }
    }
    return sortedChunkMetadataList;
  }

  public static List<Pair<Path, List<IChunkMetadata>>> splitChunkMetadataWithoutSorting(
      List<ChunkGroupMetadata> chunkGroupMetadataList,
      String currentDevice,
      List<ChunkMetadata> chunkMetadataList) {
    List<Pair<Path, List<IChunkMetadata>>> chunkMetadataPairList = new LinkedList<>();
    String prevDevice = null;
    for (ChunkGroupMetadata chunkGroupMetadata : chunkGroupMetadataList) {
      if (prevDevice != null && chunkGroupMetadata.getDevice().compareTo(prevDevice) < 0) {
        throw new RuntimeException(
            String.format(
                "Current device %s is smaller than prev device %s",
                chunkGroupMetadata.getDevice(), prevDevice));
      }
      prevDevice = chunkGroupMetadata.getDevice();

      String prevSeries = null;
      List<IChunkMetadata> chunkMetadataForCurrSeries = new ArrayList<>();
      for (IChunkMetadata chunkMetadata : chunkGroupMetadata.getChunkMetadataList()) {
        if (prevSeries != null && chunkMetadata.getMeasurementUid().compareTo(prevSeries) < 0) {
          throw new RuntimeException(
              String.format(
                  "Current series %s is smaller than prev series %s in device %s",
                  chunkMetadata.getMeasurementUid(), prevSeries, prevDevice));
        }
        if (!chunkMetadata.getMeasurementUid().equals(prevSeries)
            && chunkMetadataForCurrSeries.size() > 0) {
          chunkMetadataPairList.add(
              new Pair<>(new Path(prevDevice, prevSeries), chunkMetadataForCurrSeries));
          chunkMetadataForCurrSeries = new ArrayList<>();
        }
        prevSeries = chunkMetadata.getMeasurementUid();
        chunkMetadataForCurrSeries.add(chunkMetadata);
      }
      if (chunkMetadataForCurrSeries.size() > 0) {
        chunkMetadataPairList.add(
            new Pair<>(new Path(prevDevice, prevSeries), chunkMetadataForCurrSeries));
      }
    }

    if (currentDevice != null) {
      String prevSeries = null;
      List<IChunkMetadata> chunkMetadataForCurrSeries = new ArrayList<>();
      for (IChunkMetadata chunkMetadata : chunkMetadataList) {
        if (prevSeries != null && chunkMetadata.getMeasurementUid().compareTo(prevSeries) < 0) {
          throw new RuntimeException(
              String.format(
                  "Current series %s is smaller than prev series %s in device %s",
                  chunkMetadata.getMeasurementUid(), prevSeries, prevDevice));
        }
        if (!chunkMetadata.getMeasurementUid().equals(prevSeries)
            && chunkMetadataForCurrSeries.size() > 0) {
          chunkMetadataPairList.add(
              new Pair<>(new Path(currentDevice, prevSeries), chunkMetadataForCurrSeries));
          chunkMetadataForCurrSeries = new ArrayList<>();
        }
        prevSeries = chunkMetadata.getMeasurementUid();
        chunkMetadataForCurrSeries.add(chunkMetadata);
      }
      if (chunkMetadataForCurrSeries.size() > 0) {
        chunkMetadataPairList.add(
            new Pair<>(new Path(prevDevice, prevSeries), chunkMetadataForCurrSeries));
      }
    }
    return chunkMetadataPairList;
  }
}
