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
public class TSMIterator implements Iterator<Pair<String, TimeseriesMetadata>> {
  private static Logger LOG = LoggerFactory.getLogger(TSMIterator.class);
  protected Map<Path, List<IChunkMetadata>> chunkMetadataListMap = new TreeMap<>();
  protected Iterator<Map.Entry<Path, List<IChunkMetadata>>> iterator;

  protected TSMIterator(List<ChunkGroupMetadata> chunkGroupMetadataList) {
    this.groupChunkMetadataListBySeries(chunkGroupMetadataList);
  }

  public static TSMIterator getTSMIteratorInMemory(
      List<ChunkGroupMetadata> chunkGroupMetadataList) {
    return new TSMIterator(chunkGroupMetadataList);
  }

  public static TSMIterator getTSMIteratorInDisk(
      File cmtFile, List<ChunkGroupMetadata> chunkGroupMetadataList, LinkedList<Long> serializePos)
      throws IOException {
    return new DiskTSMIterator(cmtFile, chunkGroupMetadataList, serializePos);
  }

  @Override
  public boolean hasNext() {
    return iterator.hasNext();
  }

  @Override
  public Pair<String, TimeseriesMetadata> next() {
    Map.Entry<Path, List<IChunkMetadata>> nextEntry = iterator.next();
    try {
      return new Pair<>(
          nextEntry.getKey().getFullPath(),
          constructOneTimeseriesMetadata(nextEntry.getKey(), nextEntry.getValue()));
    } catch (IOException e) {
      LOG.error("Meets IOException when getting next TimeseriesMetadata", e);
      return null;
    }
  }

  protected void groupChunkMetadataListBySeries(List<ChunkGroupMetadata> chunkGroupMetadataList) {
    // group ChunkMetadata by series
    for (ChunkGroupMetadata chunkGroupMetadata : chunkGroupMetadataList) {
      List<ChunkMetadata> chunkMetadatas = chunkGroupMetadata.getChunkMetadataList();
      for (IChunkMetadata chunkMetadata : chunkMetadatas) {
        Path series = new Path(chunkGroupMetadata.getDevice(), chunkMetadata.getMeasurementUid());
        chunkMetadataListMap.computeIfAbsent(series, k -> new ArrayList<>()).add(chunkMetadata);
      }
    }
    this.iterator = chunkMetadataListMap.entrySet().iterator();
  }

  protected TimeseriesMetadata constructOneTimeseriesMetadata(
      Path path, List<IChunkMetadata> chunkMetadataList) throws IOException {
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
            path.getMeasurement(),
            dataType,
            seriesStatistics,
            publicBAOS);
    return timeseriesMetadata;
  }
}
