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

package org.apache.iotdb.db.utils;

import org.apache.iotdb.db.engine.compaction.cross.rewrite.manage.CrossSpaceMergeResource;
import org.apache.iotdb.db.engine.modification.Modification;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.Chunk;
import org.apache.iotdb.tsfile.read.common.Path;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.PriorityQueue;

public class MergeUtils {

  private static final Logger logger = LoggerFactory.getLogger(MergeUtils.class);

  private MergeUtils() {
    // util class
  }

  private static List<Path> collectFileSeries(TsFileSequenceReader sequenceReader)
      throws IOException {
    return sequenceReader.getAllPaths();
  }

  // returns totalChunkNum of a file and the max number of chunks of a series
  public static long[] findTotalAndLargestSeriesChunkNum(
      TsFileResource tsFileResource, TsFileSequenceReader sequenceReader) throws IOException {
    long totalChunkNum = 0;
    long maxChunkNum = Long.MIN_VALUE;
    List<Path> paths = collectFileSeries(sequenceReader);

    for (Path path : paths) {
      List<ChunkMetadata> chunkMetadataList = sequenceReader.getChunkMetadataList(path, true);
      totalChunkNum += chunkMetadataList.size();
      maxChunkNum = chunkMetadataList.size() > maxChunkNum ? chunkMetadataList.size() : maxChunkNum;
    }
    logger.debug(
        "In file {}, total chunk num {}, series max chunk num {}",
        tsFileResource,
        totalChunkNum,
        maxChunkNum);
    return new long[] {totalChunkNum, maxChunkNum};
  }

  public static long getFileMetaSize(TsFileResource seqFile, TsFileSequenceReader sequenceReader) {
    return seqFile.getTsFileSize() - sequenceReader.getFileMetadataPos();
  }

  /**
   * Reads chunks of paths in unseqResources and put them in separated lists. When reading a file,
   * this method follows the order of positions of chunks instead of the order of timeseries, which
   * reduce disk seeks.
   *
   * @param paths names of the timeseries
   */
  public static List<Chunk>[] collectUnseqChunks(
      List<PartialPath> paths,
      List<TsFileResource> unseqResources,
      CrossSpaceMergeResource mergeResource)
      throws IOException {
    List<Chunk>[] ret = new List[paths.size()];
    for (int i = 0; i < paths.size(); i++) {
      ret[i] = new ArrayList<>();
    }
    PriorityQueue<MetaListEntry> chunkMetaHeap = new PriorityQueue<>();

    for (TsFileResource tsFileResource : unseqResources) {

      TsFileSequenceReader tsFileReader = mergeResource.getFileReader(tsFileResource);
      // prepare metaDataList
      buildMetaHeap(paths, tsFileReader, mergeResource, tsFileResource, chunkMetaHeap);

      // read chunks order by their position
      collectUnseqChunks(chunkMetaHeap, tsFileReader, ret);
    }
    return ret;
  }

  private static void buildMetaHeap(
      List<PartialPath> paths,
      TsFileSequenceReader tsFileReader,
      CrossSpaceMergeResource resource,
      TsFileResource tsFileResource,
      PriorityQueue<MetaListEntry> chunkMetaHeap)
      throws IOException {
    for (int i = 0; i < paths.size(); i++) {
      PartialPath path = paths.get(i);
      List<ChunkMetadata> metaDataList = tsFileReader.getChunkMetadataList(path, true);
      if (metaDataList.isEmpty()) {
        continue;
      }
      List<Modification> pathModifications = resource.getModifications(tsFileResource, path);
      if (!pathModifications.isEmpty()) {
        QueryUtils.modifyChunkMetaData(metaDataList, pathModifications);
      }
      MetaListEntry entry = new MetaListEntry(i, metaDataList);
      if (entry.hasNext()) {
        entry.next();
        chunkMetaHeap.add(entry);
      }
    }
  }

  private static void collectUnseqChunks(
      PriorityQueue<MetaListEntry> chunkMetaHeap,
      TsFileSequenceReader tsFileReader,
      List<Chunk>[] ret)
      throws IOException {
    while (!chunkMetaHeap.isEmpty()) {
      MetaListEntry metaListEntry = chunkMetaHeap.poll();
      ChunkMetadata currMeta = metaListEntry.current();
      Chunk chunk = tsFileReader.readMemChunk(currMeta);
      ret[metaListEntry.pathId].add(chunk);
      if (metaListEntry.hasNext()) {
        metaListEntry.next();
        chunkMetaHeap.add(metaListEntry);
      }
    }
  }

  public static class MetaListEntry implements Comparable<MetaListEntry> {

    private int pathId;
    private int listIdx;
    private List<ChunkMetadata> chunkMetadataList;

    public MetaListEntry(int pathId, List<ChunkMetadata> chunkMetadataList) {
      this.pathId = pathId;
      this.listIdx = -1;
      this.chunkMetadataList = chunkMetadataList;
    }

    @Override
    public int compareTo(MetaListEntry o) {
      return Long.compare(
          this.current().getOffsetOfChunkHeader(), o.current().getOffsetOfChunkHeader());
    }

    public ChunkMetadata current() {
      return chunkMetadataList.get(listIdx);
    }

    public boolean hasNext() {
      return listIdx + 1 < chunkMetadataList.size();
    }

    public ChunkMetadata next() {
      return chunkMetadataList.get(++listIdx);
    }
  }
}
