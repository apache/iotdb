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

package org.apache.iotdb.db.utils.datastructure;

import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.read.common.TimeRange;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.write.UnSupportedDataTypeException;
import org.apache.tsfile.write.chunk.ChunkWriterImpl;
import org.apache.tsfile.write.chunk.IChunkWriter;

import java.util.List;
import java.util.PriorityQueue;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.iotdb.db.utils.MemUtils.getBinarySize;

public class MergeSortMultiTVListIterator extends MultiTVListIterator {
  private final List<Integer> probeIterators;
  private final PriorityQueue<Pair<Long, Integer>> minHeap =
      new PriorityQueue<>(
          (a, b) -> a.left.equals(b.left) ? b.right.compareTo(a.right) : a.left.compareTo(b.left));

  private static final IoTDBConfig CONFIG = IoTDBDescriptor.getInstance().getConfig();
  private final long TARGET_CHUNK_SIZE = CONFIG.getTargetChunkSize();
  private final long MAX_NUMBER_OF_POINTS_IN_CHUNK = CONFIG.getTargetChunkPointNum();

  public MergeSortMultiTVListIterator(
      TSDataType tsDataType,
      List<TVList> tvLists,
      List<TimeRange> deletionList,
      Integer floatPrecision,
      TSEncoding encoding) {
    super(tsDataType, tvLists, deletionList, floatPrecision, encoding);
    this.probeIterators =
        IntStream.range(0, tvListIterators.size()).boxed().collect(Collectors.toList());
  }

  @Override
  protected void prepareNext() {
    hasNext = false;
    for (int i : probeIterators) {
      TVList.TVListIterator iterator = tvListIterators.get(i);
      if (iterator.hasNextTimeValuePair()) {
        minHeap.add(new Pair<>(iterator.currentTime(), i));
      }
    }
    probeIterators.clear();

    if (!minHeap.isEmpty()) {
      Pair<Long, Integer> top = minHeap.poll();
      currentTime = top.left;
      probeIterators.add(top.right);

      iteratorIndex = top.right;
      rowIndex = tvListIterators.get(iteratorIndex).getIndex();
      hasNext = true;

      // duplicated timestamps
      while (!minHeap.isEmpty() && minHeap.peek().left == currentTime) {
        Pair<Long, Integer> element = minHeap.poll();
        probeIterators.add(element.right);
      }
    }
    probeNext = true;
  }

  @Override
  protected void next() {
    for (int index : probeIterators) {
      tvListIterators.get(index).next();
    }
    probeNext = false;
  }

  @Override
  public void encodeBatch(IChunkWriter chunkWriter, BatchEncodeInfo encodeInfo, long[] times) {
    ChunkWriterImpl chunkWriterImpl = (ChunkWriterImpl) chunkWriter;
    while (hasNextTimeValuePair()) {
      // remember current iterator and row index
      TVList.TVListIterator currIterator = tvListIterators.get(iteratorIndex);
      int row = rowIndex;
      long time = currentTime;

      // check if it is last point
      next();
      if (!hasNextTimeValuePair()) {
        chunkWriterImpl.setLastPoint(true);
      }

      switch (tsDataType) {
        case BOOLEAN:
          chunkWriterImpl.write(time, currIterator.getTVList().getBoolean(row));
          encodeInfo.dataSizeInChunk += 8L + 1L;
          break;
        case INT32:
        case DATE:
          chunkWriterImpl.write(time, currIterator.getTVList().getInt(row));
          encodeInfo.dataSizeInChunk += 8L + 4L;
          break;
        case INT64:
        case TIMESTAMP:
          chunkWriterImpl.write(time, currIterator.getTVList().getLong(row));
          encodeInfo.dataSizeInChunk += 8L + 8L;
          break;
        case FLOAT:
          chunkWriterImpl.write(time, currIterator.getTVList().getFloat(row));
          encodeInfo.dataSizeInChunk += 8L + 4L;
          break;
        case DOUBLE:
          chunkWriterImpl.write(time, currIterator.getTVList().getDouble(row));
          encodeInfo.dataSizeInChunk += 8L + 8L;
          break;
        case TEXT:
        case BLOB:
        case STRING:
          Binary value = currIterator.getTVList().getBinary(row);
          chunkWriterImpl.write(time, value);
          encodeInfo.dataSizeInChunk += 8L + getBinarySize(value);
          break;
        default:
          throw new UnSupportedDataTypeException(
              String.format("Data type %s is not supported.", tsDataType));
      }
      encodeInfo.pointNumInChunk++;

      if (encodeInfo.pointNumInChunk >= MAX_NUMBER_OF_POINTS_IN_CHUNK
          || encodeInfo.dataSizeInChunk >= TARGET_CHUNK_SIZE) {
        break;
      }
    }
  }
}
