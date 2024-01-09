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

package org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.writer;

import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.tsfile.read.TimeValuePair;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;
import org.apache.iotdb.tsfile.read.reader.IPointReader;
import org.apache.iotdb.tsfile.utils.TsPrimitiveType;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class RepairUnsortedFileCompactionWriter extends ReadPointInnerCompactionWriter {
  private List<TimeValuePair> dataOfCurrentSeries;

  public RepairUnsortedFileCompactionWriter(TsFileResource targetFileResource) throws IOException {
    super(targetFileResource);
  }

  @Override
  public void startMeasurement(List<IMeasurementSchema> measurementSchemaList, int subTaskId) {
    dataOfCurrentSeries = new ArrayList<>();
    super.startMeasurement(measurementSchemaList, subTaskId);
  }

  @Override
  public void endMeasurement(int subTaskId) throws IOException {
    if (dataOfCurrentSeries.isEmpty()) {
      super.endMeasurement(subTaskId);
      return;
    }
    // remove duplicate timestamp and sort
    dataOfCurrentSeries.sort(Comparator.comparingLong(TimeValuePair::getTimestamp));
    TimeValuePair previousTimeValuePair = null;
    for (TimeValuePair timeValuePair : dataOfCurrentSeries) {
      if (previousTimeValuePair == null) {
        writeDataPoint(
            timeValuePair.getTimestamp(), timeValuePair.getValue(), chunkWriters[subTaskId]);
        previousTimeValuePair = timeValuePair;
        chunkPointNumArray[subTaskId]++;
        continue;
      }
      if (previousTimeValuePair.getTimestamp() == timeValuePair.getTimestamp()) {
        // merge to previous TimeValuePair if is aligned series
        mergeTimeValuePair(timeValuePair, previousTimeValuePair);
      } else {
        writeDataPoint(
            timeValuePair.getTimestamp(), timeValuePair.getValue(), chunkWriters[subTaskId]);
        chunkPointNumArray[subTaskId]++;
        previousTimeValuePair = timeValuePair;
      }
      checkChunkSizeAndMayOpenANewChunk(fileWriter, chunkWriters[subTaskId], subTaskId);
    }
    dataOfCurrentSeries = null;
    super.endMeasurement(subTaskId);
  }

  private void mergeTimeValuePair(TimeValuePair from, TimeValuePair to) {
    if (!isAlign) {
      // ignore not aligned TimeValuePair with same timestamp
      return;
    }
    TsPrimitiveType[] fromVector = from.getValue().getVector();
    TsPrimitiveType[] toVector = to.getValue().getVector();
    for (int i = 0; i < toVector.length; i++) {
      if (toVector[i] == null && fromVector[i] != null) {
        toVector[i] = fromVector[i];
      }
    }
    to.getValue().setVector(toVector);
  }

  @Override
  public void write(TsBlock tsBlock, int subTaskId) throws IOException {
    IPointReader pointReader = tsBlock.getTsBlockAlignedRowIterator();
    while (pointReader.hasNextTimeValuePair()) {
      write(pointReader.nextTimeValuePair(), subTaskId);
    }
  }

  @Override
  public void write(TimeValuePair timeValuePair, int subTaskId) throws IOException {
    dataOfCurrentSeries.add(timeValuePair);
    isEmptyFile = false;
  }
}
