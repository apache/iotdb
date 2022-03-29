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
package org.apache.iotdb.db.mpp.sql.planner.plan.node.write;

import org.apache.iotdb.commons.partition.DataRegionReplicaSet;
import org.apache.iotdb.commons.partition.TimePartitionId;
import org.apache.iotdb.db.engine.StorageEngine;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.mpp.sql.analyze.Analysis;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.PlanNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.PlanNodeId;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.utils.BitMap;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import java.nio.ByteBuffer;
import java.util.*;

public class InsertTabletNode extends InsertNode {

  private long[] times; // times should be sorted. It is done in the session API.

  private BitMap[] bitMaps;
  private Object[] columns;

  private int rowCount = 0;

  // when this plan is sub-plan split from another InsertTabletPlan, this indicates the original
  // positions of values in
  // this plan. For example, if the plan contains 5 timestamps, and range = [1,4,10,12], then it
  // means that the first 3
  // timestamps in this plan are from range[1,4) of the parent plan, and the last 2 timestamps are
  // from range[10,12)
  // of the parent plan.
  // this is usually used to back-propagate exceptions to the parent plan without losing their
  // proper positions.
  private List<Integer> range;

  public void setRange(List<Integer> range) {
    this.range = range;
  }

  public InsertTabletNode(
      PlanNodeId id,
      PartialPath devicePath,
      boolean isAligned,
      MeasurementSchema[] measurements,
      TSDataType[] dataTypes,
      long[] times,
      BitMap[] bitMaps,
      Object[] columns,
      int rowCount) {
    super(id, devicePath, isAligned, measurements, dataTypes);
    this.times = times;
    this.bitMaps = bitMaps;
    this.columns = columns;
    this.rowCount = rowCount;
  }

  public long[] getTimes() {
    return times;
  }

  public void setTimes(long[] times) {
    this.times = times;
  }

  public BitMap[] getBitMaps() {
    return bitMaps;
  }

  public void setBitMaps(BitMap[] bitMaps) {
    this.bitMaps = bitMaps;
  }

  public Object[] getColumns() {
    return columns;
  }

  public void setColumns(Object[] columns) {
    this.columns = columns;
  }

  public InsertTabletNode(PlanNodeId id) {
    super(id);
  }

  public int getRowCount() {
    return rowCount;
  }

  public void setRowCount(int rowCount) {
    this.rowCount = rowCount;
  }

  public List<Integer> getRange() {
    return range;
  }

  @Override
  public List<PlanNode> getChildren() {
    return null;
  }

  @Override
  public void addChildren(PlanNode child) {}

  @Override
  public PlanNode clone() {
    return null;
  }

  @Override
  public PlanNode cloneWithChildren(List<PlanNode> children) {
    return null;
  }

  @Override
  public List<String> getOutputColumnNames() {
    return null;
  }

  public static InsertTabletNode deserialize(ByteBuffer byteBuffer) {
    return null;
  }

  @Override
  public void serialize(ByteBuffer byteBuffer) {}

  @Override
  public List<InsertNode> splitByPartition(Analysis analysis) {
    // only single device in single storage group
    List<InsertNode> result = new ArrayList<>();
    if (times.length == 0) {
      return Collections.emptyList();
    }
    long startTime =
        (times[0] / StorageEngine.getTimePartitionInterval())
            * StorageEngine.getTimePartitionInterval(); // included
    long endTime = startTime + StorageEngine.getTimePartitionInterval(); // excluded
    TimePartitionId timePartitionId = StorageEngine.getTimePartitionId(times[0]);
    int startLoc = 0; // included

    List<TimePartitionId> timePartitionIdList = new ArrayList<>();
    // for each List in split, they are range1.start, range1.end, range2.start, range2.end, ...
    List<Integer> ranges = new ArrayList<>();
    for (int i = 1; i < times.length; i++) { // times are sorted in session API.
      if (times[i] >= endTime) {
        // a new range.
        ranges.add(startLoc); // included
        ranges.add(i); // excluded
        timePartitionIdList.add(timePartitionId);
        // next init
        startLoc = i;
        startTime = endTime;
        endTime =
            (times[i] / StorageEngine.getTimePartitionInterval() + 1)
                * StorageEngine.getTimePartitionInterval();
        timePartitionId = StorageEngine.getTimePartitionId(times[i]);
      }
    }

    // the final range
    ranges.add(startLoc); // included
    ranges.add(times.length); // excluded
    timePartitionIdList.add(timePartitionId);

    // data region for each time partition
    List<DataRegionReplicaSet> dataRegionReplicaSets =
        analysis
            .getDataPartitionInfo()
            .getDataRegionReplicaSetForWriting(devicePath.getFullPath(), timePartitionIdList);

    Map<DataRegionReplicaSet, List<Integer>> splitMap = new HashMap<>();
    for (int i = 0; i < dataRegionReplicaSets.size(); i++) {
      List<Integer> sub_ranges =
          splitMap.computeIfAbsent(dataRegionReplicaSets.get(i), x -> new ArrayList<>());
      sub_ranges.add(ranges.get(i));
      sub_ranges.add(ranges.get(i + 1));
    }

    List<Integer> locs;
    for (Map.Entry<DataRegionReplicaSet, List<Integer>> entry : splitMap.entrySet()) {
      // generate a new times and values
      locs = entry.getValue();
      int count = 0;
      for (int i = 0; i < locs.size(); i += 2) {
        int start = locs.get(i);
        int end = locs.get(i + 1);
        count += end - start;
      }
      long[] subTimes = new long[count];
      int destLoc = 0;
      Object[] values = initTabletValues(dataTypes.length, count, dataTypes);
      BitMap[] bitMaps = this.bitMaps == null ? null : initBitmaps(dataTypes.length, count);
      for (int i = 0; i < locs.size(); i += 2) {
        int start = locs.get(i);
        int end = locs.get(i + 1);
        System.arraycopy(times, start, subTimes, destLoc, end - start);
        for (int k = 0; k < values.length; k++) {
          System.arraycopy(columns[k], start, values[k], destLoc, end - start);
          if (bitMaps != null && this.bitMaps[k] != null) {
            BitMap.copyOfRange(this.bitMaps[k], start, bitMaps[k], destLoc, end - start);
          }
        }
        destLoc += end - start;
      }
      InsertTabletNode subNode =
          new InsertTabletNode(
              getId(),
              devicePath,
              isAligned,
              measurements,
              dataTypes,
              subTimes,
              bitMaps,
              values,
              subTimes.length);
      subNode.setRange(locs);
      subNode.setDataRegionReplicaSet(entry.getKey());
      result.add(subNode);
    }
    return result;
  }

  private Object[] initTabletValues(int columnSize, int rowSize, TSDataType[] dataTypes) {
    Object[] values = new Object[columnSize];
    for (int i = 0; i < values.length; i++) {
      switch (dataTypes[i]) {
        case TEXT:
          values[i] = new Binary[rowSize];
          break;
        case FLOAT:
          values[i] = new float[rowSize];
          break;
        case INT32:
          values[i] = new int[rowSize];
          break;
        case INT64:
          values[i] = new long[rowSize];
          break;
        case DOUBLE:
          values[i] = new double[rowSize];
          break;
        case BOOLEAN:
          values[i] = new boolean[rowSize];
          break;
      }
    }
    return values;
  }

  private BitMap[] initBitmaps(int columnSize, int rowSize) {
    BitMap[] bitMaps = new BitMap[columnSize];
    for (int i = 0; i < columnSize; i++) {
      bitMaps[i] = new BitMap(rowSize);
    }
    return bitMaps;
  }
}
