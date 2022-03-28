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

import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.mpp.sql.analyze.Analysis;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.PlanNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.PlanNodeId;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.utils.BitMap;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import java.nio.ByteBuffer;
import java.util.*;

public class InsertTabletNode extends InsertNode {

  private long[] times; // times should be sorted. It is done in the session API.

  private BitMap[] bitMaps;
  private Object[] columns;

  private int start;
  private int end;
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

  public InsertTabletNode(PlanNodeId id) {
    super(id);
  }

  public InsertTabletNode(
      PlanNodeId id,
      PartialPath devicePath,
      boolean isAligned,
      MeasurementSchema[] measurements,
      TSDataType[] dataTypes,
      long[] times,
      BitMap[] bitMaps,
      Object[] columns) {
    super(id, devicePath, isAligned, measurements, dataTypes);
    this.times = times;
    this.bitMaps = bitMaps;
    this.columns = columns;
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

  public int getStart() {
    return start;
  }

  public void setStart(int start) {
    this.start = start;
  }

  public int getEnd() {
    return end;
  }

  public void setEnd(int end) {
    this.end = end;
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
    // PartialPath storageGroup = SchemaEngine().getBelongedStorageGroup(plan.getDevicePath());
    List<InsertNode> result = new ArrayList<>();
    //    if (times.length == 0) {
    //      return Collections.emptyList();
    //    }
    //    long startTime =
    //        (times[0] / StorageEngine.getTimePartitionInterval())
    //            * StorageEngine.getTimePartitionInterval(); // included
    //    long endTime = startTime + StorageEngine.getTimePartitionInterval(); // excluded
    //    int startLoc = 0; // included
    //
    //    Map<PartitionGroup, List<Integer>> splitMap = new HashMap<>();
    //    // for each List in split, they are range1.start, range1.end, range2.start, range2.end,
    // ...
    //    for (int i = 1; i < times.length; i++) { // times are sorted in session API.
    //      if (times[i] >= endTime) {
    //        // a new range.
    //        PartitionGroup group = partitionTable.route(storageGroup.getFullPath(), startTime);
    //        List<Integer> ranges = splitMap.computeIfAbsent(group, x -> new ArrayList<>());
    //        ranges.add(startLoc); // included
    //        ranges.add(i); // excluded
    //        // next init
    //        startLoc = i;
    //        startTime = endTime;
    //        endTime =
    //            (times[i] / StorageEngine.getTimePartitionInterval() + 1)
    //                * StorageEngine.getTimePartitionInterval();
    //      }
    //    }
    //    // the final range
    //    PartitionGroup group = partitionTable.route(storageGroup.getFullPath(), startTime);
    //    List<Integer> ranges = splitMap.computeIfAbsent(group, x -> new ArrayList<>());
    //    ranges.add(startLoc); // included
    //    ranges.add(times.length); // excluded
    //
    //    List<Integer> locs;
    //    for (Map.Entry<PartitionGroup, List<Integer>> entry : splitMap.entrySet()) {
    //      // generate a new times and values
    //      locs = entry.getValue();
    //      int count = 0;
    //      for (int i = 0; i < locs.size(); i += 2) {
    //        int start = locs.get(i);
    //        int end = locs.get(i + 1);
    //        count += end - start;
    //      }
    //      long[] subTimes = new long[count];
    //      int destLoc = 0;
    //      Object[] values = initTabletValues(plan.getDataTypes().length, count,
    // plan.getDataTypes());
    //      BitMap[] bitMaps =
    //          plan.getBitMaps() == null ? null : initBitmaps(plan.getDataTypes().length, count);
    //      for (int i = 0; i < locs.size(); i += 2) {
    //        int start = locs.get(i);
    //        int end = locs.get(i + 1);
    //        System.arraycopy(plan.getTimes(), start, subTimes, destLoc, end - start);
    //        for (int k = 0; k < values.length; k++) {
    //          System.arraycopy(plan.getColumns()[k], start, values[k], destLoc, end - start);
    //          if (bitMaps != null && plan.getBitMaps()[k] != null) {
    //            BitMap.copyOfRange(plan.getBitMaps()[k], start, bitMaps[k], destLoc, end - start);
    //          }
    //        }
    //        destLoc += end - start;
    //      }
    //      InsertTabletPlan newBatch = PartitionUtils.copy(plan, subTimes, values, bitMaps);
    //      newBatch.setRange(locs);
    //      newBatch.setAligned(plan.isAligned());
    //      result.put(newBatch, entry.getKey());
    //    }
    return result;
  }

  @Override
  public boolean needSplit() {
    return true;
  }
}
