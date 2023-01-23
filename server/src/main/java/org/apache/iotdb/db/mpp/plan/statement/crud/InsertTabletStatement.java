/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iotdb.db.mpp.plan.statement.crud;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.common.rpc.thrift.TTimePartitionSlot;
import org.apache.iotdb.commons.partition.DataPartition;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.mpp.plan.statement.StatementType;
import org.apache.iotdb.db.mpp.plan.statement.StatementVisitor;
import org.apache.iotdb.db.utils.TimePartitionUtils;
import org.apache.iotdb.tsfile.utils.BitMap;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class InsertTabletStatement extends InsertBaseStatement {

  private long[] times; // times should be sorted. It is done in the session API.
  private BitMap[] bitMaps;
  private Object[] columns;

  private int rowCount = 0;

  public InsertTabletStatement() {
    super();
    statementType = StatementType.BATCH_INSERT;
  }

  public int getRowCount() {
    return rowCount;
  }

  public void setRowCount(int rowCount) {
    this.rowCount = rowCount;
  }

  public Object[] getColumns() {
    return columns;
  }

  public void setColumns(Object[] columns) {
    this.columns = columns;
  }

  public BitMap[] getBitMaps() {
    return bitMaps;
  }

  public void setBitMaps(BitMap[] bitMaps) {
    this.bitMaps = bitMaps;
  }

  public long[] getTimes() {
    return times;
  }

  public void setTimes(long[] times) {
    this.times = times;
  }

  @Override
  public boolean isEmpty() {
    return rowCount == 0
        || times.length == 0
        || measurements.length == 0
        || dataTypes.length == 0
        || columns.length == 0;
  }

  public List<TTimePartitionSlot> getTimePartitionSlots() {
    List<TTimePartitionSlot> result = new ArrayList<>();
    long startTime =
        (times[0] / TimePartitionUtils.timePartitionInterval)
            * TimePartitionUtils.timePartitionInterval; // included
    long endTime = startTime + TimePartitionUtils.timePartitionInterval; // excluded
    TTimePartitionSlot timePartitionSlot = TimePartitionUtils.getTimePartition(times[0]);
    for (int i = 1; i < times.length; i++) { // times are sorted in session API.
      if (times[i] >= endTime) {
        result.add(timePartitionSlot);
        // next init
        endTime =
            (times[i] / TimePartitionUtils.timePartitionInterval + 1)
                * TimePartitionUtils.timePartitionInterval;
        timePartitionSlot = TimePartitionUtils.getTimePartition(times[i]);
      }
    }
    result.add(timePartitionSlot);
    return result;
  }

  @Override
  public List<TEndPoint> collectRedirectInfo(DataPartition dataPartition) {
    TRegionReplicaSet regionReplicaSet =
        dataPartition.getDataRegionReplicaSetForWriting(
            devicePath.getFullPath(), TimePartitionUtils.getTimePartition(times[times.length - 1]));
    return Collections.singletonList(
        regionReplicaSet.getDataNodeLocations().get(0).getClientRpcEndPoint());
  }

  @Override
  public <R, C> R accept(StatementVisitor<R, C> visitor, C context) {
    return visitor.visitInsertTablet(this, context);
  }

  @Override
  public List<PartialPath> getPaths() {
    List<PartialPath> ret = new ArrayList<>();
    for (String m : measurements) {
      PartialPath fullPath = devicePath.concatNode(m);
      ret.add(fullPath);
    }
    return ret;
  }
}
