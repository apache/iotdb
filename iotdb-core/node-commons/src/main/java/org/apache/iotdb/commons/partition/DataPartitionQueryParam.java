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

package org.apache.iotdb.commons.partition;

import org.apache.iotdb.common.rpc.thrift.TTimePartitionSlot;

import org.apache.tsfile.file.metadata.IDeviceID;

import java.util.ArrayList;
import java.util.List;

public class DataPartitionQueryParam {

  private String databaseName;
  private IDeviceID deviceID;
  private List<TTimePartitionSlot> timePartitionSlotList = new ArrayList<>();

  // it will be set to true in query when there exist filter like: time <= XXX
  // (-oo, timePartitionSlotList.get(0))
  private boolean needLeftAll = false;

  // it will be set to true query when there exist filter like: time >= XXX
  // (timePartitionSlotList.get(timePartitionSlotList.size() - 1), +oo)
  private boolean needRightAll = false;

  public DataPartitionQueryParam(
      IDeviceID deviceID, List<TTimePartitionSlot> timePartitionSlotList) {
    this.deviceID = deviceID;
    this.timePartitionSlotList = timePartitionSlotList;
  }

  public DataPartitionQueryParam(
      IDeviceID deviceID,
      List<TTimePartitionSlot> timePartitionSlotList,
      boolean needLeftAll,
      boolean needRightAll) {
    this.deviceID = deviceID;
    this.timePartitionSlotList = timePartitionSlotList;
    this.needLeftAll = needLeftAll;
    this.needRightAll = needRightAll;
  }

  public DataPartitionQueryParam() {}

  public IDeviceID getDeviceID() {
    return deviceID;
  }

  public void setDeviceID(IDeviceID deviceID) {
    this.deviceID = deviceID;
  }

  public List<TTimePartitionSlot> getTimePartitionSlotList() {
    return timePartitionSlotList;
  }

  public void setTimePartitionSlotList(List<TTimePartitionSlot> timePartitionSlotList) {
    this.timePartitionSlotList = timePartitionSlotList;
  }

  public boolean isNeedLeftAll() {
    return needLeftAll;
  }

  public void setNeedLeftAll(boolean needLeftAll) {
    this.needLeftAll = needLeftAll;
  }

  public boolean isNeedRightAll() {
    return needRightAll;
  }

  public void setNeedRightAll(boolean needRightAll) {
    this.needRightAll = needRightAll;
  }

  public String getDatabaseName() {
    return databaseName;
  }

  public void setDatabaseName(String databaseName) {
    this.databaseName = databaseName;
  }
}
