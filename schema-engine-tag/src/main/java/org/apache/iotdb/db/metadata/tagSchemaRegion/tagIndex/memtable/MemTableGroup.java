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
package org.apache.iotdb.db.metadata.tagSchemaRegion.tagIndex.memtable;

import org.apache.iotdb.lsm.manager.IMemManager;
import org.apache.iotdb.lsm.request.IFlushRequest;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/** used to manage working and immutableMemTables */
public class MemTableGroup implements IMemManager {

  // the maximum number of device ids managed by a working memTable
  private int numOfDeviceIdsInMemTable;

  // the maximum number of immutableMemTable, over num will flush to disk
  private int numOfImmutableMemTable;

  // (maxDeviceID / numOfDeviceIdsInMemTable) -> MemTable
  private Map<Integer, MemTable> immutableMemTables;

  // TODO add flushTables(immutableMemTables backup,and clear immutableMemTables)

  private MemTable workingMemTable;

  // the largest device id saved by the current MemTable
  private int maxDeviceID;

  public MemTableGroup(int numOfDeviceIdsInMemTable, int numOfImmutableMemTable) {
    this.numOfDeviceIdsInMemTable = numOfDeviceIdsInMemTable;
    this.numOfImmutableMemTable = numOfImmutableMemTable;
    workingMemTable = new MemTable(MemTable.WORKING);
    immutableMemTables = new HashMap<>();
    maxDeviceID = 0;
  }

  public int getNumOfDeviceIdsInMemTable() {
    return numOfDeviceIdsInMemTable;
  }

  public void setNumOfDeviceIdsInMemTable(int numOfDeviceIdsInMemTable) {
    this.numOfDeviceIdsInMemTable = numOfDeviceIdsInMemTable;
  }

  public Map<Integer, MemTable> getImmutableMemTables() {
    return immutableMemTables;
  }

  public void setImmutableMemTables(Map<Integer, MemTable> immutableMemTables) {
    this.immutableMemTables = immutableMemTables;
  }

  public MemTable getWorkingMemTable() {
    return workingMemTable;
  }

  public void setWorkingMemTable(MemTable workingMemTable) {
    this.workingMemTable = workingMemTable;
  }

  public int getMaxDeviceID() {
    return maxDeviceID;
  }

  public void setMaxDeviceID(int maxDeviceID) {
    this.maxDeviceID = maxDeviceID;
  }

  /**
   * determine whether the id can be saved to the current MemTable
   *
   * @param id INT32 device id
   * @return return true if it can, otherwise return false
   */
  public boolean inWorkingMemTable(int id) {
    return id / numOfDeviceIdsInMemTable == maxDeviceID / numOfDeviceIdsInMemTable;
  }

  @Override
  public String toString() {
    return "MemTableGroup{"
        + "numOfDeviceIdsInMemTable="
        + numOfDeviceIdsInMemTable
        + ", immutableMemTables="
        + immutableMemTables
        + ", workingMemTable="
        + workingMemTable
        + ", maxDeviceID="
        + maxDeviceID
        + '}';
  }

  @Override
  public boolean isNeedFlush() {
    return immutableMemTables.size() > numOfImmutableMemTable;
  }

  @Override
  public List<IFlushRequest> getFlushRequests() {
    List<IFlushRequest> flushRequests =
        immutableMemTables.entrySet().stream()
            .map(entry -> new IFlushRequest(entry.getKey(), entry.getValue()))
            .collect(Collectors.toList());
    return flushRequests;
  }

  @Override
  public void removeMemData(IFlushRequest request) {
    immutableMemTables.remove(request.getIndex());
  }
}
